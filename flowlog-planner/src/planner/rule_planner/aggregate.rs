//! The aggregate step: plans an aggregate's braces as a body of their own,
//! keyed by the variables it reads from the enclosing body, and reads its
//! answers back as an atom of that body.
//!
//! For `R(x, c) :- Node(x), !Bad(x), c = count : { Edge(x, y) }`, once the
//! antijoin has folded into `Node`:
//!
//! ```text
//! K = Node'                       the keys: Node' holds the group key alone
//! I = plan of  @agg_0(x, y) :- K(x), Edge(x, y)
//! G = count over I by x, 0 for a key of K with no match
//! body becomes  G(x, @agg_0)      G replaces Node'
//! ```
//!
//! An atom with columns beyond the group key stays, and `G` is joined to
//! it on the key; `keys_of` says what the braces start from then, and
//! `groups_of` what `G` answers for. An aggregate with an empty group key
//! starts from no keys and answers once, for all of its matches.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use flowlog_parser::AggregateExpr;
use flowlog_parser::Arithmetic;
use flowlog_parser::Atom;
use flowlog_parser::AtomArg;
use flowlog_parser::FlowLogRule;
use flowlog_parser::Head;
use flowlog_parser::HeadArg;
use flowlog_parser::Predicate;
use flowlog_parser::Variable;
use tracing::trace;

use super::RulePlanner;
use crate::catalog::ArithmeticPos;
use crate::catalog::AtomArgumentSignature;
use crate::catalog::AtomSignature;
use crate::catalog::Catalog;
use crate::catalog::JoinPredicates;
use crate::catalog::KvPredicates;
use crate::optimizer::Optimizer;
use crate::planner::KeyValueLayout;
use crate::planner::PlanError;
use crate::planner::TransformationInfo;

/// In an aggregate info the matches are atom 0, as the braces' plan leaves
/// them, the groups atom 1, and the answer argument 0 of atom 2: the
/// info's own numbering, since its inputs come from two bodies.
const GROUPS_ATOM: AtomSignature = AtomSignature::new(true, 1);
const ANSWER: AtomArgumentSignature = AtomArgumentSignature::new(AtomSignature::new(true, 2), 0);

/// A collection of group keys: the keys an aggregate's braces start
/// from, or the groups it answers for.
struct Keys {
    /// The collection's fingerprint.
    fp: u64,
    /// The collection's name.
    name: String,
    /// The variable each column holds, in column order.
    names: Vec<Variable>,
}

/// The braces of an aggregate, planned to their matches.
struct Matches {
    /// The matches' fingerprint.
    fp: u64,
    /// The matches' name.
    name: String,
    /// The matches' columns, each by the variable it holds, in the order
    /// the braces' plan leaves them.
    columns: Vec<(Variable, AtomArgumentSignature)>,
    /// The column holding the target, `None` for a count.
    target: Option<AtomArgumentSignature>,
}

impl RulePlanner {
    /// Takes aggregate `index`: plans its braces, keyed by the variables it
    /// reads from this body, and reads its answers back as an atom, in
    /// place of the atom covering it when that atom holds the group key
    /// alone, else joined to it; then folds to a fixpoint. An aggregate
    /// reading nothing from the body answers once, and its answer joins
    /// the body as an atom of its own.
    ///
    /// # Errors
    ///
    /// Returns an internal error if the aggregate binds no variable or has
    /// no type, if no atom covers the variables it reads, or if a step of
    /// the braces' plan fails.
    pub(crate) fn aggregate(
        &mut self,
        catalog: &mut Catalog,
        index: usize,
        optimizer: &mut Optimizer,
    ) -> Result<(), PlanError> {
        let aggregate = catalog.aggregate_predicate(index)?.clone();
        let var = aggregate.var().cloned().ok_or_else(|| {
            PlanError::internal(format!("aggregate `{aggregate}` binds no variable"))
        })?;
        let ty = aggregate
            .var_type()
            .cloned()
            .ok_or_else(|| PlanError::internal(format!("aggregate `{aggregate}` has no type")))?;
        // An empty group key needs no keys and no groups: the one group,
        // the empty key, is answered whether or not any atom holds it,
        // and the join back on the empty key keeps the answer only where
        // the body has rows.
        let needs = &catalog.aggregate_variables()[index];
        let covering = if needs.is_empty() {
            None
        } else {
            let Some(covering) = self.covering_atom(catalog, index)? else {
                let needs: Vec<String> = needs.iter().map(ToString::to_string).collect();
                return Err(PlanError::internal(format!(
                    "aggregate `{aggregate}` of rule `{}` reads {} but no atom covers it",
                    catalog.rule(),
                    needs.join(", ")
                )));
            };
            Some(covering)
        };
        trace!("Aggregate:\n  {aggregate}\n  covering atom: {covering:?}");
        let keys = covering
            .map(|covering| {
                // The covering atom is read keyed, as the keys and by the
                // join back; a relation is arranged first, as before a join.
                self.premap_original_atom(catalog, covering, true)?;
                self.keys_of(catalog, index, covering)
            })
            .transpose()?;
        // The answers replace the covering atom when it is the keys: it
        // holds the group key alone.
        let replaces_covering = match (covering, &keys) {
            (Some(covering), Some(keys)) => catalog.positive_atom_fingerprint(covering)? == keys.fp,
            (Some(_), None) | (None, _) => false,
        };

        let braces_start = self.transformation_infos.len();
        let matches = self.plan_braces(&aggregate, &var, keys.as_ref(), optimizer)?;
        let key_names = keys.as_ref().map_or(Vec::new(), |keys| keys.names.clone());
        // Only an operator answering a group without a match needs the
        // groups; the others answer the keys the matches hold.
        let groups = keys
            .filter(|_| aggregate.operator().defined_on_empty())
            .map(|keys| self.groups_of(keys, braces_start))
            .transpose()?;

        // The answers: one row per group, keyed by the group key.
        let current_transformation_index = self.transformation_infos.len();
        let matches_keys = key_names
            .iter()
            .map(|name| column_of(&matches.columns, name).map(ArithmeticPos::from_var_signature))
            .collect::<Result<Vec<_>, PlanError>>()?;
        let matches_values: Vec<ArithmeticPos> = matches
            .columns
            .iter()
            .filter(|(name, _)| !key_names.contains(name))
            .map(|&(_, signature)| ArithmeticPos::from_var_signature(signature))
            .collect();
        let groups_keys: Vec<ArithmeticPos> = (0..key_names.len())
            .map(|argument| {
                ArithmeticPos::from_var_signature(AtomArgumentSignature::new(GROUPS_ATOM, argument))
            })
            .collect();
        let answers_name = format!(
            "{}[{}]({})",
            aggregate.operator(),
            names_label(&key_names),
            matches.name
        );
        let groups_fp = groups.as_ref().map(|groups| groups.fp);
        let mut answers = TransformationInfo::aggregate_to_kv(
            matches.fp,
            matches.name,
            groups.map(|groups| {
                (
                    groups.fp,
                    groups.name,
                    KeyValueLayout::new(groups_keys, Vec::new()),
                )
            }),
            answers_name.clone(),
            KeyValueLayout::new(matches_keys.clone(), matches_values),
            KeyValueLayout::new(
                matches_keys,
                vec![ArithmeticPos::from_var_signature(ANSWER)],
            ),
            aggregate.operator(),
            matches.target,
            ty,
            ANSWER,
            JoinPredicates::default(),
        );
        // The matches' columns keep the names the braces gave them, the
        // groups' the key's, and the answer the aggregate's variable.
        let mut names: BTreeMap<AtomArgumentSignature, Variable> = matches
            .columns
            .into_iter()
            .map(|(name, signature)| (signature, name))
            .collect();
        for (argument, name) in key_names.iter().enumerate() {
            names.insert(
                AtomArgumentSignature::new(GROUPS_ATOM, argument),
                name.clone(),
            );
        }
        names.insert(ANSWER, var.clone());
        answers.set_variables(names);
        let answers_fp = answers.output_info_fp();
        // Both inputs are produced: the matches by the braces' plan, the
        // groups by a premap at the least.
        self.insert_consumer(&BTreeSet::new(), matches.fp, current_transformation_index)?;
        if let Some(groups_fp) = groups_fp {
            self.insert_consumer(&BTreeSet::new(), groups_fp, current_transformation_index)?;
        }
        self.insert_producer(answers_fp, current_transformation_index);
        trace!("Aggregate transformation:\n{answers}");
        self.transformation_infos.push(answers);

        // Back into the body: the answers stand in for an atom holding the
        // group key alone, and join any other on the key.
        let mut arguments = key_names;
        arguments.push(var);
        match covering {
            Some(covering) if !replaces_covering => {
                catalog.aggregate_modify(index, None, arguments, answers_name, answers_fp)?;
                let answers_atom = catalog.positive_atom_number() - 1;
                self.join(catalog, (covering, answers_atom))
            }
            covering => {
                let replaced = covering.map(|covering| AtomSignature::new(true, covering));
                catalog.aggregate_modify(index, replaced, arguments, answers_name, answers_fp)?;
                self.fold_to_fixpoint(catalog)
            }
        }
    }

    /// The atom covering aggregate `index` that the answers best join
    /// back to: one outside the stratum's loop before one inside it, one
    /// holding the group key alone before one with other columns, then
    /// the narrowest, then the first. `None` when no atom covers it.
    /// Undefined for an empty group key, which every atom covers and
    /// which needs no keys.
    fn covering_atom(&self, catalog: &Catalog, index: usize) -> Result<Option<usize>, PlanError> {
        let needs = &catalog.aggregate_variables()[index];
        let mut best: Option<(usize, (bool, bool, usize))> = None;
        for &atom in &catalog.aggregate_supersets()[index] {
            let arity = catalog.positive_atom_argument_signature(atom)?.len();
            let rank = (
                self.derives_from_recursive(catalog.positive_atom_fingerprint(atom)?),
                arity != needs.len(),
                arity,
            );
            if best.is_none_or(|(_, best)| rank < best) {
                best = Some((atom, rank));
            }
        }
        Ok(best.map(|(atom, _)| atom))
    }

    /// The keys the braces of aggregate `index` start from: the covering
    /// atom when it holds the group key alone, else the projection of its
    /// keys, reading the atom keyed by the group key as the join back
    /// does. The key is in the covering atom's argument order.
    fn keys_of(
        &mut self,
        catalog: &Catalog,
        index: usize,
        covering: usize,
    ) -> Result<Keys, PlanError> {
        let needs = &catalog.aggregate_variables()[index];
        let covering_args = catalog.positive_atom_argument_signature(covering)?.to_vec();
        let covering_fp = catalog.positive_atom_fingerprint(covering)?;
        let covering_name = catalog.positive_atom_name(covering)?.to_string();
        let mut key_signatures = Vec::new();
        let mut key_names: Vec<Variable> = Vec::new();
        for signature in &covering_args {
            let name = catalog.signature_to_variable(signature)?;
            if needs.contains(name) && !key_names.iter().any(|key| key == name) {
                key_signatures.push(*signature);
                key_names.push(name.clone());
            }
        }
        if key_signatures.len() == covering_args.len() {
            return Ok(Keys {
                fp: covering_fp,
                name: covering_name,
                names: key_names,
            });
        }
        let keys: Vec<ArithmeticPos> = key_signatures
            .iter()
            .map(|&signature| ArithmeticPos::from_var_signature(signature))
            .collect();
        let values = covering_args
            .iter()
            .filter(|signature| !key_signatures.contains(signature))
            .map(|&signature| ArithmeticPos::from_var_signature(signature))
            .collect();
        let names = covering_args
            .iter()
            .map(|&signature| {
                Ok((
                    signature,
                    catalog.signature_to_variable(&signature)?.clone(),
                ))
            })
            .collect::<Result<_, PlanError>>()?;
        let (fp, name) = self.project_keys(
            covering_fp,
            covering_name.clone(),
            format!("keys[{}]({covering_name})", names_label(&key_names)),
            KeyValueLayout::new(keys.clone(), values),
            keys,
            names,
        )?;
        Ok(Keys {
            fp,
            name,
            names: key_names,
        })
    }

    /// The groups to answer for, with the group key in their key order:
    /// `keys` themselves when the braces' plan, from transformation
    /// `braces_start` on, arranged them by the group key, else their
    /// projection onto the group key, read as the braces read them.
    ///
    /// Returns an internal error if the braces read the keys nowhere.
    fn groups_of(&mut self, keys: Keys, braces_start: usize) -> Result<Keys, PlanError> {
        let (read, read_names) = self.reading_of(keys.fp, braces_start)?;
        let by_key = read.value().is_empty()
            && read_names.len() == keys.names.len()
            && keys.names.iter().all(|name| read_names.contains(name));
        if by_key {
            return Ok(Keys {
                names: read_names,
                ..keys
            });
        }
        // Column `i` of the keys holds the `i`th variable of the key.
        let column =
            |argument: usize| AtomArgumentSignature::new(AtomSignature::new(true, 0), argument);
        let group_key: Vec<ArithmeticPos> = (0..keys.names.len())
            .map(|argument| ArithmeticPos::from_var_signature(column(argument)))
            .collect();
        let names = keys
            .names
            .iter()
            .enumerate()
            .map(|(argument, name)| (column(argument), name.clone()))
            .collect();
        let (fp, name) = self.project_keys(
            keys.fp,
            keys.name.clone(),
            format!("groups[{}]({})", names_label(&keys.names), keys.name),
            read,
            group_key,
            names,
        )?;
        Ok(Keys {
            fp,
            name,
            names: keys.names,
        })
    }

    /// How the braces read the keys: the layout the one transformation
    /// from index `braces_start` on reads collection `fp` by, with the
    /// variable each of its keys holds, in key order. The positions are
    /// rebuilt on the keys' own columns, atom 0, whatever atom the reader
    /// knew them as.
    ///
    /// Returns an internal error if no transformation reads `fp`, or if
    /// the reader reads a column as other than a plain variable or names
    /// no variable for a key.
    fn reading_of(
        &self,
        fp: u64,
        braces_start: usize,
    ) -> Result<(KeyValueLayout, Vec<Variable>), PlanError> {
        for tx in &self.transformation_infos[braces_start..] {
            let (left, right) = tx.input_info_fp();
            let (left_layout, right_layout) = tx.input_kv_layout();
            let layout = if left == fp {
                left_layout
            } else if right == Some(fp) {
                right_layout.ok_or_else(|| {
                    PlanError::internal(format!(
                        "aggregate: {} has a right input but no layout",
                        tx.output_name()
                    ))
                })?
            } else {
                continue;
            };
            let signature = |position: &ArithmeticPos| {
                position.plain_var().ok_or_else(|| {
                    PlanError::internal(format!(
                        "aggregate: {} reads the keys' column {position} as other than a \
                             variable",
                        tx.output_name()
                    ))
                })
            };
            let rebuilt = |positions: &[ArithmeticPos]| -> Result<Vec<ArithmeticPos>, PlanError> {
                positions
                    .iter()
                    .map(|position| {
                        Ok(ArithmeticPos::from_var_signature(
                            AtomArgumentSignature::new(
                                AtomSignature::new(true, 0),
                                signature(position)?.argument_id(),
                            ),
                        ))
                    })
                    .collect()
            };
            let names = layout
                .key()
                .iter()
                .map(|position| {
                    tx.variables()
                        .get(&signature(position)?)
                        .cloned()
                        .ok_or_else(|| {
                            PlanError::internal(format!(
                                "aggregate: key {position} of {} has no variable name",
                                tx.output_name()
                            ))
                        })
                })
                .collect::<Result<_, _>>()?;
            return Ok((
                KeyValueLayout::new(rebuilt(layout.key())?, rebuilt(layout.value())?),
                names,
            ));
        }
        Err(PlanError::internal(format!(
            "aggregate: the braces read nothing of the keys {fp:#018x}"
        )))
    }

    /// Emits the projection of the produced collection `input_fp`, read
    /// as `input`, onto `key` as keys with no value, under `output_name`,
    /// naming the positions by `names`. Returns the projection's
    /// fingerprint and name.
    ///
    /// Returns an internal error if nothing produces `input_fp`.
    fn project_keys(
        &mut self,
        input_fp: u64,
        input_name: String,
        output_name: String,
        input: KeyValueLayout,
        key: Vec<ArithmeticPos>,
        names: BTreeMap<AtomArgumentSignature, Variable>,
    ) -> Result<(u64, String), PlanError> {
        let current_transformation_index = self.transformation_infos.len();
        let mut tx = TransformationInfo::kv_to_kv(
            input_fp,
            input_name,
            output_name,
            false,
            input,
            KeyValueLayout::new(key, Vec::new()),
            KvPredicates::default(),
        );
        tx.set_variables(names);
        let fp = tx.output_info_fp();
        let name = tx.output_name().to_string();
        self.insert_consumer(&BTreeSet::new(), input_fp, current_transformation_index)?;
        self.insert_producer(fp, current_transformation_index);
        trace!("Aggregate projection transformation:\n{tx}");
        self.transformation_infos.push(tx);
        Ok((fp, name))
    }

    /// Plans the braces as a body of their own, `<var>(key..., locals...)
    /// :- keys(key...), braces...`, so that every match keeps its named
    /// local variables and nothing of them is projected away; a computed
    /// target is added as a column. With no keys, the braces alone are
    /// the body.
    fn plan_braces(
        &mut self,
        aggregate: &AggregateExpr,
        var: &Variable,
        keys: Option<&Keys>,
        optimizer: &mut Optimizer,
    ) -> Result<Matches, PlanError> {
        let key_names: &[Variable] = keys.map_or(&[], |keys| &keys.names);
        let locals: BTreeSet<&Variable> = aggregate
            .bound_vars()
            .into_iter()
            .filter(|local| !key_names.iter().any(|key| key == *local))
            .collect();
        let head = Head::new(
            var.to_string(),
            key_names
                .iter()
                .chain(locals)
                .map(|name| HeadArg::Var(name.clone()))
                .collect(),
        );
        let keys_atom = keys.map(|keys| {
            Predicate::PositiveAtom(Atom::new(
                &keys.name,
                keys.names.iter().cloned().map(AtomArg::Var).collect(),
                keys.fp,
            ))
        });
        let body = keys_atom
            .into_iter()
            .chain(aggregate.body().iter().cloned())
            .collect();
        let rule = FlowLogRule::new(head, body);
        trace!("Aggregate braces as a rule:\n  {rule}");
        let produced = keys.map(|keys| keys.fp).into_iter().collect();
        let mut catalog = Catalog::from_rule(&rule, &produced)?;
        self.plan_body(&mut catalog, optimizer)?;
        // Braces of one atom with nothing to fold, project or join leave
        // the relation itself; the matches are read keyed, as any
        // produced collection, so it is arranged first.
        self.premap_original_atom(&mut catalog, 0, true)?;

        let target = match aggregate.target() {
            None => None,
            Some(target) if target.is_var() => Some(target.vars()[0].clone()),
            Some(target) => Some(self.add_target_column(&mut catalog, var, target)?),
        };
        let columns = columns_of(&catalog)?;
        let target = target.map(|name| column_of(&columns, &name)).transpose()?;
        Ok(Matches {
            fp: catalog.positive_atom_fingerprint(0)?,
            name: catalog.positive_atom_name(0)?.to_string(),
            columns,
            target,
        })
    }

    /// Adds the computed `target` to the braces' rows as a column, the
    /// aggregate's [`Variable::AggregateTarget`], and returns it.
    fn add_target_column(
        &mut self,
        catalog: &mut Catalog,
        var: &Variable,
        target: &Arithmetic,
    ) -> Result<Variable, PlanError> {
        let current_transformation_index = self.transformation_infos.len();
        let columns = columns_of(catalog)?;
        let signatures = target
            .vars()
            .iter()
            .map(|name| column_of(&columns, name))
            .collect::<Result<Vec<_>, PlanError>>()?;
        let target_position = ArithmeticPos::from_arithmetic(target, &signatures)?;
        let (args, rows_fp, _, rows_name) = catalog.resolve_atom(&AtomSignature::new(true, 0))?;
        let rows_name = rows_name.to_string();
        let values: Vec<ArithmeticPos> = args
            .iter()
            .map(|&signature| ArithmeticPos::from_var_signature(signature))
            .collect();
        self.insert_consumer(
            catalog.original_atom_fingerprints(),
            rows_fp,
            current_transformation_index,
        )?;
        let Variable::Aggregate(index) = var else {
            return Err(PlanError::internal(format!(
                "aggregate target column for `{var}`, which is not an aggregate's variable"
            )));
        };
        let name = Variable::AggregateTarget(*index);
        let new_name = format!("π⁺[{name}={target}]({rows_name})");
        let tx = TransformationInfo::kv_to_kv(
            rows_fp,
            rows_name,
            new_name.clone(),
            catalog.original_atom_fingerprints().contains(&rows_fp),
            KeyValueLayout::new(Vec::new(), values.clone()),
            KeyValueLayout::new(
                Vec::new(),
                values.into_iter().chain([target_position]).collect(),
            ),
            KvPredicates::default(),
        );
        let new_fp = tx.output_info_fp();
        self.insert_producer(new_fp, current_transformation_index);
        self.push_transformation(tx, catalog, "Aggregate target")?;
        catalog.append_arguments_modify(
            AtomSignature::new(true, 0),
            vec![name.clone()],
            new_name,
            new_fp,
        )?;
        Ok(name)
    }
}

/// `names`, comma-separated, for a transformation's label.
fn names_label(names: &[Variable]) -> String {
    names
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

/// The columns of a planned body's one atom, each by the variable it
/// holds, in the atom's argument order.
fn columns_of(catalog: &Catalog) -> Result<Vec<(Variable, AtomArgumentSignature)>, PlanError> {
    catalog
        .positive_atom_argument_signature(0)?
        .iter()
        .map(|&signature| {
            Ok((
                catalog.signature_to_variable(&signature)?.clone(),
                signature,
            ))
        })
        .collect()
}

/// The column holding variable `name`.
///
/// Returns an internal error if no column holds it: the braces bind every
/// variable the aggregate reads, so a miss is a planner bug.
fn column_of(
    columns: &[(Variable, AtomArgumentSignature)],
    name: &Variable,
) -> Result<AtomArgumentSignature, PlanError> {
    columns
        .iter()
        .find(|(column, _)| column == name)
        .map(|&(_, signature)| signature)
        .ok_or_else(|| {
            PlanError::internal(format!("aggregate matches hold no column for `{name}`"))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_harness::recursive_rule_planner;
    use crate::test_harness::rule_planner;

    /// The relations the rules under test read.
    const DECLS: &str = "\
        .decl Node(x: int32)\n\
        .decl Bad(x: int32)\n\
        .decl Edge(x: int32, y: int32)\n\
        .decl F(y: int32, w: int32)\n\
        .decl A(x: int32, y: int32)\n\
        .decl B(x: int32, z: int32)\n\
        .input Node(IO=\"file\", filename=\"Node.csv\", delimiter=\",\")\n\
        .input Bad(IO=\"file\", filename=\"Bad.csv\", delimiter=\",\")\n\
        .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
        .input F(IO=\"file\", filename=\"F.csv\", delimiter=\",\")\n\
        .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
        .input B(IO=\"file\", filename=\"B.csv\", delimiter=\",\")\n";

    /// Plans the body of the rule `source` declares, as the stratum does
    /// before pushdown, and returns the planner and the planned catalog.
    fn plan(source: &str) -> (RulePlanner, Catalog) {
        let (mut planner, mut catalog) = rule_planner(&format!("{DECLS}{source}"));
        planner
            .plan_body(&mut catalog, &mut Optimizer::new())
            .expect("plan");
        (planner, catalog)
    }

    /// The output names of the planner's transformations, in order.
    fn names(planner: &RulePlanner) -> Vec<&str> {
        planner
            .transformation_infos()
            .iter()
            .map(TransformationInfo::output_name)
            .collect()
    }

    /// The aggregate infos the planner emitted, in order.
    fn aggregates(planner: &RulePlanner) -> Vec<&TransformationInfo> {
        planner
            .transformation_infos()
            .iter()
            .filter(|info| matches!(info, TransformationInfo::AggregateToKV { .. }))
            .collect()
    }

    /// The name of the groups an aggregate info answers for.
    fn groups_name(info: &TransformationInfo) -> &str {
        info.input_name().1.expect("an aggregate with groups")
    }

    /// When the covering atom holds the group key alone, the answers stand
    /// in for it, and it is the groups.
    #[test]
    fn answers_replace_an_atom_holding_the_key_alone() {
        let (planner, catalog) = plan(
            ".decl Deg(x: int32, c: int32)\n.output Deg\n\
             Deg(x, c) :- Node(x), c = count : Edge(x, _).\n",
        );
        assert_eq!(
            catalog.rule().to_string(),
            "deg(x, @agg_0) :- count[x]((node ⋉[x] π[x](edge)))(x, @agg_0)."
        );
        let [answers] = aggregates(&planner)[..] else {
            panic!("one aggregate");
        };
        assert_eq!(groups_name(answers), "node");
    }

    /// An atom with columns beyond the key stays, and the answers join it
    /// on the key; the groups are the projection of its keys, read keyed
    /// as the join reads the atom.
    #[test]
    fn answers_join_an_atom_with_other_columns() {
        let (planner, catalog) = plan(
            ".decl R(x: int32, c: int32, y: int32)\n.output R\n\
             R(x, c, y) :- A(x, y), c = count : Edge(x, _).\n",
        );
        assert_eq!(
            catalog.rule().to_string(),
            "r(x, @agg_0, y) :- (a ⋈[x] count[x]((keys[x](a) ⋉[x] π[x](edge))))(x, y, @agg_0)."
        );
        let groups = planner
            .transformation_infos()
            .iter()
            .find(|info| info.output_name() == "keys[x](a)")
            .expect("groups projection");
        let (input, _) = groups.input_kv_layout();
        assert_eq!((input.key().len(), input.value().len()), (1, 1));
        assert_eq!(groups.output_variables(), vec![Some(&Variable::from("x"))]);
    }

    /// The group key keeps the covering atom's argument order, whatever
    /// order the braces read it in.
    #[test]
    fn the_group_key_follows_the_covering_atom() {
        let (planner, _) = plan(
            ".decl O(y: int32, x: int32, c: int32)\n.output O\n\
             O(y, x, c) :- A(x, y), c = count : Edge(y, x).\n",
        );
        let [answers] = aggregates(&planner)[..] else {
            panic!("one aggregate");
        };
        assert_eq!(
            answers.output_variables(),
            vec![
                Some(&Variable::from("x")),
                Some(&Variable::from("y")),
                Some(&Variable::Aggregate(0))
            ]
        );
    }

    /// A computed target is a column the braces' rows gain, and the
    /// aggregate reads that column.
    #[test]
    fn a_computed_target_is_a_column_of_the_matches() {
        let (planner, _) = plan(
            ".decl S(x: int32, s: int32)\n.output S\n\
             S(x, s) :- Node(x), s = sum y + 1 : Edge(x, y).\n",
        );
        let [answers] = aggregates(&planner)[..] else {
            panic!("one aggregate");
        };
        let TransformationInfo::AggregateToKV {
            matches_input_name,
            target,
            variables,
            ..
        } = answers
        else {
            unreachable!()
        };
        assert_eq!(
            matches_input_name,
            "π⁺[@agg_0.target=y + 1]((node ⋉[x] edge))"
        );
        let target = target.expect("a sum has a target");
        assert_eq!(variables[&target], Variable::AggregateTarget(0));
    }

    /// A target that is a variable is read from the column holding it.
    #[test]
    fn a_variable_target_is_its_column() {
        let (planner, _) = plan(
            ".decl S(x: int32, s: int32)\n.output S\n\
             S(x, s) :- Node(x), s = sum y : Edge(x, y).\n",
        );
        let [answers] = aggregates(&planner)[..] else {
            panic!("one aggregate");
        };
        let TransformationInfo::AggregateToKV {
            target, variables, ..
        } = answers
        else {
            unreachable!()
        };
        assert_eq!(
            variables[&target.expect("a sum has a target")],
            Variable::from("y")
        );
    }

    /// A nested aggregate plans inside the braces, before the aggregate
    /// over them, and its filter applies to the matches.
    #[test]
    fn a_nested_aggregate_plans_inside_the_braces() {
        let (planner, catalog) = plan(
            ".decl N(x: int32, c: int32)\n.output N\n\
             N(x, c) :- Node(x), c = count : { Edge(x, y), sum w : F(y, w) > 2 }.\n",
        );
        let names: Vec<&str> = aggregates(&planner)
            .iter()
            .map(|info| info.output_name())
            .collect();
        assert_eq!(
            names,
            vec![
                "sum[y]((keys[y]((node ⋉[x] edge)) ⋉[y] f))",
                "count[x](σ[@agg_1 > 2](((node ⋉[x] edge) ⋈[y] \
                 sum[y]((keys[y]((node ⋉[x] edge)) ⋉[y] f)))))",
            ]
        );
        assert_eq!(
            catalog.rule().to_string(),
            "n(x, @agg_0) :- count[x](σ[@agg_1 > 2](((node ⋉[x] edge) ⋈[y] \
             sum[y]((keys[y]((node ⋉[x] edge)) ⋉[y] f)))))(x, @agg_0)."
        );
    }

    /// A second aggregate over the same key reads the first's answers as
    /// its covering atom.
    #[test]
    fn a_second_aggregate_covers_itself_by_the_first_answers() {
        let (_, catalog) = plan(
            ".decl T(x: int32, a: int32, b: int32)\n.output T\n\
             T(x, a, b) :- Node(x), a = count : Edge(x, _), b = sum y : Edge(x, y).\n",
        );
        assert_eq!(
            catalog.rule().to_string(),
            "t(x, @agg_0, @agg_1) :- (count[x]((node ⋉[x] π[x](edge))) ⋈[x] \
             sum[x]((keys[x](count[x]((node ⋉[x] π[x](edge)))) ⋉[x] edge)))(x, @agg_0, @agg_1)."
        );
    }

    /// A comparison on the answer folds onto the answers once they are an
    /// atom, and an answer nothing else reads is projected away.
    #[test]
    fn a_comparison_on_the_answer_folds_onto_the_answers() {
        let (_, catalog) = plan(
            ".decl Hub(x: int32)\n.output Hub\n\
             Hub(x) :- Node(x), count : Edge(x, _) > 2.\n",
        );
        assert_eq!(
            catalog.rule().to_string(),
            "hub(x) :- π[x](σ[@agg_0 > 2](count[x]((node ⋉[x] π[x](edge)))))(x)."
        );
    }

    /// The head reads the answers' columns in its own order.
    #[test]
    fn the_head_reads_the_answers() {
        let (mut planner, mut catalog) = plan(
            ".decl R(x: int32, c: int32, y: int32)\n.output R\n\
             R(x, c, y) :- A(x, y), c = count : Edge(x, _).\n",
        );
        planner
            .fuse(catalog.original_atom_fingerprints())
            .expect("fuse");
        planner.post(&mut catalog).expect("post");
        let last = planner.transformation_infos().last().expect("post");
        assert_eq!(
            last.output_variables(),
            vec![
                Some(&Variable::from("x")),
                Some(&Variable::Aggregate(0)),
                Some(&Variable::from("y"))
            ]
        );
    }

    /// An aggregate reading nothing from a body without atoms answers
    /// once, with no groups, and its braces start from no keys.
    #[test]
    fn an_aggregate_reading_nothing_answers_with_no_groups() {
        let (planner, catalog) = plan(
            ".decl Total(c: int32)\n.output Total\n\
             Total(c) :- c = count : Edge(_, _).\n",
        );
        assert_eq!(
            catalog.rule().to_string(),
            "total(@agg_0) :- count[](π[](π[](edge)))(@agg_0)."
        );
        let [answers] = aggregates(&planner)[..] else {
            panic!("one aggregate");
        };
        assert_eq!(answers.input_info_fp().1, None);
        assert_eq!(
            answers.output_variables(),
            vec![Some(&Variable::Aggregate(0))]
        );
    }

    /// An aggregate reading nothing from a body with atoms has no groups
    /// either: its one answer joins the body on the empty key, which keeps
    /// it only where the body has rows.
    #[test]
    fn an_aggregate_reading_nothing_joins_the_body_on_the_empty_key() {
        let (planner, catalog) = plan(
            ".decl E(x: int32, c: int32)\n.output E\n\
             E(x, c) :- Node(x), c = count : Edge(_, _).\n",
        );
        let [answers] = aggregates(&planner)[..] else {
            panic!("one aggregate");
        };
        assert_eq!(answers.input_info_fp().1, None);
        assert_eq!(
            catalog.rule().to_string(),
            "e(x, @agg_0) :- (node ⋈[] count[](π[](π[](edge))))(x, @agg_0)."
        );
    }

    /// A fold of the body is copied nowhere into the braces of an
    /// aggregate reading nothing, which cover none of its variables.
    #[test]
    fn a_fold_of_the_body_stays_out_of_braces_reading_nothing() {
        let (mut planner, catalog) = plan(
            ".decl E(x: int32, c: int32)\n.output E\n\
             E(x, c) :- Node(x), Bad(x), c = count : Edge(_, _).\n",
        );
        planner.pushdown(&catalog).expect("pushdown");
        let folds: Vec<&str> = names(&planner)
            .into_iter()
            .filter(|name| name.ends_with("⋉[x] bad)"))
            .collect();
        assert_eq!(folds, vec!["(node ⋉[x] bad)"]);
    }

    /// Nested braces reading nothing answer with no groups at every
    /// depth.
    #[test]
    fn nested_aggregates_reading_nothing_answer_with_no_groups() {
        let (planner, _) = plan(
            ".decl Total(c: int32)\n.output Total\n\
             Total(c) :- c = count : { sum w : F(_, w) > 2 }.\n",
        );
        let groups: Vec<Option<u64>> = aggregates(&planner)
            .into_iter()
            .map(|info| info.input_info_fp().1)
            .collect();
        assert_eq!(groups, vec![None, None]);
    }

    /// The groups are the keys themselves when the braces arrange them by
    /// the group key, as a semijoin on it does.
    #[test]
    fn the_groups_are_the_keys_the_braces_arranged_by_the_key() {
        let (planner, _) = plan(
            ".decl O(y: int32, x: int32, c: int32)\n.output O\n\
             O(y, x, c) :- A(x, y), c = count : { Edge(x, z), F(y, z), B(y, x) }.\n",
        );
        let [answers] = aggregates(&planner)[..] else {
            panic!("one aggregate");
        };
        assert_eq!(groups_name(answers), "a");
        assert_eq!(
            answers.output_variables(),
            vec![
                Some(&Variable::from("x")),
                Some(&Variable::from("y")),
                Some(&Variable::Aggregate(0))
            ]
        );
    }

    /// When the braces arrange the keys by part of the group key, to join
    /// an atom on it, the groups are a projection of the keys onto the
    /// group key, read as the braces read them.
    #[test]
    fn the_groups_project_keys_the_braces_arranged_by_part_of_the_key() {
        let (planner, _) = plan(
            ".decl Z(x: int32, y: int32, c: int32)\n.output Z\n\
             Z(x, y, c) :- A(x, y), c = count : { Edge(x, z), F(y, z) }.\n",
        );
        let groups = planner
            .transformation_infos()
            .iter()
            .find(|info| info.output_name() == "groups[x,y](a)")
            .expect("groups projection");
        let (input, _) = groups.input_kv_layout();
        assert_eq!((input.key().len(), input.value().len()), (1, 1));
        assert_eq!(
            groups.output_variables(),
            vec![Some(&Variable::from("x")), Some(&Variable::from("y"))]
        );
    }

    /// Braces of comparisons alone filter the keys, which the braces then
    /// read as rows; the groups are the projection of the keys.
    #[test]
    fn braces_of_comparisons_alone_filter_the_keys() {
        let (planner, catalog) = plan(
            ".decl G(x: int32, c: int32)\n.output G\n\
             G(x, c) :- Node(x), c = count : { x > 3 }.\n",
        );
        assert_eq!(
            catalog.rule().to_string(),
            "g(x, @agg_0) :- count[x](σ[x > 3](node))(x, @agg_0)."
        );
        assert_eq!(
            names(&planner),
            vec![
                "node",
                "σ[x > 3](node)",
                "groups[x](node)",
                "count[x](σ[x > 3](node))"
            ]
        );
    }

    /// Of the atoms covering an aggregate, one outside the stratum's loop
    /// is preferred, so the answers are computed once rather than every
    /// round.
    #[test]
    fn a_covering_atom_outside_the_loop_is_preferred() {
        let (mut planner, mut catalog) = recursive_rule_planner(
            &format!(
                "{DECLS}.decl D(x: int32, d: int32)\n.output D\n\
                 D(y, d + n) :- D(x, d), Edge(x, y), n = count : F(x, _).\n"
            ),
            &["d"],
        );
        planner
            .plan_body(&mut catalog, &mut Optimizer::new())
            .expect("plan");
        let [answers] = aggregates(&planner)[..] else {
            panic!("one aggregate");
        };
        assert_eq!(groups_name(answers), "keys[x](edge)");
    }

    /// Of the atoms covering an aggregate, one holding the group key alone
    /// is preferred, so the answers replace it rather than join back to a
    /// projection of the other's keys.
    #[test]
    fn a_covering_atom_holding_the_key_alone_is_preferred() {
        let (_, catalog) = plan(
            ".decl R(x: int32, y: int32, z: int32, c: int32)\n.output R\n\
             R(x, y, z, c) :- B(x, z), A(x, y), c = count : Edge(x, y).\n",
        );
        assert_eq!(
            catalog.rule().to_string(),
            "r(x, y, z, @agg_0) :- (b ⋈[x] count[x,y]((a ⋉[x,y] edge)))(x, z, y, @agg_0)."
        );
    }

    /// A fold inside the braces holds for the matches alone, so it is
    /// copied nowhere outside them, where the group it filters out is
    /// still answered. Here the keys fold into `Node`, and the atom the
    /// answers join back to, which covers the fold, takes no copy.
    #[test]
    fn a_fold_inside_the_braces_is_copied_nowhere_outside() {
        let (mut planner, catalog) = plan(
            ".decl R(x: int32, y: int32, c: int32)\n.output R\n\
             R(x, y, c) :- A(x, y), c = count : { Edge(x, z), Node(x) }.\n",
        );
        planner.pushdown(&catalog).expect("pushdown");
        let names = names(&planner);
        assert!(names.contains(&"(keys[x](a) ⋉[x] node)"), "{names:?}");
        assert!(
            names.iter().all(|name| !name.ends_with("⋉[x] a)")),
            "{names:?}"
        );
    }

    /// A fold inside the braces reaches nothing below the keys the braces
    /// start from: the enclosing scope's rows stay unfiltered, so a group
    /// with no match is still answered.
    #[test]
    fn a_fold_inside_the_braces_stops_at_the_keys() {
        let (mut planner, catalog) = plan(
            ".decl R(x: int32, y: int32, w: int32, c: int32)\n.output R\n\
             R(x, y, w, c) :- A(x, y), B(x, w), c = count : { Edge(x, z), Bad(x), F(y, w) }.\n",
        );
        planner.pushdown(&catalog).expect("pushdown");
        let names = names(&planner);
        let folds: Vec<&&str> = names
            .iter()
            .filter(|name| name.starts_with("(bad ⋉"))
            .collect();
        assert_eq!(folds, vec![&"(bad ⋉[x] edge)"], "{names:?}");
    }

    /// Braces of one atom and nothing else leave the relation itself as
    /// the matches, arranged for the aggregate to read.
    #[test]
    fn braces_of_one_atom_read_it_arranged() {
        let (planner, catalog) = plan(
            ".decl T(c: int32)\n.output T\n\
             T(c) :- c = count : { Edge(x, y) }.\n",
        );
        assert_eq!(names(&planner), vec!["edge", "count[](edge)"]);
        assert_eq!(
            catalog.rule().to_string(),
            "t(@agg_0) :- count[](edge)(@agg_0)."
        );
    }

    /// A fold of the body reaches into the braces through the keys, which
    /// carry it, so no copy lands on an atom inside them.
    #[test]
    fn a_fold_of_the_body_is_carried_into_the_braces_by_the_keys() {
        let (mut planner, catalog) = plan(
            ".decl R(x: int32, c: int32)\n.output R\n\
             R(x, c) :- Node(x), Bad(x), c = count : { Edge(x, y), F(y, w), A(x, w) }.\n",
        );
        planner.pushdown(&catalog).expect("pushdown");
        let folds: Vec<&str> = names(&planner)
            .into_iter()
            .filter(|name| name.ends_with("⋉[x] bad)"))
            .collect();
        assert_eq!(folds, vec!["(node ⋉[x] bad)"]);
    }
}
