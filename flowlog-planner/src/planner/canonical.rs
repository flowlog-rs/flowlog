//! Canonical form of a planned collection: the query it computes over the
//! rule's relations, spelled the same way however the plan built it.
//!
//! # Why
//!
//! A collection's fingerprint is its identity in the plan: it hashes the
//! operation, its inputs' fingerprints, and layouts that embed body
//! positions, so two collections holding the same rows never share one
//! once their plans diverged, even by an early projection or by reading
//! the same relation at another position.
//! For `T1(x, z) :- R(x, y), S(y, z, w)` and `T2(x, w) :- R(x, y),
//! S(y, z, w)` the join fingerprints differ, because each rule projects
//! `S` down to its own columns first, yet the two heads read
//!
//! ```text
//! T1: atoms(r, s) not() eq(0.1 = 1.0) where() key() value(0.0, 1.1)
//! T2: atoms(r, s) not() eq(0.1 = 1.0) where() key() value(0.0, 1.2)
//! ```
//!
//! Forms that agree on everything but `key` and `value` are the candidates
//! for serving one collection from another.
//!
//! # Deriving a form
//!
//! [`CanonicalForm::relation`] is the form of a relation read as rows;
//! [`CanonicalForm::derive`] folds one [`TransformationInfo`] onto the
//! forms of its inputs. The link between an info and its inputs is
//! positional: column `i` of a reader's input layout is column `i` of the
//! producer's form, key columns first, the invariant codegen already
//! relies on when it indexes `KV` and `Jn` arguments. The rule-local
//! signatures in the layouts are only looked up through that map and
//! never stored. A form numbers its own atoms.
//!
//! # What equality means
//!
//! [`CanonicalForm::normalized`] rewrites every derived form into one
//! spelling, so that structural equality is query equality: two equal
//! forms hold the same set of rows.
//!
//! The set is the whole answer, because every relation is a set. Codegen
//! dedups each EDB as it is read and each IDB once its rules' heads are
//! unioned, before any aggregate sees it, so a row's multiplicity inside
//! a rule never reaches a result: it is a count of derivations that the
//! next dedup flattens. A step may therefore drop a read that another
//! read already witnesses. What no step may do is equate two forms whose
//! row sets differ; a spelling this module fails to unify only costs a
//! sharing opportunity, while a wrong match would corrupt a result.
//! `docs/design/canonical-form.md` proves this, step by step.
//!
//! One thing a form cannot say is *when* it is read. It names a relation,
//! and a relation holds different rows at different points of the run: a
//! recursive relation read inside its fixpoint holds what has been derived
//! so far, and any relation read after a later stratum rewrote it holds
//! more than it did before. Two forms are therefore comparable only within
//! one evaluation scope. One stratum is such a scope, since a stratum's
//! own relations are read only by its recursive part, but a reader
//! comparing forms across strata needs the check
//! `prune_cross_stratum_duplicates` makes on fingerprints, for the same
//! reason and to the same effect.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;

use flowlog_parser::ComparisonOperator;
use itertools::Itertools;
use rustc_hash::FxHasher;

use crate::catalog::ArithmeticPos;
use crate::catalog::AtomArgumentSignature;
use crate::catalog::AtomSignature;
use crate::catalog::ComparisonExprPos;
use crate::catalog::FactorPos;
use crate::planner::ArithmeticArgument;
use crate::planner::FactorArgument;
use crate::planner::KeyValueLayout;
use crate::planner::PlanError;
use crate::planner::TransformationArgument;
use crate::planner::TransformationInfo;

/// The query a collection computes, over the relations of one rule.
///
/// Column `i.j` is argument `j` of `atoms[i]` and column `!i.j` is
/// argument `j` of `negated[i]`, written as an [`AtomArgumentSignature`]
/// of that polarity. `atoms` is never empty. A negated atom's columns
/// appear only in classes and filters, and a filter naming one constrains
/// that negation rather than the row.
///
/// Equal forms are the same query. The converse is the goal but not a
/// guarantee: one query can still reach two forms when an equality folds
/// into a class along one plan and stays a filter along another, or when
/// two expressions differ only algebraically. Each gap costs a sharing
/// opportunity, never a wrong match.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CanonicalForm {
    /// One entry per read of a relation, its canonical name, sorted;
    /// `compute_fp` of the name is the relation's fingerprint.
    atoms: Vec<String>,
    /// One entry per relation that must hold no matching row, its
    /// canonical name, sorted.
    negated: Vec<String>,
    /// Columns held equal, as classes of two or more, sorted. Within a
    /// class the positive columns come first, then each polarity in
    /// order; the first member stands for the class in every expression
    /// below.
    classes: Vec<Vec<AtomArgumentSignature>>,
    /// Row filters, sorted and distinct: comparisons, and equalities with
    /// a constant or a computed value.
    filters: Vec<ComparisonExprPos>,
    /// Expression behind each key column; empty for a row collection.
    key: Vec<ArithmeticPos>,
    /// Expression behind each value column.
    value: Vec<ArithmeticPos>,
}

// =============================================================================
// Construction
// =============================================================================
impl CanonicalForm {
    /// The form of a relation read as rows: every argument is a value
    /// column.
    pub(crate) fn relation(name: &str, arity: usize) -> Self {
        let atom = AtomSignature::new(true, 0);
        Self {
            atoms: vec![name.to_string()],
            negated: Vec::new(),
            classes: Vec::new(),
            filters: Vec::new(),
            key: Vec::new(),
            value: (0..arity)
                .map(|argument| {
                    ArithmeticPos::from_var_signature(AtomArgumentSignature::new(atom, argument))
                })
                .collect(),
        }
    }

    /// The form of `info`'s output, from the forms of its inputs. `right`
    /// is the second input of a join or antijoin.
    ///
    /// # Errors
    ///
    /// Returns an internal error if an input layout disagrees with its
    /// form, a layout or predicate names a column its input lacks, a
    /// binary info comes without `right`, or an antijoin's filter side is
    /// not one relation under column and constant equalities.
    pub(crate) fn derive(
        info: &TransformationInfo,
        left: &Self,
        right: Option<&Self>,
    ) -> Result<Self, PlanError> {
        let output = info.output_kv_layout();
        let right = || {
            right.ok_or_else(|| {
                PlanError::internal(format!(
                    "canonical form: binary transformation {} has no right input",
                    info.output_name()
                ))
            })
        };
        match info {
            TransformationInfo::KVToKV {
                input_kv_layout,
                predicates,
                ..
            } => {
                let exprs = left.column_exprs(input_kv_layout)?;
                let mut form = left.clone();
                let (mut equalities, compare_filters) =
                    Self::partition_compares(&predicates.compare_exprs, &exprs)?;
                form.filters.extend(compare_filters);
                for (signature, constant) in &predicates.const_eq {
                    equalities.push((
                        Self::atom_expr(&ArithmeticPos::from_var_signature(*signature), &exprs)?,
                        ArithmeticPos::new(FactorPos::Const(constant.clone()), Vec::new()),
                    ));
                }
                for (a, b) in &predicates.var_eq {
                    equalities.push((
                        Self::atom_expr(&ArithmeticPos::from_var_signature(*a), &exprs)?,
                        Self::atom_expr(&ArithmeticPos::from_var_signature(*b), &exprs)?,
                    ));
                }
                form.key = output
                    .key()
                    .iter()
                    .map(|expr| Self::atom_expr(expr, &exprs))
                    .collect::<Result<_, _>>()?;
                form.value = output
                    .value()
                    .iter()
                    .map(|expr| Self::atom_expr(expr, &exprs))
                    .collect::<Result<_, _>>()?;
                Ok(form.normalized(equalities))
            }
            TransformationInfo::JoinToKV {
                left_input_kv_layout,
                right_input_kv_layout,
                predicates,
                ..
            } => {
                // Both inputs number their atoms from zero. The right
                // side's atoms follow the left side's, so its column
                // references move up by the left side's atom counts before
                // the two atom lists are appended to each other.
                let mut right = right()?.clone();
                let (positive, negative) = (left.atoms.len(), left.negated.len());
                right.map_columns(&|column| {
                    let atom = column.atom_signature();
                    let offset = if atom.is_positive() {
                        positive
                    } else {
                        negative
                    };
                    AtomArgumentSignature::new(
                        AtomSignature::new(atom.is_positive(), atom.rhs_id() + offset),
                        column.argument_id(),
                    )
                });
                if left.key.len() != right.key.len() {
                    return Err(PlanError::internal(format!(
                        "canonical form: join {} matches {} left key columns against {} right ones",
                        info.output_name(),
                        left.key.len(),
                        right.key.len()
                    )));
                }
                let mut exprs = left.column_exprs(left_input_kv_layout)?;
                for (column, expr) in right.column_exprs(right_input_kv_layout)? {
                    if exprs.insert(column, expr).is_some() {
                        return Err(PlanError::internal(format!(
                            "canonical form: join {} reads column {column} on both inputs",
                            info.output_name()
                        )));
                    }
                }
                let (compare_equalities, compare_filters) =
                    Self::partition_compares(&predicates.compare_exprs, &exprs)?;
                // A join matches the two keys position by position; that
                // is the only constraint it adds beyond its predicates.
                let equalities: Vec<_> = left
                    .key
                    .iter()
                    .cloned()
                    .zip(right.key.iter().cloned())
                    .chain(compare_equalities)
                    .collect();
                let form = Self {
                    atoms: [left.atoms.clone(), right.atoms].concat(),
                    negated: [left.negated.clone(), right.negated].concat(),
                    classes: [left.classes.clone(), right.classes].concat(),
                    filters: [left.filters.clone(), right.filters, compare_filters].concat(),
                    key: output
                        .key()
                        .iter()
                        .map(|expr| Self::atom_expr(expr, &exprs))
                        .collect::<Result<_, _>>()?,
                    value: output
                        .value()
                        .iter()
                        .map(|expr| Self::atom_expr(expr, &exprs))
                        .collect::<Result<_, _>>()?,
                };
                Ok(form.normalized(equalities))
            }
            TransformationInfo::AntiJoinToKV {
                left_input_kv_layout,
                right_input_kv_layout,
                ..
            } => {
                let right = right()?;
                let violated = |detail: String| {
                    PlanError::internal(format!(
                        "canonical form: antijoin {}: {detail}",
                        info.output_name()
                    ))
                };
                // The planner builds an antijoin's filter side from one
                // negated body atom: a projection to the matched columns,
                // repeated variables, and constant arguments. That is the
                // shape a negated atom in a form can express; anything
                // else means the planner changed under this module.
                let [relation] = left.atoms.as_slice() else {
                    return Err(violated(format!(
                        "filter side reads {} relations, expected one: {left}",
                        left.atoms.len()
                    )));
                };
                if !left.negated.is_empty() || !left.value.is_empty() {
                    return Err(violated(format!(
                        "filter side is not a key-only relation without negation: {left}"
                    )));
                }
                if let Some(filter) = left.filters.iter().find(|filter| {
                    *filter.operator() != ComparisonOperator::Equal
                        || filter.left().plain_var().is_none()
                        || !filter.right().signatures().is_empty()
                }) {
                    return Err(violated(format!(
                        "filter side carries {filter}, not an equality with a constant: {left}"
                    )));
                }
                if left.key.len() != right.key.len() {
                    return Err(violated(format!(
                        "filter side has {} key columns but {} are matched: {left}",
                        left.key.len(),
                        right.key.len()
                    )));
                }
                // The filter side contributes no columns; checking its
                // layout still catches a form that drifted from the plan.
                left.column_exprs(left_input_kv_layout)?;
                let exprs = right.column_exprs(right_input_kv_layout)?;

                // The filter side's atom becomes negated atom `!n`, with
                // `n` the count of negated atoms so far. Its classes and
                // constant filters come along under that polarity, and its
                // keys are held equal to the positive keys they match:
                // `!N(k, k, 7)` against key `k` gives the class
                // `{k, !n.0, !n.1}` and the filter `!n.2 = 7`.
                let index = right.negated.len();
                let mut filter_side = left.clone();
                filter_side.map_columns(&|column| {
                    AtomArgumentSignature::new(
                        AtomSignature::new(false, index),
                        column.argument_id(),
                    )
                });
                let equalities = filter_side
                    .key
                    .into_iter()
                    .zip(right.key.iter().cloned())
                    .collect();
                let form = Self {
                    atoms: right.atoms.clone(),
                    negated: [right.negated.clone(), vec![relation.clone()]].concat(),
                    classes: [right.classes.clone(), filter_side.classes].concat(),
                    filters: [right.filters.clone(), filter_side.filters].concat(),
                    key: output
                        .key()
                        .iter()
                        .map(|expr| Self::atom_expr(expr, &exprs))
                        .collect::<Result<_, _>>()?,
                    value: output
                        .value()
                        .iter()
                        .map(|expr| Self::atom_expr(expr, &exprs))
                        .collect::<Result<_, _>>()?,
                };
                Ok(form.normalized(equalities))
            }
        }
    }

    /// The expression this form holds at each column `layout` names:
    /// `layout` is how a reader sees this form's output, key columns then
    /// value columns, position by position.
    ///
    /// # Errors
    ///
    /// Returns an internal error if `layout` has a different column count
    /// than this form, names a column by a computed position, or names one
    /// column twice.
    fn column_exprs(
        &self,
        layout: &KeyValueLayout,
    ) -> Result<HashMap<AtomArgumentSignature, ArithmeticPos>, PlanError> {
        if layout.key().len() != self.key.len() || layout.value().len() != self.value.len() {
            return Err(PlanError::internal(format!(
                "canonical form: input layout has {} key and {} value columns, but its form \
                 has {} and {}: {self}",
                layout.key().len(),
                layout.value().len(),
                self.key.len(),
                self.value.len()
            )));
        }
        let mut exprs = HashMap::new();
        for (position, expr) in layout
            .key()
            .iter()
            .zip(&self.key)
            .chain(layout.value().iter().zip(&self.value))
        {
            let column = position.plain_var().ok_or_else(|| {
                PlanError::internal(format!(
                    "canonical form: input layout position {position} is computed"
                ))
            })?;
            if exprs.insert(column, expr.clone()).is_some() {
                return Err(PlanError::internal(format!(
                    "canonical form: input layout names column {column} twice"
                )));
            }
        }
        Ok(exprs)
    }

    /// `expr`, which names the columns of an input layout, rewritten over
    /// this form's atoms: each column becomes the expression `exprs` holds
    /// for it. A computed column substituted inside a larger expression is
    /// wrapped as a group, so `x * 2` over the column `x = a + 1` reads
    /// `(a + 1) * 2`; substituted for a whole column it is taken as is.
    ///
    /// # Errors
    ///
    /// Returns an internal error if `expr` names a column `exprs` lacks.
    fn atom_expr(
        expr: &ArithmeticPos,
        exprs: &HashMap<AtomArgumentSignature, ArithmeticPos>,
    ) -> Result<ArithmeticPos, PlanError> {
        let missing = |signature: &AtomArgumentSignature| {
            PlanError::internal(format!(
                "canonical form: expression {expr} reads column {signature}, which its input \
                 does not carry"
            ))
        };
        if let Some(signature) = expr.plain_var() {
            return exprs
                .get(&signature)
                .cloned()
                .ok_or_else(|| missing(&signature));
        }
        if let Some(signature) = expr
            .signatures()
            .into_iter()
            .find(|signature| !exprs.contains_key(signature))
        {
            return Err(missing(signature));
        }
        Ok(expr.map_vars(&|signature| {
            let column = &exprs[signature];
            if column.rest().is_empty() {
                column.init().clone()
            } else {
                FactorPos::Group(Box::new(column.clone()))
            }
        }))
    }

    /// `compares` rewritten over this form's atoms and split in two: the
    /// equalities, which [`Self::fold_equalities`] turns into classes or
    /// filters, and every other comparison, which is a filter already.
    ///
    /// A filter is spelled one way: `>` and `>=` are turned around into
    /// `<` and `<=`, so `x > y` and `y < x` read alike, and the sides of
    /// `!=` are ordered, since it has no direction. String constraints
    /// have no mirror and keep their sides.
    ///
    /// # Errors
    ///
    /// Returns the first error [`Self::atom_expr`] reports.
    #[allow(clippy::type_complexity)]
    fn partition_compares(
        compares: &[ComparisonExprPos],
        exprs: &HashMap<AtomArgumentSignature, ArithmeticPos>,
    ) -> Result<(Vec<(ArithmeticPos, ArithmeticPos)>, Vec<ComparisonExprPos>), PlanError> {
        let mut equalities = Vec::new();
        let mut filters = Vec::new();
        for compare in compares {
            let left = Self::atom_expr(compare.left(), exprs)?;
            let right = Self::atom_expr(compare.right(), exprs)?;
            let operator = compare.operator().clone();
            filters.push(match operator {
                ComparisonOperator::Equal => {
                    equalities.push((left, right));
                    continue;
                }
                ComparisonOperator::GreaterThan => {
                    ComparisonExprPos::from_parts(right, ComparisonOperator::LessThan, left)
                }
                ComparisonOperator::GreaterEqualThan => {
                    ComparisonExprPos::from_parts(right, ComparisonOperator::LessEqualThan, left)
                }
                ComparisonOperator::NotEqual if right < left => {
                    ComparisonExprPos::from_parts(right, operator, left)
                }
                ComparisonOperator::NotEqual
                | ComparisonOperator::LessThan
                | ComparisonOperator::LessEqualThan
                | ComparisonOperator::Match { .. }
                | ComparisonOperator::Contains { .. } => {
                    ComparisonExprPos::from_parts(left, operator, right)
                }
            });
        }
        Ok((equalities, filters))
    }
}

// =============================================================================
// Sharing
// =============================================================================
impl CanonicalForm {
    /// A hash of everything but the outputs, equal for forms with the
    /// same body; [`Self::same_body`] tells a collision from a match.
    pub(crate) fn body_hash(&self) -> u64 {
        // Bodies come from the program, never from an adversary, and a
        // collision only costs a `same_body` comparison, so FxHasher's
        // speed over SipHash is free here.
        let mut hasher = FxHasher::default();
        (&self.atoms, &self.negated, &self.classes, &self.filters).hash(&mut hasher);
        hasher.finish()
    }

    /// How much the body constrains: relations read or negated, then
    /// filters. A transformation's output ranks at least as high as its
    /// inputs, since a join adds a relation, an antijoin a negated one, a
    /// filter a filter, and a projection or arrangement keeps the body.
    pub(crate) fn body_rank(&self) -> (usize, usize) {
        (self.atoms.len() + self.negated.len(), self.filters.len())
    }

    /// Whether the two forms agree on everything but the outputs: the
    /// relations read and negated, the classes and the filters. Such forms
    /// hold the same rows and differ only in which expressions of those
    /// rows they carry, so one can serve the other by a map when it
    /// carries the columns the other needs.
    pub(crate) fn same_body(&self, other: &Self) -> bool {
        self.atoms == other.atoms
            && self.negated == other.negated
            && self.classes == other.classes
            && self.filters == other.filters
    }

    /// This form's key and value columns read over `server`'s output, the
    /// flow of the map that would produce this form's rows from it. An
    /// output expression `server` also outputs is read from that
    /// position; any other is rewritten over the positions of the plain
    /// columns it names. `None` unless the two forms have the same body
    /// and every output can be read one of these two ways.
    pub(crate) fn flow_over(
        &self,
        server: &Self,
    ) -> Option<(Vec<ArithmeticArgument>, Vec<ArithmeticArgument>)> {
        if !self.same_body(server) {
            return None;
        }
        let mut outputs: HashMap<&ArithmeticPos, TransformationArgument> = HashMap::new();
        let mut columns: HashMap<AtomArgumentSignature, TransformationArgument> = HashMap::new();
        for (is_key, exprs) in [(true, &server.key), (false, &server.value)] {
            for (index, expr) in exprs.iter().enumerate() {
                let position = TransformationArgument::KV((is_key, index));
                outputs.entry(expr).or_insert(position);
                if let Some(column) = expr.plain_var() {
                    columns.entry(column).or_insert(position);
                }
            }
        }
        let read = |exprs: &[ArithmeticPos]| {
            exprs
                .iter()
                .map(|expr| match outputs.get(expr) {
                    Some(&position) => Some(ArithmeticArgument {
                        init: FactorArgument::Var(position),
                        rest: Vec::new(),
                    }),
                    None => ArithmeticArgument::from_arithmetic_pos(expr, &mut |column| {
                        columns.get(column).copied()
                    }),
                })
                .collect::<Option<Vec<_>>>()
        };
        Some((read(&self.key)?, read(&self.value)?))
    }
}

// =============================================================================
// Normalization
// =============================================================================
impl CanonicalForm {
    /// Folds `equalities` into the form and rewrites it into its canonical
    /// spelling in six steps: (1) classes, (2) duplicate reads, (3) atom
    /// labeling, then under each candidate labeling (4) representatives,
    /// (5) aliases, and (6) filter order; the smallest result is the form.
    ///
    /// # Panics
    ///
    /// Panics if the form reads no relation at all, which
    /// [`CanonicalForm::relation`] and [`CanonicalForm::derive`] never
    /// produce.
    fn normalized(mut self, equalities: Vec<(ArithmeticPos, ArithmeticPos)>) -> Self {
        self.fold_equalities(equalities);
        self.merge_duplicate_reads();
        self.labelings()
            .into_iter()
            .map(|form| {
                form.with_representatives()
                    .with_aliases()
                    .with_sorted_filters()
            })
            .min()
            .expect("Planner error: labelings yields at least one form")
    }

    /// Step 1: an equality between two plain columns merges their
    /// classes, so `R(x, y), S(x)` gives `{r.0, s.0}`. An equality with a
    /// constant or a computed value, `x = 5` or `x + 1 = y`, becomes a
    /// filter instead, with the plain column on the left when there is
    /// one and the smaller side on the left otherwise, so `5 = x` and
    /// `x = 5` read alike.
    fn fold_equalities(&mut self, equalities: Vec<(ArithmeticPos, ArithmeticPos)>) {
        // Classes are few and small, so merging sets directly beats a
        // union-find.
        let mut classes: Vec<BTreeSet<AtomArgumentSignature>> = self
            .classes
            .drain(..)
            .map(|class| class.into_iter().collect())
            .collect();
        for (left, right) in equalities {
            let (Some(a), Some(b)) = (left.plain_var(), right.plain_var()) else {
                // Not a class merge, so a filter. The plain column goes
                // first, and the smaller side first when neither is one,
                // so `5 = x` and `x = 5` read alike.
                let (first, second) = match (left.plain_var(), right.plain_var()) {
                    (None, Some(_)) => (right, left),
                    (None, None) if right < left => (right, left),
                    (None, None) | (Some(_), None) | (Some(_), Some(_)) => (left, right),
                };
                self.filters.push(ComparisonExprPos::from_parts(
                    first,
                    ComparisonOperator::Equal,
                    second,
                ));
                continue;
            };
            let of_a = classes.iter().position(|class| class.contains(&a));
            let of_b = classes.iter().position(|class| class.contains(&b));
            match (of_a, of_b) {
                (Some(i), Some(j)) if i != j => {
                    let absorbed = classes.swap_remove(i.max(j));
                    classes[i.min(j)].extend(absorbed);
                }
                (Some(_), Some(_)) => {}
                (Some(i), None) => {
                    classes[i].insert(b);
                }
                (None, Some(j)) => {
                    classes[j].insert(a);
                }
                (None, None) => classes.push([a, b].into_iter().collect()),
            }
        }
        self.classes = classes
            .into_iter()
            .filter(|class| class.len() > 1)
            .map(|class| class.into_iter().collect())
            .collect();
    }

    /// Step 2: collapses two reads of one relation, of one polarity, into
    /// the earlier one when the later one adds nothing, and moves every
    /// reference to the dropped read onto the kept one. Repeats until no
    /// pair qualifies.
    ///
    /// Two positive reads collapse when every argument the form refers to
    /// on one of them is held equal to the other's: that read's
    /// constraints all hold on the other's row, which therefore witnesses
    /// it. `F(k)` folded into one atom and copied onto another by pushdown
    /// reads `F` twice under one key: one read stays. `A(x, y, _),
    /// A(x, _, w)` asks for two rows, not one row with both `y` and `w`:
    /// both reads stay.
    ///
    /// Two negated reads collapse only when each implies the other, since
    /// a negation with fewer constraints is the stronger one and the merge
    /// keeps the constraints of both: `!N(x, _), !N(x, 5)` would weaken to
    /// `!N(x, 5)`, so both reads stay.
    fn merge_duplicate_reads(&mut self) {
        loop {
            // Arguments the form refers to, per atom: a class member, a
            // filter operand, or an output column.
            let mut referenced: BTreeMap<AtomSignature, BTreeSet<usize>> = BTreeMap::new();
            for column in self.classes.iter().flatten().chain(
                self.filters
                    .iter()
                    .flat_map(|filter| [filter.left(), filter.right()])
                    .chain(&self.key)
                    .chain(&self.value)
                    .flat_map(ArithmeticPos::signatures),
            ) {
                referenced
                    .entry(*column.atom_signature())
                    .or_default()
                    .insert(column.argument_id());
            }
            let held_equal = |a: AtomArgumentSignature, b: AtomArgumentSignature| {
                a == b
                    || self
                        .classes
                        .iter()
                        .any(|class| class.contains(&a) && class.contains(&b))
            };
            let duplicate = [(true, &self.atoms), (false, &self.negated)]
                .into_iter()
                .find_map(|(is_positive, names)| {
                    (0..names.len())
                        .flat_map(|keep| (keep + 1..names.len()).map(move |drop| (keep, drop)))
                        .find(|&(keep, drop)| {
                            let (keep, drop) = (
                                AtomSignature::new(is_positive, keep),
                                AtomSignature::new(is_positive, drop),
                            );
                            // A column held equal to the other read's is
                            // referenced there too, so "every referenced
                            // argument of one read is held equal" already
                            // puts that read's references within the
                            // other's. Reads that each constrain an
                            // argument the other leaves free fail both
                            // directions.
                            let implied = |smaller: AtomSignature, larger: AtomSignature| {
                                referenced.get(&smaller).is_none_or(|arguments| {
                                    arguments.iter().all(|&argument| {
                                        held_equal(
                                            AtomArgumentSignature::new(smaller, argument),
                                            AtomArgumentSignature::new(larger, argument),
                                        )
                                    })
                                })
                            };
                            names[keep.rhs_id()] == names[drop.rhs_id()]
                                && if is_positive {
                                    implied(drop, keep) || implied(keep, drop)
                                } else {
                                    implied(drop, keep) && implied(keep, drop)
                                }
                        })
                        .map(|(keep, drop)| (is_positive, keep, drop))
                });
            let Some((is_positive, keep, drop)) = duplicate else {
                return;
            };
            self.map_columns(&|column| {
                let atom = column.atom_signature();
                if atom.is_positive() != is_positive {
                    return *column;
                }
                let index = match atom.rhs_id() {
                    index if index == drop => keep,
                    index if index > drop => index - 1,
                    index => index,
                };
                AtomArgumentSignature::new(
                    AtomSignature::new(is_positive, index),
                    column.argument_id(),
                )
            });
            if is_positive {
                self.atoms.remove(drop);
            } else {
                self.negated.remove(drop);
            }
            for class in &mut self.classes {
                class.sort_unstable();
                class.dedup();
            }
            self.classes.retain(|class| class.len() > 1);
        }
    }

    /// Step 3: every candidate labeling of the atoms. Atoms of each
    /// polarity sort by relation name; reads of one relation are tried in
    /// every order, so `E(x, y), E(y, z)` and `E(y, z), E(x, y)` yield the
    /// same candidates and later spell alike. Never empty.
    ///
    /// The count is the product of the factorials of the run lengths, so
    /// a rule reading one relation many times pays for it here.
    fn labelings(&self) -> Vec<Self> {
        let runs = |names: &[String]| -> Vec<Vec<usize>> {
            let mut order: Vec<usize> = (0..names.len()).collect();
            order.sort_by(|&a, &b| names[a].cmp(&names[b]));
            order
                .chunk_by(|&a, &b| names[a] == names[b])
                .map(<[usize]>::to_vec)
                .collect()
        };
        let positive = runs(&self.atoms);
        let negative = runs(&self.negated);
        let orders: Vec<(Vec<usize>, Vec<usize>)> = positive
            .iter()
            .chain(&negative)
            .map(|run| run.iter().copied().permutations(run.len()))
            .multi_cartesian_product()
            .map(|parts| {
                let (front, back) = parts.split_at(positive.len());
                (front.concat(), back.concat())
            })
            .collect();
        orders
            .into_iter()
            .map(|(positive, negative)| {
                // `order[position]` is the atom moved to `position`.
                let inverse = |order: &[usize]| {
                    let mut label = vec![0; order.len()];
                    for (position, &index) in order.iter().enumerate() {
                        label[index] = position;
                    }
                    label
                };
                let (positive_label, negative_label) = (inverse(&positive), inverse(&negative));
                let mut form = self.clone();
                form.map_columns(&|column| {
                    let atom = column.atom_signature();
                    let label = if atom.is_positive() {
                        positive_label[atom.rhs_id()]
                    } else {
                        negative_label[atom.rhs_id()]
                    };
                    AtomArgumentSignature::new(
                        AtomSignature::new(atom.is_positive(), label),
                        column.argument_id(),
                    )
                });
                form.atoms = positive
                    .iter()
                    .map(|&index| self.atoms[index].clone())
                    .collect();
                form.negated = negative
                    .iter()
                    .map(|&index| self.negated[index].clone())
                    .collect();
                form
            })
            .collect()
    }

    /// Step 4: the first member of a class stands for it wherever a column
    /// is named, positive members first, so `B(x), !N(x)` with the class
    /// `{b.0, !n.0}` spells its output `b.0`. Safety binds every negated
    /// column to a positive one, so every class has a positive member. An
    /// equality the substitution turns into `0.0 = 0.0` is dropped.
    fn with_representatives(mut self) -> Self {
        for class in &mut self.classes {
            class.sort_unstable_by_key(|column| (!column.atom_signature().is_positive(), *column));
        }
        self.classes.sort_unstable();
        let representative: BTreeMap<AtomArgumentSignature, AtomArgumentSignature> = self
            .classes
            .iter()
            .flat_map(|class| class.iter().map(move |member| (*member, class[0])))
            .collect();
        // A filter naming a negated column constrains that negation, not
        // the row, so only positive columns move: rewriting `!N(x)` with
        // the filter `!n.0 = 5` to `0.0 = 5` would lift the constraint out
        // of the negation and drop every row whose `x` is not 5.
        let substitute = |signature: &AtomArgumentSignature| {
            let target = if signature.atom_signature().is_positive() {
                representative.get(signature).unwrap_or(signature)
            } else {
                signature
            };
            FactorPos::Var(*target)
        };
        self.filters = self
            .filters
            .iter()
            .map(|filter| {
                ComparisonExprPos::from_parts(
                    filter.left().map_vars(&substitute),
                    filter.operator().clone(),
                    filter.right().map_vars(&substitute),
                )
            })
            .filter(|filter| {
                *filter.operator() != ComparisonOperator::Equal || filter.left() != filter.right()
            })
            .collect();
        for expr in self.key.iter_mut().chain(self.value.iter_mut()) {
            *expr = expr.map_vars(&substitute);
        }
        self
    }

    /// Step 5: a computed value a filter holds equal to a plain column is
    /// spelled by that column wherever it appears, whole or as a grouped
    /// factor. With `c.0 = v` a join keyed on `c.0` then reads the same
    /// whichever side the plan took `v` from. The smallest column wins
    /// when several qualify. `x = 5` defines no alias, so other `5`s stay
    /// constants. The defining equality itself is left alone: rewritten
    /// through its own alias it would become `v = v`.
    fn with_aliases(self) -> Self {
        let mut aliases: BTreeMap<&ArithmeticPos, AtomArgumentSignature> = BTreeMap::new();
        for (computed, column) in self.filters.iter().filter_map(Self::alias_of) {
            aliases
                .entry(computed)
                .and_modify(|current| *current = (*current).min(column))
                .or_insert(column);
        }
        let unalias = |expr: &ArithmeticPos| -> ArithmeticPos {
            if let Some(column) = aliases.get(expr) {
                return ArithmeticPos::from_var_signature(*column);
            }
            expr.map_factors(&|factor| match factor {
                FactorPos::Group(inner) => aliases
                    .get(inner.as_ref())
                    .map(|column| FactorPos::Var(*column)),
                FactorPos::Var(_)
                | FactorPos::Const(_)
                | FactorPos::FnCall { .. }
                | FactorPos::Builtin { .. }
                | FactorPos::Tuple { .. }
                | FactorPos::TupleProj { .. } => None,
            })
        };
        let filters = self
            .filters
            .iter()
            .map(|filter| {
                if Self::alias_of(filter).is_some() {
                    filter.clone()
                } else {
                    ComparisonExprPos::from_parts(
                        unalias(filter.left()),
                        filter.operator().clone(),
                        unalias(filter.right()),
                    )
                }
            })
            .collect();
        let key = self.key.iter().map(&unalias).collect();
        let value = self.value.iter().map(&unalias).collect();
        Self {
            filters,
            key,
            value,
            ..self
        }
    }

    /// The computed value and the plain positive column an equality filter
    /// holds equal, or `None` for any other filter. A constant is not a
    /// computed value, and a negated column cannot stand for one: it names
    /// a value inside a negation, which no output column may read.
    fn alias_of(filter: &ComparisonExprPos) -> Option<(&ArithmeticPos, AtomArgumentSignature)> {
        if *filter.operator() != ComparisonOperator::Equal {
            return None;
        }
        let (computed, column) = match (filter.left().plain_var(), filter.right().plain_var()) {
            (Some(column), None) if !filter.right().signatures().is_empty() => {
                (filter.right(), column)
            }
            (None, Some(column)) if !filter.left().signatures().is_empty() => {
                (filter.left(), column)
            }
            (Some(_), Some(_)) | (None, None) | (Some(_), None) | (None, Some(_)) => return None,
        };
        column
            .atom_signature()
            .is_positive()
            .then_some((computed, column))
    }

    /// Step 6: filters sorted and distinct.
    fn with_sorted_filters(mut self) -> Self {
        self.filters.sort_unstable();
        self.filters.dedup();
        self
    }

    /// Passes every column reference through `column`; the atom lists
    /// themselves are left for the caller to arrange.
    fn map_columns(&mut self, column: &impl Fn(&AtomArgumentSignature) -> AtomArgumentSignature) {
        let factor = |signature: &AtomArgumentSignature| FactorPos::Var(column(signature));
        for class in &mut self.classes {
            for member in class.iter_mut() {
                *member = column(member);
            }
        }
        self.filters = self
            .filters
            .iter()
            .map(|filter| {
                ComparisonExprPos::from_parts(
                    filter.left().map_vars(&factor),
                    filter.operator().clone(),
                    filter.right().map_vars(&factor),
                )
            })
            .collect();
        for expr in self.key.iter_mut().chain(self.value.iter_mut()) {
            *expr = expr.map_vars(&factor);
        }
    }
}

// =============================================================================
// Display
// =============================================================================
impl fmt::Display for CanonicalForm {
    /// One line with every section, empty ones included:
    /// `atoms(r, s) not(n) eq(0.1 = 1.0 = !0.0) where(5 < 0.2) key()
    /// value(0.0, 1.1)`. Columns are `atom.argument` into `atoms`, or
    /// `!atom.argument` into `not`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let list = |items: Vec<String>| items.join(", ");
        let exprs = |exprs: &[ArithmeticPos]| list(exprs.iter().map(ToString::to_string).collect());
        write!(f, "atoms({})", list(self.atoms.clone()))?;
        write!(f, " not({})", list(self.negated.clone()))?;
        write!(
            f,
            " eq({})",
            self.classes
                .iter()
                .map(|class| {
                    class
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(" = ")
                })
                .collect::<Vec<_>>()
                .join("; ")
        )?;
        write!(
            f,
            " where({})",
            list(
                self.filters
                    .iter()
                    .map(|filter| format!(
                        "{} {} {}",
                        filter.left(),
                        filter.operator(),
                        filter.right()
                    ))
                    .collect()
            )
        )?;
        write!(
            f,
            " key({}) value({})",
            exprs(&self.key),
            exprs(&self.value)
        )
    }
}

// =============================================================================
// Tests
// =============================================================================
#[cfg(test)]
mod tests {
    use flowlog_common::compute_fp;
    use flowlog_parser::ArithmeticOperator;
    use flowlog_parser::Constant;
    use flowlog_parser::DataType;

    use super::*;
    use crate::catalog::JoinPredicates;
    use crate::catalog::KvPredicates;
    use crate::planner::ProgramPlanner;

    fn column(atom: usize, argument: usize) -> ArithmeticPos {
        ArithmeticPos::from_var_signature(AtomArgumentSignature::new(
            AtomSignature::new(true, atom),
            argument,
        ))
    }

    fn signature(atom: usize, argument: usize) -> AtomArgumentSignature {
        AtomArgumentSignature::new(AtomSignature::new(true, atom), argument)
    }

    fn int(text: &str) -> ArithmeticPos {
        ArithmeticPos::new(
            FactorPos::Const(Constant::new(DataType::Int32, text)),
            Vec::new(),
        )
    }

    fn layout(key: &[ArithmeticPos], value: &[ArithmeticPos]) -> KeyValueLayout {
        KeyValueLayout::new(key.to_vec(), value.to_vec())
    }

    /// A unary info over one input; the layouts' signatures are the
    /// reader's own labels and only their positions matter.
    fn map(
        input: KeyValueLayout,
        output: KeyValueLayout,
        predicates: KvPredicates,
    ) -> TransformationInfo {
        TransformationInfo::kv_to_kv(
            1,
            "in".into(),
            "out".into(),
            true,
            input,
            output,
            predicates,
        )
    }

    fn join(
        left: KeyValueLayout,
        right: KeyValueLayout,
        output: KeyValueLayout,
        predicates: JoinPredicates,
    ) -> TransformationInfo {
        TransformationInfo::join_to_kv(
            1,
            "left".into(),
            2,
            "right".into(),
            "out".into(),
            left,
            right,
            output,
            predicates,
        )
    }

    fn antijoin(
        left: KeyValueLayout,
        right: KeyValueLayout,
        output: KeyValueLayout,
    ) -> TransformationInfo {
        TransformationInfo::anti_join_to_kv(
            1,
            "left".into(),
            2,
            "right".into(),
            "out".into(),
            left,
            right,
            output,
        )
    }

    /// `relation(name, 2)` arranged with argument `key` as the key and the
    /// other argument as the value.
    fn arranged(name: &str, key: usize) -> CanonicalForm {
        let info = map(
            layout(&[], &[column(0, 0), column(0, 1)]),
            layout(&[column(0, key)], &[column(0, 1 - key)]),
            KvPredicates::default(),
        );
        CanonicalForm::derive(&info, &CanonicalForm::relation(name, 2), None).unwrap()
    }

    #[test]
    fn relation_reads_every_argument_as_a_value() {
        assert_eq!(
            CanonicalForm::relation("r", 2).to_string(),
            "atoms(r) not() eq() where() key() value(0.0, 0.1)"
        );
    }

    #[test]
    fn join_equates_the_key_columns_and_appends_the_right_atoms() {
        let info = join(
            layout(&[column(0, 0)], &[column(0, 1)]),
            layout(&[column(1, 0)], &[column(1, 1)]),
            layout(&[column(0, 0)], &[column(0, 1), column(1, 1)]),
            JoinPredicates::default(),
        );

        let form =
            CanonicalForm::derive(&info, &arranged("r", 0), Some(&arranged("s", 0))).unwrap();

        assert_eq!(
            form.to_string(),
            "atoms(r, s) not() eq(0.0 = 1.0) where() key(0.0) value(0.1, 1.1)"
        );
    }

    /// The same join with the relations swapped: atoms are relabeled into
    /// relation order and the key is spelled by its representative.
    #[test]
    fn atom_order_does_not_change_the_form() {
        let r_left = join(
            layout(&[column(0, 0)], &[column(0, 1)]),
            layout(&[column(1, 0)], &[column(1, 1)]),
            layout(&[column(0, 0)], &[column(0, 1), column(1, 1)]),
            JoinPredicates::default(),
        );
        let s_left = join(
            layout(&[column(0, 0)], &[column(0, 1)]),
            layout(&[column(1, 0)], &[column(1, 1)]),
            layout(&[column(1, 0)], &[column(1, 1), column(0, 1)]),
            JoinPredicates::default(),
        );

        let from_r =
            CanonicalForm::derive(&r_left, &arranged("r", 0), Some(&arranged("s", 0))).unwrap();
        let from_s =
            CanonicalForm::derive(&s_left, &arranged("s", 0), Some(&arranged("r", 0))).unwrap();

        assert_eq!(from_r, from_s);
    }

    /// `e(x, y), e(y, z)` keyed on `y` with `(x, z)` as values, built with
    /// either read on the left; both spell the smaller labeling.
    #[test]
    fn reads_of_one_relation_are_labeled_canonically() {
        let x_side_left = join(
            layout(&[column(0, 1)], &[column(0, 0)]),
            layout(&[column(1, 0)], &[column(1, 1)]),
            layout(&[column(0, 1)], &[column(0, 0), column(1, 1)]),
            JoinPredicates::default(),
        );
        let z_side_left = join(
            layout(&[column(0, 0)], &[column(0, 1)]),
            layout(&[column(1, 1)], &[column(1, 0)]),
            layout(&[column(0, 0)], &[column(1, 0), column(0, 1)]),
            JoinPredicates::default(),
        );

        let from_x =
            CanonicalForm::derive(&x_side_left, &arranged("e", 1), Some(&arranged("e", 0)))
                .unwrap();
        let from_z =
            CanonicalForm::derive(&z_side_left, &arranged("e", 0), Some(&arranged("e", 1)))
                .unwrap();

        assert_eq!(
            from_x.to_string(),
            "atoms(e, e) not() eq(0.0 = 1.1) where() key(0.0) value(1.0, 0.1)"
        );
        assert_eq!(from_x, from_z);
    }

    /// The equal columns form a class and the output spells both by the
    /// class representative.
    #[test]
    fn variable_equality_becomes_a_class() {
        let info = map(
            layout(&[], &[column(0, 0), column(0, 1)]),
            layout(&[], &[column(0, 1), column(0, 0)]),
            KvPredicates {
                var_eq: vec![(signature(0, 0), signature(0, 1))],
                ..Default::default()
            },
        );

        let form = CanonicalForm::derive(&info, &CanonicalForm::relation("a", 2), None).unwrap();

        assert_eq!(
            form.to_string(),
            "atoms(a) not() eq(0.0 = 0.1) where() key() value(0.0, 0.0)"
        );
    }

    #[test]
    fn constant_equality_becomes_a_filter() {
        let info = map(
            layout(&[], &[column(0, 0), column(0, 1)]),
            layout(&[], &[column(0, 0)]),
            KvPredicates {
                const_eq: vec![(signature(0, 1), Constant::new(DataType::Int32, "5"))],
                ..Default::default()
            },
        );

        let form = CanonicalForm::derive(&info, &CanonicalForm::relation("a", 2), None).unwrap();

        assert_eq!(
            form.to_string(),
            "atoms(a) not() eq() where(0.1 = 5) key() value(0.0)"
        );
    }

    #[test]
    fn comparison_filters_are_sorted_and_distinct() {
        let info = map(
            layout(&[], &[column(0, 0), column(0, 1)]),
            layout(&[], &[column(0, 0)]),
            KvPredicates {
                compare_exprs: vec![
                    ComparisonExprPos::from_parts(
                        column(0, 1),
                        ComparisonOperator::GreaterThan,
                        int("3"),
                    ),
                    ComparisonExprPos::from_parts(
                        column(0, 0),
                        ComparisonOperator::LessThan,
                        int("2"),
                    ),
                    ComparisonExprPos::from_parts(
                        column(0, 1),
                        ComparisonOperator::GreaterThan,
                        int("3"),
                    ),
                ],
                ..Default::default()
            },
        );

        let form = CanonicalForm::derive(&info, &CanonicalForm::relation("a", 2), None).unwrap();

        assert_eq!(
            form.to_string(),
            "atoms(a) not() eq() where(0.0 < 2, 3 < 0.1) key() value(0.0)"
        );
    }

    #[test]
    fn equality_comparison_between_columns_joins_their_classes() {
        let info = map(
            layout(&[], &[column(0, 0), column(0, 1)]),
            layout(&[], &[column(0, 1)]),
            KvPredicates {
                compare_exprs: vec![ComparisonExprPos::from_parts(
                    column(0, 1),
                    ComparisonOperator::Equal,
                    column(0, 0),
                )],
                ..Default::default()
            },
        );

        let form = CanonicalForm::derive(&info, &CanonicalForm::relation("a", 2), None).unwrap();

        assert_eq!(
            form.to_string(),
            "atoms(a) not() eq(0.0 = 0.1) where() key() value(0.0)"
        );
    }

    /// A join keyed on computed columns keeps the equality as a filter
    /// between the two expressions.
    #[test]
    fn equality_between_computed_columns_stays_a_filter() {
        let shadow = |name: &str, constant: &str| {
            let info = map(
                layout(&[], &[column(0, 0)]),
                layout(
                    &[ArithmeticPos::new(
                        FactorPos::Var(signature(0, 0)),
                        vec![(
                            ArithmeticOperator::Plus,
                            FactorPos::Const(Constant::new(DataType::Int32, constant)),
                        )],
                    )],
                    &[column(0, 0)],
                ),
                KvPredicates::default(),
            );
            CanonicalForm::derive(&info, &CanonicalForm::relation(name, 1), None).unwrap()
        };
        let info = join(
            layout(&[column(0, 0)], &[column(0, 1)]),
            layout(&[column(1, 0)], &[column(1, 1)]),
            layout(&[], &[column(0, 1), column(1, 1)]),
            JoinPredicates::default(),
        );

        let form =
            CanonicalForm::derive(&info, &shadow("a", "1"), Some(&shadow("b", "2"))).unwrap();

        assert_eq!(
            form.to_string(),
            "atoms(a, b) not() eq() where(0.0 + 1 = 1.0 + 2) key() value(0.0, 1.0)"
        );
    }

    #[test]
    fn computed_column_keeps_its_grouping_inside_a_larger_expression() {
        let plus_one = ArithmeticPos::new(
            FactorPos::Var(signature(0, 0)),
            vec![(
                ArithmeticOperator::Plus,
                FactorPos::Const(Constant::new(DataType::Int32, "1")),
            )],
        );
        let computed = map(
            layout(&[], &[column(0, 0)]),
            layout(&[], &[plus_one]),
            KvPredicates::default(),
        );
        let doubled = ArithmeticPos::new(
            FactorPos::Var(signature(0, 0)),
            vec![(
                ArithmeticOperator::Multiply,
                FactorPos::Const(Constant::new(DataType::Int32, "2")),
            )],
        );
        let reader = map(
            layout(&[], &[column(0, 0)]),
            layout(&[], &[doubled, column(0, 0)]),
            KvPredicates::default(),
        );

        let input =
            CanonicalForm::derive(&computed, &CanonicalForm::relation("a", 1), None).unwrap();
        let form = CanonicalForm::derive(&reader, &input, None).unwrap();

        assert_eq!(
            form.to_string(),
            "atoms(a) not() eq() where() key() value((0.0 + 1) * 2, 0.0 + 1)"
        );
    }

    /// `n(k, k, 7)` as a key-only filter on `k`: the negated atom's
    /// columns join the positive key's class, and its constant filter
    /// comes along under the negated polarity.
    #[test]
    fn antijoin_carries_the_filter_side_under_the_negated_polarity() {
        let filter = map(
            layout(&[], &[column(0, 0), column(0, 1), column(0, 2)]),
            layout(&[column(0, 0)], &[]),
            KvPredicates {
                const_eq: vec![(signature(0, 2), Constant::new(DataType::Int32, "7"))],
                var_eq: vec![(signature(0, 0), signature(0, 1))],
                compare_exprs: vec![],
            },
        );
        let filter =
            CanonicalForm::derive(&filter, &CanonicalForm::relation("n", 3), None).unwrap();
        let info = antijoin(
            layout(&[column(0, 0)], &[]),
            layout(&[column(1, 1)], &[column(1, 0)]),
            layout(&[column(1, 1)], &[column(1, 0)]),
        );

        let form = CanonicalForm::derive(&info, &filter, Some(&arranged("b", 1))).unwrap();

        assert_eq!(
            form.to_string(),
            "atoms(b) not(n) eq(0.1 = !0.0 = !0.1) where(!0.2 = 7) key(0.1) value(0.0)"
        );
    }

    #[test]
    fn antijoin_leaves_an_unconstrained_negated_argument_unmentioned() {
        let filter = map(
            layout(&[], &[column(0, 0), column(0, 1)]),
            layout(&[column(0, 1)], &[]),
            KvPredicates::default(),
        );
        let filter =
            CanonicalForm::derive(&filter, &CanonicalForm::relation("n", 2), None).unwrap();
        let info = antijoin(
            layout(&[column(0, 0)], &[]),
            layout(&[column(1, 0)], &[column(1, 1)]),
            layout(&[], &[column(1, 0), column(1, 1)]),
        );

        let form = CanonicalForm::derive(&info, &filter, Some(&arranged("b", 0))).unwrap();

        assert_eq!(
            form.to_string(),
            "atoms(b) not(n) eq(0.0 = !0.1) where() key() value(0.0, 0.1)"
        );
    }

    #[test]
    fn antijoin_rejects_a_filter_side_reading_two_relations() {
        let joined = join(
            layout(&[column(0, 0)], &[column(0, 1)]),
            layout(&[column(1, 0)], &[column(1, 1)]),
            layout(&[column(0, 0)], &[]),
            JoinPredicates::default(),
        );
        let filter =
            CanonicalForm::derive(&joined, &arranged("r", 0), Some(&arranged("s", 0))).unwrap();
        let info = antijoin(
            layout(&[column(0, 0)], &[]),
            layout(&[column(1, 0)], &[column(1, 1)]),
            layout(&[], &[column(1, 0), column(1, 1)]),
        );

        let error = CanonicalForm::derive(&info, &filter, Some(&arranged("b", 0))).unwrap_err();

        assert!(matches!(error, PlanError::Internal(_)));
        assert!(
            error.to_string().contains("filter side reads 2 relations"),
            "{error}"
        );
    }

    #[test]
    fn binary_info_without_a_right_input_is_rejected() {
        let info = join(
            layout(&[column(0, 0)], &[column(0, 1)]),
            layout(&[column(1, 0)], &[column(1, 1)]),
            layout(&[column(0, 0)], &[]),
            JoinPredicates::default(),
        );

        let error = CanonicalForm::derive(&info, &arranged("r", 0), None).unwrap_err();

        assert!(matches!(error, PlanError::Internal(_)));
        assert!(error.to_string().contains("has no right input"), "{error}");
    }

    #[test]
    fn layout_disagreeing_with_its_form_is_rejected() {
        let info = map(
            layout(&[], &[column(0, 0), column(0, 1), column(0, 2)]),
            layout(&[], &[column(0, 0)]),
            KvPredicates::default(),
        );

        let error =
            CanonicalForm::derive(&info, &CanonicalForm::relation("r", 2), None).unwrap_err();

        assert!(matches!(error, PlanError::Internal(_)));
        assert!(
            error
                .to_string()
                .contains("input layout has 0 key and 3 value columns"),
            "{error}"
        );
    }

    #[test]
    fn output_naming_a_column_the_input_lacks_is_rejected() {
        let info = map(
            layout(&[], &[column(0, 0), column(0, 1)]),
            layout(&[], &[column(0, 5)]),
            KvPredicates::default(),
        );

        let error =
            CanonicalForm::derive(&info, &CanonicalForm::relation("r", 2), None).unwrap_err();

        assert!(matches!(error, PlanError::Internal(_)));
        assert!(error.to_string().contains("reads column 0.5"), "{error}");
    }

    /// The filter `f` folded once and copied by pushdown reads the same
    /// rows under the same key twice; the form keeps one read.
    #[test]
    fn reads_of_one_relation_held_equal_collapse_into_one() {
        // `f(k)` arranged as a key-only filter.
        let key_only = || {
            let info = map(
                layout(&[], &[column(0, 0)]),
                layout(&[column(0, 0)], &[]),
                KvPredicates::default(),
            );
            CanonicalForm::derive(&info, &CanonicalForm::relation("f", 1), None).unwrap()
        };
        let semijoin = || {
            join(
                layout(&[column(0, 0)], &[]),
                layout(&[column(1, 0)], &[column(1, 1)]),
                layout(&[column(1, 0)], &[column(1, 1)]),
                JoinPredicates::default(),
            )
        };
        let once =
            CanonicalForm::derive(&semijoin(), &key_only(), Some(&arranged("s", 0))).unwrap();

        let twice = CanonicalForm::derive(&semijoin(), &key_only(), Some(&once)).unwrap();

        assert_eq!(
            twice.to_string(),
            "atoms(f, s) not() eq(0.0 = 1.0) where() key(0.0) value(1.1)"
        );
        assert_eq!(twice, once);
    }

    /// Two reads of one relation that differ in one column stay two reads.
    #[test]
    fn reads_of_one_relation_differing_in_a_column_both_stay() {
        let info = join(
            layout(&[column(0, 0)], &[column(0, 1)]),
            layout(&[column(1, 0)], &[column(1, 1)]),
            layout(&[column(0, 0)], &[column(0, 1), column(1, 1)]),
            JoinPredicates::default(),
        );

        let form =
            CanonicalForm::derive(&info, &arranged("e", 0), Some(&arranged("e", 0))).unwrap();

        assert_eq!(
            form.to_string(),
            "atoms(e, e) not() eq(0.0 = 1.0) where() key(0.0) value(0.1, 1.1)"
        );
    }

    /// `A(x, y, _), A(x, _, w)`: each read constrains a column the other
    /// leaves free, so one row cannot stand for both. The reads stay, and
    /// the form differs from the single read `A(_, y, w)`, which pairs
    /// `y` with `w` on one row and answers a different question.
    #[test]
    fn reads_each_constraining_a_column_the_other_frees_both_stay() {
        let keep = |argument| {
            let info = map(
                layout(&[], &[column(0, 0), column(0, 1), column(0, 2)]),
                layout(&[column(0, 0)], &[column(0, argument)]),
                KvPredicates::default(),
            );
            CanonicalForm::derive(&info, &CanonicalForm::relation("a", 3), None).unwrap()
        };
        let join = join(
            layout(&[column(0, 0)], &[column(0, 1)]),
            layout(&[column(1, 0)], &[column(1, 1)]),
            layout(&[], &[column(0, 1), column(1, 1)]),
            JoinPredicates::default(),
        );
        let one_read = map(
            layout(&[], &[column(0, 0), column(0, 1), column(0, 2)]),
            layout(&[], &[column(0, 1), column(0, 2)]),
            KvPredicates::default(),
        );

        let two_reads = CanonicalForm::derive(&join, &keep(1), Some(&keep(2))).unwrap();
        let one_read =
            CanonicalForm::derive(&one_read, &CanonicalForm::relation("a", 3), None).unwrap();

        assert_eq!(
            two_reads.to_string(),
            "atoms(a, a) not() eq(0.0 = 1.0) where() key() value(0.1, 1.2)"
        );
        assert_ne!(two_reads, one_read);
    }

    /// `A(x, y), A(x, z), y < z`: the filter tells the two reads apart
    /// although their keys are held equal, so they must not collapse. A
    /// merge here would rewrite the filter to `y < y` and answer a
    /// question no rule asked.
    #[test]
    fn a_filter_spanning_two_reads_keeps_them_apart() {
        let info = join(
            layout(&[column(0, 0)], &[column(0, 1)]),
            layout(&[column(1, 0)], &[column(1, 1)]),
            layout(&[], &[column(0, 1), column(1, 1)]),
            JoinPredicates {
                compare_exprs: vec![ComparisonExprPos::from_parts(
                    column(0, 1),
                    ComparisonOperator::LessThan,
                    column(1, 1),
                )],
            },
        );

        let form =
            CanonicalForm::derive(&info, &arranged("a", 0), Some(&arranged("a", 0))).unwrap();

        assert_eq!(
            form.to_string(),
            "atoms(a, a) not() eq(0.0 = 1.0) where(0.1 < 1.1) key() value(0.1, 1.1)"
        );
    }

    /// The second read's other column is projected away, so nothing in the
    /// form tells the two reads apart and they collapse.
    #[test]
    fn reads_differing_only_in_an_unreferenced_column_collapse() {
        let projected = map(
            layout(&[], &[column(0, 0), column(0, 1)]),
            layout(&[column(0, 0)], &[]),
            KvPredicates::default(),
        );
        let projected =
            CanonicalForm::derive(&projected, &CanonicalForm::relation("e", 2), None).unwrap();
        let info = join(
            layout(&[column(0, 0)], &[]),
            layout(&[column(1, 0)], &[column(1, 1)]),
            layout(&[column(1, 0)], &[column(1, 1)]),
            JoinPredicates::default(),
        );

        let form = CanonicalForm::derive(&info, &projected, Some(&arranged("e", 0))).unwrap();

        assert_eq!(
            form.to_string(),
            "atoms(e) not() eq() where() key(0.0) value(0.1)"
        );
    }

    /// `x = 5` keeps `5` a constant everywhere else; only computed values
    /// take a column's name.
    #[test]
    fn constant_equality_does_not_rename_other_constants() {
        let info = map(
            layout(&[], &[column(0, 0), column(0, 1)]),
            layout(&[], &[column(0, 0), int("5")]),
            KvPredicates {
                const_eq: vec![(signature(0, 1), Constant::new(DataType::Int32, "5"))],
                ..Default::default()
            },
        );

        let form = CanonicalForm::derive(&info, &CanonicalForm::relation("a", 2), None).unwrap();

        assert_eq!(
            form.to_string(),
            "atoms(a) not() eq() where(0.1 = 5) key() value(0.0, 5)"
        );
    }

    /// `!N(k)` whose one argument is both matched and held to a constant.
    /// The constant belongs to the negation: a row whose `k` is not 5
    /// satisfies `!N(k)` rather than failing it, so the filter must keep
    /// naming the negated column even though a class ties it to `b.0`.
    #[test]
    fn a_negated_atom_keeps_its_constant_filter_inside_the_negation() {
        let filter_side = map(
            layout(&[], &[column(0, 0)]),
            layout(&[column(0, 0)], &[]),
            KvPredicates {
                const_eq: vec![(signature(0, 0), Constant::new(DataType::Int32, "5"))],
                ..Default::default()
            },
        );
        let filter_side =
            CanonicalForm::derive(&filter_side, &CanonicalForm::relation("n", 1), None).unwrap();
        let info = antijoin(
            layout(&[column(0, 0)], &[]),
            layout(&[column(1, 0)], &[column(1, 1)]),
            layout(&[], &[column(1, 0), column(1, 1)]),
        );

        let form = CanonicalForm::derive(&info, &filter_side, Some(&arranged("b", 0))).unwrap();

        assert_eq!(
            form.to_string(),
            "atoms(b) not(n) eq(0.0 = !0.0) where(!0.0 = 5) key() value(0.0, 0.1)"
        );
    }

    /// A string constraint has no mirrored operator, so its sides stay as
    /// written even with the column on the right.
    #[test]
    fn string_constraint_keeps_its_sides() {
        let info = map(
            layout(&[], &[column(0, 0)]),
            layout(&[], &[column(0, 0)]),
            KvPredicates {
                compare_exprs: vec![ComparisonExprPos::from_parts(
                    ArithmeticPos::new(
                        FactorPos::Const(Constant::new(DataType::String, "a")),
                        Vec::new(),
                    ),
                    ComparisonOperator::Contains { negated: false },
                    column(0, 0),
                )],
                ..Default::default()
            },
        );

        let form = CanonicalForm::derive(&info, &CanonicalForm::relation("a", 1), None).unwrap();

        assert_eq!(
            form.to_string(),
            "atoms(a) not() eq() where(\"a\" contains 0.0) key() value(0.0)"
        );
    }

    /// `!N(x, _)` is the stronger negation; collapsing it into `!N(x, 5)`
    /// would weaken the rule, so both negated reads stay.
    #[test]
    fn negated_reads_differing_in_a_constraint_both_stay() {
        let free = map(
            layout(&[], &[column(0, 0), column(0, 1)]),
            layout(&[column(0, 0)], &[]),
            KvPredicates::default(),
        );
        let five = map(
            layout(&[], &[column(0, 0), column(0, 1)]),
            layout(&[column(0, 0)], &[]),
            KvPredicates {
                const_eq: vec![(signature(0, 1), Constant::new(DataType::Int32, "5"))],
                ..Default::default()
            },
        );
        let relation = CanonicalForm::relation("n", 2);
        let free = CanonicalForm::derive(&free, &relation, None).unwrap();
        let five = CanonicalForm::derive(&five, &relation, None).unwrap();
        let antijoin = || {
            antijoin(
                layout(&[column(0, 0)], &[]),
                layout(&[column(1, 0)], &[column(1, 1)]),
                layout(&[column(1, 0)], &[column(1, 1)]),
            )
        };

        let once = CanonicalForm::derive(&antijoin(), &free, Some(&arranged("b", 0))).unwrap();
        let form = CanonicalForm::derive(&antijoin(), &five, Some(&once)).unwrap();

        assert_eq!(
            form.to_string(),
            "atoms(b) not(n, n) eq(0.0 = !0.0 = !1.0) where(!0.1 = 5) key(0.0) value(0.1)"
        );
    }

    /// The same negated read applied twice, as pushdown does with an
    /// antijoin copy, is one negated read.
    #[test]
    fn identical_negated_reads_collapse_into_one() {
        let filter = map(
            layout(&[], &[column(0, 0), column(0, 1)]),
            layout(&[column(0, 0)], &[]),
            KvPredicates::default(),
        );
        let filter =
            CanonicalForm::derive(&filter, &CanonicalForm::relation("n", 2), None).unwrap();
        let antijoin = || {
            antijoin(
                layout(&[column(0, 0)], &[]),
                layout(&[column(1, 0)], &[column(1, 1)]),
                layout(&[column(1, 0)], &[column(1, 1)]),
            )
        };

        let once = CanonicalForm::derive(&antijoin(), &filter, Some(&arranged("b", 0))).unwrap();
        let twice = CanonicalForm::derive(&antijoin(), &filter, Some(&once)).unwrap();

        assert_eq!(
            twice.to_string(),
            "atoms(b) not(n) eq(0.0 = !0.0) where() key(0.0) value(0.1)"
        );
        assert_eq!(twice, once);
    }

    /// `5 < x` and `x > 5` are one filter, spelled with `<`.
    #[test]
    fn greater_than_is_turned_around_into_less_than() {
        let filtered = |compare_exprs| {
            let info = map(
                layout(&[], &[column(0, 0)]),
                layout(&[], &[column(0, 0)]),
                KvPredicates {
                    compare_exprs,
                    ..Default::default()
                },
            );
            CanonicalForm::derive(&info, &CanonicalForm::relation("a", 1), None).unwrap()
        };

        let turned = filtered(vec![ComparisonExprPos::from_parts(
            int("5"),
            ComparisonOperator::LessThan,
            column(0, 0),
        )]);
        let direct = filtered(vec![ComparisonExprPos::from_parts(
            column(0, 0),
            ComparisonOperator::GreaterThan,
            int("5"),
        )]);

        assert_eq!(
            turned.to_string(),
            "atoms(a) not() eq() where(5 < 0.0) key() value(0.0)"
        );
        assert_eq!(turned, direct);
    }

    /// `5 = x` written as a comparison spells its column first, like the
    /// constant argument `A(5)` does.
    #[test]
    fn equality_with_the_column_on_the_right_is_turned_around() {
        let info = map(
            layout(&[], &[column(0, 0)]),
            layout(&[], &[column(0, 0)]),
            KvPredicates {
                compare_exprs: vec![ComparisonExprPos::from_parts(
                    int("5"),
                    ComparisonOperator::Equal,
                    column(0, 0),
                )],
                ..Default::default()
            },
        );

        let form = CanonicalForm::derive(&info, &CanonicalForm::relation("a", 1), None).unwrap();

        assert_eq!(
            form.to_string(),
            "atoms(a) not() eq() where(0.0 = 5) key() value(0.0)"
        );
    }

    /// The join key is `a.0 + 1` on the left and the plain column `b.0`
    /// on the right; the output spells it by the column whichever side
    /// the plan took it from.
    #[test]
    fn computed_value_equal_to_a_column_is_spelled_by_the_column() {
        let shadow = map(
            layout(&[], &[column(0, 0)]),
            layout(
                &[ArithmeticPos::new(
                    FactorPos::Var(signature(0, 0)),
                    vec![(
                        ArithmeticOperator::Plus,
                        FactorPos::Const(Constant::new(DataType::Int32, "1")),
                    )],
                )],
                &[column(0, 0)],
            ),
            KvPredicates::default(),
        );
        let shadow =
            CanonicalForm::derive(&shadow, &CanonicalForm::relation("a", 1), None).unwrap();
        let from_left = join(
            layout(&[column(0, 0)], &[column(0, 1)]),
            layout(&[column(1, 0)], &[column(1, 1)]),
            layout(&[], &[column(0, 0), column(1, 1)]),
            JoinPredicates::default(),
        );
        let from_right = join(
            layout(&[column(0, 0)], &[column(0, 1)]),
            layout(&[column(1, 0)], &[column(1, 1)]),
            layout(&[], &[column(1, 0), column(1, 1)]),
            JoinPredicates::default(),
        );

        let left = CanonicalForm::derive(&from_left, &shadow, Some(&arranged("b", 0))).unwrap();
        let right = CanonicalForm::derive(&from_right, &shadow, Some(&arranged("b", 0))).unwrap();

        assert_eq!(
            left.to_string(),
            "atoms(a, b) not() eq() where(1.0 = 0.0 + 1) key() value(1.0, 1.1)"
        );
        assert_eq!(left, right);
    }

    /// A chain of three reads of one relation searches all six labelings
    /// and settles on one spelling, so the search is not limited to the
    /// two-read case.
    #[test]
    fn a_three_read_chain_settles_on_one_labeling() {
        let mut chain = arranged("e", 1);
        for _ in 1..3 {
            let step = join(
                layout(&[column(0, 0)], &[column(0, 1)]),
                layout(&[column(1, 0)], &[column(1, 1)]),
                layout(&[column(1, 1)], &[column(0, 1)]),
                JoinPredicates::default(),
            );
            chain = CanonicalForm::derive(&step, &chain, Some(&arranged("e", 0))).unwrap();
        }

        assert_eq!(
            chain.to_string(),
            "atoms(e, e, e) not() eq(0.0 = 1.1; 0.1 = 2.0) where() key(2.1) value(1.0)"
        );
    }

    // Forms derived through the planner: `derive` runs on real plans, so
    // these drive a source snippet end to end and read the head's form.

    /// Canonical form of the head collection derived for relation `name`,
    /// rendered; the relation must be produced by exactly one rule.
    fn head_form(pp: &ProgramPlanner, name: &str) -> String {
        let idb = compute_fp(name);
        let stratum = pp
            .strata()
            .iter()
            .find(|stratum| stratum.idb_to_heads_map().contains_key(&idb))
            .expect("relation is produced by some stratum");
        let [head] = stratum.idb_to_heads_map()[&idb].as_slice() else {
            panic!("relation {name} has more than one rule");
        };
        stratum
            .non_recursive_transformations()
            .iter()
            .chain(stratum.recursive_transformations())
            .find(|tx| tx.output().fingerprint() == *head)
            .expect("head is produced by a transformation")
            .output()
            .canonical()
            .to_string()
    }

    /// The user-facing motivation for canonical forms: two rules over the
    /// same join keep different columns, so their fingerprints diverge at
    /// the first projection, while their forms differ only in the values.
    #[test]
    fn projections_of_one_join_share_a_form_up_to_their_outputs() {
        let pp = ProgramPlanner::analyze(
            "\
            .decl R(x: int32, y: int32)\n\
            .decl S(y: int32, z: int32, w: int32)\n\
            .decl T1(x: int32, z: int32)\n\
            .decl T2(x: int32, w: int32)\n\
            .input R(IO=\"file\", filename=\"R.csv\", delimiter=\",\")\n\
            .input S(IO=\"file\", filename=\"S.csv\", delimiter=\",\")\n\
            .output T1\n\
            .output T2\n\
            T1(x, z) :- R(x, y), S(y, z, w).\n\
            T2(x, w) :- R(x, y), S(y, z, w).\n",
        );

        assert_eq!(
            head_form(&pp, "t1"),
            "atoms(r, s) not() eq(0.1 = 1.0) where() key() value(0.0, 1.1)"
        );
        assert_eq!(
            head_form(&pp, "t2"),
            "atoms(r, s) not() eq(0.1 = 1.0) where() key() value(0.0, 1.2)"
        );
    }

    /// A semijoin is the join with the filter side's columns projected
    /// away, and its form says so.
    #[test]
    fn semijoin_form_is_the_join_form_without_the_filter_columns() {
        let pp = ProgramPlanner::analyze(
            "\
            .decl R(k: int32, a: int32)\n\
            .decl S(k: int32, b: int32)\n\
            .decl Wide(a: int32, b: int32)\n\
            .decl Semi(a: int32)\n\
            .input R(IO=\"file\", filename=\"R.csv\", delimiter=\",\")\n\
            .input S(IO=\"file\", filename=\"S.csv\", delimiter=\",\")\n\
            .output Wide\n\
            .output Semi\n\
            Wide(a, b) :- R(k, a), S(k, b).\n\
            Semi(a) :- R(k, a), S(k, _).\n",
        );

        assert_eq!(
            head_form(&pp, "wide"),
            "atoms(r, s) not() eq(0.0 = 1.0) where() key() value(0.1, 1.1)"
        );
        assert_eq!(
            head_form(&pp, "semi"),
            "atoms(r, s) not() eq(0.0 = 1.0) where() key() value(0.1)"
        );
    }

    #[test]
    fn negated_atom_is_bound_to_the_positive_column_it_filters() {
        let pp = ProgramPlanner::analyze(
            "\
            .decl B(x: int32, y: int32)\n\
            .decl C(x: int32, z: int32)\n\
            .decl N(x: int32)\n\
            .decl Out(x: int32, y: int32, z: int32)\n\
            .input B(IO=\"file\", filename=\"B.csv\", delimiter=\",\")\n\
            .input C(IO=\"file\", filename=\"C.csv\", delimiter=\",\")\n\
            .input N(IO=\"file\", filename=\"N.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x, y, z) :- B(x, y), C(x, z), !N(x).\n",
        );

        assert_eq!(
            head_form(&pp, "out"),
            "atoms(b, c) not(n) eq(0.0 = 1.0 = !0.0) where() key() value(0.0, 0.1, 1.1)"
        );
    }

    #[test]
    fn constant_arguments_and_comparisons_are_filters_of_the_head_form() {
        let pp = ProgramPlanner::analyze(
            "\
            .decl A(x: int32, y: int32)\n\
            .decl Out(x: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x) :- A(x, 5), x > 3.\n",
        );

        assert_eq!(
            head_form(&pp, "out"),
            "atoms(a) not() eq() where(0.1 = 5, 3 < 0.0) key() value(0.0)"
        );
    }

    /// A comparison between two columns has one spelling: `>` turns
    /// around into `<`, so `x > y` and `y < x` share a head.
    #[test]
    fn a_comparison_reads_the_same_in_either_direction() {
        let pp = ProgramPlanner::analyze(
            "\
            .decl R(x: int32, y: int32)\n\
            .decl Gt(x: int32)\n\
            .decl Lt(x: int32)\n\
            .input R(IO=\"file\", filename=\"R.csv\", delimiter=\",\")\n\
            .output Gt\n\
            .output Lt\n\
            Gt(x) :- R(x, y), x > y.\n\
            Lt(x) :- R(x, y), y < x.\n",
        );
        assert_eq!(head_form(&pp, "gt"), head_form(&pp, "lt"));
        assert_eq!(
            head_form(&pp, "gt"),
            "atoms(r) not() eq() where(0.1 < 0.0) key() value(0.0)"
        );
    }

    /// `!=` has no direction, so its sides are ordered and `x != y` and
    /// `y != x` share a head.
    #[test]
    fn inequality_sides_are_ordered() {
        let pp = ProgramPlanner::analyze(
            "\
            .decl R(x: int32, y: int32)\n\
            .decl A(x: int32)\n\
            .decl B(x: int32)\n\
            .input R(IO=\"file\", filename=\"R.csv\", delimiter=\",\")\n\
            .output A\n\
            .output B\n\
            A(x) :- R(x, y), x != y.\n\
            B(x) :- R(x, y), y != x.\n",
        );
        assert_eq!(head_form(&pp, "a"), head_form(&pp, "b"));
    }

    /// Body order decides which read the plan joins on the left; the
    /// form does not depend on it.
    #[test]
    fn body_order_does_not_change_the_head_form() {
        let pp = ProgramPlanner::analyze(
            "\
            .decl E(x: int32, y: int32)\n\
            .decl P(x: int32, z: int32)\n\
            .decl Q(x: int32, z: int32)\n\
            .input E(IO=\"file\", filename=\"E.csv\", delimiter=\",\")\n\
            .output P\n\
            .output Q\n\
            P(x, z) :- E(x, y), E(y, z).\n\
            Q(x, z) :- E(y, z), E(x, y).\n",
        );

        assert_eq!(
            head_form(&pp, "p"),
            "atoms(e, e) not() eq(0.0 = 1.1) where() key() value(1.0, 0.1)"
        );
        assert_eq!(head_form(&pp, "q"), head_form(&pp, "p"));
    }

    #[test]
    fn spanning_equality_between_computed_values_is_a_filter() {
        let pp = ProgramPlanner::analyze(
            "\
            .decl A(x: int32)\n\
            .decl B(y: int32)\n\
            .decl Out(x: int32, y: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .input B(IO=\"file\", filename=\"B.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x, y) :- A(x), B(y), x + 1 = y + 2.\n",
        );

        assert_eq!(
            head_form(&pp, "out"),
            "atoms(a, b) not() eq() where(0.0 + 1 = 1.0 + 2) key() value(0.0, 1.0)"
        );
    }

    /// Core folds `A` into `B` and pushdown copies it onto `C`; the head
    /// form reads `A` once.
    #[test]
    fn pushdown_copies_of_a_filter_collapse_into_one_read() {
        let pp = ProgramPlanner::analyze(
            "\
            .decl A(x: int32)\n\
            .decl B(x: int32, y: int32)\n\
            .decl C(x: int32, z: int32, v: int32)\n\
            .decl D(z: int32, w: int32)\n\
            .decl Out(x: int32, y: int32, z: int32, w: int32, v: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .input B(IO=\"file\", filename=\"B.csv\", delimiter=\",\")\n\
            .input C(IO=\"file\", filename=\"C.csv\", delimiter=\",\")\n\
            .input D(IO=\"file\", filename=\"D.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x, y, z, w, v) :- C(x, z, v), D(z, w), B(x, y), A(x).\n",
        );

        assert_eq!(
            head_form(&pp, "out"),
            "atoms(a, b, c, d) not() eq(0.0 = 1.0 = 2.0; 2.1 = 3.0) where() key() \
             value(0.0, 1.1, 2.1, 3.1, 2.2)"
        );
    }

    /// Two reads of one relation that each keep a column the other drops
    /// ask for two rows; the head form keeps both reads.
    #[test]
    fn self_join_on_different_columns_keeps_both_reads() {
        let pp = ProgramPlanner::analyze(
            "\
            .decl A(x: int32, y: int32, z: int32)\n\
            .decl Out(y: int32, w: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(y, w) :- A(x, y, _), A(x, _, w).\n",
        );

        assert_eq!(
            head_form(&pp, "out"),
            "atoms(a, a) not() eq(0.0 = 1.0) where() key() value(0.1, 1.2)"
        );
    }

    /// The tuple projection `c.0` keys the join from the `Mk` side; the
    /// head spells `v` by the plain column it equals.
    #[test]
    fn tuple_projection_key_is_spelled_by_the_column_it_equals() {
        let pp = ProgramPlanner::analyze(
            "\
            .type P = (a: int32, b: int32)\n\
            .decl Base(a: int32, b: int32)\n\
            .decl Mk(c: P)\n\
            .decl Val(v: int32, t: int32)\n\
            .decl Out(v: int32, t: int32)\n\
            .input Base(IO=\"file\", filename=\"Base.csv\", delimiter=\",\")\n\
            .input Val(IO=\"file\", filename=\"Val.csv\", delimiter=\",\")\n\
            .output Out\n\
            Mk(c) :- Base(a, b), c = (a, b).\n\
            Out(v, t) :- Mk(c), Val(v, t), c = (v, w).\n",
        );

        assert_eq!(
            head_form(&pp, "out"),
            "atoms(mk, val) not() eq() where(1.0 = (0.0).0) key() value(1.0, 1.1)"
        );
    }
}
