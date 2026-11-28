//! Resolves relation names against the declarations: rejects a name
//! declared twice, a malformed lattice declaration, or a lattice column
//! beside a choice domain, attaches each directive to the relation it
//! names, and rejects a rule, fact, or directive naming an undeclared
//! relation. Also resolves each `as()` target type name against the type
//! registry.

use std::collections::HashMap;
use std::collections::HashSet;

use flowlog_common::MAX_EGRAPH_ROUNDS;
use flowlog_common::Span;
use flowlog_common::compute_fp;

use crate::Arithmetic;
use crate::Atom;
use crate::AtomArg;
use crate::DataType;
use crate::Factor;
use crate::Head;
use crate::HeadArg;
use crate::Predicate;
use crate::TypeId;
use crate::TypeRegistry;
use crate::Variable;
use crate::ast::CastTarget;
use crate::ast::FlowLogRule;
use crate::declaration::Attribute;
use crate::declaration::InputDirective;
use crate::declaration::OutputDirective;
use crate::declaration::PrintSizeDirective;
use crate::declaration::Relation;
use crate::error::DirectiveKind;
use crate::error::ParseError;

/// Rejects duplicate names across top-level and component declarations, a
/// malformed lattice declaration, and a lattice column beside a choice
/// domain.
pub(super) fn check_declarations(relations: &[Relation]) -> Result<(), ParseError> {
    let mut spans = HashMap::new();
    for relation in relations {
        if let Some(prior) = spans.insert(relation.name(), relation.span()) {
            return Err(ParseError::DuplicateDecl {
                span: relation.span(),
                prior,
                name: relation.raw_name().to_string(),
            });
        }
        check_lattice(relation)?;
        check_choice(relation)?;
    }
    Ok(())
}

/// Rejects a relation with more than one lattice column, or with one whose
/// type its lattice does not admit.
fn check_lattice(relation: &Relation) -> Result<(), ParseError> {
    let mut lattices = relation
        .attributes()
        .iter()
        .filter_map(|attr| attr.lattice().map(|lattice| (attr, lattice)));
    let Some((first, lattice)) = lattices.next() else {
        return Ok(());
    };
    if lattices.next().is_some() {
        return Err(ParseError::MultipleLatticeColumns {
            span: relation.span(),
            relation: relation.raw_name().to_string(),
        });
    }
    let ty = first.data_type();
    if !lattice.admits(ty) {
        return Err(ParseError::LatticeColumnType {
            span: relation.span(),
            relation: relation.raw_name().to_string(),
            column: first.name().to_string(),
            lattice,
            ty: ty.clone(),
        });
    }
    Ok(())
}

/// Rejects a relation with both a lattice column and a choice domain: each
/// merges the relation's rows, and neither order of the two merges is the
/// evident one.
fn check_choice(relation: &Relation) -> Result<(), ParseError> {
    if relation.choice_domain().is_some() && relation.lattice().is_some() {
        return Err(ParseError::LatticeWithChoiceDomain {
            span: relation.span(),
            relation: relation.raw_name().to_string(),
        });
    }
    Ok(())
}

/// Rejects a malformed `union<T>` column, `eqrel` relation, or `.run`
/// directive, then marks each relation's id columns: those of a type some
/// union column or `eqrel` relation uses.
///
/// `run` is the `.run` directive's rounds and span, if the program has one.
pub(super) fn check_eqsat(
    relations: &mut [Relation],
    registry: &TypeRegistry,
    run: Option<(u32, Span)>,
) -> Result<(), ParseError> {
    let mut merging = HashSet::new();
    for relation in relations.iter() {
        let name = relation.raw_name();
        let error = |detail: String| ParseError::Eqsat {
            span: relation.span(),
            detail,
        };
        let unions: Vec<&Attribute> = relation
            .attributes()
            .iter()
            .filter(|attr| attr.is_union())
            .collect();
        if unions.len() > 1 {
            return Err(error(format!("`{name}` has more than one union column")));
        }
        if !relation.is_eqsat() {
            continue;
        }
        if relation.lattice().is_some() {
            return Err(error(format!(
                "`{name}` merges values, so it cannot also have a lattice column"
            )));
        }
        if relation.choice_domain().is_some() {
            return Err(error(format!(
                "`{name}` merges values, so it cannot also have a choice domain"
            )));
        }
        let merged = if relation.is_eqrel() {
            let [first, second] = relation.attributes() else {
                return Err(error(format!(
                    "`eqrel` relation `{name}` needs two columns, not {}",
                    relation.arity()
                )));
            };
            if !unions.is_empty() {
                return Err(error(format!(
                    "`eqrel` relation `{name}` cannot also have a union column"
                )));
            }
            if first.declared_id() != second.declared_id() {
                return Err(error(format!(
                    "the two columns of `eqrel` relation `{name}` must have one type"
                )));
            }
            first
        } else {
            unions[0]
        };
        let ty = merged.declared_id();
        if !registry.is_subtype(ty) {
            return Err(error(format!(
                "`{name}` merges values of `{}`, which is not a subtype: declare one, \
                 as in `.type T <: {}`",
                registry.name_of(ty),
                merged.data_type()
            )));
        }
        if *merged.data_type() != DataType::Int64 {
            return Err(error(format!(
                "`{name}` merges values of `{}`, whose root type is not `int64`: \
                 a fresh id needs all 64 bits",
                registry.name_of(ty)
            )));
        }
        merging.insert(ty);
    }
    if let Some((rounds, span)) = run {
        if merging.is_empty() {
            return Err(ParseError::Eqsat {
                span,
                detail: "`.run` bounds the loop of `union<T>` and `eqrel` relations, \
                         and the program has none"
                    .to_string(),
            });
        }
        if rounds > MAX_EGRAPH_ROUNDS {
            return Err(ParseError::Eqsat {
                span,
                detail: format!(
                    "`.run {rounds}` asks for more than the {MAX_EGRAPH_ROUNDS} rounds \
                     an e-graph's loop can stamp"
                ),
            });
        }
    }
    for relation in relations.iter_mut() {
        let id_columns = relation
            .attribute_declared_ids()
            .iter()
            .enumerate()
            .filter(|(_, ty)| merging.contains(*ty))
            .map(|(column, _)| column)
            .collect();
        relation.set_id_columns(id_columns);
    }
    Ok(())
}

/// Closes the program's e-graph over its rules, once every directive is
/// applied and every reference resolved.
///
/// Rejects a rule that reads an `eqrel` relation and an `eqrel` relation
/// with an output: it keeps no rows, only the merges they make. Then marks
/// the e-graph's tables: each relation with a union column or `eqrel`, each
/// relation with id columns that a table's rule reads, and each relation
/// with id columns that no rule derives, as each holds ids that merges
/// make stale. Last, gives each table no rule derives the rule
/// `R(x0, ..., xn) :- R(x0, ..., xn).`, so the e-graph's loop holds it and
/// keeps its rows canonical.
pub(super) fn close_egraph(
    relations: &mut [Relation],
    rules: &mut Vec<FlowLogRule>,
) -> Result<(), ParseError> {
    if !relations.iter().any(Relation::is_eqsat) {
        return Ok(());
    }
    let eqrels: HashSet<&str> = relations
        .iter()
        .filter(|relation| relation.is_eqrel())
        .map(Relation::name)
        .collect();
    for atom in rules.iter().flat_map(FlowLogRule::body_atoms) {
        if eqrels.contains(atom.name()) {
            return Err(ParseError::Eqsat {
                span: atom.span(),
                detail: format!(
                    "`{}` is `eqrel`, which keeps no rows to read: read the classes it \
                     merges from the tables that hold them",
                    atom.raw_name()
                ),
            });
        }
    }
    if let Some(relation) = relations
        .iter()
        .find(|relation| relation.is_eqrel() && relation.is_output_printsize())
    {
        return Err(ParseError::Eqsat {
            span: relation.span(),
            detail: format!(
                "`{}` is `eqrel`, which keeps no rows to output",
                relation.raw_name()
            ),
        });
    }

    let holds_ids: HashSet<String> = relations
        .iter()
        .filter(|relation| !relation.id_columns().is_empty())
        .map(|relation| relation.name().to_string())
        .collect();
    let derived: HashSet<String> = rules
        .iter()
        .map(|rule| rule.head().name().to_string())
        .collect();
    let mut tables: HashSet<String> = relations
        .iter()
        .filter(|relation| {
            relation.is_eqsat()
                || (!relation.id_columns().is_empty() && !derived.contains(relation.name()))
        })
        .map(|relation| relation.name().to_string())
        .collect();
    // A table's rule reading ids from a relation makes that relation a
    // table too, so the loop derives it from canonical rows each round.
    loop {
        let read: Vec<String> = rules
            .iter()
            .filter(|rule| tables.contains(rule.head().name()))
            .flat_map(FlowLogRule::body_atoms)
            .map(|atom| atom.name().to_string())
            .filter(|name| holds_ids.contains(name) && !tables.contains(name))
            .collect();
        if read.is_empty() {
            break;
        }
        tables.extend(read);
    }

    for relation in relations.iter_mut() {
        if !tables.contains(relation.name()) {
            continue;
        }
        relation.set_in_egraph();
        if !derived.contains(relation.name()) {
            rules.push(identity_rule(relation));
        }
    }
    Ok(())
}

/// The rule `R(x0, ..., xn) :- R(x0, ..., xn).` of relation `R`, which
/// derives each row of `R` from itself.
fn identity_rule(relation: &Relation) -> FlowLogRule {
    let columns: Vec<Variable> = (0..relation.arity())
        .map(|column| Variable::Named(format!("x{column}")))
        .collect();
    let head = Head::new(
        relation.name().to_string(),
        columns.iter().cloned().map(HeadArg::Var).collect(),
    );
    let atom = Atom::new(
        relation.name(),
        columns.into_iter().map(AtomArg::Var).collect(),
        compute_fp(relation.name()),
    );
    FlowLogRule::new(head, vec![Predicate::PositiveAtom(atom)])
}

/// Applies `.input`, `.output`, and `.printsize` directives to `relations`.
///
/// Errors if a directive names a relation with no corresponding `.decl`, or if
/// two directives of the same kind name the same relation.
pub(super) fn apply_directives(
    relations: &mut [Relation],
    input_directives: Vec<InputDirective>,
    output_directives: Vec<OutputDirective>,
    printsize_directives: Vec<PrintSizeDirective>,
) -> Result<(), ParseError> {
    check_duplicate_directives(
        &input_directives,
        DirectiveKind::Input,
        |d| d.relation_name(),
        |d| d.span(),
    )?;
    check_duplicate_directives(
        &output_directives,
        DirectiveKind::Output,
        |d| d.relation_name(),
        |d| d.span(),
    )?;
    check_duplicate_directives(
        &printsize_directives,
        DirectiveKind::PrintSize,
        |d| d.relation_name(),
        |d| d.span(),
    )?;

    for d in input_directives {
        match relations.iter_mut().find(|r| r.name() == d.relation_name()) {
            Some(rel) => {
                // Tuples are constructed by rules, never read from EDB
                // facts: reject `.input` on a tuple-column relation here
                // rather than panicking the fact-reader codegen later.
                if rel.data_type().iter().any(|dt| dt.is_tuple()) {
                    return Err(ParseError::TupleInInput {
                        span: d.span(),
                        name: rel.raw_name().to_string(),
                    });
                }
                rel.set_input(d.parameters(), d.span())?;
            }
            None => {
                return Err(ParseError::UndeclaredInDirective {
                    span: d.span(),
                    kind: DirectiveKind::Input,
                    name: d.relation_name().to_string(),
                });
            }
        }
    }
    for d in output_directives {
        match relations.iter_mut().find(|r| r.name() == d.relation_name()) {
            Some(rel) => rel.set_output(d.parameters(), d.span())?,
            None => {
                return Err(ParseError::UndeclaredInDirective {
                    span: d.span(),
                    kind: DirectiveKind::Output,
                    name: d.relation_name().to_string(),
                });
            }
        }
    }
    for d in printsize_directives {
        match relations.iter_mut().find(|r| r.name() == d.relation_name()) {
            Some(rel) => rel.set_printsize(true),
            None => {
                return Err(ParseError::UndeclaredInDirective {
                    span: d.span(),
                    kind: DirectiveKind::PrintSize,
                    name: d.relation_name().to_string(),
                });
            }
        }
    }
    Ok(())
}

/// Rejects a relation name repeated within one directive kind, reporting the
/// later occurrence against the first.
fn check_duplicate_directives<T>(
    dirs: &[T],
    kind: DirectiveKind,
    name_of: impl Fn(&T) -> &str,
    get_span: impl Fn(&T) -> Span,
) -> Result<(), ParseError> {
    let mut seen: HashMap<&str, Span> = HashMap::new();
    for d in dirs {
        let name = name_of(d);
        let span = get_span(d);
        if let Some(prior) = seen.get(name) {
            return Err(ParseError::DuplicateDirective {
                span,
                prior: *prior,
                kind,
                name: name.to_string(),
            });
        }
        seen.insert(name, span);
    }
    Ok(())
}

/// Rejects any rule head, body atom, or ground fact whose relation
/// name has no matching `.decl`. Mirrors the check directives already
/// do via [`ParseError::UndeclaredInDirective`]; covering the rule and
/// fact paths here lets later stages assume every reference is
/// declared.
pub(super) fn check_references(
    relations: &[Relation],
    rules: &[FlowLogRule],
    raw_facts: &[FlowLogRule],
) -> Result<(), ParseError> {
    let declared: HashSet<&str> = relations.iter().map(|r| r.name()).collect();

    for rule in rules {
        let head = rule.head();
        if !declared.contains(head.name()) {
            return Err(ParseError::UndeclaredInRule {
                span: head.span(),
                name: head.raw_name().to_string(),
            });
        }
        for atom in rule.body_atoms() {
            if !declared.contains(atom.name()) {
                return Err(ParseError::UndeclaredInRule {
                    span: atom.span(),
                    name: atom.raw_name().to_string(),
                });
            }
        }
    }

    // Validate raw facts before folding them into the map keyed by canonical
    // name: their heads still carry the user's spelling for diagnostics.
    for fact in raw_facts {
        let head = fact.head();
        if !declared.contains(head.name()) {
            return Err(ParseError::UndeclaredInFact {
                span: head.span(),
                name: head.raw_name().to_string(),
            });
        }
    }

    Ok(())
}

/// Resolves the target type of every `as()` in `rules`. A fact's head holds
/// only constants, so facts have no casts to resolve.
///
/// Errors with [`ParseError::UnknownCastType`] on a target no `.type` or
/// primitive declares.
pub(super) fn resolve_casts(
    rules: &mut [FlowLogRule],
    registry: &TypeRegistry,
) -> Result<(), ParseError> {
    for rule in rules {
        resolve_predicate_casts(rule.rhs_mut(), registry)?;
        for arg in rule.head_mut().head_arguments_mut() {
            match arg {
                HeadArg::Var(_) => {}
                HeadArg::Arith(a) => resolve_arith_casts(a, registry)?,
            }
        }
    }
    Ok(())
}

fn resolve_predicate_casts(
    predicates: &mut [Predicate],
    registry: &TypeRegistry,
) -> Result<(), ParseError> {
    for predicate in predicates {
        match predicate {
            Predicate::PositiveAtom(_) | Predicate::NegativeAtom(_) => {}
            Predicate::Compare(cmp) => {
                resolve_arith_casts(cmp.left_mut(), registry)?;
                resolve_arith_casts(cmp.right_mut(), registry)?;
            }
            Predicate::Aggregate(aggregate) => {
                if let Some(target) = aggregate.target_mut() {
                    resolve_arith_casts(target, registry)?;
                }
                resolve_predicate_casts(aggregate.body_mut(), registry)?;
            }
        }
    }
    Ok(())
}

fn resolve_arith_casts(a: &mut Arithmetic, registry: &TypeRegistry) -> Result<(), ParseError> {
    for f in a.factors_mut() {
        resolve_factor_casts(f, registry)?;
    }
    Ok(())
}

/// Resolves the type `id` as a cast's target, with the target of each
/// field when it is a tuple type.
fn cast_target(id: TypeId, registry: &TypeRegistry) -> CastTarget {
    let fields = registry
        .tuple_field_ids(id)
        .unwrap_or_default()
        .iter()
        .map(|&field| cast_target(field, registry))
        .collect();
    CastTarget::new(
        id,
        registry.name_of(id).to_string(),
        registry.root_primitive(id),
        fields,
    )
}

fn resolve_factor_casts(f: &mut Factor, registry: &TypeRegistry) -> Result<(), ParseError> {
    match f {
        // Rule construction hoisted every aggregate into a binding and named
        // every placeholder.
        Factor::Var(_) | Factor::Const(_) | Factor::Placeholder | Factor::Aggregate(_) => Ok(()),
        Factor::FnCall(fc) => fc
            .args_mut()
            .iter_mut()
            .try_for_each(|a| resolve_arith_casts(a, registry)),
        Factor::Builtin(bc) => bc
            .args_mut()
            .iter_mut()
            .try_for_each(|a| resolve_arith_casts(a, registry)),
        Factor::Cast(c) => {
            let id =
                registry
                    .lookup(c.target_type())
                    .ok_or_else(|| ParseError::UnknownCastType {
                        span: c.span(),
                        name: c.target_type().to_string(),
                    })?;
            c.set_target(cast_target(id, registry));
            resolve_factor_casts(c.inner_mut(), registry)
        }
        Factor::Group(a) => resolve_arith_casts(a, registry),
        Factor::Tuple(fields) => fields
            .iter_mut()
            .try_for_each(|a| resolve_arith_casts(a, registry)),
        Factor::TupleProj { tuple, .. } => resolve_arith_casts(tuple, registry),
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::DataType;
    use crate::Lattice;
    use crate::assert_err;
    use crate::ast::Atom;
    use crate::ast::Head;
    use crate::ast::Predicate;
    use crate::test_harness::assembled;

    // --- Declarations ---

    /// Shared tail: an input and an output so assembly gets this far.
    const IO: &str = ".decl In(x: int32) .input In .output R\n";

    #[test]
    fn a_lattice_relation_with_one_ordered_column_is_accepted() {
        assembled(&format!("{IO}.decl R(k: symbol, v: max<symbol>)")).unwrap();
    }

    #[test]
    fn two_lattice_columns_are_rejected() {
        assert_err!(
            assembled(&format!("{IO}.decl R(a: min<int32>, b: max<int32>)")),
            ParseError::MultipleLatticeColumns { relation, .. } if relation == "R"
        );
    }

    /// The error names the column and the lattice it was declared with.
    // Cases: lattice, column type.
    #[rstest]
    #[case(Lattice::Min, "bool")]
    #[case(Lattice::Min, "(a: int32, b: int32)")]
    #[case(Lattice::Max, "bool")]
    #[case(Lattice::Max, "(a: int32, b: int32)")]
    fn a_lattice_over_an_unordered_type_is_rejected(#[case] lattice: Lattice, #[case] ty: &str) {
        let src = format!("{IO}.type T = {ty}\n.decl R(k: int32, v: {lattice}<T>)");
        assert_err!(
            assembled(&src),
            ParseError::LatticeColumnType { column, lattice: declared, .. }
                if column == "v" && *declared == lattice
        );
    }

    #[test]
    fn a_lattice_column_beside_a_choice_domain_is_rejected() {
        assert_err!(
            assembled(&format!("{IO}.decl R(k: int32, v: min<int32>) choice-domain k")),
            ParseError::LatticeWithChoiceDomain { relation, .. } if relation == "R"
        );
    }

    // --- Directives ---

    #[test]
    fn a_relation_named_twice_by_one_directive_kind_is_rejected() {
        // Generic over the directive type: a `&str` slice + trivial accessors
        // exercise the dedup directly: no directive structs needed.
        assert_err!(
            check_duplicate_directives(
                &["edge", "path", "edge"],
                DirectiveKind::Output,
                |s| *s,
                |_| Span::DUMMY,
            ),
            ParseError::DuplicateDirective {
                kind: DirectiveKind::Output,
                ..
            }
        );
    }

    #[test]
    fn directives_naming_distinct_relations_are_accepted() {
        assert!(
            check_duplicate_directives(
                &["edge", "path", "node"],
                DirectiveKind::Output,
                |s| *s,
                |_| Span::DUMMY,
            )
            .is_ok()
        );
    }

    #[test]
    fn a_directive_on_an_undeclared_relation_is_rejected() {
        // `.output missing_rel` with no matching `.decl`.
        let output = OutputDirective::new("missing_rel".to_string(), HashMap::new(), Span::DUMMY);
        assert_err!(
            apply_directives(&mut [], vec![], vec![output], vec![]),
            ParseError::UndeclaredInDirective { .. }
        );
    }

    /// `.input` on a relation with a tuple column is rejected: tuples are built
    /// by rules, never read from EDB files, so the fact-reader codegen never
    /// meets one. A clean assemble-time error, not a codegen panic.
    #[test]
    fn input_on_a_tuple_column_is_rejected() {
        let src = "\
            .type Pair = ( a: symbol, b: symbol )\n\
            .decl In(p: Pair)\n\
            .decl Out(p: Pair)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(p) :- In(p).\n";
        assert_err!(assembled(src), ParseError::TupleInInput { .. });
    }

    // --- References ---

    /// Build `head(...) :- body_atoms...`. Names are already lowercase, so
    /// canonicalization is a no-op.
    fn rule(head: &str, body_atoms: &[&str]) -> FlowLogRule {
        let rhs = body_atoms
            .iter()
            .map(|n| Predicate::PositiveAtom(Atom::new(n, vec![], 0)))
            .collect();
        FlowLogRule::new(Head::new(head.to_string(), vec![]), rhs)
    }

    #[test]
    fn an_undeclared_body_atom_is_rejected() {
        let relations = vec![Relation::new("foo", vec![])]; // only `foo` declared
        let rules = vec![rule("foo", &["ghost"])];
        assert_err!(
            check_references(&relations, &rules, &[]),
            ParseError::UndeclaredInRule { .. }
        );
    }

    #[test]
    fn an_undeclared_fact_is_rejected() {
        let relations = vec![Relation::new("foo", vec![])];
        assert_err!(
            check_references(&relations, &[], &[rule("ghost", &[])]),
            ParseError::UndeclaredInFact { .. }
        );
    }

    #[test]
    fn a_fully_declared_program_is_accepted() {
        let relations = vec![Relation::new("foo", vec![]), Relation::new("bar", vec![])];
        let rules = vec![rule("foo", &["bar"])];
        assert!(check_references(&relations, &rules, &[]).is_ok());
    }

    /// Returns a program of one rule, `rule`, over an `int32` input `K`, with
    /// `Big`, a subtype of `int64`, and a UDF `f` declared.
    fn program_with_rule(rule: &str) -> String {
        format!(
            ".type Big <: int64\n\
             .extern fn f(a: int32) -> int32\n\
             .decl K(x: int32)\n\
             .decl O(x: int32)\n\
             .input K(IO=\"file\", filename=\"K.csv\", delimiter=\",\")\n\
             .output O\n\
             {rule}\n"
        )
    }

    /// A cast records its target's registry identity and the primitive that
    /// identity erases to: a subtype of `int64` keeps its own identity but
    /// erases to `int64`.
    #[test]
    fn cast_records_its_resolved_target() {
        let program = assembled(&program_with_rule("O(x) :- K(x), as(x, Big) > 0.")).unwrap();
        let Predicate::Compare(cmp) = &program.rules()[0].rhs()[1] else {
            panic!("expected the comparison second");
        };
        let Factor::Cast(cast) = cmp.left().init() else {
            panic!("expected a cast, got {}", cmp.left());
        };
        let big = program.type_registry.lookup("Big").unwrap();
        assert_eq!(
            cast.target().unwrap(),
            &CastTarget::new(big, "big".to_string(), DataType::Int64, Vec::new())
        );
    }

    /// Every position a cast can take is resolved: an undeclared target in
    /// any of them is reported, not left for type checking to trip over.
    // Cases: rule.
    #[rstest]
    #[case::head("O(as(x, Nope)) :- K(x).")]
    #[case::comparison("O(x) :- K(x), as(x, Nope) > 0.")]
    #[case::udf_argument("O(f(as(x, Nope))) :- K(x).")]
    #[case::nested_cast("O(as(as(x, Nope), int32)) :- K(x).")]
    #[case::aggregate_target("O(n) :- K(_), n = sum as(y, Nope) : { K(y) }.")]
    #[case::aggregate_body("O(n) :- K(_), n = count : { K(y), as(y, Nope) > 0 }.")]
    #[case::over_a_tuple_taken_apart("O(a) :- K(x), p = as((x,), Nope), p = (a,).")]
    fn cast_to_an_undeclared_type_is_rejected(#[case] rule: &str) {
        assert_err!(
            assembled(&program_with_rule(rule)),
            ParseError::UnknownCastType { name, .. } if name == "Nope"
        );
    }

    // --- E-graphs ---

    /// The type whose values the e-graph tests' relations merge.
    const ID: &str = ".type Id <: int64\n";

    /// The relation `name` of `program`.
    fn relation<'a>(program: &'a crate::Program, name: &str) -> &'a Relation {
        program
            .relations()
            .iter()
            .find(|relation| relation.name() == name)
            .unwrap_or_else(|| panic!("`{name}` is declared"))
    }

    /// Every column of a type some relation merges is an id column, in any
    /// relation; a column of another type is not, though it erases to the
    /// same `int64`.
    // Cases: relation, id columns.
    #[rstest]
    #[case::union_column("num", vec![1])]
    #[case::eqrel("same", vec![0, 1])]
    #[case::reading_the_ids("out", vec![2])]
    fn the_columns_of_a_merged_type_are_id_columns(
        #[case] name: &str,
        #[case] expected: Vec<usize>,
    ) {
        let program = assembled(&format!(
            "{ID}.type Other <: int64\n\
             .decl Num(n: int64, e: union<Id>)\n\
             .decl Same(a: Id, b: Id) eqrel\n\
             .decl Out(n: int64, o: Other, e: Id)\n.output Out\n\
             Out(n, n, e) :- Num(n, e).\n"
        ))
        .unwrap();
        assert_eq!(relation(&program, name).id_columns(), expected);
    }

    // Cases: declarations, detail.
    #[rstest]
    #[case::two_union_columns(
        ".decl R(a: union<Id>, b: union<Id>)",
        "`R` has more than one union column"
    )]
    #[case::union_beside_a_lattice(
        ".decl R(m: min<int64>, e: union<Id>)",
        "`R` merges values, so it cannot also have a lattice column"
    )]
    #[case::union_beside_a_choice_domain(
        ".decl R(a: int64, e: union<Id>) choice-domain a",
        "`R` merges values, so it cannot also have a choice domain"
    )]
    #[case::eqrel_of_three_columns(
        ".decl R(a: Id, b: Id, c: Id) eqrel",
        "`eqrel` relation `R` needs two columns, not 3"
    )]
    #[case::eqrel_with_a_union_column(
        ".decl R(a: Id, b: union<Id>) eqrel",
        "`eqrel` relation `R` cannot also have a union column"
    )]
    #[case::eqrel_over_two_types(
        ".type J <: int64\n.decl R(a: Id, b: J) eqrel",
        "the two columns of `eqrel` relation `R` must have one type"
    )]
    #[case::merging_a_primitive(
        ".decl R(e: union<int64>)",
        "`R` merges values of `int64`, which is not a subtype: declare one, as in `.type T <: int64`"
    )]
    #[case::merging_a_narrow_subtype(
        ".type Small <: int32\n.decl R(e: union<Small>)",
        "`R` merges values of `small`, whose root type is not `int64`: a fresh id needs all 64 bits"
    )]
    fn a_malformed_merging_relation_is_rejected(#[case] decls: &str, #[case] expected: &str) {
        assert_err!(
            assembled(&format!("{ID}{decls}\n")),
            ParseError::Eqsat { detail, .. } if detail == expected
        );
    }

    /// `.run` bounds the e-graph's loop by its rounds, up to the most a loop
    /// can stamp; without it, the loop runs to its fixpoint.
    // Cases: directive, rounds.
    #[rstest]
    #[case::absent("", None)]
    #[case::one(".run 1", Some(1))]
    #[case::the_most_a_loop_stamps(".run 16382", Some(16382))]
    fn run_records_its_rounds(#[case] directive: &str, #[case] expected: Option<u32>) {
        let program = assembled(&format!(
            "{ID}.decl Num(n: int64, e: union<Id>)\n.output Num\n{directive}\n"
        ))
        .unwrap();
        assert_eq!(program.run_rounds(), expected);
    }

    // Cases: program, detail.
    #[rstest]
    #[case::without_an_egraph(
        ".decl R(x: int64)\n.run 3",
        "`.run` bounds the loop of `union<T>` and `eqrel` relations, and the program has none"
    )]
    #[case::past_what_a_loop_stamps(
        ".decl Num(n: int64, e: union<Id>)\n.run 16383",
        "`.run 16383` asks for more than the 16382 rounds an e-graph's loop can stamp"
    )]
    #[case::past_a_u32(
        ".decl Num(n: int64, e: union<Id>)\n.run 4294967296",
        "`.run 4294967296` asks for too many rounds"
    )]
    #[case::twice(
        ".decl Num(n: int64, e: union<Id>)\n.run 2\n.run 3",
        "a program has at most one `.run` directive"
    )]
    fn a_malformed_run_directive_is_rejected(#[case] program: &str, #[case] expected: &str) {
        assert_err!(
            assembled(&format!("{ID}{program}\n")),
            ParseError::Eqsat { detail, .. } if detail == expected
        );
    }

    /// An `eqrel` relation keeps only the merges its rows make, so a rule
    /// cannot read it and an output cannot write it.
    // Cases: program, detail.
    #[rstest]
    #[case::read_by_a_rule(
        ".decl Out(a: Id)\n.output Out\nOut(a) :- Same(a, _).",
        "`Same` is `eqrel`, which keeps no rows to read: read the classes it merges from the \
         tables that hold them"
    )]
    #[case::output(".output Same", "`Same` is `eqrel`, which keeps no rows to output")]
    fn an_eqrel_relation_cannot_be_read(#[case] program: &str, #[case] expected: &str) {
        assert_err!(
            assembled(&format!(
                "{ID}.decl Same(a: Id, b: Id) eqrel\n\
                 .decl Num(n: int64, e: union<Id>)\n\
                 Same(a, b) :- Num(1, a), Num(2, b).\n{program}\n"
            )),
            ParseError::Eqsat { detail, .. } if detail == expected
        );
    }

    /// The e-graph's tables are its merging relations, each relation with
    /// ids that no rule derives, and each a table's rule reads ids from. A
    /// relation that only reads the tables' ids, or holds none, is not one.
    // Cases: relation, in the e-graph.
    #[rstest]
    #[case::union_column("num", true)]
    #[case::eqrel("same", true)]
    #[case::ids_no_rule_derives("leaf", true)]
    #[case::ids_a_tables_rule_reads("seed", true)]
    #[case::ids_read_from_the_tables("out", false)]
    #[case::no_ids("plain", false)]
    fn the_egraph_closes_over_the_ids_its_rules_read(#[case] name: &str, #[case] expected: bool) {
        let program = assembled(&format!(
            "{ID}.decl Num(n: int64, e: union<Id>)\n\
             .decl Same(a: Id, b: Id) eqrel\n\
             .decl Leaf(e: Id)\nLeaf(1).\n\
             .decl Plain(n: int64)\nPlain(1).\n\
             .decl Seed(n: int64, e: Id)\n\
             .decl Out(e: Id)\n.output Out\n\
             Seed(n, e) :- Plain(n), Leaf(e).\n\
             Num(n, e) :- Seed(n, e).\n\
             Same(a, b) :- Num(_, a), Leaf(b).\n\
             Out(e) :- Num(_, e).\n"
        ))
        .unwrap();
        assert_eq!(relation(&program, name).in_egraph(), expected);
    }

    /// A table no rule derives gets the rule that derives each of its rows
    /// from itself, so the loop holds and renames its rows; a table that a
    /// rule derives gets none.
    #[test]
    fn a_table_no_rule_derives_derives_itself() {
        let program = assembled(&format!(
            "{ID}.decl Leaf(e: Id)\nLeaf(1).\n\
             .decl Twin(a: Id, e: union<Id>)\n.output Twin\n\
             Twin(e, e) :- Leaf(e).\n"
        ))
        .unwrap();
        let rules: Vec<String> = program.rules().iter().map(ToString::to_string).collect();
        assert_eq!(
            rules,
            vec!["twin(e, e) :- Leaf(e).", "leaf(x0) :- leaf(x0)."]
        );
    }
}
