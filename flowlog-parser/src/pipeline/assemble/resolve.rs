//! Resolves relation names against the declarations: rejects a name
//! declared twice or a malformed lattice declaration, attaches each
//! directive to the relation it names, and rejects a rule, fact, or
//! directive naming an undeclared relation. Also resolves each `as()`
//! target type name against the type registry.

use std::collections::HashMap;
use std::collections::HashSet;

use flowlog_common::Span;

use crate::Arithmetic;
use crate::DataType;
use crate::Factor;
use crate::HeadArg;
use crate::Predicate;
use crate::TypeRegistry;
use crate::ast::CastTarget;
use crate::ast::FlowLogRule;
use crate::declaration::InputDirective;
use crate::declaration::OutputDirective;
use crate::declaration::PrintSizeDirective;
use crate::declaration::Relation;
use crate::error::DirectiveKind;
use crate::error::ParseError;

/// Rejects duplicate names across top-level and component declarations, and
/// a malformed lattice declaration.
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
    }
    Ok(())
}

/// Rejects a relation with more than one lattice column, or with one whose
/// type has no order for `min` and `max` to pick from.
fn check_lattice(relation: &Relation) -> Result<(), ParseError> {
    let mut lattices = relation
        .attributes()
        .iter()
        .filter(|attr| attr.lattice().is_some());
    let Some(first) = lattices.next() else {
        return Ok(());
    };
    if lattices.next().is_some() {
        return Err(ParseError::MultipleLatticeColumns {
            span: relation.span(),
            relation: relation.raw_name().to_string(),
        });
    }
    let ty = first.data_type();
    if !(ty.is_numeric() || matches!(ty, DataType::String)) {
        return Err(ParseError::LatticeColumnType {
            span: relation.span(),
            relation: relation.raw_name().to_string(),
            column: first.name().to_string(),
            ty: ty.clone(),
        });
    }
    Ok(())
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
            c.set_target(CastTarget::new(id, registry.root_primitive(id)));
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

    // Cases: column type.
    #[rstest]
    #[case("bool")]
    #[case("(a: int32, b: int32)")]
    fn a_lattice_over_an_unordered_type_is_rejected(#[case] ty: &str) {
        let src = format!("{IO}.type T = {ty}\n.decl R(k: int32, v: min<T>)");
        assert_err!(
            assembled(&src),
            ParseError::LatticeColumnType { column, .. } if column == "v"
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
            &CastTarget::new(big, DataType::Int64)
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
    fn cast_to_an_undeclared_type_is_rejected(#[case] rule: &str) {
        assert_err!(
            assembled(&program_with_rule(rule)),
            ParseError::UnknownCastType { name, .. } if name == "Nope"
        );
    }
}
