//! Apply the `.input` / `.output` / `.printsize` directives to their relations,
//! and validate the assembled program's references and directives.

use std::collections::HashMap;
use std::collections::HashSet;

use flowlog_common::Span;

use crate::ast::FlowLogRule;
use crate::ast::Predicate;
use crate::declaration::InputDirective;
use crate::declaration::OutputDirective;
use crate::declaration::PrintSizeDirective;
use crate::declaration::Relation;
use crate::error::DirectiveKind;
use crate::error::ParseError;

/// Reject any rule head, body atom, or ground fact whose relation
/// name has no matching `.decl`. Mirrors the check directives already
/// do via [`ParseError::UndeclaredInDirective`]; covering the rule and
/// fact paths here lets later stages assume every reference is
/// declared.
pub(super) fn validate_relation_references(
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
        for pred in rule.rhs() {
            if let Predicate::PositiveAtom(atom) | Predicate::NegativeAtom(atom) = pred
                && !declared.contains(atom.name())
            {
                return Err(ParseError::UndeclaredInRule {
                    span: atom.span(),
                    name: atom.raw_name().to_string(),
                });
            }
        }
    }

    // Validate the raw facts here, before `extract_fact` folds them into
    // the `facts` map (which is keyed by canonical name only): the head
    // still carries the user's original spelling for the diagnostic.
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

/// Reject a relation name repeated within one directive kind, reporting the
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

/// Apply `.input`, `.output`, and `.printsize` directives to `relations`.
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

/// Reject `.output R` and `.printsize R` on the same relation: both target
/// `<RawName>.csv`, so the second would silently clobber the first. Checked
/// after the directives are applied, so it catches the conflict whether they
/// came from the top level, from inside a `.comp`, or one of each.
pub(super) fn validate_output_printsize_exclusion(
    relations: &[Relation],
) -> Result<(), ParseError> {
    for rel in relations {
        if rel.has_output() && rel.printsize() {
            return Err(ParseError::OutputAndPrintsizeConflict {
                span: rel.span(),
                name: rel.raw_name().to_string(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_err;
    use crate::ast::Atom;
    use crate::ast::Head;
    use crate::test_util::assembled;

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
    fn validate_relation_references_rejects_undeclared_body_atom() {
        let relations = vec![Relation::new("foo", vec![])]; // only `foo` declared
        let rules = vec![rule("foo", &["ghost"])];
        assert_err!(
            validate_relation_references(&relations, &rules, &[]),
            ParseError::UndeclaredInRule { .. }
        );
    }

    #[test]
    fn validate_relation_references_rejects_undeclared_fact() {
        let relations = vec![Relation::new("foo", vec![])];
        assert_err!(
            validate_relation_references(&relations, &[], &[rule("ghost", &[])]),
            ParseError::UndeclaredInFact { .. }
        );
    }

    #[test]
    fn validate_relation_references_accepts_a_fully_declared_program() {
        let relations = vec![Relation::new("foo", vec![]), Relation::new("bar", vec![])];
        let rules = vec![rule("foo", &["bar"])];
        assert!(validate_relation_references(&relations, &rules, &[]).is_ok());
    }

    #[test]
    fn check_duplicate_directives_rejects_a_repeated_name() {
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
    fn check_duplicate_directives_accepts_distinct_names() {
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
    fn apply_directives_rejects_directive_on_undeclared_relation() {
        // `.output missing_rel` with no matching `.decl`.
        let output = OutputDirective::new("missing_rel".to_string(), HashMap::new(), Span::DUMMY);
        assert_err!(
            apply_directives(&mut [], vec![], vec![output], vec![]),
            ParseError::UndeclaredInDirective { .. }
        );
    }

    #[test]
    fn validate_output_printsize_exclusion_rejects_both_on_one_relation() {
        // `.output R` + `.printsize R` both target `R.csv`: a conflict.
        let mut r = Relation::new("r", vec![]);
        r.set_output(&HashMap::new(), Span::DUMMY).unwrap();
        r.set_printsize(true);
        assert_err!(
            validate_output_printsize_exclusion(&[r]),
            ParseError::OutputAndPrintsizeConflict { .. }
        );
    }

    /// `.input` on a relation with a tuple column is rejected: tuples are built
    /// by rules, never read from EDB files, so the fact-reader codegen never
    /// meets one. A clean assemble-time error, not a codegen panic.
    #[test]
    fn input_on_tuple_column_rejected() {
        let src = "\
            .type Pair = ( a: symbol, b: symbol )\n\
            .decl In(p: Pair)\n\
            .decl Out(p: Pair)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(p) :- In(p).\n";
        assert_err!(assembled(src), ParseError::TupleInInput { .. });
    }
}
