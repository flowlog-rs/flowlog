//! Check and pin declared fact tuples against their `.decl`.

use std::collections::HashMap;

use crate::InlineFact;
use crate::ParseError;
use crate::error::grammar_bug;
use crate::pipeline::typecheck::env::PrimitiveEnv;

/// Validate each declared fact tuple's column families against its `.decl`
/// and pin polymorphic literals. Diagnostics cite the fact's head span.
pub(super) fn check_and_pin_facts(
    facts: &mut HashMap<String, Vec<InlineFact>>,
    env: &PrimitiveEnv,
) -> Result<(), ParseError> {
    for (rel_name, entries) in facts.iter_mut() {
        let Some(col_types) = env.decls.get(rel_name) else {
            return Err(grammar_bug(format!(
                "fact references undeclared relation `{rel_name}`"
            )));
        };
        for fact in entries.iter_mut() {
            if fact.columns.len() != col_types.len() {
                return Err(ParseError::HeadArity {
                    span: fact.span,
                    rel: fact.raw_name.clone(),
                    expected: col_types.len(),
                    found: fact.columns.len(),
                });
            }
            for (c, col_ty) in fact.columns.iter_mut().zip(col_types.iter()) {
                if !c.ty().fits(col_ty) {
                    return Err(ParseError::LiteralColumnMismatch {
                        span: fact.span,
                        literal: c.to_string(),
                        expected: col_ty.clone(),
                    });
                }
                if c.is_polymorphic() {
                    c.pin(col_ty.clone(), fact.span)?;
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::Constant;
    use crate::DataType;
    use crate::test_util::checked;

    /// Fact tuple literal: `P(5)` with `.decl P(x: uint64)` must pin via
    /// `check_and_pin_facts`. A separate path from rule-body pinning; a
    /// regression here would leak polymorphic literals into `program.facts()`
    /// even when every rule literal is concrete.
    #[test]
    fn fact_tuple_const_pinned_to_declared_column_width() {
        let src = "\
            .decl P(x: uint64)\n\
            .decl Out(x: uint64)\n\
            .output Out\n\
            P(5).\n\
            Out(x) :- P(x).\n";
        let program = checked(src).expect("type-check should succeed");
        let p_facts = program.facts().get("p").expect("p facts");
        assert_eq!(p_facts[0].columns[0], Constant::new(DataType::UInt64, "5"));
    }
}
