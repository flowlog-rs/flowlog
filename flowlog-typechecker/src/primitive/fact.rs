//! Check and pin declared fact tuples against their `.decl`.

use std::collections::HashMap;

use flowlog_parser::DataType;
use flowlog_parser::InlineFact;

use crate::TypeCheckError;
use crate::env::PrimitiveEnv;
use crate::primitive::ty::LitKind;

/// Validate each declared fact tuple's column families against its `.decl`
/// and pin polymorphic literals. Diagnostics cite the fact's head span.
pub(crate) fn check_and_pin_facts(
    facts: &mut HashMap<String, Vec<InlineFact>>,
    env: &PrimitiveEnv,
) -> Result<(), TypeCheckError> {
    for (rel_name, entries) in facts.iter_mut() {
        let Some(col_types) = env.decls.get(rel_name) else {
            return Err(TypeCheckError::internal(format!(
                "fact references undeclared relation `{rel_name}`"
            )));
        };
        for fact in entries.iter_mut() {
            check_and_pin_fact(fact, col_types)?;
        }
    }
    Ok(())
}

/// Check one fact tuple against its relation's declared column types and pin
/// each polymorphic literal in place.
fn check_and_pin_fact(fact: &mut InlineFact, col_types: &[DataType]) -> Result<(), TypeCheckError> {
    if fact.columns.len() != col_types.len() {
        return Err(TypeCheckError::HeadArity {
            span: fact.span,
            rel: fact.raw_name.clone(),
            expected: col_types.len(),
            found: fact.columns.len(),
        });
    }
    for (c, col_ty) in fact.columns.iter_mut().zip(col_types.iter()) {
        if !LitKind::of(c)?.fits(col_ty) {
            return Err(TypeCheckError::LiteralColumnMismatch {
                span: fact.span,
                literal: c.to_string(),
                expected: col_ty.clone(),
            });
        }
        if c.is_polymorphic() {
            c.pin(col_ty.clone());
        }
    }
    Ok(())
}
