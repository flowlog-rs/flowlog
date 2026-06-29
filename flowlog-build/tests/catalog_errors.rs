mod errors;

use errors::fixture;
use errors::render;
use flowlog_build::catalog::Catalog;
use flowlog_build::catalog::CatalogError;
use flowlog_common::SourceMap;
use flowlog_parser::Program;

/// Parse `name` and build a catalog for each rule. Returns the first
/// catalog error encountered (or `Ok(())` if none).
fn catalog_for(name: &str) -> (Result<(), CatalogError>, SourceMap) {
    let mut sm = SourceMap::new();
    let program = Program::parse(&fixture("catalog", name), false, &[], &mut sm)
        .expect("fixture should parse cleanly");
    let mut result = Ok(());
    for rule in program.rules() {
        if let Err(e) = Catalog::from_rule(rule) {
            result = Err(e);
            break;
        }
    }
    (result, sm)
}

#[test]
fn unsafe_variable_in_negation() {
    assert_err!(
        catalog_for("unsafe_variable_in_negation.dl"),
        CatalogError::UnsafeVariable { ref var, ref predicate, .. }
            if var == "other" && predicate.contains("blocked"),
        [
            "unsafe variable",
            "`other`",
            "negated atom",
            "unsafe_variable_in_negation.dl",
        ]
    );
}

#[test]
fn unsafe_variable_in_comparison() {
    assert_err!(
        catalog_for("unsafe_variable_in_comparison.dl"),
        CatalogError::UnsafeVariable { ref var, ref predicate, .. }
            if var == "z" && predicate.contains("z"),
        [
            "unsafe variable",
            "`z`",
            "comparison",
            "unsafe_variable_in_comparison.dl",
        ]
    );
}

#[test]
fn unsafe_variable_in_fn_call() {
    // UDFs are value-only, so a UDF filter is a comparison (`f(z) = True`).
    // An unbound var inside it is reported through the comparison predicate.
    assert_err!(
        catalog_for("unsafe_variable_in_fn_call.dl"),
        CatalogError::UnsafeVariable { ref var, ref predicate, .. }
            if var == "z" && predicate.contains("is_positive"),
        [
            "unsafe variable",
            "`z`",
            "comparison",
            "unsafe_variable_in_fn_call.dl",
        ]
    );
}
