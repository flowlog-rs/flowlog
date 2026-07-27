//! Catalog error diagnostics, driven end-to-end from `.dl` fixtures under
//! `tests/errors/catalog/`.

use flowlog_common::BoxError;
use flowlog_common::Config;
use flowlog_common::SourceMap;
use flowlog_common::emit;
use flowlog_planner::catalog::Catalog;
use flowlog_planner::catalog::CatalogError;

/// Absolute path of the fixture `tests/errors/catalog/<name>`.
fn fixture(name: &str) -> String {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("errors")
        .join("catalog")
        .join(name);
    path.into_os_string().into_string().unwrap()
}

/// Render `err` against `sm` the way the compiler would print it.
fn render<E: Into<BoxError>>(err: E, sm: &SourceMap) -> String {
    let err = err.into();
    let mut buf: Vec<u8> = Vec::new();
    emit(&err, sm, &mut buf).unwrap();
    String::from_utf8(buf).unwrap()
}

/// Asserts `(Result<_, CatalogError>, SourceMap)` yields the given variant
/// (with optional `if`-guard, same as [`matches!`]) and that the rendered
/// diagnostic contains every `expected` substring.
macro_rules! assert_err {
    ($res_sm:expr, $pat:pat $(if $guard:expr)?, [$($expected:expr),* $(,)?]) => {{
        let (res, sm) = $res_sm;
        let err = res.expect_err("expected stage error");
        assert!(matches!(&err, $pat $(if $guard)?), "got {err:?}");
        let out = render(err, &sm);
        $(
            assert!(
                out.contains($expected),
                "render missing `{}`\n--- got ---\n{out}",
                $expected,
            );
        )*
    }};
}

/// Parse `name` and build a catalog for each rule. Returns the first
/// catalog error encountered (or `Ok(())` if none).
fn catalog_for(name: &str) -> (Result<(), CatalogError>, SourceMap) {
    let mut sm = SourceMap::new();
    let program = flowlog_parser::parse(&fixture(name), &[], &mut sm, &mut Config::default())
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
        CatalogError::UnsafeVariable { var, predicate, .. }
            if var == "other" && predicate.contains("Blocked"),
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
        CatalogError::UnsafeVariable { var, predicate, .. }
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
        CatalogError::UnsafeVariable { var, predicate, .. }
            if var == "z" && predicate.contains("is_positive"),
        [
            "unsafe variable",
            "`z`",
            "comparison",
            "unsafe_variable_in_fn_call.dl",
        ]
    );
}
