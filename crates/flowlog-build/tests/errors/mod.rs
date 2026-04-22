//! Shared helpers for the per-stage `*_errors.rs` integration tests.
//! Lives under `tests/errors/` so Rust's test harness treats it as a
//! module, not a separate integration binary. Fixtures sit as sibling
//! subdirs (`tests/errors/<stage>/*.dl`).

use flowlog_build::common::{emit, BoxError};
use flowlog_build::common::SourceMap;

pub fn fixture(stage: &str, name: &str) -> String {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("errors")
        .join(stage)
        .join(name);
    path.into_os_string().into_string().unwrap()
}

pub fn render<E: Into<BoxError>>(err: E, sm: &SourceMap) -> String {
    let err = err.into();
    let mut buf: Vec<u8> = Vec::new();
    emit(&err, sm, &mut buf).unwrap();
    String::from_utf8(buf).unwrap()
}

/// Asserts `(Result<_, StageError>, SourceMap)` yields the given variant
/// (with optional `if`-guard, same as [`matches!`]) and that the rendered
/// diagnostic contains every `expected` substring. Requires `render` to
/// be in scope at the call site.
#[macro_export]
macro_rules! assert_err {
    ($res_sm:expr, $pat:pat $(if $guard:expr)?, [$($expected:expr),* $(,)?]) => {{
        let (res, sm) = $res_sm;
        let err = res.expect_err("expected stage error");
        assert!(matches!(err, $pat $(if $guard)?), "got {err:?}");
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
