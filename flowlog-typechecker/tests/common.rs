//! Shared helpers for the `flowlog-typechecker` integration tests.
//!
//! Both helpers are reachable from every test binary (`parse_and_check_result`
//! wraps `parse_program`), so neither goes dead — no blanket allow needed.

use std::io::Write;

use flowlog_common::Config;
use flowlog_common::SourceMap;
use flowlog_parser::ParseError;
use flowlog_parser::Program;
use flowlog_typechecker::TypeCheckError;
use flowlog_typechecker::check_program;
use tempfile::NamedTempFile;

/// Write `src` to a temp file and parse it (extended syntax, no type-check),
/// returning the parse `Result`. The shared prologue behind the check helpers,
/// and usable directly by tests that assert on a *parse* failure.
pub fn parse_program(src: &str) -> Result<Program, ParseError> {
    let mut tmp = NamedTempFile::new().expect("tempfile");
    tmp.write_all(src.as_bytes()).expect("write");
    let mut sm = SourceMap::new();
    Program::parse(&tmp.path().to_string_lossy(), true, &[], &mut sm)
}

/// Parse (panicking on parse errors) then return the type-check `Result`.
/// Success-case tests wrap this in a local `.expect(...)` for the pinned
/// `Program`; error-case tests inspect the `Err`.
pub fn parse_and_check_result(src: &str) -> Result<Program, TypeCheckError> {
    let mut program =
        parse_program(src).expect("parse should succeed; this test exercises typecheck only");
    check_program(&mut program, &Config::default())?;
    Ok(program)
}
