//! Shared helpers for the `flowlog-typechecker` integration tests.
//!
//! Compiled into each test binary; not every binary uses every helper, so
//! allow dead code here rather than force artificial uses.
#![allow(dead_code)]

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
pub fn parse_and_check_result(src: &str) -> Result<Program, TypeCheckError> {
    let mut program =
        parse_program(src).expect("parse should succeed; this test exercises typecheck only");
    check_program(&mut program, &Config::default())?;
    Ok(program)
}

/// Parse + type-check `src`, panicking on any error. Returns the pinned program.
pub fn parse_and_check(src: &str) -> Program {
    parse_and_check_result(src).expect("check failed")
}
