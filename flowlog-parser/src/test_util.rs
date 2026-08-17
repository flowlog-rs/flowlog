//! Shared helpers for `flowlog-parser`'s own tests, one per test layer.
//!
//! - [`parse_node`] is **node-level**: it runs the grammar from a single rule
//!   and lowers exactly one AST node, so a node's tests exercise only that
//!   node. A node is verified here; higher layers treat it as already-correct.
//! - [`assembled`] / [`checked`] / [`folded`] / [`pruned`] are the **pipeline
//!   ladder**: each runs one more stage than the last (assemble, +type-check,
//!   +fold, +prune), so a pass's test drives it on the previous rung's output.
//!   All return the parse `Result`; success-case callers `.expect()`.

use std::io::Write;

use flowlog_common::Config;
use flowlog_error::FileId;
use flowlog_error::SourceMap;
use pest::Parser as _;
use pest::iterators::Pair;
use tempfile::NamedTempFile;

use crate::FlowLogParser;
use crate::FlowLogRule;
use crate::Lexeme;
use crate::Node;
use crate::ParseError;
use crate::Program;
use crate::Rule;

/// The single pest `Pair` from parsing `src` starting at grammar `start_rule`.
///
/// The low-level entry for functions that consume a `Pair` directly (e.g.
/// `split_type_alias`), so their error paths can be tested at the function
/// level. [`parse_node`] builds on this.
pub(crate) fn parse_pair(start_rule: Rule, src: &str) -> Pair<'_, Rule> {
    FlowLogParser::parse(start_rule, src)
        .unwrap_or_else(|e| panic!("grammar parse of {src:?} as {start_rule:?} failed: {e}"))
        .next()
        .unwrap_or_else(|| panic!("no `{start_rule:?}` node produced for {src:?}"))
}

/// Lower `src` into AST node `T` by parsing from grammar rule `start_rule`.
///
/// Panics on any grammar or lowering error: the common case for tests that
/// assert on a successfully-parsed node's shape.
pub(crate) fn parse_node<T: Lexeme>(start_rule: Rule, src: &str) -> T {
    T::from_parsed_rule(Node::new(parse_pair(start_rule, src), FileId::new(0)))
        .unwrap_or_else(|e| panic!("lowering {src:?} as {start_rule:?} failed: {e:?}"))
}

/// Parse a single datalog rule (`h :- b.`) into a `FlowLogRule`.
///
/// A rule lowers via `expand_from_parsed_rule` (multi-head / disjunction
/// expansion yields a `Vec`), unlike the one-node [`parse_node`]; callers pass
/// single-clause sources and get back the sole expanded rule.
pub(crate) fn parse_rule(src: &str) -> FlowLogRule {
    FlowLogRule::expand_from_parsed_rule(parse_pair(Rule::rule, src), FileId::new(0))
        .unwrap_or_else(|e| panic!("lowering rule {src:?} failed: {e:?}"))
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("no rule expanded from {src:?}"))
}

/// Rung 1: the assembled `Program` (includes resolved, components inlined,
/// directives applied, equality assignments substituted), before type-check.
pub(crate) fn assembled(src: &str) -> Result<Program, ParseError> {
    let mut tmp = NamedTempFile::new().expect("tempfile");
    tmp.write_all(src.as_bytes()).expect("write");
    let mut sm = SourceMap::new();
    crate::parse_syntactic(&tmp.path().to_string_lossy(), true, &[], &mut sm)
}

/// Rung 2: [`assembled`] then type-check (literals pinned, casts lowered). Its
/// `Err` is a type error.
pub(crate) fn checked(src: &str) -> Result<Program, ParseError> {
    let mut program = assembled(src)?;
    let mut config = Config::default();
    crate::check_program(&mut program, &mut config)?;
    Ok(program)
}

/// Rung 3: [`checked`] then constant-fold.
pub(crate) fn folded(src: &str) -> Result<Program, ParseError> {
    let mut program = checked(src)?;
    crate::fold_constants(&mut program)?;
    Ok(program)
}

/// Rung 4: [`folded`] then prune. Stops before the `validate` stage.
pub(crate) fn pruned(src: &str) -> Result<Program, ParseError> {
    let mut program = folded(src)?;
    crate::prune(&mut program);
    Ok(program)
}

/// Assert that `$result` is an `Err` matching `$pat` (with an optional
/// `matches!`-style `if` guard), reporting the actual value on mismatch.
///
/// The function-level error-assertion: at the unit level there is no
/// `SourceMap`, so this matches the `ParseError` *variant* the producing
/// function returned, not a rendered diagnostic. `use crate::assert_err;`.
#[macro_export]
macro_rules! assert_err {
    ($result:expr, $pat:pat $(if $guard:expr)?) => {{
        let err = $result.expect_err("expected an Err");
        // Match on a reference so guards can compare non-Copy fields
        // (bindings become references) without consuming `err`.
        assert!(matches!(&err, $pat $(if $guard)?), "got {err:?}");
    }};
}
