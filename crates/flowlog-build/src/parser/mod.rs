//! FlowLog Parser Library
//!
//! A parser for FlowLog, an efficient, scalable, and extensible Datalog language engine.
//! Provides structured representations for Datalog programs including
//! relation declarations, logic rules, and primitive types.

mod declaration;
mod desugar;
mod error;
mod grounding;
mod inliner;
mod logic;
mod primitive;
mod program;
mod segment;

pub use error::{DirectiveKind, ParseError};

// External API (reached by flowlog-compiler and integration tests).
// ArithmeticOperator / ComparisonOperator leak through TypeCheckError's
// pub variants; AggregationOperator leaks through Features::agg_semirings.
pub use declaration::Relation;
pub use logic::{
    AggregationOperator, ArithmeticOperator, BuiltinOperator, ComparisonOperator, FlowLogRule,
};
pub use primitive::{ConstType, DataType};
pub(crate) use primitive::{TypeId, TypeRegistry};
pub use program::Program;

// Intra-crate shortcuts — these logic types are imported from elsewhere
// in the crate (catalog, planner, codegen, etc.) as `crate::parser::X`.
// Declaration/segment/program inner types are reached via their own
// submodule paths and don't need a shortcut here.
pub(crate) use logic::{
    Aggregation, Arithmetic, Atom, AtomArg, BuiltinCall, ComparisonExpr, Factor, FnCall, HeadArg,
    IterativeDirective, LoopCondition, LoopConnective, Predicate,
};
pub(crate) use segment::Segment;

use crate::common::{FileId, Span};
use pest::iterators::Pair;
use pest_derive::Parser;

/// FlowLog Parser is powered by Pest, a PEG parser framework.
#[derive(Parser)]
#[grammar = "parser/grammar.pest"]
pub(crate) struct FlowLogParser;

/// Trait for converting Pest parse trees into FlowLog types.
///
/// All FlowLog language constructs implement this trait to enable
/// conversion from parse trees to structured types. `file` identifies the
/// source file the pair came from; it is stored in every produced span so
/// later diagnostics can cite the user's source.
pub(crate) trait Lexeme: Sized {
    /// Converts a Pest parse rule into a structured FlowLog type.
    ///
    /// Returns `Err(ParseError)` on grammar-contract violations that the
    /// grammar should have made unreachable — those surface as
    /// `ParseError::Internal`.
    fn from_parsed_rule(parsed_rule: Pair<Rule>, file: FileId) -> Result<Self, ParseError>;
}

/// Build a [`Span`] from a Pest `Span`, anchored to the given [`FileId`].
pub(crate) fn span_of(pair: &Pair<Rule>, file: FileId) -> Span {
    let s = pair.as_span();
    Span::new(file, s.start() as u32, s.end() as u32)
}

/// Surface text of a `type_ref` pair.
pub(crate) fn type_ref_name(pair: &Pair<Rule>) -> String {
    pair.as_str().trim().to_string()
}
