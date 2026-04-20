//! FlowLog Parser Library
//!
//! A parser for FlowLog, an efficient, scalable, and extensible Datalog language engine.
//! Provides structured representations for Datalog programs including
//! relation declarations, logic rules, and primitive types.

pub mod declaration;
pub mod error;
pub mod logic;
pub mod primitive;
pub mod program;
pub mod segment;

pub use error::ParseError;

// Re-export core types for convenient access
pub use declaration::{Attribute, ExternFn, Relation};
pub use logic::{
    Aggregation, AggregationOperator, Arithmetic, ArithmeticOperator, Atom, AtomArg,
    ComparisonExpr, ComparisonOperator, Factor, FlowLogRule, FnCall, Head, HeadArg,
    IterativeDirective, LoopBlock, LoopCondition, LoopConnective, Predicate, StopGroup,
    StopRelation,
};
pub use primitive::{ConstType, DataType};
pub use program::Program;
pub use segment::Segment;

use crate::common::source::{FileId, Span};
use pest::iterators::Pair;
use pest_derive::Parser;

/// FlowLog Parser is powered by Pest, a PEG parser framework.
#[derive(Parser)]
#[grammar = "parser/grammar.pest"]
pub struct FlowLogParser;

/// Trait for converting Pest parse trees into FlowLog types.
///
/// All FlowLog language constructs implement this trait to enable
/// conversion from parse trees to structured types. `file` identifies the
/// source file the pair came from; it is stored in every produced span so
/// later diagnostics can cite the user's source.
pub trait Lexeme: Sized {
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
