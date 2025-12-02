//! FlowLog Parser Library
//!
//! A parser for the FlowLog, an efficient, scalable and extensible Datalog language engine.
//! Provides structured representations for Datalog programs including
//! relation declarations, logic rules, and primitive types.

pub mod declaration;
pub mod logic;
pub mod primitive;
pub mod program;

// Re-export core types for convenient access
pub use declaration::{Attribute, Relation};
pub use logic::{
    Aggregation, AggregationOperator, Arithmetic, ArithmeticOperator, Atom, AtomArg,
    ComparisonExpr, ComparisonOperator, Factor, FlowLogRule, Head, HeadArg, Predicate,
};
pub use primitive::{ConstType, DataType};
pub use program::Program;

use pest::iterators::Pair;
use pest_derive::Parser;

/// FlowLog Parser is powered by Pest, a PEG parser framework.
#[derive(Parser)]
#[grammar = "grammar.pest"]
pub struct FlowLogParser;

/// Trait for converting Pest parse trees into FlowLog types.
///
/// All FlowLog language constructs implement this trait to enable
/// conversion from parse trees to structured types.
pub trait Lexeme {
    /// Converts a Pest parse rule into a structured FlowLog type.
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self;
}
