//! Macaron Parser Library
//!
//! A parser for the Macaron declarative logic programming language.
//! Provides structured representations for Macaron programs including
//! relation declarations, logic rules, and primitive types.

pub mod declaration;
pub mod logic;
pub mod primitive;
pub mod program;

// Re-export core types for convenient access
pub use declaration::{Attribute, Relation};
pub use logic::{
    Aggregation, AggregationOperator, Arithmetic, ArithmeticOperator, Atom, AtomArg,
    ComparisonExpr, ComparisonOperator, FLRule, Factor, Head, HeadArg, Predicate,
};
pub use primitive::{ConstType, DataType};
pub use program::Program;

use pest::iterators::Pair;
use pest_derive::Parser;

/// Macaron grammar parser using Pest.
#[derive(Parser)]
#[grammar = "grammar.pest"]
pub struct MacaronParser;

/// Macaron Parser is powered by Pest, a PEG parser framework.
///
/// Trait for converting Pest parse trees into Macaron types.
///
/// All Macaron language constructs implement this trait to enable
/// conversion from parse trees to structured types.
pub trait Lexeme {
    /// Converts a Pest parse rule into a structured Macaron type.
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self;
}
