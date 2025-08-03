pub mod declaration;
pub mod logic;
pub mod primitive;
pub mod program;

pub use declaration::{Attribute, Relation};
pub use logic::{
    Arithmetic, ArithmeticOperator, Atom, AtomArg, ComparisonExpr, ComparisonOperator, FLRule,
    Factor, Head, HeadArg, Predicate,
};
pub use primitive::{ConstType, DataType};
pub use program::Program;

use pest::iterators::Pair;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "grammar.pest"]
pub struct FlowLogParser;

/// A trait for converting Pest parse rules into specific structure
pub trait Lexeme {
    /// Convert a Pest parse result into the specific structure
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self;
}
