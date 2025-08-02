pub mod declaration;
pub mod expression;
pub mod primitive;
pub mod program;
pub mod rule;

pub use declaration::{Attribute, RelationDecl};
pub use expression::{
    Arithmetic, ArithmeticOperator, Atom, AtomArg, ComparisonExpr, ComparisonOperator, Factor,
};
pub use primitive::{Const, DataType};
pub use program::Program;
pub use rule::{FLRule, Head, HeadArg, Predicate};

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
