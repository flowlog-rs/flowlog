//! FlowLog Parser Library
//!
//! A parser for FlowLog, an efficient, scalable, and extensible Datalog language engine.
//! Provides structured representations for Datalog programs including
//! relation declarations, logic rules, and primitive types.

mod declaration;
mod desugar;
mod error;
mod inliner;
mod logic;
mod primitive;
mod program;
mod segment;

// Public API: the parsed AST.
pub use declaration::Attribute;
pub use declaration::ExternFn;
pub use declaration::Relation;
pub use error::DirectiveKind;
pub use error::ParseError;
use flowlog_common::FileId;
use flowlog_common::Span;
pub use logic::Aggregation;
pub use logic::AggregationOperator;
pub use logic::Arithmetic;
pub use logic::ArithmeticOperator;
pub use logic::Atom;
pub use logic::AtomArg;
pub use logic::BuiltinCall;
pub use logic::BuiltinOperator;
pub use logic::Cast;
pub use logic::ComparisonExpr;
pub use logic::ComparisonOperator;
pub use logic::Factor;
pub use logic::FlowLogRule;
pub use logic::FnCall;
pub use logic::Head;
pub use logic::HeadArg;
pub use logic::IterativeDirective;
pub use logic::LoopBlock;
pub use logic::LoopCondition;
pub use logic::LoopConnective;
pub use logic::Predicate;
pub use logic::StopGroup;
pub use logic::StopRelation;
pub use logic::TupleElem;
pub use logic::TupleLit;
use pest::iterators::Pair;
use pest_derive::Parser;
pub use primitive::ConstType;
pub use primitive::DataType;
pub use primitive::TypeId;
pub use primitive::TypeRegistry;
pub use program::Program;
pub use segment::Segment;

/// Pest parser over individual grammar rules; most consumers want
/// [`Program::parse`]. The generated [`Rule`] enum mirrors `grammar.pest` and
/// is not a stability surface.
#[derive(Parser)]
#[grammar = "grammar.pest"]
pub struct FlowLogParser;

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
pub fn span_of(pair: &Pair<Rule>, file: FileId) -> Span {
    let s = pair.as_span();
    Span::new(file, s.start() as u32, s.end() as u32)
}

/// Surface text of a `type_ref` pair.
pub fn type_ref_name(pair: &Pair<Rule>) -> String {
    pair.as_str().trim().to_string()
}
