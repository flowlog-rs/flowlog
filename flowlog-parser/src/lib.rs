//! FlowLog Parser Library
//!
//! A parser for FlowLog, an efficient, scalable, and extensible Datalog
//! language engine. The crate is laid out as the compilation story:
//!
//! - `types`: the type vocabulary ([`DataType`], the registry).
//! - `syntax`: what the user wrote; grammar, string decoding, and the
//!   AST node layers (`ast`, `declaration`).
//! - `pipeline`: what happens to it; the stages from source text to a
//!   checked, optimized [`Program`], in execution order.
//! - `program`: what comes out; the [`Program`] container.
//!
//! [`parse`] runs the whole pipeline; [`check_program`],
//! [`fold_constants`], and [`prune`] expose the individual stages.

mod error;
mod pipeline;
mod program;
mod syntax;
mod types;

#[cfg(test)]
mod test_util;

// The node layers keep their crate-root spelling (`crate::ast::...`)
// while living under `syntax/`; the alias saves every consumer from
// carrying the extra path segment.
// Public API: the parsed AST.
pub use ast::Aggregation;
pub use ast::AggregationOperator;
pub use ast::Arithmetic;
pub use ast::ArithmeticOperator;
pub use ast::Atom;
pub use ast::AtomArg;
pub use ast::BuiltinCall;
pub use ast::BuiltinOperator;
pub use ast::Cast;
pub use ast::ComparisonExpr;
pub use ast::ComparisonOperator;
pub use ast::Constant;
pub use ast::Factor;
pub use ast::FlowLogRule;
pub use ast::FnCall;
pub use ast::Head;
pub use ast::HeadArg;
pub use ast::Predicate;
pub use ast::TupleElem;
pub use ast::TupleLit;
pub use declaration::Attribute;
pub use declaration::ExternFn;
pub use declaration::InputSource;
pub use declaration::OrderKey;
pub use declaration::OutputSink;
pub use declaration::Relation;
pub use error::DirectiveKind;
pub use error::ParseError;
pub use pipeline::fold::fold_constants;
pub use pipeline::parse;
// Not public API: the bottom rung of the `test_util` stage ladder.
#[cfg(test)]
pub(crate) use pipeline::parse_syntactic;
pub use pipeline::prune::prune;
pub use pipeline::typecheck::check_program;
pub use program::InlineFact;
pub use program::Program;
pub(crate) use syntax::ast;
pub(crate) use syntax::declaration;
// The pest bridge is crate-internal: downstream crates consume the AST,
// never raw pairs.
pub(crate) use syntax::grammar::FlowLogParser;
pub(crate) use syntax::grammar::Rule;
pub(crate) use syntax::grammar::decode_string;
pub(crate) use syntax::grammar::span_of;
pub(crate) use syntax::lexeme::Lexeme;
pub(crate) use syntax::node::Node;
pub use types::DataType;
pub(crate) use types::TypeId;
pub(crate) use types::TypeRegistry;
