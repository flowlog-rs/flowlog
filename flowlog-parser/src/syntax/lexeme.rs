//! The [`Lexeme`] trait: the conversion contract from pest parse
//! trees to FlowLog AST nodes.

use crate::Node;
use crate::error::ParseError;

/// Trait for converting parse-tree nodes into FlowLog types.
///
/// All FlowLog language constructs implement this trait to enable
/// conversion from parse trees to structured types. The [`Node`] carries
/// the source file, which is stored in every produced span so later
/// diagnostics can cite the user's source.
pub(crate) trait Lexeme: Sized {
    /// Converts a parse-tree node into a structured FlowLog type.
    ///
    /// Returns `Err(ParseError)` on grammar-contract violations that the
    /// grammar should have made unreachable; those surface as
    /// `ParseError::Internal`.
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError>;
}
