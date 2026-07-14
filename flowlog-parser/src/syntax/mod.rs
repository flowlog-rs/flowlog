//! Parsing layer: source text to typed AST.
//!
//! The only module that touches the raw pest tree.
//!
//! - `grammar`: the pest binding and the token readers over its output
//!   (spans, type-ref text, string-literal decoding).
//! - `node`: the typed cursor over parse pairs.
//! - `lexeme`: the pair-to-node conversion contract every AST type
//!   implements.
//! - `ast` / `declaration` / `segment`: the AST node types, in
//!   containment order.

pub(crate) mod ast;
pub(crate) mod declaration;
pub(crate) mod grammar;
pub(crate) mod lexeme;
pub(crate) mod node;
pub(crate) mod segment;
