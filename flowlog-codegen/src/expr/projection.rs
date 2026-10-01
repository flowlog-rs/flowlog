//! Projections: the key or value tuple an operator's closure emits, one
//! function per closure shape (row, key-value, join). Each lowers every
//! argument through the matching builder in [`term::arithmetic`] and packs
//! the results with [`tuple_tokens`].
//!
//! [`term::arithmetic`]: crate::expr::term::arithmetic

use flowlog_planner::planner::ArithmeticArgument;
use proc_macro2::Ident;
use proc_macro2::TokenStream;

use crate::Codegen;
use crate::CodegenError;
use crate::tuple_tokens;

impl Codegen {
    /// Returns the tuple a row closure emits, reading each variable from
    /// the row pattern's `fields`.
    pub(crate) fn row_projection(
        &mut self,
        args: &[ArithmeticArgument],
        fields: &[Ident],
        string_intern: bool,
    ) -> Result<TokenStream, CodegenError> {
        let parts: Vec<TokenStream> = args
            .iter()
            .map(|arg| self.row_arithmetic(arg, fields, string_intern))
            .collect::<Result<_, _>>()?;
        Ok(tuple_tokens(parts))
    }

    /// Returns the tuple a key-value closure emits, reading each variable
    /// from the `(k, v)` parameters.
    pub(crate) fn kv_projection(
        &mut self,
        args: &[ArithmeticArgument],
        string_intern: bool,
    ) -> Result<TokenStream, CodegenError> {
        let parts: Vec<TokenStream> = args
            .iter()
            .map(|arg| self.kv_arithmetic(arg, string_intern))
            .collect::<Result<_, _>>()?;
        Ok(tuple_tokens(parts))
    }

    /// Returns the tuple a join closure emits, reading each variable from
    /// the `(k, lv, rv)` parameters.
    pub(crate) fn join_projection(
        &mut self,
        args: &[ArithmeticArgument],
        string_intern: bool,
    ) -> Result<TokenStream, CodegenError> {
        let parts: Vec<TokenStream> = args
            .iter()
            .map(|arg| self.join_arithmetic(arg, string_intern))
            .collect::<Result<_, _>>()?;
        Ok(tuple_tokens(parts))
    }
}
