//! Projections: the key or value row an operator's closure emits, one
//! function per closure shape (row, key-value, join). Each lowers every
//! argument through the matching builder in [`term::arithmetic`] and packs
//! the results with [`row_tokens`].
//!
//! [`term::arithmetic`]: crate::expr::term::arithmetic

use flowlog_planner::planner::ArithmeticArgument;
use proc_macro2::Ident;
use proc_macro2::TokenStream;

use crate::Codegen;
use crate::CodegenError;
use crate::row_tokens;
use crate::ty::data::KvTypes;

impl Codegen {
    /// Returns the row a row closure emits, reading each variable from
    /// the row pattern's `fields`.
    pub(crate) fn row_projection(
        &mut self,
        args: &[ArithmeticArgument],
        fields: &[Ident],
        string_intern: bool,
        input_type: &KvTypes,
    ) -> Result<TokenStream, CodegenError> {
        let parts: Vec<TokenStream> = args
            .iter()
            .map(|arg| self.row_arithmetic(arg, fields, string_intern, input_type))
            .collect::<Result<_, _>>()?;
        Ok(row_tokens(parts))
    }

    /// Returns the row a key-value closure emits, reading each variable
    /// from the `(k, v)` parameters.
    pub(crate) fn kv_projection(
        &mut self,
        args: &[ArithmeticArgument],
        string_intern: bool,
        input_type: &KvTypes,
    ) -> Result<TokenStream, CodegenError> {
        let parts: Vec<TokenStream> = args
            .iter()
            .map(|arg| self.kv_arithmetic(arg, string_intern, input_type))
            .collect::<Result<_, _>>()?;
        Ok(row_tokens(parts))
    }

    /// Returns the row a join closure emits, reading each variable from
    /// the `(k, lv, rv)` parameters.
    pub(crate) fn join_projection(
        &mut self,
        args: &[ArithmeticArgument],
        string_intern: bool,
        left_type: &KvTypes,
        right_type: &KvTypes,
    ) -> Result<TokenStream, CodegenError> {
        let parts: Vec<TokenStream> = args
            .iter()
            .map(|arg| self.join_arithmetic(arg, string_intern, left_type, right_type))
            .collect::<Result<_, _>>()?;
        Ok(row_tokens(parts))
    }
}
