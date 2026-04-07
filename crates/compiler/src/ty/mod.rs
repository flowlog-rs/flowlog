//! Type system for the `(Data, Diff, Time)` triple.
//!
//! - [`data`] — data type inference and Rust type token generation.
//! - [`diff`] — diff type alias (`Present` vs `i32`) and `SEMIRING_ONE` constant.
//! - [`time`] — outer timestamp and inner iteration type aliases.

use proc_macro2::TokenStream;
use quote::quote;

use crate::Compiler;

pub(super) mod data;
pub(super) mod diff;
pub(super) mod time;

impl Compiler {
    /// Emit all type aliases and constants for the `(Data, Diff, Time)` triple.
    pub(crate) fn gen_type_declarations(&self) -> TokenStream {
        let diff_type = self.diff_type();
        let semiring_one = self.semiring_one_value();
        let ts_alias = self.timestamp_alias();
        let inner_time_type = self.inner_time_type();

        quote! {
            #diff_type
            #semiring_one
            #ts_alias
            #inner_time_type
        }
    }
}
