//! Type system for the `(Data, Diff, Time)` triple.
//!
//! - [`data`] — data type inference and Rust type token generation.
//! - [`diff`] — diff type alias (`Present` vs `i32`) and `SEMIRING_ONE` constant.
//! - [`time`] — outer timestamp and inner iteration type aliases.

use proc_macro2::TokenStream;
use quote::quote;

use parser::Relation;

use crate::Generator;
use data::data_type_tokens;

pub(crate) mod data;
pub(super) mod diff;
pub(super) mod time;

/// Tuple element type: `(data, timestamp, difference)`.
pub(crate) fn tuple_type(idb: &Relation, string_intern: bool) -> TokenStream {
    let tuple_ty = data_type_tokens(&idb.data_type(), string_intern);
    quote! { (#tuple_ty, Ts, i32) }
}

impl Generator {
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
