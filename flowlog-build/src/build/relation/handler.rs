//! Per-EDB input-handler codegen.
//!
//! One `{Name}Input` struct + inherent impl per input relation, wrapping an
//! `InputSession` and exposing `apply_inline` (for `.fact` rows declared
//! in the program), the timely epoch methods (`advance_to`, `flush`,
//! `close`), and a typed `update_tuple` used by the library engine.
//!
//! There is no `apply_file` — library mode is a pure typed API.

use flowlog_common::Span;
use flowlog_parser::ConstType;
use flowlog_parser::Relation;
use proc_macro2::TokenStream;
use quote::quote;

use super::input_struct_ident;
use crate::codegen::CodegenError;
use crate::codegen::const_to_token;
use crate::codegen::tuple_tokens;
use crate::data_type_tokens;

pub(super) fn gen_input_struct(
    rel: &Relation,
    facts: Option<&Vec<(Span, Vec<ConstType>)>>,
    string_intern: bool,
) -> Result<TokenStream, CodegenError> {
    let struct_name = input_struct_ident(rel);
    let tuple_ty = if rel.arity() == 0 {
        quote! { () }
    } else {
        data_type_tokens(&rel.data_type(), string_intern)
    };
    let apply_inline = gen_apply_inline(rel, facts, string_intern)?;

    Ok(quote! {
        pub(crate) struct #struct_name {
            h: Option<InputSession<Ts, #tuple_ty, Diff>>,
        }

        impl #struct_name {
            pub fn new(h: InputSession<Ts, #tuple_ty, Diff>) -> Self {
                Self { h: Some(h) }
            }

            #[inline]
            fn h_mut(&mut self) -> &mut InputSession<Ts, #tuple_ty, Diff> {
                self.h.as_mut().unwrap()
            }

            #apply_inline

            pub fn advance_to(&mut self, t: Ts) {
                self.h_mut().advance_to(t);
            }

            pub fn flush(&mut self) {
                self.h_mut().flush();
            }

            pub fn close(&mut self) {
                if let Some(h) = self.h.take() {
                    h.close();
                }
            }

            pub fn update_tuple(&mut self, tuple: #tuple_ty, diff: Diff) {
                self.h_mut().update(tuple, diff);
            }
        }
    })
}

/// Emit `apply_inline` — a no-op if the relation has no `.fact` rows,
/// otherwise a guarded push on worker 0. Nullary relations collapse any
/// number of rows to a single presence marker.
fn gen_apply_inline(
    rel: &Relation,
    facts: Option<&Vec<(Span, Vec<ConstType>)>>,
    string_intern: bool,
) -> Result<TokenStream, CodegenError> {
    let rows = match facts {
        Some(rows) if !rows.is_empty() => rows,
        _ => return Ok(quote! { pub fn apply_inline(&mut self, _index: usize) {} }),
    };

    let body = if rel.arity() == 0 {
        quote! { self.h_mut().update((), SEMIRING_ONE); }
    } else {
        let tuples = rows
            .iter()
            .map(|(_, row)| fact_tuple(row, string_intern))
            .collect::<Result<Vec<_>, _>>()?;
        quote! {
            for row in [ #(#tuples),* ] {
                self.h_mut().update(row, SEMIRING_ONE);
            }
        }
    };

    Ok(quote! {
        pub fn apply_inline(&mut self, index: usize) {
            if index != 0 { return; }
            #body
        }
    })
}

fn fact_tuple(row: &[ConstType], string_intern: bool) -> Result<TokenStream, CodegenError> {
    let parts = row
        .iter()
        .map(|c| const_to_token(c, string_intern))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(tuple_tokens(parts))
}
