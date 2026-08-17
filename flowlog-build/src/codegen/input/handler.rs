//! One input handler per EDB, shared by both modes.
//!
//! Per relation: a `RelationSpec` static, the runtime's `relation!`
//! expansion, and the `Ingest` impl naming its types. A relation with
//! `.fact` rows also gets an `inline_facts` body; a nullary one overrides
//! `load_line`, because `True`/`False` sets the multiplicity rather than
//! the tuple and no decoder can express that.

use flowlog_parser::DataType;
use flowlog_parser::InlineFact;
use flowlog_parser::InputIo;
use flowlog_parser::Program;
use flowlog_parser::Relation;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use crate::codegen::CodegenError;
use crate::codegen::Features;
use crate::codegen::const_to_token;
use crate::codegen::data_type_tokens;
use crate::codegen::input::container::gen_inputs_container;
use crate::codegen::input::input_struct_ident;
use crate::codegen::user_tuple_tokens;

/// Emit the shared relation-handler module body for binary mode.
pub fn gen_relation(
    program: &Program,
    features: &Features,
    is_batch: bool,
    uses_ord: bool,
) -> Result<TokenStream, CodegenError> {
    let edbs = program.edbs();
    let str_intern = features.string_intern();
    let facts = program.facts();
    let has_any_inline = edbs.iter().any(|rel| facts.contains_key(rel.name()));
    // A nullary relation reads `True`/`False` rather than a delimited row,
    // so it overrides `put`; the names that override needs are imported
    // only when one exists.
    let has_nullary = edbs.iter().any(|rel| rel.arity() == 0);

    let needs_ordered_float = edbs
        .iter()
        .any(|rel| rel.data_type().iter().any(|dt| dt.is_float()));

    // `Spur` appears in every string relation's tuple type; bare `intern` is
    // only called by inline string facts now that file and put decoding
    // live in the runtime, so its import is gated exactly on those or the
    // generated crate's -Dwarnings build fails on the unused name.
    let any_inline_string = edbs.iter().any(|rel| {
        facts.get(rel.name()).is_some_and(|rows| !rows.is_empty())
            && rel
                .data_type()
                .iter()
                .any(|dt| matches!(dt, DataType::String))
    });
    let spur_import = if str_intern {
        quote! { use ::flowlog_runtime::lasso::Spur; }
    } else {
        quote! {}
    };
    let intern_import = if str_intern && any_inline_string {
        quote! { use ::flowlog_runtime::intern::intern; }
    } else {
        quote! {}
    };

    let ordered_float_import = if needs_ordered_float {
        quote! { use ::flowlog_runtime::ordered_float::OrderedFloat; }
    } else {
        quote! {}
    };

    let semiring_one_import = if has_any_inline {
        quote! { use super::SEMIRING_ONE; }
    } else {
        quote! {}
    };

    let rel_impls: Vec<TokenStream> = edbs
        .iter()
        .map(|rel| {
            let rel_facts = facts.get(rel.name());
            if rel.arity() == 0 {
                gen_one_rel_nullary(rel, rel_facts, is_batch)
            } else {
                gen_one_rel_nonnullary(rel, rel_facts, str_intern, uses_ord)
            }
        })
        .collect::<Result<_, _>>()?;

    let nullary_imports = if has_nullary {
        quote! {
            use ::flowlog_runtime::differential_dataflow::input::InputSession;
            use std::path::Path;
            use std::time::Instant;
        }
    } else {
        quote! {}
    };

    let preamble = quote! {
        #nullary_imports

        use super::{Diff, Ts};
        #semiring_one_import
        #spur_import
        #intern_import
        #ordered_float_import
    };

    // The same container library mode uses, so both modes hold their
    // handlers as named fields rather than two different ways.
    let inputs = gen_inputs_container(&edbs);

    Ok(quote! {
        #preamble

        #(#rel_impls)*

        #inputs
    })
}

// ------------------------------------------------------------
// Per-relation generators
// ------------------------------------------------------------

fn gen_one_rel_nullary(
    rel: &Relation,
    facts: Option<&Vec<InlineFact>>,
    is_batch: bool,
) -> Result<TokenStream, CodegenError> {
    let raw_name = rel.raw_name();
    let struct_name = input_struct_ident(rel);

    let nullary_apply_inline = match facts {
        Some(rows) if !rows.is_empty() => quote! {
            fn apply_inline(&mut self, index: usize) {
                if index != 0 { return; }
                self.session.update((), SEMIRING_ONE);
            }
        },
        _ => quote! { fn apply_inline(&mut self, _index: usize) {} },
    };

    // Batch mode uses the `Present` semiring which has no i32 representation
    // or negation, so "false" collapses to a no-op; absence is indistinguishable.
    let apply_tuple_body = if is_batch {
        quote! {
            if index != 0 { return; }
            let s = tuple.trim();
            if s.eq_ignore_ascii_case("true") {
                self.session.update((), SEMIRING_ONE);
            } else if !s.eq_ignore_ascii_case("false") {
                eprintln!(
                    "[relation][{}] nullary expects tuple 'True' or 'False', got {:?}",
                    #raw_name,
                    s
                );
            }
        }
    } else {
        quote! {
            if index != 0 { return; }
            let s = tuple.trim();
            let d: Diff = if s.eq_ignore_ascii_case("true") {
                diff
            } else if s.eq_ignore_ascii_case("false") {
                -diff
            } else {
                eprintln!(
                    "[relation][{}] nullary expects tuple 'True' or 'False', got {:?}",
                    #raw_name,
                    s
                );
                return;
            };
            self.session.update((), d);
        }
    };

    let spec_name = format_ident!("{}_SPEC", struct_name.to_string().to_uppercase());

    Ok(quote! {
        static #spec_name: ::flowlog_runtime::io::RelationSpec =
            ::flowlog_runtime::io::RelationSpec {
                name: #raw_name,
                arity: 0usize,
                delim: b'\t',
                format: ::flowlog_runtime::io::Format::Text {
                    delim: b'\t',
                    has_header: false,
                },
                shard: ::flowlog_runtime::io::ShardKey::Bool,
                uses_ord: false,
            };

        ::flowlog_runtime::relation!(#struct_name, Ts, Diff, ());

        /// A nullary relation carries no columns: its rows are the single
        /// fact's presence, so `put` reads `True`/`False` as a sign rather
        /// than as data, and only worker 0 applies it so the diff is not
        /// multiplied across workers.
        impl ::flowlog_runtime::io::Ingest for #struct_name {
            type Ts = Ts;
            type Diff = Diff;
            type Tuple = ();
            type Rows = ();

            fn spec(&self) -> &'static ::flowlog_runtime::io::RelationSpec {
                &#spec_name
            }

            fn session(
                &mut self,
            ) -> &mut ::flowlog_runtime::io::Session<Ts, (), Diff> {
                &mut self.session
            }

            fn load_line(&mut self, tuple: &str, diff: Diff, _peers: usize, index: usize) {
                #apply_tuple_body
            }

            #nullary_apply_inline
        }
    })
}

fn gen_one_rel_nonnullary(
    rel: &Relation,
    facts: Option<&Vec<InlineFact>>,
    string_intern: bool,
    uses_ord: bool,
) -> Result<TokenStream, CodegenError> {
    let raw_name = rel.raw_name();
    let struct_name = input_struct_ident(rel);

    let arity = rel.arity();
    debug_assert!(arity > 0);

    let dts = rel.data_type();

    let delim_byte: u8 = rel
        .input_delimiter()
        .as_bytes()
        .first()
        .copied()
        .unwrap_or(b',');

    let shard_key = shard_key_for(&dts[0], string_intern);
    let format_expr = match rel.input_io() {
        Some(InputIo::File) | None => {
            let has_header = rel.input_has_header();
            quote! { ::flowlog_runtime::io::Format::Text { delim: #delim_byte, has_header: #has_header } }
        }
    };
    // Absent unless the relation declares `.fact` rows: `Ingest` supplies a
    // do-nothing default, so most relations emit nothing here.
    let inline_body = gen_inline_facts(facts, string_intern)?;
    let inline_facts_impl = if inline_body.is_empty() {
        quote! {}
    } else {
        quote! {
            fn inline_facts(&mut self, index: usize) {
                if index != 0 { return; }
                #inline_body
            }
        }
    };

    let user_ty = user_tuple_tokens(&dts);
    let tuple_ty = data_type_tokens(&dts, string_intern);
    let spec_name = format_ident!("{}_SPEC", struct_name.to_string().to_uppercase());

    Ok(quote! {
        static #spec_name: ::flowlog_runtime::io::RelationSpec = ::flowlog_runtime::io::RelationSpec {
            name: #raw_name,
            arity: #arity,
            delim: #delim_byte,
            format: #format_expr,
            shard: #shard_key,
            uses_ord: #uses_ord,
        };

        ::flowlog_runtime::relation!(#struct_name, Ts, Diff, #tuple_ty);

        impl ::flowlog_runtime::io::Ingest for #struct_name {
            type Ts = Ts;
            type Diff = Diff;
            type Tuple = #tuple_ty;
            type Rows = #user_ty;

            fn spec(&self) -> &'static ::flowlog_runtime::io::RelationSpec {
                &#spec_name
            }

            fn session(
                &mut self,
            ) -> &mut ::flowlog_runtime::io::Session<Ts, #tuple_ty, Diff> {
                &mut self.session
            }

            #inline_facts_impl
        }
    })
}

fn gen_inline_facts(
    facts: Option<&Vec<InlineFact>>,
    string_intern: bool,
) -> Result<TokenStream, CodegenError> {
    let Some(rows) = facts else {
        return Ok(quote! {});
    };
    if rows.is_empty() {
        return Ok(quote! {});
    }

    let tuples: Vec<TokenStream> = rows
        .iter()
        .map(|fact| {
            let elems: Vec<TokenStream> = fact
                .columns
                .iter()
                .map(|c| const_to_token(c, string_intern))
                .collect::<Result<_, _>>()?;

            Ok(if elems.len() == 1 {
                let e0 = &elems[0];
                quote! { ( #e0, ) }
            } else {
                quote! { ( #(#elems),* ) }
            })
        })
        .collect::<Result<_, CodegenError>>()?;

    Ok(quote! {
        for row in [ #(#tuples),* ] {
            self.session.update(row, SEMIRING_ONE);
        }
    })
}

// ------------------------------------------------------------
// Type + parsing helpers
// ------------------------------------------------------------

/// The ownership rule for a relation's first column, mirroring how
/// the row's typed accessors decode it.
///
/// # Panics
///
/// A tuple-typed or unpinned-literal first column is unreachable: neither
/// can appear in an EDB input relation once the typechecker has run.
fn shard_key_for(dt: &DataType, string_intern: bool) -> TokenStream {
    match dt {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            quote! { ::flowlog_runtime::io::ShardKey::Int }
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            quote! { ::flowlog_runtime::io::ShardKey::UInt }
        }
        DataType::Bool => quote! { ::flowlog_runtime::io::ShardKey::Bool },
        DataType::Float32 => quote! { ::flowlog_runtime::io::ShardKey::F32Bits },
        DataType::Float64 => quote! { ::flowlog_runtime::io::ShardKey::F64Bits },
        DataType::String if string_intern => quote! { ::flowlog_runtime::io::ShardKey::Spur },
        DataType::String => quote! { ::flowlog_runtime::io::ShardKey::Str },
        DataType::FixedTuple(_) => {
            unreachable!("tuple-typed columns cannot appear in EDB input relations")
        }
        DataType::IntLit | DataType::FloatLit => {
            unreachable!("unpinned literal type reached codegen; the typechecker pins all literals")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every first-column type maps to the ownership rule that decodes it
    /// the same way decode will, so owner and decoder cannot disagree.
    /// One row per EDB-legal DataType; the unreachable arms are pinned by
    /// the typechecker, not here.
    #[test]
    fn shard_key_mirrors_the_decode_family() {
        //    first column         intern  emitted rule
        let rows = [
            (
                DataType::Int8,
                false,
                ":: flowlog_runtime :: io :: ShardKey :: Int",
            ),
            (
                DataType::Int16,
                false,
                ":: flowlog_runtime :: io :: ShardKey :: Int",
            ),
            (
                DataType::Int32,
                false,
                ":: flowlog_runtime :: io :: ShardKey :: Int",
            ),
            (
                DataType::Int64,
                false,
                ":: flowlog_runtime :: io :: ShardKey :: Int",
            ),
            (
                DataType::UInt8,
                false,
                ":: flowlog_runtime :: io :: ShardKey :: UInt",
            ),
            (
                DataType::UInt16,
                false,
                ":: flowlog_runtime :: io :: ShardKey :: UInt",
            ),
            (
                DataType::UInt32,
                false,
                ":: flowlog_runtime :: io :: ShardKey :: UInt",
            ),
            (
                DataType::UInt64,
                false,
                ":: flowlog_runtime :: io :: ShardKey :: UInt",
            ),
            (
                DataType::Bool,
                false,
                ":: flowlog_runtime :: io :: ShardKey :: Bool",
            ),
            (
                DataType::Float32,
                false,
                ":: flowlog_runtime :: io :: ShardKey :: F32Bits",
            ),
            (
                DataType::Float64,
                false,
                ":: flowlog_runtime :: io :: ShardKey :: F64Bits",
            ),
            (
                DataType::String,
                true,
                ":: flowlog_runtime :: io :: ShardKey :: Spur",
            ),
            (
                DataType::String,
                false,
                ":: flowlog_runtime :: io :: ShardKey :: Str",
            ),
        ];
        for (dt, intern, expected) in rows {
            assert_eq!(
                shard_key_for(&dt, intern).to_string(),
                expected,
                "{dt:?} intern={intern}"
            );
        }
    }
}
