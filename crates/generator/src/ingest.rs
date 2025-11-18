//! Codegen for input ingestion inside a timely scope.
//!
//! Responsibilities:
//! - Declare per-input (handle, collection) pairs.
//! - Provide a mutable binding for handles.
//! - Generate per-input CSV readers based on configured paths.
//! - Close all handles at the end of ingestion.

use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};

use parser::{DataType, Relation};

/// Declare inputs as `(handle, collection)` pairs:
/// `let (h_<name>, c_<name>) = scope.new_collection::<_, Diff>();`
pub fn gen_input_decls(input_relations: Vec<&Relation>) -> Vec<TokenStream> {
    input_relations
        .iter()
        .map(|rel| {
            let handle = format_ident!("h_{}", rel.name());
            let coll = format_ident!("{}", rel.name());
            quote! { let (#handle, #coll) = scope.new_collection::<_, Diff>(); }
        })
        .collect()
}

/// Create a single binding for one or more handles, returning:
/// - the `let` binding TokenStream used in code,
/// - an expression TokenStream referencing the handle(s).
pub fn build_handle_binding(input_names: &[String]) -> (TokenStream, TokenStream) {
    match input_names.len() {
        0 => (quote! { let _ret }, quote! { () }),
        1 => {
            let h = format_ident!("h_{}", input_names[0]);
            (quote! { let mut #h }, quote! { #h })
        }
        _ => {
            let hs: Vec<_> = input_names
                .iter()
                .map(|n| format_ident!("h_{}", n))
                .collect();
            let muts: Vec<_> = hs.iter().map(|h| quote! { mut #h }).collect();
            (quote! { let ( #(#muts),* ) }, quote! { ( #(#hs),* ) })
        }
    }
}

/// Generate CSV ingestion loops per input, using the configured file paths.
///
/// Each input is sharded on its first field to distribute records across workers.
pub fn gen_ingest_stmts(input_relations: Vec<&Relation>) -> Vec<TokenStream> {
    input_relations
        .iter()
        .map(|rel| {
            let arity = rel.arity();
            let handle = format_ident!("h_{}", rel.name());
            let path = rel.input_params().unwrap().get("filename").unwrap();
            let itype = rel.data_type();

            let field_idents: Vec<Ident> = (0..arity).map(|i| format_ident!("f{}", i)).collect();

            let parse_stmts: Vec<TokenStream> = match itype {
                DataType::Integer => field_idents
                    .iter()
                    .enumerate()
                    .map(|(i, ident)| {
                        let miss = format!("missing field {}", i);
                        let bad = format!("bad field {}", i);
                        quote! {
                            let #ident: u64 = tuple.next().expect(#miss).parse().expect(#bad);
                        }
                    })
                    .collect(),
                DataType::String => field_idents
                    .iter()
                    .enumerate()
                    .map(|(i, ident)| {
                        let miss = format!("missing field {}", i);
                        quote! {
                            let #ident: String = tuple.next().expect(#miss).to_string();
                        }
                    })
                    .collect(),
            };
            // Determine whether this worker should ingest the record.
            // Numeric inputs are sharded by first field modulo peers.
            // Text inputs use a stable FNV-1a hash of the first field to shard across workers,
            // while keeping the record values as Strings (no hashing of data itself).
            let should_send_stmt: TokenStream = match itype {
                DataType::Integer => {
                    let shard_key = field_idents[0].clone();
                    quote! { let should_send = ((#shard_key as usize) % peers) == index; }
                }
                DataType::String => {
                    let shard_key = field_idents[0].clone();
                    quote! {
                        let should_send = {
                            // Stable 64-bit FNV-1a over bytes of the first field
                            let mut hash: u64 = 0xcbf29ce484222325;
                            for &b in #shard_key.as_bytes() {
                                hash ^= b as u64;
                                hash = hash.wrapping_mul(0x100000001b3);
                            }
                            ((hash as usize) % peers) == index
                        };
                    }
                }
            };

            let elem_expr = if arity == 1 {
                let x0 = field_idents[0].clone();
                quote! { ( #x0, ) }
            } else {
                quote! { ( #(#field_idents),* ) }
            };

            quote! {
                {
                    let reader = BufReader::new(
                        File::open(#path)
                            .unwrap_or_else(|e| panic!("failed to open {}: {}", #path, e))
                    );
                    for line in reader.lines() {
                        let line = line.expect("read error");
                        let mut tuple = line.split(',');
                        #(#parse_stmts)*
                        #should_send_stmt
                        if should_send {
                            #handle.update(#elem_expr, 1 as Diff);
                        }
                    }
                }
            }
        })
        .collect()
}

/// Close all input handles (end of ingestion).
pub fn gen_close_stmts(input_names: &[String]) -> Vec<TokenStream> {
    input_names
        .iter()
        .map(|name| {
            let handle = format_ident!("h_{}", name);
            quote! { #handle.close(); }
        })
        .collect()
}
