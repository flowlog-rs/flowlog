//! Codegen for input ingestion inside a timely scope.
//!
//! Responsibilities:
//! - Declare per-input (handle, collection) pairs.
//! - Provide a mutable binding for handles.
//! - Generate per-input CSV readers based on configured paths.
//! - Close all handles at the end of ingestion.

use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use std::path::Path;

use parser::{ConstType, DataType, Program, Relation};

/// Declare inputs as `(handle, collection)` pairs:
/// `let (h_<name>, c_<name>) = scope.new_collection::<_, Diff>();`
pub fn gen_input_decls(input_relations: Vec<&Relation>) -> Vec<TokenStream> {
    input_relations
        .iter()
        .map(|rel| {
            let handle = format_ident!("h{}", rel.name());
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
            let h = format_ident!("h{}", input_names[0]);
            (quote! { let mut #h }, quote! { #h })
        }
        _ => {
            let hs: Vec<_> = input_names
                .iter()
                .map(|n| format_ident!("h{}", n))
                .collect();
            let muts: Vec<_> = hs.iter().map(|h| quote! { mut #h }).collect();
            (quote! { let ( #(#muts),* ) }, quote! { ( #(#hs),* ) })
        }
    }
}

/// Generate CSV ingestion loops and boolean fact ingestion for each EDB relation.
///
/// Each input is sharded on its first field to distribute records across workers.
pub fn gen_ingest_stmts(fact_dir: Option<&str>, program: &Program) -> Vec<TokenStream> {
    let input_relations = program.edbs();
    let bool_facts = program.bool_facts();

    input_relations
        .iter()
        .map(|rel| {
            let csv_ingest = gen_csv_ingest_stmt(fact_dir, rel);
            let bool_ingest = gen_bool_fact_ingest_stmt(rel, bool_facts);

            quote! {
                #csv_ingest
                #bool_ingest
            }
        })
        .collect()
}

fn gen_csv_ingest_stmt(fact_dir: Option<&str>, rel: &Relation) -> TokenStream {
    let arity = rel.arity();
    let handle = format_ident!("h{}", rel.name());
    let file_name = rel.input_file_name().unwrap();
    let delimiter = rel.input_delimiter().unwrap_or(",");
    let path = fact_dir
        .map(|dir| Path::new(dir).join(file_name).to_string_lossy().into_owned())
        .unwrap_or_else(|| file_name.to_string());
    let itype = rel.data_type();

    let field_idents: Vec<Ident> = (0..arity).map(|i| format_ident!("f{}", i)).collect();

    let parse_stmts: Vec<TokenStream> = match itype {
        DataType::Integer => field_idents
            .iter()
            .skip(1)
            .map(|ident| {
                quote! {
                    let #ident: i32 = std::str::from_utf8(tuple.next()?).ok()?.parse::<i32>().ok()?;
                }
            })
            .collect(),
        DataType::String => field_idents
            .iter()
            .skip(1)
            .map(|ident| {
                quote! {
                    let #ident: String = std::str::from_utf8(tuple.next()?).ok()?.to_string();
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
            let first_key = field_idents[0].clone();
            quote! {
                let #first_key: i32 = std::str::from_utf8(tuple.next()?).ok()?.parse::<i32>().ok()?;
                let should_send = ((#first_key as usize) % peers) == index;
            }
        }
        DataType::String => {
            let first_key = field_idents[0].clone();
            quote! {
                let #first_key: String = std::str::from_utf8(tuple.next()?).ok()?.to_string();
                let should_send = {
                    // Stable 64-bit FNV-1a over bytes of the first field
                    let mut hash: u64 = 0xcbf29ce484222325;
                    for &b in #first_key.as_bytes() {
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

    match itype {
        DataType::Integer => {
            quote! {
                let ingest = {
                    let reader = BufReader::new(
                        File::open(#path)
                            .unwrap_or_else(|e| panic!("failed to open {}: {}", #path, e))
                    );
                    reader
                        .split(b'\n')
                        .filter_map(Result::ok) // filter out errors
                        .filter(move |line| !line.is_empty()) // skip empty lines
                        .filter_map(move |line| {
                            let mut tuple = line.split(|&bt| bt == #delimiter.as_bytes()[0]);
                            #should_send_stmt
                            if !should_send {
                                return None;
                            }
                            #(#parse_stmts)*
                            Some(#elem_expr)
                        })
                };

                ingest.for_each(|row| #handle.update(row, SEMIRING_ONE));
            }
        }
        DataType::String => {
            quote! {
                let ingest = {
                    let reader = BufReader::new(
                        File::open(#path)
                            .unwrap_or_else(|e| panic!("failed to open {}: {}", #path, e))
                    );
                    reader
                        .split(b'\n')
                        .filter_map(Result::ok) // filter out errors
                        .filter(move |line| !line.is_empty()) // skip empty lines
                        .filter_map(move |line| {
                            let mut tuple = line.split(|&bt| bt == #delimiter.as_bytes()[0]);
                            #should_send_stmt
                            if !should_send {
                                return None;
                            }
                            #(#parse_stmts)*
                            Some(#elem_expr)
                        })
                };
                ingest.for_each(|row| #handle.update(row, SEMIRING_ONE));
            }
        }
    }
}

fn gen_bool_fact_ingest_stmt(
    rel: &Relation,
    bool_facts: &std::collections::HashMap<String, Vec<(Vec<ConstType>, bool)>>,
) -> TokenStream {
    let handle = format_ident!("h{}", rel.name());
    let rel_name = rel.name().to_string();
    let facts = bool_facts.get(&rel_name);

    if let Some(facts) = facts {
        // Build a literal vector of tuples for all `true` facts.
        let tuples: Vec<TokenStream> = facts
            .iter()
            .filter(|(_, b)| *b)
            .map(|(vals, _)| {
                let elems: Vec<TokenStream> = vals
                    .iter()
                    .map(|c| match c {
                        ConstType::Integer(i) => quote! { #i },
                        ConstType::Text(s) => quote! { #s.to_string() },
                    })
                    .collect();
                if elems.len() == 1 {
                    let e0 = &elems[0];
                    quote! { ( #e0, ) }
                } else {
                    quote! { ( #(#elems),* ) }
                }
            })
            .collect();

        if tuples.is_empty() {
            quote! {}
        } else {
            quote! {
                for row in [ #(#tuples),* ] {
                    #handle.update(row, SEMIRING_ONE);
                }
            }
        }
    } else {
        quote! {}
    }
}

/// Close all input handles (end of ingestion).
pub fn gen_close_stmts(input_names: &[String]) -> Vec<TokenStream> {
    input_names
        .iter()
        .map(|name| {
            let handle = format_ident!("h{}", name);
            quote! {
                #handle.close();
                if index == 0 {
                    println!("{:?}:\tData loaded for {}", timer.elapsed(), #name);
                }
            }
        })
        .collect()
}
