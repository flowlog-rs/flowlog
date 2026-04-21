//! Binary-mode relation handler codegen.
//!
//! Emits the generated binary's `relation` module body: the `Relation`
//! trait plus per-EDB `Rel{name}` struct and `impl Relation for Rel{name}`.
//! Shard helpers and the byte-range reader are supplied by the binary's
//! `imports::gen_binary_relation_extras` (inlined into the same module).

use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use flowlog_build::{data_type_tokens, Features};
use flowlog_build::parser::{ConstType, DataType, Program, Relation};

/// Emit the shared relation-handler module body for binary mode.
pub(crate) fn gen_relation(program: &Program, features: &Features, is_batch: bool) -> TokenStream {
    let edbs = program.edbs();
    let str_intern = features.string_intern();
    let facts = program.facts();
    let has_any_inline = edbs.iter().any(|rel| facts.contains_key(rel.name()));

    let needs_ordered_float = edbs
        .iter()
        .any(|rel| rel.data_type().iter().any(|dt| dt.is_float()));

    let intern_import = if str_intern {
        quote! {
            use super::intern;
            use lasso::Spur;
        }
    } else {
        quote! {}
    };

    let ordered_float_import = if needs_ordered_float {
        quote! { use ordered_float::OrderedFloat; }
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
                gen_one_rel_nonnullary(rel, rel_facts, str_intern)
            }
        })
        .collect();

    let preamble = quote! {
        use differential_dataflow::input::InputSession;

        use std::io::BufRead;
        use std::path::Path;
        use std::time::Instant;

        use super::{Diff, Ts};
        #semiring_one_import
        #intern_import
        #ordered_float_import
    };

    quote! {
        #preamble

        /// Operations supported by a dynamic relation handler.
        ///
        /// Implementations are generated per EDB relation and backed by an
        /// [`InputSession`]. Implementations are expected to be robust:
        /// parsing and I/O errors should be reported to stderr and skipped.
        pub(crate) trait Relation {
            /// Apply a single tuple update.
            ///
            /// `tuple` is a delimited string whose delimiter is relation-specific.
            /// Implementations should shard by the first column and apply the update
            /// only on the matching worker.
            #[allow(dead_code)]
            fn apply_tuple(&mut self, tuple: &str, diff: Diff, peers: usize, index: usize);

            /// Apply updates from a file.
            ///
            /// Implementations use byte-range parallel reading: each worker reads
            /// only its ~1/N byte slice of the file, parsing and applying `diff`
            /// to each row within its range.
            #[allow(dead_code)]
            fn apply_file(&mut self, path: &Path, diff: Diff, peers: usize, index: usize);

            /// Apply inline facts directly from program.
            #[allow(dead_code)]
            fn apply_inline(&mut self, index: usize);

            /// Advance the input session to logical time.
            #[allow(dead_code)]
            fn advance_to(&mut self, t: Ts);

            /// Flush buffered updates into the dataflow.
            #[allow(dead_code)]
            fn flush(&mut self);

            /// Close the input session.
            fn close(&mut self);
        }

        #(#rel_impls)*
    }
}

// ------------------------------------------------------------
// Per-relation generators
// ------------------------------------------------------------

fn gen_one_rel_nullary(
    rel: &Relation,
    facts: Option<&Vec<Vec<ConstType>>>,
    is_batch: bool,
) -> TokenStream {
    let name = rel.name();
    let struct_name = format_ident!("Rel{}", name);

    let nullary_apply_inline = match facts {
        Some(rows) if !rows.is_empty() => quote! {
            fn apply_inline(&mut self, index: usize) {
                if index != 0 { return; }
                self.h_mut().update((), SEMIRING_ONE);
            }
        },
        _ => quote! { fn apply_inline(&mut self, _index: usize) {} },
    };

    // Batch mode uses the `Present` semiring which has no i32 representation
    // or negation, so "false" collapses to a no-op — absence is indistinguishable.
    let apply_tuple_body = if is_batch {
        quote! {
            if index != 0 { return; }
            let s = tuple.trim();
            if s.eq_ignore_ascii_case("true") {
                self.h_mut().update((), SEMIRING_ONE);
            } else if !s.eq_ignore_ascii_case("false") {
                eprintln!(
                    "[relation][{}] nullary expects tuple 'True' or 'False', got {:?}",
                    #name,
                    s
                );
            }
        }
    } else {
        quote! {
            if index != 0 { return; }
            let s = tuple.trim();
            let d: Diff = if s.eq_ignore_ascii_case("true") {
                1
            } else if s.eq_ignore_ascii_case("false") {
                -1
            } else {
                eprintln!(
                    "[relation][{}] nullary expects tuple 'True' or 'False', got {:?}",
                    #name,
                    s
                );
                return;
            };
            self.h_mut().update((), d);
        }
    };

    quote! {
        /// Input handler for the nullary relation.
        ///
        /// Nullary relations store a single boolean-like fact (`True`/`False`) and are
        /// updated only by worker 0 to avoid multiplying diffs across workers.
        pub(crate) struct #struct_name {
            h: Option<InputSession<Ts, (), Diff>>,
        }

        impl #struct_name {
            /// Create a new nullary handler.
            pub fn new(h: InputSession<Ts, (), Diff>) -> Self {
                Self { h: Some(h) }
            }

            /// Borrow the underlying input session.
            #[inline]
            fn h_mut(&mut self) -> &mut InputSession<Ts, (), Diff> {
                self.h.as_mut().unwrap()
            }
        }

        impl Relation for #struct_name {
            fn apply_tuple(&mut self, tuple: &str, _diff: Diff, _peers: usize, index: usize) {
                // Nullary: only worker0 applies (avoid multiplying diffs across workers).
                #apply_tuple_body
            }

            fn apply_file(&mut self, path: &Path, _diff: Diff, _peers: usize, index: usize) {
                if index != 0 { return; }

                // Nullary relations only allow tuple interaction.
                eprintln!(
                    "[relation][{}] nullary relation does not support file ingestion. Use: put {} True|False",
                    #name,
                    path.display()
                );
            }

            #nullary_apply_inline

            fn advance_to(&mut self, t: Ts) {
                self.h_mut().advance_to(t);
            }

            fn flush(&mut self) {
                self.h_mut().flush();
            }

            fn close(&mut self) {
                if let Some(h) = self.h.take() {
                    h.close();
                }
            }
        }
    }
}

fn gen_one_rel_nonnullary(
    rel: &Relation,
    facts: Option<&Vec<Vec<ConstType>>>,
    string_intern: bool,
) -> TokenStream {
    let name = rel.name();
    let struct_name = format_ident!("Rel{}", name);

    let arity = rel.arity();
    debug_assert!(arity > 0);

    let dts = rel.data_type();

    let delim_byte: u8 = rel
        .input_delimiter()
        .as_bytes()
        .first()
        .copied()
        .unwrap_or(b',');

    let tuple_ty = data_type_tokens(&dts, string_intern);

    let shard_tuple = match dts[0] {
        DataType::Int8 => quote! { if !shard_int(f0 as i64, peers, index) { return; } },
        DataType::Int16 => quote! { if !shard_int(f0 as i64, peers, index) { return; } },
        DataType::Int32 => quote! { if !shard_int(f0 as i64, peers, index) { return; } },
        DataType::Int64 => quote! { if !shard_int(f0, peers, index) { return; } },
        DataType::UInt8 => quote! { if !shard_int(f0 as i64, peers, index) { return; } },
        DataType::UInt16 => quote! { if !shard_int(f0 as i64, peers, index) { return; } },
        DataType::UInt32 => quote! { if !shard_int(f0 as i64, peers, index) { return; } },
        DataType::UInt64 => quote! { if !shard_int(f0 as i64, peers, index) { return; } },
        DataType::Float32 => {
            quote! { if !shard_int(f0.into_inner().to_bits() as i64, peers, index) { return; } }
        }
        DataType::Float64 => {
            quote! { if !shard_int(f0.into_inner().to_bits() as i64, peers, index) { return; } }
        }
        DataType::String => {
            if string_intern {
                quote! { if !shard_spur(f0, peers, index) { return; } }
            } else {
                quote! { if !shard_str(f0.as_str(), peers, index) { return; } }
            }
        }
        DataType::Bool => quote! { if !shard_int(f0 as i64, peers, index) { return; } },
    };
    let tuple_parse_stmts = gen_parse_from_str(name, &dts, string_intern);
    let file_parse_stmts = gen_parse_from_bytes(name, &dts, string_intern);
    let has_header = rel.input_has_header();
    let inline_body = gen_inline_facts(facts, string_intern);
    let apply_inline_impl = if inline_body.is_empty() {
        quote! { fn apply_inline(&mut self, _index: usize) {} }
    } else {
        quote! {
            fn apply_inline(&mut self, index: usize) {
                if index != 0 { return; }
                #inline_body
            }
        }
    };

    let update_expr = match arity {
        1 => quote! { (f0,) },
        _ => {
            let vars: Vec<_> = (0..arity).map(|i| format_ident!("f{i}")).collect();
            quote! { (#(#vars),*) }
        }
    };

    quote! {
        /// Input handler for the relation.
        ///
        /// - Parses tuples using the relation delimiter.
        /// - Shards updates by the first column across `peers`.
        /// - Reports parse / I/O errors to stderr and skips malformed rows.
        #[allow(dead_code)]
        pub(crate) struct #struct_name {
            h: Option<InputSession<Ts, #tuple_ty, Diff>>,
            delim: u8,
            has_header: bool,
        }

        impl #struct_name {
            /// Create a new handler.
            pub fn new(h: InputSession<Ts, #tuple_ty, Diff>) -> Self {
                Self {
                    h: Some(h),
                    delim: #delim_byte,
                    has_header: #has_header,
                }
            }

            /// Borrow the underlying input session.
            #[inline]
            fn h_mut(&mut self) -> &mut InputSession<Ts, #tuple_ty, Diff> {
                self.h.as_mut().unwrap()
            }
        }

        impl Relation for #struct_name {
            fn apply_tuple(&mut self, tuple: &str, diff: Diff, peers: usize, index: usize) {
                let tuple = tuple.trim();
                let delim = self.delim as char;
                let mut it = tuple.split(delim).map(|s| s.trim());

                #tuple_parse_stmts

                #shard_tuple
                self.h_mut().update(#update_expr, diff);
            }

            fn apply_file(&mut self, path: &Path, diff: Diff, peers: usize, index: usize) {
                let load_start = Instant::now();
                let (mut reader, byte_budget) = match __byte_range_reader(path, index, peers) {
                    Some(r) => r,
                    None => return,
                };
                let delim = self.delim;
                let has_header = self.has_header;

                let mut buf = Vec::with_capacity(256);
                let mut bytes_consumed: u64 = 0;
                if has_header && index == 0 {
                    buf.clear();
                    match reader.read_until(b'\n', &mut buf) {
                        Ok(0) => return,
                        Ok(_) => bytes_consumed += buf.len() as u64,
                        Err(e) => {
                            eprintln!("[relation][{}] I/O error reading {}: {}", #name, path.display(), e);
                            return;
                        }
                    }
                }
                while bytes_consumed < byte_budget {
                    buf.clear();
                    match reader.read_until(b'\n', &mut buf) {
                        Ok(0) => break,
                        Ok(_) => {}
                        Err(e) => {
                            eprintln!("[relation][{}] I/O error reading {}: {}", #name, path.display(), e);
                            break;
                        }
                    }
                    bytes_consumed += buf.len() as u64;

                    if buf.last() == Some(&b'\n') { buf.pop(); }
                    if buf.last() == Some(&b'\r') { buf.pop(); }

                    if buf.is_empty() { continue; }

                    let line = &buf;
                    let row = (|| -> Option<_> {
                        let mut cols = line.split(|&b| b == delim);
                        #file_parse_stmts
                        Some(#update_expr)
                    })();
                    if let Some(row) = row {
                        self.h_mut().update(row, diff);
                    }
                }
                if index == 0 {
                    println!("{:?}:\tData loaded for {}", load_start.elapsed(), #name);
                }
            }

            #apply_inline_impl

            fn advance_to(&mut self, t: Ts) {
                self.h_mut().advance_to(t);
            }

            fn flush(&mut self) {
                self.h_mut().flush();
            }

            fn close(&mut self) {
                if let Some(h) = self.h.take() {
                    h.close();
                }
            }
        }
    }
}

// ------------------------------------------------------------
// Inline fact code generation
// ------------------------------------------------------------

fn gen_inline_facts(facts: Option<&Vec<Vec<ConstType>>>, string_intern: bool) -> TokenStream {
    let Some(rows) = facts else {
        return quote! {};
    };
    if rows.is_empty() {
        return quote! {};
    }

    let tuples: Vec<TokenStream> = rows
        .iter()
        .map(|vals| {
            let elems: Vec<TokenStream> = vals
                .iter()
                .map(|c| match c {
                    ConstType::Int(i) => {
                        let lit = proc_macro2::Literal::i64_unsuffixed(*i);
                        quote! { #lit }
                    }
                    ConstType::Float(v) => {
                        let lit = proc_macro2::Literal::f64_unsuffixed(v.into_inner());
                        quote! { OrderedFloat(#lit) }
                    }
                    ConstType::Text(s) => {
                        if string_intern {
                            quote! { intern(#s) }
                        } else {
                            quote! { #s.to_string() }
                        }
                    }
                    ConstType::Bool(b) => quote! { #b },
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

    quote! {
        for row in [ #(#tuples),* ] {
            self.h_mut().update(row, SEMIRING_ONE);
        }
    }
}

// ------------------------------------------------------------
// Type + parsing helpers
// ------------------------------------------------------------

/// Parse from `it: Iterator<Item=&str>` into f0..f{n-1}.
fn gen_parse_from_str(rel: &str, dts: &[DataType], string_intern: bool) -> TokenStream {
    let mut stmts = Vec::<TokenStream>::new();

    for (i, dt) in dts.iter().enumerate() {
        let v = format_ident!("f{i}");
        let idx = i;

        let get = quote! {
            let s = match it.next() {
                Some(s) => s,
                None => {
                    eprintln!("[relation][{}] bad tuple '{}': missing col {}", #rel, tuple, #idx);
                    return;
                }
            };
        };

        let parse = match *dt {
            DataType::Int8 => quote! {
                #get
                let #v: i8 = match s.parse::<i8>() {
                    Ok(v) => v,
                    Err(_) => {
                        eprintln!("[relation][{}] bad tuple '{}': col {} not i8: '{}'", #rel, tuple, #idx, s);
                        return;
                    }
                };
            },
            DataType::Int16 => quote! {
                #get
                let #v: i16 = match s.parse::<i16>() {
                    Ok(v) => v,
                    Err(_) => {
                        eprintln!("[relation][{}] bad tuple '{}': col {} not i16: '{}'", #rel, tuple, #idx, s);
                        return;
                    }
                };
            },
            DataType::Int32 => quote! {
                #get
                let #v: i32 = match s.parse::<i32>() {
                    Ok(v) => v,
                    Err(_) => {
                        eprintln!("[relation][{}] bad tuple '{}': col {} not i32: '{}'", #rel, tuple, #idx, s);
                        return;
                    }
                };
            },
            DataType::Int64 => quote! {
                #get
                let #v: i64 = match s.parse::<i64>() {
                    Ok(v) => v,
                    Err(_) => {
                        eprintln!("[relation][{}] bad tuple '{}': col {} not i64: '{}'", #rel, tuple, #idx, s);
                        return;
                    }
                };
            },
            DataType::UInt8 => quote! {
                #get
                let #v: u8 = match s.parse::<u8>() {
                    Ok(v) => v,
                    Err(_) => {
                        eprintln!("[relation][{}] bad tuple '{}': col {} not u8: '{}'", #rel, tuple, #idx, s);
                        return;
                    }
                };
            },
            DataType::UInt16 => quote! {
                #get
                let #v: u16 = match s.parse::<u16>() {
                    Ok(v) => v,
                    Err(_) => {
                        eprintln!("[relation][{}] bad tuple '{}': col {} not u16: '{}'", #rel, tuple, #idx, s);
                        return;
                    }
                };
            },
            DataType::UInt32 => quote! {
                #get
                let #v: u32 = match s.parse::<u32>() {
                    Ok(v) => v,
                    Err(_) => {
                        eprintln!("[relation][{}] bad tuple '{}': col {} not u32: '{}'", #rel, tuple, #idx, s);
                        return;
                    }
                };
            },
            DataType::UInt64 => quote! {
                #get
                let #v: u64 = match s.parse::<u64>() {
                    Ok(v) => v,
                    Err(_) => {
                        eprintln!("[relation][{}] bad tuple '{}': col {} not u64: '{}'", #rel, tuple, #idx, s);
                        return;
                    }
                };
            },
            DataType::Float32 => quote! {
                #get
                let #v: OrderedFloat<f32> = match s.parse::<f32>() {
                    Ok(v) => OrderedFloat(v),
                    Err(_) => {
                        eprintln!("[relation][{}] bad tuple '{}': col {} not f32: '{}'", #rel, tuple, #idx, s);
                        return;
                    }
                };
            },
            DataType::Float64 => quote! {
                #get
                let #v: OrderedFloat<f64> = match s.parse::<f64>() {
                    Ok(v) => OrderedFloat(v),
                    Err(_) => {
                        eprintln!("[relation][{}] bad tuple '{}': col {} not f64: '{}'", #rel, tuple, #idx, s);
                        return;
                    }
                };
            },
            DataType::String => {
                if string_intern {
                    quote! {
                        #get
                        let #v: Spur = intern(s);
                    }
                } else {
                    quote! {
                        #get
                        let #v: String = s.to_string();
                    }
                }
            }
            DataType::Bool => quote! {
                #get
                let #v: bool = match s.parse::<bool>() {
                    Ok(v) => v,
                    Err(_) => {
                        eprintln!("[relation][{}] bad tuple '{}': col {} not bool: '{}'", #rel, tuple, #idx, s);
                        return;
                    }
                };
            },
        };

        stmts.push(parse);
    }

    quote! { #(#stmts)* }
}

/// Parse from `cols: Iterator<Item=&[u8]>` into f0..f{n-1}.
fn gen_parse_from_bytes(rel: &str, dts: &[DataType], string_intern: bool) -> TokenStream {
    let mut stmts = Vec::<TokenStream>::new();

    for (i, dt) in dts.iter().enumerate() {
        let v = format_ident!("f{i}");
        let idx = i;

        let get_raw = quote! {
            let raw = match cols.next() {
                Some(b) => b,
                None => {
                    eprintln!(
                        "[relation][{}] bad row in {}: '{:?}' (missing col {})",
                        #rel,
                        path.display(),
                        String::from_utf8_lossy(&line),
                        #idx
                    );
                    return None;
                }
            };
        };

        let parse = match *dt {
            DataType::Int8 => parse_int_bytes(&v, quote! { i8 }, "i8", rel, idx, &get_raw),
            DataType::Int16 => parse_int_bytes(&v, quote! { i16 }, "i16", rel, idx, &get_raw),
            DataType::Int32 => parse_int_bytes(&v, quote! { i32 }, "i32", rel, idx, &get_raw),
            DataType::Int64 => parse_int_bytes(&v, quote! { i64 }, "i64", rel, idx, &get_raw),
            DataType::UInt8 => parse_int_bytes(&v, quote! { u8 }, "u8", rel, idx, &get_raw),
            DataType::UInt16 => parse_int_bytes(&v, quote! { u16 }, "u16", rel, idx, &get_raw),
            DataType::UInt32 => parse_int_bytes(&v, quote! { u32 }, "u32", rel, idx, &get_raw),
            DataType::UInt64 => parse_int_bytes(&v, quote! { u64 }, "u64", rel, idx, &get_raw),
            DataType::Float32 => quote! {
                #get_raw
                let s = match std::str::from_utf8(raw) {
                    Ok(s) => s.trim(),
                    Err(_) => {
                        eprintln!(
                            "[relation][{}] bad row in {}: '{:?}' (col {} not utf8)",
                            #rel, path.display(), String::from_utf8_lossy(&line), #idx
                        );
                        return None;
                    }
                };
                let #v: OrderedFloat<f32> = match s.parse::<f32>() {
                    Ok(v) => OrderedFloat(v),
                    Err(_) => {
                        eprintln!(
                            "[relation][{}] bad row in {}: '{:?}' (col {} not f32: '{}')",
                            #rel, path.display(), String::from_utf8_lossy(&line), #idx, s
                        );
                        return None;
                    }
                };
            },
            DataType::Float64 => quote! {
                #get_raw
                let s = match std::str::from_utf8(raw) {
                    Ok(s) => s.trim(),
                    Err(_) => {
                        eprintln!(
                            "[relation][{}] bad row in {}: '{:?}' (col {} not utf8)",
                            #rel, path.display(), String::from_utf8_lossy(&line), #idx
                        );
                        return None;
                    }
                };
                let #v: OrderedFloat<f64> = match s.parse::<f64>() {
                    Ok(v) => OrderedFloat(v),
                    Err(_) => {
                        eprintln!(
                            "[relation][{}] bad row in {}: '{:?}' (col {} not f64: '{}')",
                            #rel, path.display(), String::from_utf8_lossy(&line), #idx, s
                        );
                        return None;
                    }
                };
            },
            DataType::String => {
                let field_stmt = if string_intern {
                    quote! { let #v: Spur = intern(s); }
                } else {
                    quote! { let #v: String = s.to_string(); }
                };
                quote! {
                    #get_raw
                    let s = match std::str::from_utf8(raw) {
                        Ok(s) => s.trim(),
                        Err(_) => {
                            eprintln!(
                                "[relation][{}] bad row in {}: '{:?}' (col {} not utf8)",
                                #rel, path.display(), String::from_utf8_lossy(&line), #idx
                            );
                            return None;
                        }
                    };
                    #field_stmt
                }
            }
            DataType::Bool => quote! {
                #get_raw
                let s = match std::str::from_utf8(raw) {
                    Ok(s) => s.trim(),
                    Err(_) => {
                        eprintln!(
                            "[relation][{}] bad row in {}: '{:?}' (col {} not utf8)",
                            #rel, path.display(), String::from_utf8_lossy(&line), #idx
                        );
                        return None;
                    }
                };
                let #v: bool = match s.parse::<bool>() {
                    Ok(v) => v,
                    Err(_) => {
                        eprintln!(
                            "[relation][{}] bad row in {}: '{:?}' (col {} not bool: '{}')",
                            #rel, path.display(), String::from_utf8_lossy(&line), #idx, s
                        );
                        return None;
                    }
                };
            },
        };

        stmts.push(parse);
    }

    quote! { #(#stmts)* }
}

fn parse_int_bytes(
    v: &proc_macro2::Ident,
    ty: TokenStream,
    ty_name: &str,
    rel: &str,
    idx: usize,
    get_raw: &TokenStream,
) -> TokenStream {
    quote! {
        #get_raw
        let s = match std::str::from_utf8(raw) {
            Ok(s) => s.trim(),
            Err(_) => {
                eprintln!(
                    "[relation][{}] bad row in {}: '{:?}' (col {} not utf8)",
                    #rel, path.display(), String::from_utf8_lossy(&line), #idx
                );
                return None;
            }
        };
        let #v: #ty = match s.parse::<#ty>() {
            Ok(v) => v,
            Err(_) => {
                eprintln!(
                    "[relation][{}] bad row in {}: '{:?}' (col {} not {}: '{}')",
                    #rel, path.display(), String::from_utf8_lossy(&line), #idx, #ty_name, s
                );
                return None;
            }
        };
    }
}
