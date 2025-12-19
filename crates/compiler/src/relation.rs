use parser::{DataType, Relation};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use super::Compiler;

impl Compiler {
    /// Render `src/relops.rs` for incremental mode.
    ///
    /// - Nullary relations handled by a dedicated generator (no parsing; worker0 only).
    /// - Non-nullary relations:
    ///   - No panics: parse/open errors print to stderr and skip.
    ///   - Shard by first column (int mod peers; string FNV-1a).
    ///   - Delimiter is `rel.input_delimiter()` (1 byte, default comma).
    pub(crate) fn render_relops(&self, edbs: Vec<&Relation>) -> String {
        let rel_impls: Vec<TokenStream> = edbs
            .iter()
            .map(|rel| {
                if rel.arity() == 0 {
                    gen_one_rel_nullary(rel)
                } else {
                    gen_one_rel_nonnullary(rel)
                }
            })
            .collect();

        let file = quote! {
            // relops.rs
            //
            // Dynamic relation handlers for input sessions (dynbox).
            // Each relation decides how to parse tuples/files and how to shard (by first column).

            use differential_dataflow::input::InputSession;

            use std::fs::File;
            use std::io::{BufRead, BufReader};
            use std::path::Path;

            type Diff = isize;

            pub(crate) trait RelOps {
                fn apply_tuple(&mut self, tuple: &str, diff: Diff, peers: usize, index: usize);
                fn apply_file(&mut self, path: &Path, diff: Diff, peers: usize, index: usize);
                fn advance_to(&mut self, t: u32);
                fn flush(&mut self);
                fn close(&mut self);
            }

            #[allow(dead_code)]
            #[inline]
            fn shard_i32(first: i32, peers: usize, index: usize) -> bool {
                (first as usize) % peers == index
            }

            #[allow(dead_code)]
            #[inline]
            fn shard_str(first: &str, peers: usize, index: usize) -> bool {
                // 32-bit FNV-1a
                let mut hash: u32 = 0x811c9dc5;
                for &b in first.as_bytes() {
                    hash ^= b as u32;
                    hash = hash.wrapping_mul(0x01000193);
                }
                (hash as usize) % peers == index
            }

            #(#rel_impls)*
        };

        let syntax: syn::File = syn::parse2(file).unwrap();
        prettyplease::unparse(&syntax)
    }
}

// ------------------------------------------------------------
// Per-relation generators
// ------------------------------------------------------------

fn gen_one_rel_nullary(rel: &Relation) -> TokenStream {
    let name = rel.name();
    let struct_name = format_ident!("Rel{}", name);

    quote! {
        pub(crate) struct #struct_name {
            h: Option<InputSession<u32, (), Diff>>,
        }

        impl #struct_name {
            pub fn new(h: InputSession<u32, (), Diff>) -> Self {
                Self { h: Some(h) }
            }

            #[inline]
            fn h_mut(&mut self) -> &mut InputSession<u32, (), Diff> {
                self.h.as_mut().unwrap()
            }
        }

        impl RelOps for #struct_name {
            fn apply_tuple(&mut self, _tuple: &str, diff: Diff, _peers: usize, index: usize) {
                // Nullary: apply once (avoid multiplying diffs across workers).
                if index != 0 { return; }
                self.h_mut().update((), diff);
            }

            fn apply_file(&mut self, path: &Path, diff: Diff, _peers: usize, index: usize) {
                // Nullary: if file has any non-empty line, update once on worker0.
                if index != 0 { return; }

                let f = match File::open(path) {
                    Ok(f) => f,
                    Err(e) => {
                        eprintln!("[relops][{}] failed to open {}: {}", #name, path.display(), e);
                        return;
                    }
                };
                let reader = BufReader::new(f);

                for line in reader.split(b'\n').filter_map(Result::ok) {
                    if !line.is_empty() {
                        self.h_mut().update((), diff);
                        break;
                    }
                }
            }

            fn advance_to(&mut self, t: u32) {
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

fn gen_one_rel_nonnullary(rel: &Relation) -> TokenStream {
    let name = rel.name();
    let struct_name = format_ident!("Rel{}", name);

    let arity = rel.arity();
    debug_assert!(arity > 0);

    let dts = rel.data_type();

    let delim_byte: u8 = rel
        .input_delimiter()
        .as_bytes()
        .get(0)
        .copied()
        .unwrap_or(b',');

    // tuple type: (T0,), (T0,T1,...)
    let rust_tys: Vec<TokenStream> = dts.iter().map(dt_to_rust).collect();
    let tuple_ty: TokenStream = match arity {
        1 => {
            let t0 = &rust_tys[0];
            quote! { (#t0,) }
        }
        _ => quote! { (#(#rust_tys),*) },
    };

    // shard decision (tuple path uses `return;`, file path uses `return None;`)
    let shard_tuple = match dts[0] {
        DataType::Integer => quote! { if !shard_i32(f0, peers, index) { return; } },
        DataType::String => quote! { if !shard_str(f0.as_str(), peers, index) { return; } },
    };
    let shard_file = match dts[0] {
        DataType::Integer => quote! { if !shard_i32(f0, peers, index) { return None; } },
        DataType::String => quote! { if !shard_str(f0.as_str(), peers, index) { return None; } },
    };

    let tuple_parse_stmts = gen_parse_from_str(name, &dts);
    let file_parse_stmts = gen_parse_from_bytes(name, &dts);

    let update_expr = match arity {
        1 => quote! { (f0,) },
        _ => {
            let vars: Vec<_> = (0..arity).map(|i| format_ident!("f{i}")).collect();
            quote! { (#(#vars),*) }
        }
    };

    quote! {
        pub(crate) struct #struct_name {
            h: Option<InputSession<u32, #tuple_ty, Diff>>,
            delim: u8,
        }

        impl #struct_name {
            pub fn new(h: InputSession<u32, #tuple_ty, Diff>) -> Self {
                Self { h: Some(h), delim: #delim_byte }
            }

            #[inline]
            fn h_mut(&mut self) -> &mut InputSession<u32, #tuple_ty, Diff> {
                self.h.as_mut().unwrap()
            }
        }

        impl RelOps for #struct_name {
            fn apply_tuple(&mut self, tuple: &str, diff: Diff, peers: usize, index: usize) {
                let tuple = tuple.trim();
                let delim = self.delim as char;
                let mut it = tuple.split(delim).map(|s| s.trim());

                #tuple_parse_stmts

                #shard_tuple
                self.h_mut().update(#update_expr, diff);
            }

            fn apply_file(&mut self, path: &Path, diff: Diff, peers: usize, index: usize) {
                let f = match File::open(path) {
                    Ok(f) => f,
                    Err(e) => {
                        eprintln!("[relops][{}] failed to open {}: {}", #name, path.display(), e);
                        return;
                    }
                };
                let reader = BufReader::new(f);
                let delim = self.delim;

                // optimized: build ingest iterator, then apply updates
                let ingest = reader
                    .split(b'\n')
                    .filter_map(Result::ok)
                    .filter(|line| !line.is_empty())
                    .filter_map(|line| {
                        let mut cols = line.split(|&b| b == delim);

                        #file_parse_stmts

                        #shard_file
                        Some(#update_expr)
                    });

                for row in ingest {
                    self.h_mut().update(row, diff);
                }
            }

            fn advance_to(&mut self, t: u32) {
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
// Type + parsing helpers (codegen helpers)
// ------------------------------------------------------------

fn dt_to_rust(dt: &DataType) -> TokenStream {
    match *dt {
        DataType::Integer => quote! { i32 },
        DataType::String => quote! { String },
    }
}

/// Parse from `it: Iterator<Item=&str>` into f0..f{n-1}.
/// On error: eprintln + return;
fn gen_parse_from_str(rel: &str, dts: &[DataType]) -> TokenStream {
    let mut stmts = Vec::<TokenStream>::new();

    for (i, dt) in dts.iter().enumerate() {
        let v = format_ident!("f{i}");
        let idx = i;

        let get = quote! {
            let s = match it.next() {
                Some(s) => s,
                None => {
                    eprintln!("[relops][{}] bad tuple '{}': missing col {}", #rel, tuple, #idx);
                    return;
                }
            };
        };

        let parse = match *dt {
            DataType::Integer => quote! {
                #get
                let #v: i32 = match s.parse::<i32>() {
                    Ok(v) => v,
                    Err(_) => {
                        eprintln!("[relops][{}] bad tuple '{}': col {} not i32: '{}'", #rel, tuple, #idx, s);
                        return;
                    }
                };
            },
            DataType::String => quote! {
                #get
                let #v: String = s.to_string();
            },
        };

        stmts.push(parse);
    }

    quote! { #(#stmts)* }
}

/// Parse from `cols: Iterator<Item=&[u8]>` into f0..f{n-1}.
/// This runs inside `filter_map(|line| { ... })`, so errors return None.
fn gen_parse_from_bytes(rel: &str, dts: &[DataType]) -> TokenStream {
    let mut stmts = Vec::<TokenStream>::new();

    for (i, dt) in dts.iter().enumerate() {
        let v = format_ident!("f{i}");
        let idx = i;

        let get_raw = quote! {
            let raw = match cols.next() {
                Some(b) => b,
                None => {
                    eprintln!(
                        "[relops][{}] bad row in {}: '{:?}' (missing col {})",
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
            DataType::Integer => quote! {
                #get_raw
                let s = match std::str::from_utf8(raw) {
                    Ok(s) => s.trim(),
                    Err(_) => {
                        eprintln!(
                            "[relops][{}] bad row in {}: '{:?}' (col {} not utf8)",
                            #rel,
                            path.display(),
                            String::from_utf8_lossy(&line),
                            #idx
                        );
                        return None;
                    }
                };
                let #v: i32 = match s.parse::<i32>() {
                    Ok(v) => v,
                    Err(_) => {
                        eprintln!(
                            "[relops][{}] bad row in {}: '{:?}' (col {} not i32: '{}')",
                            #rel,
                            path.display(),
                            String::from_utf8_lossy(&line),
                            #idx,
                            s
                        );
                        return None;
                    }
                };
            },
            DataType::String => quote! {
                #get_raw
                let s = match std::str::from_utf8(raw) {
                    Ok(s) => s.trim(),
                    Err(_) => {
                        eprintln!(
                            "[relops][{}] bad row in {}: '{:?}' (col {} not utf8)",
                            #rel,
                            path.display(),
                            String::from_utf8_lossy(&line),
                            #idx
                        );
                        return None;
                    }
                };
                let #v: String = s.to_string();
            },
        };

        stmts.push(parse);
    }

    quote! { #(#stmts)* }
}
