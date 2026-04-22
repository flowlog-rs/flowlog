//! Stitch the library-mode `.rs` file together from pipeline artifacts.
//!
//! Takes the [`Pipeline`] produced by [`crate::pipeline`] and emits the
//! final source as a string ready to write to `$OUT_DIR/<stem>.rs`. The
//! emitted file exposes its public API at the top level via a
//! `pub use __flowlog_gen::*;` re-export — see [`assemble`] for why.

use std::io;
use std::path::Path;

use proc_macro2::TokenStream;
use quote::quote;

use crate::common::pretty_print;

use crate::codegen::Features;
use crate::build::engine::gen_lib_engine;
use crate::build::imports::gen_lib_imports;
use crate::build::pipeline::Pipeline;
use crate::build::relation::user::gen_public_rel_module;
use crate::build::results::gen_batch_results;

/// Render the library-mode source file for one compiled program.
pub(crate) fn assemble(
    pipeline: &Pipeline,
    out_dir: &Path,
    udf_file: Option<&Path>,
) -> io::Result<String> {
    let string_intern = pipeline.features.string_intern();

    let semiring_mod = gen_semiring_mod(pipeline, out_dir);
    let lib_imports = gen_lib_imports(&pipeline.relations, &pipeline.features);
    let type_declarations = &pipeline.parts.type_declarations;
    let rel_module = gen_public_rel_module(&pipeline.program);
    let batch_results = gen_batch_results(&pipeline.program);
    let lib_engine = gen_lib_engine(&pipeline.program, string_intern, &pipeline.parts);
    let udf_mod = gen_udf_mod(&pipeline.features, udf_file)?;

    // `include!()` forbids inner attributes at the call site, so the whole
    // body lives in an inner module carrying a blanket `#[allow(..)]`, then
    // a top-level `pub use` re-exports the user-visible API. This keeps
    // warnings on unused generated items from leaking into the consumer
    // crate.
    Ok(pretty_print(quote! {
        pub use __flowlog_gen::*;

        #[allow(
            dead_code,
            unused_imports,
            unused_variables,
            unused_mut,
            non_camel_case_types,
            non_snake_case,
            clippy::all,
        )]
        mod __flowlog_gen {
            use ::flowlog_runtime::differential_dataflow;
            use ::flowlog_runtime::timely;
            use ::flowlog_runtime::serde;
            use ::flowlog_runtime::ordered_float;
            #semiring_mod
            #lib_imports
            #type_declarations
            #rel_module
            #batch_results
            #udf_mod
            #lib_engine
        }
    }))
}

/// Emit `#[path = "..."] mod udf;` when the program declares `.extern fn`,
/// pointing at the user-supplied UDF source file. The generated code calls
/// UDFs as `udf::<name>(..)`, so this module sits as a sibling of the
/// engine inside `__flowlog_gen`.
///
/// `#[path]` (rather than inlining the source) is deliberate: it preserves
/// the user's file and line numbers in compiler errors.
fn gen_udf_mod(features: &Features, udf_file: Option<&Path>) -> io::Result<TokenStream> {
    if !features.udf() {
        return Ok(quote! {});
    }

    let path = udf_file.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "program uses `.extern fn` but no UDF file was configured — \
             call `Builder::udf_file(..)` with the path to your UDF impls",
        )
    })?;
    let abs = path.canonicalize().map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("failed to resolve UDF file '{}': {e}", path.display()),
        )
    })?;
    let path_lit = abs.to_string_lossy().into_owned();

    Ok(quote! {
        #[path = #path_lit]
        mod udf;
    })
}

/// Emit `#[path = "..."] mod semiring;` when the program needs any
/// aggregation semiring module. The module file is written out separately
/// by [`crate::Builder::emit_semiring_modules`].
fn gen_semiring_mod(pipeline: &Pipeline, out_dir: &Path) -> TokenStream {
    if pipeline.parts.semiring_modules.is_empty() {
        return quote! {};
    }
    let mod_path = out_dir.join("semiring").join("mod.rs");
    let mod_path_str = mod_path.to_string_lossy().into_owned();
    quote! {
        #[path = #mod_path_str]
        mod semiring;
    }
}
