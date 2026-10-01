//! Library-mode assembly: [`assemble`] renders a [`Pipeline`]'s artifacts as
//! the source to write to `$OUT_DIR/<stem>.rs`. The file exposes its public
//! API at the top level through a `pub use __flowlog_gen::*;` re-export;
//! [`assemble`] says why.

use std::io;
use std::path::Path;

use flowlog_codegen::Features;
use flowlog_common::pretty_print;
use proc_macro2::TokenStream;
use quote::quote;

use crate::bindings::gen_public_rel_module;
use crate::engine::gen_lib_engine;
use crate::engine::gen_lib_incremental_engine;
use crate::imports::gen_lib_imports;
use crate::pipeline::Pipeline;
use crate::results::gen_batch_results;
use crate::results::gen_incremental_results;

/// Returns the library-mode source file of one compiled program, or an
/// error when the program's UDF file is missing or cannot be resolved.
pub(crate) fn assemble(pipeline: &Pipeline) -> io::Result<String> {
    let config = &pipeline.config;

    let lib_imports = gen_lib_imports(
        &pipeline.relations,
        &pipeline.features,
        config.profiling_enabled(),
    );
    let declarations = &pipeline.skeleton.declarations;
    let rel_module = gen_public_rel_module(&pipeline.program);
    let (results_struct, lib_engine) = if pipeline.program.is_incremental() {
        (
            gen_incremental_results(&pipeline.program),
            gen_lib_incremental_engine(
                &pipeline.program,
                config.serialize_load(),
                &pipeline.skeleton,
            ),
        )
    } else {
        (
            gen_batch_results(&pipeline.program),
            gen_lib_engine(
                &pipeline.program,
                config.serialize_load(),
                &pipeline.skeleton,
            ),
        )
    };
    let udf_mod = gen_udf_mod(&pipeline.features, config.udf_file().map(Path::new))?;

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
            #lib_imports
            #declarations
            #rel_module
            #results_struct
            #udf_mod
            #lib_engine
        }
    }))
}

/// Returns `#[path = "..."] mod udf;` when the program declares `.extern fn`,
/// pointing at the user-supplied UDF source file, and nothing otherwise. The generated code calls
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
            "program uses `.extern fn` but no UDF file was configured; \
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
