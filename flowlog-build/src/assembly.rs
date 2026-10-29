//! Library-mode assembly: [`assemble`] renders a [`Pipeline`]'s artifacts as
//! the source to write to `$OUT_DIR/<stem>.rs`. The file exposes its public
//! API at the top level through a `pub use __flowlog_gen::*;` re-export;
//! [`assemble`] says why.

use std::io;
use std::path::Path;

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

    let lib_imports = gen_lib_imports(&pipeline.relations, config.profiling_enabled());
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
    let udf_mod = gen_udf_mod(
        !pipeline.program.udfs().is_empty(),
        config.udf_file().map(Path::new),
    )?;

    // `include!()` forbids inner attributes at the call site, so the whole
    // body lives in an inner module carrying the `#[allow(..)]`, then a
    // top-level `pub use` re-exports the user-visible API. Per
    // docs/dev/code.md rule 8, the allowed lints are only those the user's
    // program causes, which codegen cannot satisfy.
    Ok(pretty_print(quote! {
        pub use __flowlog_gen::*;

        #[allow(
            // Names and shapes from the program's declarations.
            non_camel_case_types,
            non_snake_case,
            clippy::disallowed_names,
            clippy::type_complexity,
            // Comparisons the program writes, emitted as written: `u >= 0`
            // on a `uint64` always holds.
            unused_comparisons,
            clippy::absurd_extreme_comparisons,
        )]
        mod __flowlog_gen {
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
fn gen_udf_mod(has_udfs: bool, udf_file: Option<&Path>) -> io::Result<TokenStream> {
    if !has_udfs {
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

    // The user's file may define functions this program does not call.
    Ok(quote! {
        #[allow(dead_code)]
        #[path = #path_lit]
        mod udf;
    })
}
