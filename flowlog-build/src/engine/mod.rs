//! Library engine generation and scheduling.
//!
//! Batch and incremental engines stage host rows and control workers.
//! Runtime loaders ingest inputs; emitters construct typed output results.

mod batch;
mod incremental;

pub(crate) use batch::gen_lib_engine;
pub(crate) use incremental::gen_lib_incremental_engine;

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use flowlog_codegen::Skeleton;
    use flowlog_parser::test_harness::program;
    use proc_macro2::TokenStream;
    use quote::quote;
    use rstest::rstest;

    use super::*;

    /// A stub skeleton, whose step loop is `skeleton_step_loop()`.
    pub(super) fn skeleton(has_loops: bool) -> Skeleton {
        Skeleton {
            declarations: TokenStream::new(),
            emitters: TokenStream::new(),
            emitter_captures: TokenStream::new(),
            worker_init: TokenStream::new(),
            dataflow: TokenStream::new(),
            has_loops,
            step_loop: quote! { skeleton_step_loop(); },
            metrics_write: TokenStream::new(),
            publish: TokenStream::new(),
        }
    }

    /// Either engine returns its loops' warnings, and passes the loops to
    /// its workers only for a dataflow with loops, which alone names them.
    #[rstest]
    fn only_an_engine_with_loops_passes_them_to_its_workers(
        #[values(false, true)] incremental: bool,
        #[values(false, true)] has_loops: bool,
    ) {
        let mutable = if incremental { " mutable" } else { "" };
        let program = program(&format!(
            ".decl E(x: int32){mutable}\n.input E\n\
             .decl Out(x: int32)\nOut(x) :- E(x).\n.output Out\n"
        ));
        let skeleton = skeleton(has_loops);
        let engine = if incremental {
            gen_lib_incremental_engine(&program, /* uses_ord */ false, &skeleton)
        } else {
            gen_lib_engine(&program, /* uses_ord */ false, &skeleton)
        }
        .to_string();
        assert!(engine.contains("warnings : "), "{engine}");
        assert!(engine.contains("loops . warnings ()"), "{engine}");
        assert_eq!(
            engine.contains("let loops = loops . clone ()"),
            has_loops,
            "{engine}"
        );
    }
}
