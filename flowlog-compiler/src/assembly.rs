//! Assembles the generated `main.rs`.

mod batch;
mod inc;

use flowlog_codegen::Skeleton;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Compiler;

impl Compiler {
    /// Returns the source of the executable's entry point. The executable
    /// parses its runtime arguments, starts the timely workers, builds the
    /// dataflow on each, loads the inputs from their files and inline facts,
    /// and writes the outputs once the derivation settles. With a mutable
    /// input it keeps the workers alive afterwards and runs the transaction
    /// shell, writing each committed epoch's changes.
    pub(crate) fn assemble(&self, skeleton: &Skeleton, imports: &TokenStream) -> String {
        let output = self.gen_output();
        let input = self.gen_input();
        let runtime_args = self.gen_runtime_args(skeleton.has_loops);
        let loops = gen_loops(skeleton.has_loops);
        let main_fn = if self.program.is_incremental() {
            inc::gen_incremental_main(skeleton, &input, &runtime_args, &loops, &output)
        } else {
            batch::gen_batch_main(skeleton, &input, &runtime_args, &loops, &output)
        };
        let declarations = &skeleton.declarations;

        flowlog_common::pretty_print(quote! {
            #imports
            #declarations
            #main_fn
        })
    }

    /// Returns the parse of the runtime arguments, with the compiled
    /// directories as defaults. It binds the recursion limit only for a
    /// dataflow that `has_loops`.
    fn gen_runtime_args(&self, has_loops: bool) -> TokenStream {
        let default_fact_dir = self.options.fact_dir().unwrap_or(".");
        let default_output_dir = if self.config.output_to_stdout() {
            "-"
        } else {
            self.options.output_dir().unwrap_or(".")
        };

        let fact_dir = self.program.has_file_inputs().then(|| quote! { fact_dir, });
        let output_dir = self.program.has_outputs().then(|| quote! { output_dir, });
        let recursion_limit = has_loops.then(|| quote! { recursion_limit, });
        quote! {
            let ::flowlog_runtime::RuntimeArgs {
                config: timely_config, #fact_dir #output_dir #recursion_limit ..
            } = ::flowlog_runtime::RuntimeArgs::from_env(#default_fact_dir, #default_output_dir);
        }
    }
}

/// The fragments that pass a dataflow's `loops` in, at the recursion limit
/// of the runtime arguments, and pass their warnings on: all empty for a
/// dataflow without loops, which never names them.
#[derive(Default)]
struct Loops {
    /// After `failure`, in which the loops record an error.
    declare: TokenStream,
    /// Among the worker closure's captures.
    capture: TokenStream,
    /// Once results are out: each warning, on stderr.
    warn: TokenStream,
}

fn gen_loops(has_loops: bool) -> Loops {
    if !has_loops {
        return Loops::default();
    }
    Loops {
        declare: quote! {
            let loops = ::flowlog_runtime::Loops::new(recursion_limit, &failure);
        },
        capture: quote! { let loops = loops.clone(); },
        warn: quote! {
            for warning in loops.warnings() {
                eprintln!("warning: {warning}");
            }
        },
    }
}

/// Reports what the workers stopped on, each distinct message once, and
/// exits with status 1 on any, before output is written.
fn exit_on_worker_error() -> TokenStream {
    quote! {
        for result in guards.join() {
            match result {
                Ok(Ok(())) => {}
                // A worker may have recorded its error before returning
                // it, so the others could stop too; the set keeps one copy.
                Ok(Err(error)) => failure.record(error.to_string()),
                Err(_) => failure.record("a worker thread panicked".to_string()),
            }
        }
        let messages = failure.messages();
        if !messages.is_empty() {
            for message in &messages {
                eprintln!("{message}");
            }
            std::process::exit(1);
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use flowlog_codegen::output_emitter_ident;
    use flowlog_common::Config;
    use flowlog_parser::test_harness::program;
    use rstest::rstest;

    use super::*;
    use crate::CompileOptions;

    /// The `main` of `source` around a stub skeleton, whose step loop is
    /// `skeleton_step_loop()`.
    fn assemble(source: &str, has_loops: bool) -> String {
        let options = CompileOptions::new(
            "p.dl", None, None, None, None, None, /* check_only */ false,
        );
        let compiler = Compiler::new(Config::default(), options, program(source));
        let skeleton = Skeleton {
            declarations: TokenStream::new(),
            emitters: TokenStream::new(),
            emitter_captures: TokenStream::new(),
            worker_init: TokenStream::new(),
            dataflow: TokenStream::new(),
            has_loops,
            step_loop: quote! { skeleton_step_loop(); },
            metrics_write: TokenStream::new(),
            publish: TokenStream::new(),
        };
        compiler.assemble(&skeleton, &TokenStream::new())
    }

    /// An incremental `main` closes the static inputs before its first step
    /// loop, which waits for the static outputs that only closed inputs
    /// complete.
    #[test]
    fn static_inputs_close_before_the_static_outputs_are_awaited() {
        let main = assemble(
            ".decl S(x: int32)\n.input S\n.decl M(x: int32) mutable\n.input M\n\
             .decl Out(x: int32)\nOut(x) :- S(x), M(x).\n.output Out\n",
            false,
        );
        let close = main
            .find("inputs.close_static()")
            .expect("closes the static inputs");
        let step = main.find("skeleton_step_loop()").expect("steps");
        assert!(close < step, "{main}");
    }

    /// A `main` whose dataflow has loops reads the recursion limit, and
    /// warns once the results of its run, or of each commit, are out; a
    /// `main` without loops never names them.
    #[rstest]
    fn only_a_main_with_loops_warns_after_its_results(
        #[values(false, true)] incremental: bool,
        #[values(false, true)] has_loops: bool,
    ) {
        let mutable = if incremental { " mutable" } else { "" };
        let main = assemble(
            &format!(
                ".decl e(x: int32){mutable}\n.input e\n\
                 .decl out(x: int32)\nout(x) :- e(x).\n.output out\n"
            ),
            has_loops,
        );
        assert_eq!(main.contains("recursion_limit"), has_loops, "{main}");
        assert_eq!(main.contains("loops"), has_loops, "{main}");
        if has_loops {
            let emitter = output_emitter_ident("out").to_string();
            let emit = main.rfind(&emitter).expect("emits the results");
            let warn = main.find("loops.warnings()").expect("warns");
            assert!(emit < warn, "{main}");
        }
    }
}
