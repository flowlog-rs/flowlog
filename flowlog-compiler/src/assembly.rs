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
        let input = self.gen_input(skeleton, &output.emit);
        let runtime_args = self.gen_runtime_args();
        let main_fn = if self.program.is_incremental() {
            inc::gen_incremental_main(skeleton, &input, &runtime_args, &output)
        } else {
            batch::gen_batch_main(skeleton, &input, &runtime_args, &output)
        };
        let declarations = &skeleton.declarations;

        flowlog_common::pretty_print(quote! {
            #imports
            #declarations
            #main_fn
        })
    }

    /// Returns the parse of the runtime arguments, with the compiled
    /// directories as defaults.
    fn gen_runtime_args(&self) -> TokenStream {
        let default_fact_dir = self.options.fact_dir().unwrap_or(".");
        let default_output_dir = if self.config.output_to_stdout() {
            "-"
        } else {
            self.options.output_dir().unwrap_or(".")
        };

        let fact_dir = self.program.has_file_inputs().then(|| quote! { fact_dir, });
        let output_dir = self.program.has_outputs().then(|| quote! { output_dir, });
        quote! {
            let ::flowlog_runtime::RuntimeArgs { config: timely_config, #fact_dir #output_dir .. } =
                ::flowlog_runtime::RuntimeArgs::from_env(#default_fact_dir, #default_output_dir);
        }
    }
}

/// Reports what the workers stopped on, each distinct message once, and
/// exits with status 1 on any, before output is written.
fn exit_on_worker_error() -> TokenStream {
    quote! {
        for result in guards.join() {
            match result {
                Ok(Ok(())) => {}
                // The worker recorded its error before returning it, so
                // the others could stop too; the set keeps one copy.
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
    use flowlog_common::Config;
    use flowlog_parser::test_harness::program;

    use super::*;
    use crate::CompileOptions;

    /// An incremental `main` closes the static inputs before its first step
    /// loop, which waits for the static outputs that only closed inputs
    /// complete.
    #[test]
    fn static_inputs_close_before_the_static_outputs_are_awaited() {
        let program = program(
            ".decl S(x: int32)\n.input S\n.decl M(x: int32) mutable\n.input M\n\
             .decl Out(x: int32)\nOut(x) :- S(x), M(x).\n.output Out\n",
        );
        let options = CompileOptions::new(
            "p.dl", None, None, None, None, None, /* check_only */ false,
        );
        let compiler = Compiler::new(Config::default(), options, program);
        let skeleton = Skeleton {
            declarations: TokenStream::new(),
            emitters: TokenStream::new(),
            emitter_captures: TokenStream::new(),
            worker_init: TokenStream::new(),
            dataflow: TokenStream::new(),
            step_loop: quote! { skeleton_step_loop(); },
            metrics_write: TokenStream::new(),
            publish: TokenStream::new(),
        };
        let main = compiler.assemble(&skeleton, &TokenStream::new());
        let close = main
            .find("inputs.close_static()")
            .expect("closes the static inputs");
        let step = main.find("skeleton_step_loop()").expect("steps");
        assert!(close < step, "{main}");
    }
}
