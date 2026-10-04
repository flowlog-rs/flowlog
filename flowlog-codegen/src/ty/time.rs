//! Time type codegen for the `(Data, Diff, Time)` triple, at the two levels
//! tabled in `flowlog_runtime::time`: the engine's outer timestamp, which is
//! program-wide, and each loop's inner one, which follows its mutability.

use flowlog_parser::Mutability;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;

impl Codegen {
    /// Returns `type Ts = ...;`, the engine's outer timestamp: `time::Epoch`
    /// when some input is mutable, else `time::Once`.
    pub(crate) fn outer_time_tokens(&self) -> TokenStream {
        if self.program.is_incremental() {
            quote! { type Ts = ::flowlog_runtime::time::Epoch; }
        } else {
            quote! { type Ts = ::flowlog_runtime::time::Once; }
        }
    }

    /// Returns the inner time of a loop whose heads have `mutability`, and
    /// the summary of its feedback edge. An engine that runs once loops at
    /// `OnceLoop`. An engine whose epochs advance loops a mutable loop at
    /// `EpochLoop` and a presence one, static or append, at `LexLoop`,
    /// whose total order the presence operators need.
    ///
    /// Returns an internal error for an append or mutable loop in an engine
    /// that runs once: no input there can change, so the planner cannot
    /// produce one.
    pub(crate) fn inner_time_tokens(
        &self,
        mutability: Mutability,
    ) -> Result<(TokenStream, TokenStream), CodegenError> {
        match (self.program.is_incremental(), mutability) {
            (false, Mutability::Static) => Ok((
                quote! { ::flowlog_runtime::time::OnceLoop },
                quote! { ::flowlog_runtime::timely::order::Product::new((), 1) },
            )),
            (false, Mutability::Append | Mutability::Mutable) => Err(CodegenError::internal(
                format!("a {mutability} loop in an engine without an input that changes"),
            )),
            (true, Mutability::Static | Mutability::Append) => Ok((
                quote! { ::flowlog_runtime::time::LexLoop },
                quote! { ::flowlog_runtime::time::LexLoop::NEXT_ITERATION },
            )),
            (true, Mutability::Mutable) => Ok((
                quote! { ::flowlog_runtime::time::EpochLoop },
                quote! { ::flowlog_runtime::timely::order::Product::new(0, 1) },
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::test_harness::codegen;

    /// A static input only, so the engine runs once.
    const ONCE: &str = ".decl R(x: int32)\n.input R\n.output R\n";
    /// An append input, so the engine's epochs advance.
    const EPOCHS: &str = ".decl R(x: int32) append\n.input R\n.output R\n";

    /// A loop's time follows its heads' mutability and the engine: presence
    /// loops of an advancing engine take the lexicographic time, a mutable
    /// loop the product, and a once engine loops at `OnceLoop`.
    // Cases: program, mutability, inner time.
    #[rstest]
    #[case(ONCE, Mutability::Static, ":: flowlog_runtime :: time :: OnceLoop")]
    #[case(EPOCHS, Mutability::Static, ":: flowlog_runtime :: time :: LexLoop")]
    #[case(EPOCHS, Mutability::Append, ":: flowlog_runtime :: time :: LexLoop")]
    #[case(EPOCHS, Mutability::Mutable, ":: flowlog_runtime :: time :: EpochLoop")]
    fn a_loops_time_follows_its_mutability_and_engine(
        #[case] program: &str,
        #[case] mutability: Mutability,
        #[case] expected: &str,
    ) {
        let (time, _) = codegen(program)
            .inner_time_tokens(mutability)
            .expect("a loop the engine admits");
        assert_eq!(time.to_string(), expected);
    }

    /// An engine that runs once has no input that changes, so a loop whose
    /// heads could change is a planner bug.
    #[rstest]
    #[case(Mutability::Append)]
    #[case(Mutability::Mutable)]
    fn a_changing_loop_in_a_once_engine_is_an_internal_error(#[case] mutability: Mutability) {
        let error = codegen(ONCE)
            .inner_time_tokens(mutability)
            .expect_err("no input of a once engine changes");
        assert!(matches!(error, CodegenError::Internal(_)));
    }
}
