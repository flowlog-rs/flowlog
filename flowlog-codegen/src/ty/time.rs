//! Time type codegen for the `(Data, Diff, Time)` triple, at the two levels
//! tabled in `flowlog_runtime::time`: the clock of each step outside a
//! loop, which follows its mutability, and each loop's inner time, which
//! refines it.

use flowlog_parser::Mutability;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;

// =============================================================================
// Clock
// =============================================================================

/// The outer time a collection is computed at. A static collection never
/// changes, so every engine computes it once, in the dataflow's own scope at
/// `time::Once`. A collection that can change is recomputed each epoch, in
/// the scope an incremental engine nests at `time::Epoch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Clock {
    Once,
    Epoch,
}

impl Clock {
    /// Returns the clock of a collection with `mutability`.
    pub(crate) fn of(mutability: Mutability) -> Self {
        match mutability {
            Mutability::Static => Self::Once,
            Mutability::Append | Mutability::Mutable => Self::Epoch,
        }
    }

    /// Returns the runtime time type of the clock.
    pub(crate) fn time_tokens(self) -> TokenStream {
        match self {
            Self::Once => quote! { ::flowlog_runtime::time::Once },
            Self::Epoch => quote! { ::flowlog_runtime::time::Epoch },
        }
    }
}

// =============================================================================
// Engine and loop times
// =============================================================================

impl Codegen {
    /// Returns `type Ts = ...;`, the engine's time, which its driver
    /// advances and its outputs are emitted at: `time::Epoch` when some
    /// input can change, else `time::Once`.
    pub(crate) fn outer_time_tokens(&self) -> TokenStream {
        let time = self.engine_clock().time_tokens();
        quote! { type Ts = #time; }
    }

    /// Returns the clock of the engine's time: `Epoch` when some input can
    /// change, else `Once`.
    pub(crate) fn engine_clock(&self) -> Clock {
        if self.program.is_incremental() {
            Clock::Epoch
        } else {
            Clock::Once
        }
    }

    /// Returns the inner time of a loop whose heads have `mutability`, and
    /// the summary of its feedback edge. A static loop runs at its clock,
    /// `Once`, so it loops at `OnceLoop` in every engine. A loop that can
    /// change runs at `Epoch`: a mutable loop at `EpochLoop`, and an append
    /// one at `LexLoop`, whose total order the presence operators need.
    ///
    /// Returns an internal error for an append or mutable loop in an engine
    /// that runs once: no input there can change, so the planner cannot
    /// produce one.
    pub(crate) fn inner_time_tokens(
        &self,
        mutability: Mutability,
    ) -> Result<(TokenStream, TokenStream), CodegenError> {
        match (self.program.is_incremental(), mutability) {
            (_, Mutability::Static) => Ok((
                quote! { ::flowlog_runtime::time::OnceLoop },
                quote! { ::flowlog_runtime::timely::order::Product::new((), 1) },
            )),
            (false, Mutability::Append | Mutability::Mutable) => Err(CodegenError::internal(
                format!("a {mutability} loop in an engine without an input that changes"),
            )),
            (true, Mutability::Append) => Ok((
                quote! { ::flowlog_runtime::time::LexLoop },
                quote! { (0, 1) },
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

    /// A loop's time follows its heads' mutability: a static loop runs once
    /// in every engine, at `OnceLoop`; an append loop takes the
    /// lexicographic time, and a mutable loop the product.
    // Cases: program, mutability, inner time.
    #[rstest]
    #[case(ONCE, Mutability::Static, ":: flowlog_runtime :: time :: OnceLoop")]
    #[case(EPOCHS, Mutability::Static, ":: flowlog_runtime :: time :: OnceLoop")]
    #[case(EPOCHS, Mutability::Append, ":: flowlog_runtime :: time :: LexLoop")]
    #[case(EPOCHS, Mutability::Mutable, ":: flowlog_runtime :: time :: EpochLoop")]
    fn a_loops_time_follows_its_mutability(
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
