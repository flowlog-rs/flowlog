//! Time type codegen for the `(Data, Diff, Time)` triple, at the two levels
//! tabled in `flowlog_runtime::time`: the clock of each step outside a
//! loop, which follows its mutability, and each loop's inner time, which
//! refines it.

use flowlog_parser::Mutability;
use flowlog_profiler::Order;
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
    #[must_use]
    pub(crate) fn from_mutability(mutability: Mutability) -> Self {
        match mutability {
            Mutability::Static => Self::Once,
            Mutability::Append | Mutability::Mutable => Self::Epoch,
        }
    }

    /// Returns the runtime time type of the clock.
    #[must_use]
    pub(crate) fn time_tokens(self) -> TokenStream {
        match self {
            Self::Once => quote! { ::flowlog_runtime::time::Once },
            Self::Epoch => quote! { ::flowlog_runtime::time::Epoch },
        }
    }
}

// =============================================================================
// LoopTime
// =============================================================================

/// The inner time of a loop: its clock refined with an iteration counter,
/// in the order the loop's mutability allows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LoopTime {
    /// `time::OnceLoop`: the iterations of a static loop, which every
    /// engine runs once.
    Once,
    /// `time::LexLoop`: epoch first, then iteration, for a loop whose
    /// collections never lose a row.
    Lex,
    /// `time::EpochLoop`: an epoch and an iteration, ordered only when
    /// both agree.
    Epoch,
}

impl LoopTime {
    /// Returns the runtime time type of the loop.
    #[must_use]
    pub(crate) fn time_tokens(self) -> TokenStream {
        match self {
            Self::Once => quote! { ::flowlog_runtime::time::OnceLoop },
            Self::Lex => quote! { ::flowlog_runtime::time::LexLoop },
            Self::Epoch => quote! { ::flowlog_runtime::time::EpochLoop },
        }
    }

    /// Returns how the loop's times are ordered: totally for every loop
    /// time but `EpochLoop`. An operator that reads changes in time order
    /// needs a total one.
    #[must_use]
    pub(crate) fn order(self) -> Order {
        match self {
            Self::Once | Self::Lex => Order::Total,
            Self::Epoch => Order::Partial,
        }
    }
}

/// Returns how the times of a step are ordered, given the inner time of
/// the loop it runs in, or `None` outside any loop, where it runs at its
/// clock, `Once` or `Epoch`, and both are total.
#[must_use]
pub(crate) fn time_order(loop_time: Option<LoopTime>) -> Order {
    loop_time.map_or(Order::Total, LoopTime::order)
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
    #[must_use]
    pub(crate) fn engine_clock(&self) -> Clock {
        if self.program.is_incremental() {
            Clock::Epoch
        } else {
            Clock::Once
        }
    }

    /// Returns the inner time of a loop whose heads have `mutability`. A
    /// static loop runs at its clock, `Once`, so it loops at `OnceLoop` in
    /// every engine. A loop that can change runs at `Epoch`: a mutable loop
    /// at `EpochLoop`, and an append one at `LexLoop`, whose total order the
    /// presence operators need.
    ///
    /// Returns an internal error for an append or mutable loop in an engine
    /// that runs once: no input there can change, so the planner cannot
    /// produce one.
    pub(crate) fn loop_time(&self, mutability: Mutability) -> Result<LoopTime, CodegenError> {
        match (self.engine_clock(), mutability) {
            (Clock::Once | Clock::Epoch, Mutability::Static) => Ok(LoopTime::Once),
            (Clock::Once, Mutability::Append | Mutability::Mutable) => Err(CodegenError::internal(
                format!("a {mutability} loop in an engine without an input that changes"),
            )),
            (Clock::Epoch, Mutability::Append) => Ok(LoopTime::Lex),
            (Clock::Epoch, Mutability::Mutable) => Ok(LoopTime::Epoch),
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
    /// lexicographic time, and a mutable loop the product, the one time
    /// whose pairs are not all ordered.
    // Cases: program, mutability, (inner time, order).
    #[rstest]
    #[case(
        ONCE,
        Mutability::Static,
        (quote! { ::flowlog_runtime::time::OnceLoop }, Order::Total)
    )]
    #[case(
        EPOCHS,
        Mutability::Static,
        (quote! { ::flowlog_runtime::time::OnceLoop }, Order::Total)
    )]
    #[case(
        EPOCHS,
        Mutability::Append,
        (quote! { ::flowlog_runtime::time::LexLoop }, Order::Total)
    )]
    #[case(
        EPOCHS,
        Mutability::Mutable,
        (quote! { ::flowlog_runtime::time::EpochLoop }, Order::Partial)
    )]
    fn a_loops_time_follows_its_mutability(
        #[case] program: &str,
        #[case] mutability: Mutability,
        #[case] expected: (TokenStream, Order),
    ) {
        let time = codegen(program)
            .loop_time(mutability)
            .expect("a loop the engine admits");
        let (inner_time, order) = expected;
        assert_eq!(
            (time.time_tokens().to_string(), time.order()),
            (inner_time.to_string(), order),
        );
    }

    /// An engine that runs once has no input that changes, so a loop whose
    /// heads could change is a planner bug.
    #[rstest]
    #[case(Mutability::Append)]
    #[case(Mutability::Mutable)]
    fn a_changing_loop_in_a_once_engine_is_an_internal_error(#[case] mutability: Mutability) {
        let error = codegen(ONCE)
            .loop_time(mutability)
            .expect_err("no input of a once engine changes");
        assert!(matches!(error, CodegenError::Internal(_)));
    }
}
