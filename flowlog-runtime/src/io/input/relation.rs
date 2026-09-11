//! Relation declarations for runtime input loading.
//!
//! [`Relation`] supplies the declaration shared across input sources;
//! [`Loader`](super::loader::Loader) owns the loading state.

use std::iter;

use differential_dataflow::Data;

// =============================================================================
// Relation
// =============================================================================

/// Declares one relation independently of its input sources.
pub trait Relation {
    /// The name as the `.decl` spells it, for diagnostics.
    const NAME: &'static str;

    /// The declared number of columns; zero denotes a nullary fact.
    const ARITY: usize;

    /// The row representation stored in the dataflow, after input decoding.
    type Tuple: Data;

    /// The `.fact` rows written in the program itself, or none.
    ///
    /// A method rather than a constant slice because an interned string
    /// has no constant form: its key exists only once the interner does.
    fn facts() -> impl IntoIterator<Item = Self::Tuple> {
        iter::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn facts_default_to_none() {
        struct WithoutFacts;

        impl Relation for WithoutFacts {
            const NAME: &'static str = "WithoutFacts";
            const ARITY: usize = 1;
            type Tuple = (i32,);
        }

        assert_eq!(WithoutFacts::facts().into_iter().next(), None);
    }
}
