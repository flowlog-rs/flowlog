//! Relation declarations for runtime I/O.
//!
//! [`Relation`] supplies the name, tuple type, and inline facts independently
//! of the state used to read or write rows.

use std::iter;

use differential_dataflow::Data;

/// Declares one relation independently of how its rows are read or written.
pub trait Relation {
    /// The name as the `.decl` spells it, for diagnostics.
    const NAME: &'static str;

    /// The declared number of columns; zero denotes a nullary fact.
    const ARITY: usize;

    /// The row representation stored in the dataflow.
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
