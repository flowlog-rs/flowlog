//! Relation declarations for runtime I/O.
//!
//! [`Relation`] supplies the name, tuple type, and inline facts independently
//! of the state used to read or write rows.

use std::cmp::Ordering;
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

    /// Separates cells in input files and text puts. Must be ASCII.
    const INPUT_DELIMITER: u8 = b'\t';

    /// Skips the first line of each input file, once across all workers.
    const INPUT_HAS_HEADER: bool = false;

    /// Separates columns in output files; typed and stdout output ignore it.
    const OUTPUT_DELIMITER: u8 = b'\t';

    /// Whether output rows use [`compare`](Self::compare) for ordering.
    const ORDERED: bool = false;

    /// The output row limit, applied only when [`ORDERED`](Self::ORDERED).
    const LIMIT: Option<usize> = None;

    /// Compares output tuples without their timestamps or update weights.
    /// Interned string columns must compare their resolved contents.
    fn compare(a: &Self::Tuple, b: &Self::Tuple) -> Ordering {
        a.cmp(b)
    }

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
