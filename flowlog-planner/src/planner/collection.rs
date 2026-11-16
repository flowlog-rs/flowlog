//! Collection types used during planning.
//!
//! A Collection models a relation of tuples represented by a key/value
//! layout of argument positions. Collections can be row-based (no keys) or
//! key/value-based. A fingerprint says which plan node a collection is;
//! a canonical form says what rows it holds.

use std::fmt;

use flowlog_parser::Mutability;
use flowlog_parser::Relation;

use crate::catalog::AtomArgumentSignature;
use crate::planner::CanonicalForm;
use crate::planner::KeyValueLayout;
use crate::planner::PlanError;
use crate::planner::TransformationArgument;

/// Represents a data collection with key-value structure.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Collection {
    /// Identity of this collection in the plan graph, which readers name
    /// their inputs by. It is the lineage fingerprint the rule planner
    /// built the collection under, so equal fingerprints mean the same
    /// operation over the same inputs at the same body positions, and
    /// nothing about rows: the same rows reached through another plan, or
    /// the same step planned by another rule, carry a different
    /// fingerprint.
    fingerprint: u64,

    /// Hierarchical name describing how this collection was built from EDBs
    /// (e.g. `join[y](reach, arc)` or `project[x](filter[x > 0](arc))`).
    /// Used for log/debug rendering. Empty for internal placeholders.
    name: String,

    /// Key and value argument signatures; the key is empty for a row-only
    /// collection.
    kv_layout: KeyValueLayout,

    /// The query this collection computes, independent of the plan that
    /// built it. Where the fingerprint says which node this is, the form
    /// says what rows it holds, so equal forms are what sharing acts on.
    canonical: CanonicalForm,

    /// How the rows change after the first epoch, determined by the
    /// relations `canonical` reads and negates.
    mutability: Mutability,
}

impl Collection {
    /// Returns `relation`'s rows in declared column order at `mutability`,
    /// the weight of the binding being read, which a later binding of the
    /// same relation may raise.
    #[must_use]
    pub(crate) fn from_relation(relation: &Relation, mutability: Mutability) -> Self {
        let canonical = CanonicalForm::relation(relation.name(), relation.arity());
        Self::new(
            relation.fingerprint(),
            relation.name().to_string(),
            KeyValueLayout::new(Vec::new(), canonical.value().to_vec()),
            canonical,
            mutability,
        )
    }

    /// Creates a new collection with the given fingerprint, name, layout,
    /// canonical form, and the mutability that form determines.
    pub(crate) fn new(
        fingerprint: u64,
        name: String,
        kv_layout: KeyValueLayout,
        canonical: CanonicalForm,
        mutability: Mutability,
    ) -> Self {
        Self {
            fingerprint,
            name,
            kv_layout,
            canonical,
            mutability,
        }
    }

    /// Returns the arity as (key_count, value_count).
    #[inline]
    pub fn arity(&self) -> (usize, usize) {
        (self.kv_layout.key().len(), self.kv_layout.value().len())
    }

    /// Returns `true` if this collection has only keys (no values).
    pub fn is_k_only(&self) -> bool {
        self.kv_layout.value().is_empty()
    }

    /// Returns the collection fingerprint.
    pub fn fingerprint(&self) -> u64 {
        self.fingerprint
    }

    /// Returns the query this collection computes.
    pub(crate) fn canonical(&self) -> &CanonicalForm {
        &self.canonical
    }

    /// Returns how this collection's rows change after the first epoch.
    #[must_use]
    pub fn mutability(&self) -> Mutability {
        self.mutability
    }

    /// Where each of a relation's `arity` declared columns is held in this
    /// collection, which holds that relation's columns: at a column's
    /// declared position, its key or value slot as `KV((is_key, index))`.
    ///
    /// # Errors
    ///
    /// Returns an internal error if a held position is not a plain column
    /// of the relation, or the collection does not hold each declared
    /// column exactly once.
    pub(crate) fn relation_layout(
        &self,
        arity: usize,
    ) -> Result<Vec<TransformationArgument>, PlanError> {
        let mut slots: Vec<Option<TransformationArgument>> = vec![None; arity];
        let key = self.kv_layout.key().iter().enumerate();
        let value = self.kv_layout.value().iter().enumerate();
        let held = key
            .map(|(index, position)| (true, index, position))
            .chain(value.map(|(index, position)| (false, index, position)));
        for (is_key, index, position) in held {
            let column = position
                .rest()
                .is_empty()
                .then(|| position.init().as_var_signature())
                .flatten()
                .map(AtomArgumentSignature::argument_id);
            let Some(slot) = column
                .and_then(|column| slots.get_mut(column))
                .filter(|slot| slot.is_none())
            else {
                return Err(PlanError::internal(format!(
                    "{self} holds {position} where one of {arity} relation columns, \
                     each once, is expected"
                )));
            };
            *slot = Some(TransformationArgument::KV((is_key, index)));
        }
        slots.into_iter().collect::<Option<_>>().ok_or_else(|| {
            PlanError::internal(format!(
                "{self} does not hold every one of its relation's {arity} columns"
            ))
        })
    }

    /// This collection held as rows: the same columns, keys first, with no
    /// key. The fingerprint stays, since the rows are the same plan node.
    pub(crate) fn unkeyed(&self) -> Self {
        let layout = &self.kv_layout;
        Self {
            fingerprint: self.fingerprint,
            name: self.name.clone(),
            kv_layout: KeyValueLayout::new(
                Vec::new(),
                layout.key().iter().chain(layout.value()).cloned().collect(),
            ),
            canonical: self.canonical.unkeyed(),
            mutability: self.mutability,
        }
    }
}

impl fmt::Display for Collection {
    /// Canonical form: `<name> [0x{:016x}], key:(..), value:(..)`.
    /// When `name` is empty (internal placeholder), only the hex form appears.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.name.is_empty() {
            write!(f, "0x{:016x}, {}", self.fingerprint, self.kv_layout)
        } else {
            write!(
                f,
                "{} [0x{:016x}], {}",
                self.name, self.fingerprint, self.kv_layout
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use flowlog_parser::Constant;
    use flowlog_parser::DataType;
    use rstest::rstest;

    use super::*;
    use crate::catalog::ArithmeticPos;
    use crate::catalog::AtomSignature;
    use crate::catalog::FactorPos;

    /// Column `column` of the relation atom.
    fn column(column: usize) -> ArithmeticPos {
        ArithmeticPos::from_var_signature(AtomArgumentSignature::new(
            AtomSignature::new(true, 0),
            column,
        ))
    }

    /// A collection of relation `r` holding `key` then `value`.
    fn held(key: Vec<ArithmeticPos>, value: Vec<ArithmeticPos>) -> Collection {
        Collection::new(
            0,
            "r".to_string(),
            KeyValueLayout::new(key, value),
            CanonicalForm::relation("r", 3),
            Mutability::Static,
        )
    }

    fn kv(is_key: bool, index: usize) -> TransformationArgument {
        TransformationArgument::KV((is_key, index))
    }

    // Cases: held key, held value, where each declared column is held.
    #[rstest]
    #[case::declared_row(
        vec![],
        vec![column(0), column(1), column(2)],
        vec![kv(false, 0), kv(false, 1), kv(false, 2)]
    )]
    #[case::permuted_row(
        vec![],
        vec![column(2), column(0), column(1)],
        vec![kv(false, 1), kv(false, 2), kv(false, 0)]
    )]
    #[case::keyed(
        vec![column(1)],
        vec![column(2), column(0)],
        vec![kv(false, 1), kv(true, 0), kv(false, 0)]
    )]
    fn a_relation_layout_places_each_declared_column(
        #[case] key: Vec<ArithmeticPos>,
        #[case] value: Vec<ArithmeticPos>,
        #[case] expected: Vec<TransformationArgument>,
    ) {
        assert_eq!(held(key, value).relation_layout(3).unwrap(), expected);
    }

    // Cases: held value that is not each of three columns once.
    #[rstest]
    #[case::repeated(vec![column(0), column(0), column(1)])]
    #[case::missing(vec![column(0), column(1)])]
    #[case::out_of_range(vec![column(0), column(1), column(3)])]
    #[case::constant(vec![
        column(0),
        column(1),
        ArithmeticPos::new(FactorPos::Const(Constant::new(DataType::Int32, "7")), Vec::new()),
    ])]
    fn a_relation_layout_rejects_a_collection_not_holding_each_column_once(
        #[case] value: Vec<ArithmeticPos>,
    ) {
        let error = held(Vec::new(), value)
            .relation_layout(3)
            .expect_err("not each of three columns once");
        assert!(matches!(error, PlanError::Internal(_)), "{error}");
    }
}
