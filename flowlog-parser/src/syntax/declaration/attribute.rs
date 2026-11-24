//! Relation column schema.
//!
//! An `Attribute` carries two type facts in parallel:
//!
//! - `primitive_type` ([`DataType`]): the storage type. Read by every
//!   stage below the typechecker.
//! - `declared_id` ([`TypeId`]): the user-written type name. Read
//!   only by the typechecker to enforce subtype identity; dead weight
//!   downstream.
//!
//! A lattice column (`dist: min<int32>`) also carries its [`Lattice`].

use std::fmt;

use crate::AggregationOperator;
use crate::Lexeme;
use crate::Node;
use crate::Rule;
use crate::error::ParseError;
use crate::error::grammar_bug;
use crate::types::DataType;
use crate::types::TypeId;

// =============================================================================
// Lattice
// =============================================================================

/// How a lattice column merges the values a relation's rules derive for one
/// value of its other columns: the relation keeps only the least or the
/// greatest.
///
/// Whatever differs by kind is a method here, each one exhaustive match, so
/// a new kind fails to compile until every method names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Lattice {
    Min,
    Max,
}

impl Lattice {
    /// Returns the aggregation that merges a column of this kind.
    #[must_use]
    pub fn aggregation(self) -> AggregationOperator {
        match self {
            Self::Min => AggregationOperator::Min,
            Self::Max => AggregationOperator::Max,
        }
    }

    /// Returns `true` if a column of type `ty` can merge by this kind:
    /// `min` and `max` pick by `<`, so they take the types it orders.
    #[must_use]
    pub(crate) fn admits(self, ty: &DataType) -> bool {
        match self {
            Self::Min | Self::Max => ty.is_ordered(),
        }
    }
}

impl fmt::Display for Lattice {
    /// Writes the keyword of the kind's aggregation, which the grammar
    /// spells a lattice with.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.aggregation(), f)
    }
}

impl Lexeme for Lattice {
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        Ok(match node.rule() {
            Rule::min => Self::Min,
            Rule::max => Self::Max,
            other => return Err(grammar_bug(format!("unexpected lattice rule: {other:?}"))),
        })
    }
}

/// Splits an `attribute_decl` node into its name, its type reference, and
/// the lattice wrapping that type, if any.
pub(super) fn split_attribute_decl(
    node: Node<'_>,
) -> Result<(Node<'_>, Node<'_>, Option<Lattice>), ParseError> {
    let mut parts = node.children();
    let name = parts.next_any("attribute name")?;
    let ty = parts.next_any("attribute type")?;
    if ty.rule() != Rule::lattice_type {
        return Ok((name, ty, None));
    }
    let mut lattice = ty.children();
    let op = lattice.lower_next("lattice operator")?;
    let type_ref = lattice.require(Rule::type_ref)?;
    Ok((name, type_ref, Some(op)))
}

// =============================================================================
// Attribute
// =============================================================================

/// One `name: type` column of a relation's schema.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Attribute {
    name: String,
    primitive_type: DataType,
    declared_id: TypeId,
    lattice: Option<Lattice>,
}

impl Attribute {
    /// A column named `name`, lowercased, of the given types; a lattice
    /// column when `lattice` is `Some`.
    #[must_use]
    #[inline]
    pub(crate) fn new(
        name: String,
        primitive_type: DataType,
        declared_id: TypeId,
        lattice: Option<Lattice>,
    ) -> Self {
        Self {
            name: name.to_lowercase(),
            primitive_type,
            declared_id,
            lattice,
        }
    }

    #[must_use]
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.primitive_type
    }

    #[must_use]
    #[inline]
    pub(crate) fn declared_id(&self) -> TypeId {
        self.declared_id
    }

    /// The merge of a lattice column; `None` for a plain one.
    #[must_use]
    #[inline]
    pub fn lattice(&self) -> Option<Lattice> {
        self.lattice
    }
}

impl fmt::Display for Attribute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.lattice {
            Some(lattice) => write!(f, "{}: {lattice}<{}>", self.name, self.primitive_type),
            None => write!(f, "{}: {}", self.name, self.primitive_type),
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::types::TypeRegistry;

    fn int32_id() -> TypeId {
        TypeRegistry::new()
            .primitive_id(DataType::Int32)
            .expect("int32 is a seeded primitive")
    }

    /// A kind merges by the aggregation of its name, an idempotent one,
    /// since a lattice must not move when a row is derived twice.
    // Cases: lattice, aggregation, spelling.
    #[rstest]
    #[case(Lattice::Min, AggregationOperator::Min, "min")]
    #[case(Lattice::Max, AggregationOperator::Max, "max")]
    fn a_lattice_merges_by_the_idempotent_aggregation_of_its_name(
        #[case] lattice: Lattice,
        #[case] aggregation: AggregationOperator,
        #[case] spelling: &str,
    ) {
        assert_eq!(lattice.aggregation(), aggregation);
        assert!(lattice.aggregation().is_idempotent(), "{lattice}");
        assert_eq!(lattice.to_string(), spelling);
    }

    /// `min` and `max` take the column types `<` orders, and no other.
    // Cases: lattice, column type, admitted.
    #[rstest]
    #[case(Lattice::Min, DataType::UInt8, true)]
    #[case(Lattice::Min, DataType::String, true)]
    #[case(Lattice::Min, DataType::Bool, false)]
    #[case(Lattice::Max, DataType::Float64, true)]
    #[case(Lattice::Max, DataType::String, true)]
    #[case(Lattice::Max, DataType::FixedTuple(vec![DataType::Int32]), false)]
    fn a_lattice_admits_only_ordered_columns(
        #[case] lattice: Lattice,
        #[case] ty: DataType,
        #[case] admitted: bool,
    ) {
        assert_eq!(lattice.admits(&ty), admitted, "{lattice}<{ty}>");
    }

    /// `new` lowercases the column name (names are case-insensitive) and
    /// stores the types and lattice verbatim; the accessors read them back.
    #[test]
    fn new_lowercases_name_and_keeps_types() {
        let id = int32_id();
        let attr = Attribute::new("Age".into(), DataType::Int32, id, Some(Lattice::Min));
        assert_eq!(attr.name(), "age");
        assert_eq!(attr.data_type(), &DataType::Int32);
        assert_eq!(attr.declared_id(), id);
        assert_eq!(attr.lattice(), Some(Lattice::Min));
    }

    /// `Display` renders `name: type` with the canonical (lowercased) name;
    /// an empty (default) rendering is caught.
    #[test]
    fn display_renders_name_and_type() {
        let attr = Attribute::new("Age".into(), DataType::Int32, int32_id(), None);
        assert_eq!(attr.to_string(), "age: int32");
    }
}
