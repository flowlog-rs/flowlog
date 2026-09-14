//! Data types of FlowLog programs.
//!
//! - [`DataType`]: the type vocabulary and its compatibility algebra.
//!   After type checking only the concrete types remain; codegen,
//!   planner, and storage work in those exclusively.

use std::fmt;
use std::str::FromStr;

// =============================================================================
// DataType
// =============================================================================

/// The type vocabulary of FlowLog programs.
///
/// Twelve scalar primitives and fixed tuples are the column types a
/// `.decl` can declare; `IntLit`/`FloatLit` type not-yet-pinned
/// literals and never survive type checking.
///
/// The grammar spelling of each primitive (canonical name plus accepted
/// aliases, e.g. `number` for `Int32`) is defined once in `PRIM_NAMES`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    /// An integer literal's type before the typechecker pins its width.
    #[doc(hidden)]
    IntLit,
    /// A float literal's type before the typechecker pins its width.
    #[doc(hidden)]
    FloatLit,

    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    /// UTF-8 string.
    String,
    Bool,
    /// Fixed-arity tuple column; fields may nest but not recurse.
    FixedTuple(Vec<DataType>),
}

/// Surface spellings for each primitive, the single source of truth for
/// grammar names: the first name in each row is canonical, the rest are
/// accepted aliases.
///
/// A row's position is also the primitive's [`TypeId`](super::TypeId),
/// so row order must not change.
pub(super) const PRIM_NAMES: [(DataType, &[&str]); 12] = [
    (DataType::Int8, &["int8"]),
    (DataType::Int16, &["int16"]),
    (DataType::Int32, &["int32", "number"]),
    (DataType::Int64, &["int64"]),
    (DataType::UInt8, &["uint8"]),
    (DataType::UInt16, &["uint16"]),
    (DataType::UInt32, &["uint32", "unsigned"]),
    (DataType::UInt64, &["uint64"]),
    (DataType::Float32, &["f32", "float"]),
    (DataType::Float64, &["f64"]),
    (DataType::String, &["string", "symbol"]),
    (DataType::Bool, &["bool"]),
];

impl DataType {
    /// Returns `true` for all integer and floating-point types.
    pub fn is_numeric(&self) -> bool {
        self.is_integer() || self.is_float()
    }

    /// Returns `true` for signed and unsigned integer types.
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            Self::Int8
                | Self::Int16
                | Self::Int32
                | Self::Int64
                | Self::UInt8
                | Self::UInt16
                | Self::UInt32
                | Self::UInt64
        )
    }

    /// Returns `true` for floating-point types.
    pub fn is_float(&self) -> bool {
        matches!(self, Self::Float32 | Self::Float64)
    }

    /// Returns `true` for fixed tuple types.
    pub fn is_tuple(&self) -> bool {
        matches!(self, DataType::FixedTuple(_))
    }

    /// Returns `true` for the polymorphic literal families
    /// (`IntLit`/`FloatLit`), which the typechecker pins away.
    pub(crate) fn is_literal(&self) -> bool {
        matches!(self, Self::IntLit | Self::FloatLit)
    }

    /// Returns `true` if `pred` holds for any scalar in this type: the
    /// type itself if it is a scalar, or any scalar reached through
    /// (possibly nested) tuple fields.
    pub fn any_scalar(&self, pred: &impl Fn(&DataType) -> bool) -> bool {
        match self {
            DataType::FixedTuple(fields) => fields.iter().any(|f| f.any_scalar(pred)),
            other => pred(other),
        }
    }

    /// Combines two operand types across an arithmetic operator: equal
    /// types combine to themselves, and a polymorphic literal family
    /// adopts a concrete partner of the same family. `None` on any other
    /// pairing (cross-family, or two different concrete widths).
    pub(crate) fn merge(&self, other: &Self) -> Option<Self> {
        match (self, other) {
            (x, y) if x == y => Some(x.clone()),
            (Self::IntLit, t) | (t, Self::IntLit) if t.is_integer() => Some(t.clone()),
            (Self::FloatLit, t) | (t, Self::FloatLit) if t.is_float() => Some(t.clone()),
            _ => None,
        }
    }

    /// Returns `true` if a value of this type can inhabit a column of
    /// type `expected`: a polymorphic literal family fits any concrete
    /// width of its family; anything else must match exactly.
    pub(crate) fn fits(&self, expected: &DataType) -> bool {
        match self {
            Self::IntLit => expected.is_integer(),
            Self::FloatLit => expected.is_float(),
            t => t == expected,
        }
    }

    /// This type with a polymorphic literal family replaced by its
    /// representative width: `IntLit` becomes `Int32`, `FloatLit` becomes
    /// `Float32`; concrete types return themselves.
    pub(crate) fn defaulted(&self) -> DataType {
        match self {
            Self::IntLit => DataType::Int32,
            Self::FloatLit => DataType::Float32,
            t => t.clone(),
        }
    }
}

impl FromStr for DataType {
    type Err = String;

    /// Parses a [`DataType`] from its grammar spelling, accepting any
    /// canonical name or alias listed in the `PRIM_NAMES` table.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        PRIM_NAMES
            .iter()
            .find(|(_, names)| names.contains(&s))
            .map(|(dt, _)| dt.clone())
            .ok_or_else(|| {
                format!(
                    "Parser error: '{s}'. Invalid data type. Expected one of: \
                    int8, int16, int32 (number), int64, \
                    uint8, uint16, uint32 (unsigned), uint64, \
                    f32 (float), f64, string (symbol), bool."
                )
            })
    }
}

impl fmt::Display for DataType {
    /// Formats the grammar spelling of this type: a scalar's canonical name
    /// from the `PRIM_NAMES` table, or a `FixedTuple` rendered structurally
    /// as `(t0, t1, ...)`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IntLit => write!(f, "integer literal"),
            Self::FloatLit => write!(f, "float literal"),
            Self::FixedTuple(fields) => {
                let inner = fields
                    .iter()
                    .map(DataType::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "({inner})")
            }
            scalar => {
                let name = PRIM_NAMES
                    .iter()
                    .find(|(dt, _)| dt == scalar)
                    .map_or("<unknown-type>", |(_, names)| names[0]);
                write!(f, "{name}")
            }
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rstest::rstest;

    use super::*;

    // --- DataType ---

    /// Every concrete scalar variant (the lit families and tuples are
    /// covered case-by-case where their behavior differs).
    const ALL: [DataType; 12] = [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float32,
        DataType::Float64,
        DataType::String,
        DataType::Bool,
    ];

    #[test]
    fn display_roundtrip() {
        for t in ALL {
            let parsed = DataType::from_str(&t.to_string()).unwrap();
            assert_eq!(t, parsed);
        }
    }

    /// Every spelling in `PRIM_NAMES`, canonical and alias, parses to its
    /// primitive. Guards the single-source table that `FromStr` derives from.
    #[test]
    fn from_str_accepts_every_prim_name() {
        for (dt, names) in PRIM_NAMES {
            for name in names {
                assert_eq!(DataType::from_str(name).unwrap(), dt, "{name}");
            }
        }
    }

    /// Spellings outside `PRIM_NAMES` are refused: unknown words, wrong
    /// case, and the `Display` renderings of tuples and lit families
    /// (which deliberately do not roundtrip).
    #[rstest]
    #[case("invalid")]
    #[case("Int32")]
    #[case("(int32, string)")]
    #[case("integer literal")]
    fn from_str_rejects_spellings_outside_the_table(#[case] spelling: &str) {
        let err = DataType::from_str(spelling).unwrap_err();
        assert!(err.contains("Invalid data type"), "{spelling}");
    }

    #[rstest]
    //    type                is_integer  is_float  is_numeric  is_tuple  is_literal
    #[case(DataType::Int8,    (true,      false,    true,       false, false))]
    #[case(DataType::Int16,   (true,      false,    true,       false, false))]
    #[case(DataType::Int32,   (true,      false,    true,       false, false))]
    #[case(DataType::Int64,   (true,      false,    true,       false, false))]
    #[case(DataType::UInt8,   (true,      false,    true,       false, false))]
    #[case(DataType::UInt16,  (true,      false,    true,       false, false))]
    #[case(DataType::UInt32,  (true,      false,    true,       false, false))]
    #[case(DataType::UInt64,  (true,      false,    true,       false, false))]
    #[case(DataType::Float32, (false,     true,     true,       false, false))]
    #[case(DataType::Float64, (false,     true,     true,       false, false))]
    #[case(DataType::String,  (false,     false,    false,      false, false))]
    #[case(DataType::Bool,    (false,     false,    false,      false, false))]
    #[case(DataType::FixedTuple(vec![DataType::Int32]), (false, false, false, true, false))]
    // The public predicates answer for concrete types only; the lit
    // families classify as nothing.
    #[case(DataType::IntLit,  (false,     false,    false,      false, true))]
    #[case(DataType::FloatLit, (false,    false,    false,      false, true))]
    fn classification_matrix(
        #[case] dt: DataType,
        #[case] expected: (bool, bool, bool, bool, bool),
    ) {
        let (integer, float, numeric, tuple, literal) = expected;
        assert_eq!(dt.is_integer(), integer, "is_integer({dt})");
        assert_eq!(dt.is_float(), float, "is_float({dt})");
        assert_eq!(dt.is_numeric(), numeric, "is_numeric({dt})");
        assert_eq!(dt.is_tuple(), tuple, "is_tuple({dt})");
        assert_eq!(dt.is_literal(), literal, "is_literal({dt})");
    }

    #[test]
    fn any_scalar_applies_predicate_to_a_scalar_type() {
        assert!(DataType::Float32.any_scalar(&DataType::is_float));
        assert!(!DataType::Int32.any_scalar(&DataType::is_float));
    }

    #[test]
    fn any_scalar_recurses_through_nested_tuples() {
        // `((int32, string), f64)`: the only float scalar is nested two deep.
        let nested = DataType::FixedTuple(vec![
            DataType::FixedTuple(vec![DataType::Int32, DataType::String]),
            DataType::Float64,
        ]);
        assert!(nested.any_scalar(&DataType::is_float));
        assert!(nested.any_scalar(&|l| matches!(l, DataType::String)));
        assert!(!nested.any_scalar(&|l| matches!(l, DataType::Bool)));
        // Zero fields reach no scalar: vacuously false for any predicate.
        assert!(!DataType::FixedTuple(vec![]).any_scalar(&|_| true));
    }

    /// Fixtures pin only `merge`'s rejection paths; a regression that
    /// silently widened acceptance (e.g. Int8+Int16 -> Int16) would let
    /// bad programs through and surface only as wrong codegen width.
    #[test]
    fn merge_table() {
        use DataType::*;

        // Both sides polymorphic: must stay polymorphic so outer context
        // can still pin. Collapsing to Int32 would break narrow-width
        // columns consuming `1 + 2`.
        assert_eq!(IntLit.merge(&IntLit), Some(IntLit));
        assert_eq!(FloatLit.merge(&FloatLit), Some(FloatLit));

        // Polymorphic meets concrete: concrete wins, picks the exact width.
        assert_eq!(IntLit.merge(&Int8), Some(Int8));
        assert_eq!(UInt16.merge(&IntLit), Some(UInt16));
        assert_eq!(FloatLit.merge(&Float64), Some(Float64));

        // Same family, different width: rejects. Any "promote to wider"
        // rule added here would silently accept type-mismatched programs.
        assert_eq!(Int8.merge(&Int16), None);
        assert_eq!(Float32.merge(&Float64), None);

        // Cross-family: rejects.
        assert_eq!(IntLit.merge(&FloatLit), None);
        assert_eq!(Int32.merge(&Float32), None);
        assert_eq!(Bool.merge(&IntLit), None);

        // Tuples (reachable via tuple-typed variables in comparisons):
        // structural equality merges, anything else rejects.
        let pair = FixedTuple(vec![Int32, String]);
        assert_eq!(pair.merge(&pair), Some(pair.clone()));
        assert_eq!(pair.merge(&FixedTuple(vec![Int32])), None);
        assert_eq!(pair.merge(&Int32), None);
    }

    #[test]
    fn fits_lit_families_and_exact_match() {
        use DataType::*;
        assert!(IntLit.fits(&Int8) && IntLit.fits(&UInt64));
        assert!(FloatLit.fits(&Float32) && FloatLit.fits(&Float64));
        assert!(!IntLit.fits(&Float32) && !FloatLit.fits(&Int32));
        assert!(Int32.fits(&Int32) && !Int32.fits(&Int64));
        assert!(!String.fits(&Bool));
    }

    /// A changed default would silently repin every orphan all-literal
    /// expression (`5 > 10` with no variables) to a different width.
    #[test]
    fn defaulted_maps_literals_to_defaults_and_concrete_to_itself() {
        assert_eq!(DataType::IntLit.defaulted(), DataType::Int32);
        assert_eq!(DataType::FloatLit.defaulted(), DataType::Float32);
        assert_eq!(DataType::Bool.defaulted(), DataType::Bool);
    }

    /// The lit families have no grammar spelling; they render as prose
    /// so a diagnostic that carries one stays readable.
    #[test]
    fn display_lit_families_as_prose() {
        assert_eq!(DataType::IntLit.to_string(), "integer literal");
        assert_eq!(DataType::FloatLit.to_string(), "float literal");
    }

    #[test]
    fn display_formats_tuples_structurally() {
        let pair = DataType::FixedTuple(vec![DataType::Int32, DataType::String]);
        assert_eq!(pair.to_string(), "(int32, string)");
        // Nested tuples render recursively.
        let nested = DataType::FixedTuple(vec![pair, DataType::Bool]);
        assert_eq!(nested.to_string(), "((int32, string), bool)");
    }
}
