//! The literal-kind lattice: how polymorphic literals and concrete column
//! widths relate. Pass 1's primitive type algebra.

use flowlog_parser::ConstType;
use flowlog_parser::DataType;

use crate::TypeCheckError;

/// Numeric literals stay polymorphic within their family (`IntLit` /
/// `FloatLit`) until a concrete context fixes the type. Concrete literals
/// carry their resolved [`DataType`] directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LitKind {
    IntLit,
    FloatLit,
    Concrete(DataType),
}

impl LitKind {
    /// The kind of a literal constant. Errors only on a polymorphic literal
    /// that escaped the `Int`/`Float` arms — an internal invariant breach.
    pub(crate) fn of(c: &ConstType) -> Result<Self, TypeCheckError> {
        Ok(match c {
            ConstType::Int(_) => LitKind::IntLit,
            ConstType::Float(_) => LitKind::FloatLit,
            _ => LitKind::Concrete(c.data_type().ok_or_else(|| {
                TypeCheckError::internal(format!(
                    "LitKind::of: polymorphic literal {c:?} escaped Int/Float arms"
                ))
            })?),
        })
    }

    /// Combine two operand kinds across an arithmetic operator. `None` on
    /// a family mismatch.
    pub(crate) fn merge(&self, other: &Self) -> Option<Self> {
        match (self, other) {
            (x, y) if x == y => Some(x.clone()),
            (LitKind::Concrete(t), LitKind::IntLit) | (LitKind::IntLit, LitKind::Concrete(t))
                if t.is_integer() =>
            {
                Some(LitKind::Concrete(t.clone()))
            }
            (LitKind::Concrete(t), LitKind::FloatLit)
            | (LitKind::FloatLit, LitKind::Concrete(t))
                if t.is_float() =>
            {
                Some(LitKind::Concrete(t.clone()))
            }
            _ => None,
        }
    }

    pub(crate) fn fits(&self, expected: &DataType) -> bool {
        match self {
            LitKind::IntLit => expected.is_integer(),
            LitKind::FloatLit => expected.is_float(),
            LitKind::Concrete(t) => t == expected,
        }
    }

    pub(crate) fn is_numeric(&self) -> bool {
        match self {
            LitKind::IntLit | LitKind::FloatLit => true,
            LitKind::Concrete(t) => t.is_numeric(),
        }
    }

    /// Representative concrete type for diagnostic rendering **and** for
    /// pinning all-literal expressions that never met a concrete partner.
    pub(crate) fn report_ty(&self) -> DataType {
        match self {
            LitKind::IntLit => DataType::Int32,
            LitKind::FloatLit => DataType::Float32,
            LitKind::Concrete(t) => t.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use flowlog_parser::DataType;

    use super::*;

    /// `LitKind::merge` is the single source of truth for arithmetic operand
    /// unification. Integration fixtures exercise the rejection paths by
    /// watching for `ArithmeticTypeMismatch`, but a regression that silently
    /// *widened* acceptance (e.g. Int8+Int16 → Int16) would let bad programs
    /// through and only surface as wrong codegen width. Each row below is a
    /// specific bug class.
    #[test]
    fn merge_table() {
        use DataType::*;
        use LitKind::*;

        // Both sides polymorphic: must stay polymorphic so outer context
        // can still pin. Collapsing to Concrete(Int32) would break
        // narrow-width columns consuming `1 + 2`.
        assert_eq!(IntLit.merge(&IntLit), Some(IntLit));
        assert_eq!(FloatLit.merge(&FloatLit), Some(FloatLit));

        // Polymorphic meets concrete: concrete wins, picks the exact width.
        assert_eq!(IntLit.merge(&Concrete(Int8)), Some(Concrete(Int8)));
        assert_eq!(Concrete(UInt16).merge(&IntLit), Some(Concrete(UInt16)));
        assert_eq!(FloatLit.merge(&Concrete(Float64)), Some(Concrete(Float64)));

        // Same family, different width: rejects. Any "promote to wider"
        // rule added here would silently accept type-mismatched programs.
        assert_eq!(Concrete(Int8).merge(&Concrete(Int16)), None);
        assert_eq!(Concrete(Float32).merge(&Concrete(Float64)), None);

        // Cross-family: rejects.
        assert_eq!(IntLit.merge(&FloatLit), None);
        assert_eq!(Concrete(Int32).merge(&Concrete(Float32)), None);
        assert_eq!(Concrete(Bool).merge(&IntLit), None);
    }

    /// `report_ty` on polymorphic literals returns the default width used
    /// both for diagnostic rendering and for pinning orphan all-literal
    /// expressions (`5 > 10` with no variables). A regression that changed
    /// these defaults would shift every diagnostic's "expected" type AND
    /// silently change what width orphan constants get pinned to.
    #[test]
    fn report_ty_polymorphic_defaults() {
        assert_eq!(LitKind::IntLit.report_ty(), DataType::Int32);
        assert_eq!(LitKind::FloatLit.report_ty(), DataType::Float32);
    }
}
