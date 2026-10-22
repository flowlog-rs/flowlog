//! Type terms and their unification, the engine of type inference.
//!
//! A [`TypeTerm`] is a concrete scalar, a tuple of terms, or a variable. A
//! variable stands for a type not yet known: a literal's, whose width its
//! uses pick; an aggregate's answer; or a rule variable's, until a column
//! or an expression fixes it. [`Solver::unify`] makes two terms equal, and
//! once every constraint is in, [`Solver::resolve`] reads a term back,
//! giving each width nothing fixed its default.

use crate::DataType;

// =============================================================================
// TypeTerm
// =============================================================================

/// A type term.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum TypeTerm {
    /// A concrete scalar type: never a literal family or a tuple.
    Scalar(DataType),
    Tuple(Vec<TypeTerm>),
    Var(TypeVar),
}

impl TypeTerm {
    /// The term of a declared type, with each tuple as a tuple term.
    /// Requires a concrete type: a literal family is a variable, not a
    /// scalar.
    pub(super) fn of(ty: &DataType) -> Self {
        match ty {
            DataType::FixedTuple(fields) => Self::Tuple(fields.iter().map(Self::of).collect()),
            scalar => {
                debug_assert!(
                    !scalar.is_literal(),
                    "`{scalar}` is a literal family, not a type"
                );
                Self::Scalar(scalar.clone())
            }
        }
    }
}

/// A type variable, an index into the solver's table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TypeVar(usize);

// =============================================================================
// Kind
// =============================================================================

/// What a variable's type must turn out to be. Each kind below `Any`
/// admits a subset of the scalars, and the kinds form a lattice under
/// [`Kind::meet`], with `Integer` and `Float` disjoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    /// Any type, a tuple included.
    Any,
    /// A type `<` orders: a number or a string.
    Ordered,
    Numeric,
    Integer,
    Float,
}

impl Kind {
    /// Returns `true` if a value of `scalar` may have this kind.
    fn admits(self, scalar: &DataType) -> bool {
        match self {
            Self::Any => true,
            Self::Ordered => scalar.is_numeric() || *scalar == DataType::String,
            Self::Numeric => scalar.is_numeric(),
            Self::Integer => scalar.is_integer(),
            Self::Float => scalar.is_float(),
        }
    }

    /// How far below `Any` this kind sits; a kind admits every scalar a
    /// kind of higher rank does, except that `Integer` and `Float`, of one
    /// rank, admit disjoint scalars.
    fn rank(self) -> u8 {
        match self {
            Self::Any => 0,
            Self::Ordered => 1,
            Self::Numeric => 2,
            Self::Integer | Self::Float => 3,
        }
    }

    /// The kind admitting exactly what both admit; `None` when nothing does.
    fn meet(self, other: Self) -> Option<Self> {
        if self == other {
            Some(self)
        } else if self.rank() == other.rank() {
            None
        } else if self.rank() > other.rank() {
            Some(self)
        } else {
            Some(other)
        }
    }

    /// The width a variable of this kind takes when nothing fixes one, for
    /// the kinds whose variables have one: a literal or an aggregate.
    fn default(self) -> Option<DataType> {
        match self {
            Self::Integer => Some(DataType::Int32),
            Self::Float => Some(DataType::Float32),
            Self::Any | Self::Ordered | Self::Numeric => None,
        }
    }
}

// =============================================================================
// Solver
// =============================================================================

/// A variable's state: its union-find parent, and on a root, its kind,
/// its default width, and the term it is bound to, if any.
#[derive(Debug, Clone)]
struct VarState {
    parent: usize,
    kind: Kind,
    default: Option<DataType>,
    bound: Option<TypeTerm>,
}

/// Two terms that do not unify. The site that asked knows both terms, so
/// the clash carries nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Clash;

/// The variables of one rule and the constraints among them, as a
/// union-find of type variables.
#[derive(Debug, Default)]
pub(super) struct Solver {
    vars: Vec<VarState>,
}

impl Solver {
    /// A fresh variable of `kind`, with its kind's default width.
    pub(super) fn fresh(&mut self, kind: Kind) -> TypeTerm {
        self.fresh_with_default(kind, kind.default())
    }

    /// A fresh variable of `kind` that takes `default` when nothing fixes
    /// its width.
    pub(super) fn fresh_with_default(&mut self, kind: Kind, default: Option<DataType>) -> TypeTerm {
        self.vars.push(VarState {
            parent: self.vars.len(),
            kind,
            default,
            bound: None,
        });
        TypeTerm::Var(TypeVar(self.vars.len() - 1))
    }

    /// A copy of `ty` with every variable replaced by a fresh one of the
    /// same kind and default: the term of a value with no variables, such
    /// as `1 + 2`, instantiated for one use of the variable it defines.
    pub(super) fn instantiate(&mut self, ty: &TypeTerm) -> TypeTerm {
        match ty {
            TypeTerm::Scalar(_) => ty.clone(),
            TypeTerm::Tuple(fields) => {
                TypeTerm::Tuple(fields.iter().map(|f| self.instantiate(f)).collect())
            }
            TypeTerm::Var(v) => {
                let root = self.find(v.0);
                match self.vars[root].bound.clone() {
                    Some(bound) => self.instantiate(&bound),
                    None => self
                        .fresh_with_default(self.vars[root].kind, self.vars[root].default.clone()),
                }
            }
        }
    }

    /// Makes `a` and `b` the same type. On success every later reading of
    /// either shows the other's constraints. On a clash the two are left
    /// apart, except that the fields of two tuples before the clashing one
    /// have been unified.
    pub(super) fn unify(&mut self, a: &TypeTerm, b: &TypeTerm) -> Result<(), Clash> {
        match (self.representative(a), self.representative(b)) {
            (TypeTerm::Var(x), TypeTerm::Var(y)) => self.union(x.0, y.0),
            (TypeTerm::Var(x), t) | (t, TypeTerm::Var(x)) => self.bind(x.0, t),
            (TypeTerm::Scalar(s), TypeTerm::Scalar(t)) => (s == t).then_some(()).ok_or(Clash),
            (TypeTerm::Tuple(xs), TypeTerm::Tuple(ys)) => {
                if xs.len() != ys.len() {
                    return Err(Clash);
                }
                xs.iter().zip(&ys).try_for_each(|(x, y)| self.unify(x, y))
            }
            (TypeTerm::Scalar(_), TypeTerm::Tuple(_))
            | (TypeTerm::Tuple(_), TypeTerm::Scalar(_)) => Err(Clash),
        }
    }

    /// Requires `ty` to be of `kind`.
    pub(super) fn constrain(&mut self, ty: &TypeTerm, kind: Kind) -> Result<(), Clash> {
        match self.representative(ty) {
            TypeTerm::Var(x) => {
                let root = x.0;
                let met = self.vars[root].kind.meet(kind).ok_or(Clash)?;
                self.vars[root].kind = met;
                if self.vars[root].default.is_none() {
                    self.vars[root].default = met.default();
                }
                Ok(())
            }
            TypeTerm::Scalar(s) => kind.admits(&s).then_some(()).ok_or(Clash),
            TypeTerm::Tuple(_) => (kind == Kind::Any).then_some(()).ok_or(Clash),
        }
    }

    /// Returns `true` if `ty` may be one of `allowed`: it is a scalar among
    /// them, or a variable whose kind admits one of them. A parameter that
    /// takes several types constrains nothing, so this reads without
    /// unifying.
    pub(super) fn admits_any(&self, ty: &TypeTerm, allowed: &[DataType]) -> bool {
        match self.representative(ty) {
            TypeTerm::Var(x) => {
                let kind = self.vars[self.find(x.0)].kind;
                allowed.iter().any(|t| kind.admits(t))
            }
            TypeTerm::Scalar(s) => allowed.contains(&s),
            TypeTerm::Tuple(_) => false,
        }
    }

    /// The type `ty` stands for, with each unfixed width at its default;
    /// `None` while a part of it is a variable nothing constrains.
    pub(super) fn resolve(&self, ty: &TypeTerm) -> Option<DataType> {
        match ty {
            TypeTerm::Scalar(s) => Some(s.clone()),
            TypeTerm::Tuple(fields) => fields
                .iter()
                .map(|f| self.resolve(f))
                .collect::<Option<Vec<_>>>()
                .map(DataType::FixedTuple),
            TypeTerm::Var(v) => {
                let root = self.find(v.0);
                match &self.vars[root].bound {
                    Some(bound) => self.resolve(bound),
                    None => self.vars[root].default.clone(),
                }
            }
        }
    }

    /// `ty` as a type for a diagnostic: [`Self::resolve`], with each part
    /// nothing constrains shown as the corresponding part of `like`, the
    /// type `ty` was compared with.
    pub(super) fn describe(&self, ty: &TypeTerm, like: &DataType) -> DataType {
        match self.representative(ty) {
            TypeTerm::Tuple(fields) => {
                let likes: Vec<&DataType> = match like {
                    DataType::FixedTuple(likes) if likes.len() == fields.len() => {
                        likes.iter().collect()
                    }
                    other => vec![other; fields.len()],
                };
                DataType::FixedTuple(
                    fields
                        .iter()
                        .zip(likes)
                        .map(|(f, l)| self.describe(f, l))
                        .collect(),
                )
            }
            ty => self.resolve(&ty).unwrap_or_else(|| like.clone()),
        }
    }

    /// The term standing for `ty` at the top: a variable's root, or what the
    /// root is bound to.
    fn representative(&self, ty: &TypeTerm) -> TypeTerm {
        let TypeTerm::Var(v) = ty else {
            return ty.clone();
        };
        let root = self.find(v.0);
        match &self.vars[root].bound {
            Some(bound) => bound.clone(),
            None => TypeTerm::Var(TypeVar(root)),
        }
    }

    fn find(&self, mut index: usize) -> usize {
        while self.vars[index].parent != index {
            index = self.vars[index].parent;
        }
        index
    }

    /// Merges the roots `x` and `y`: their kinds meet, and the wider of
    /// their defaults is kept (a `mean`'s `float64` over a literal's
    /// `float32`).
    fn union(&mut self, x: usize, y: usize) -> Result<(), Clash> {
        if x == y {
            return Ok(());
        }
        let kind = self.vars[x].kind.meet(self.vars[y].kind).ok_or(Clash)?;
        let default = match (self.vars[x].default.take(), self.vars[y].default.take()) {
            (Some(a), Some(b)) => Some(if bit_width(&b) > bit_width(&a) { b } else { a }),
            (a, b) => a.or(b).or_else(|| kind.default()),
        };
        self.vars[y].parent = x;
        self.vars[x].kind = kind;
        self.vars[x].default = default;
        Ok(())
    }

    /// Binds the root `x` to `t`, a scalar or a tuple, which must be of its
    /// kind. `unify` unions two variables rather than binding one to the
    /// other, so `t` is never a variable.
    fn bind(&mut self, x: usize, t: TypeTerm) -> Result<(), Clash> {
        let admitted = match &t {
            TypeTerm::Scalar(s) => self.vars[x].kind.admits(s),
            TypeTerm::Tuple(_) => self.vars[x].kind == Kind::Any,
            TypeTerm::Var(_) => {
                debug_assert!(false, "bind({x}, {t:?}) with a variable term");
                false
            }
        };
        if !admitted {
            return Err(Clash);
        }
        self.vars[x].bound = Some(t);
        Ok(())
    }
}

/// The bits a scalar's values take, for choosing between two defaults.
fn bit_width(ty: &DataType) -> u8 {
    match ty {
        DataType::Int8 | DataType::UInt8 => 8,
        DataType::Int16 | DataType::UInt16 => 16,
        DataType::Int32 | DataType::UInt32 | DataType::Float32 => 32,
        DataType::Int64 | DataType::UInt64 | DataType::Float64 => 64,
        DataType::IntLit
        | DataType::FloatLit
        | DataType::String
        | DataType::Bool
        | DataType::FixedTuple(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn int(ty: DataType) -> TypeTerm {
        TypeTerm::Scalar(ty)
    }

    /// A fresh literal variable resolves to its kind's default until a
    /// concrete type fixes it.
    #[test]
    fn a_literal_variable_takes_the_width_it_is_unified_with_else_its_default() {
        let mut s = Solver::default();
        let a = s.fresh(Kind::Integer);
        assert_eq!(s.resolve(&a), Some(DataType::Int32));
        s.unify(&a, &int(DataType::Int8)).unwrap();
        assert_eq!(s.resolve(&a), Some(DataType::Int8));
    }

    /// Unification is transitive: a width reaches a variable through the
    /// variables it was unified with earlier.
    #[test]
    fn a_width_reaches_a_variable_through_others() {
        let mut s = Solver::default();
        let (a, b, c) = (
            s.fresh(Kind::Integer),
            s.fresh(Kind::Any),
            s.fresh(Kind::Any),
        );
        s.unify(&a, &b).unwrap();
        s.unify(&b, &c).unwrap();
        s.unify(&c, &int(DataType::Int64)).unwrap();
        assert_eq!(s.resolve(&a), Some(DataType::Int64));
    }

    /// Two literal variables of different families, or one and a scalar
    /// of another family, or two different scalars, clash.
    // Cases: kind, scalar, unifies.
    #[rstest]
    #[case(Kind::Integer, DataType::Float32, false)]
    #[case(Kind::Integer, DataType::String, false)]
    #[case(Kind::Float, DataType::Float64, true)]
    #[case(Kind::Ordered, DataType::String, true)]
    #[case(Kind::Ordered, DataType::Bool, false)]
    #[case(Kind::Numeric, DataType::Bool, false)]
    #[case(Kind::Any, DataType::Bool, true)]
    fn a_variable_unifies_with_a_scalar_its_kind_admits(
        #[case] kind: Kind,
        #[case] scalar: DataType,
        #[case] unifies: bool,
    ) {
        let mut s = Solver::default();
        let a = s.fresh(kind);
        assert_eq!(s.unify(&a, &int(scalar)).is_ok(), unifies);
    }

    #[test]
    fn integer_and_float_variables_clash() {
        let mut s = Solver::default();
        let (a, b) = (s.fresh(Kind::Integer), s.fresh(Kind::Float));
        assert_eq!(s.unify(&a, &b), Err(Clash));
    }

    #[test]
    fn different_scalars_clash() {
        let mut s = Solver::default();
        assert_eq!(
            s.unify(&int(DataType::Int8), &int(DataType::Int16)),
            Err(Clash)
        );
    }

    /// Constraining a variable narrows its kind, and a constraint no kind
    /// admits clashes; a variable that becomes a literal kind gains that
    /// kind's default.
    #[test]
    fn a_constraint_narrows_a_variable_and_gives_it_a_default() {
        let mut s = Solver::default();
        let a = s.fresh(Kind::Any);
        assert_eq!(s.resolve(&a), None);
        s.constrain(&a, Kind::Numeric).unwrap();
        assert_eq!(s.resolve(&a), None);
        s.constrain(&a, Kind::Integer).unwrap();
        assert_eq!(s.resolve(&a), Some(DataType::Int32));
        assert_eq!(s.constrain(&a, Kind::Float), Err(Clash));
    }

    /// Tuples unify field by field and must have one width; a tuple never
    /// unifies with a scalar or with a kinded variable.
    #[test]
    fn tuples_unify_field_by_field() {
        let mut s = Solver::default();
        let a = s.fresh(Kind::Integer);
        let pattern = TypeTerm::Tuple(vec![a.clone(), s.fresh(Kind::Any)]);
        let pair = TypeTerm::of(&DataType::FixedTuple(vec![
            DataType::Int16,
            DataType::String,
        ]));
        s.unify(&pattern, &pair).unwrap();
        assert_eq!(s.resolve(&a), Some(DataType::Int16));
        assert_eq!(
            s.resolve(&pattern),
            Some(DataType::FixedTuple(vec![
                DataType::Int16,
                DataType::String
            ]))
        );
        let single = TypeTerm::Tuple(vec![s.fresh(Kind::Any)]);
        assert_eq!(s.unify(&pattern, &single), Err(Clash));
        assert_eq!(s.unify(&pattern, &int(DataType::Int16)), Err(Clash));
        let n = s.fresh(Kind::Numeric);
        assert_eq!(s.unify(&n, &pair), Err(Clash));
    }

    /// When two unfixed variables merge, the wider default wins.
    #[test]
    fn the_wider_default_wins() {
        let mut s = Solver::default();
        let mean = s.fresh_with_default(Kind::Float, Some(DataType::Float64));
        let literal = s.fresh(Kind::Float);
        s.unify(&literal, &mean).unwrap();
        assert_eq!(s.resolve(&literal), Some(DataType::Float64));
    }

    /// Instantiating a term copies its variables, so two uses of a value
    /// with no variables take widths independently.
    #[test]
    fn instantiation_copies_variables() {
        let mut s = Solver::default();
        let scheme = s.fresh(Kind::Integer);
        let (one, two) = (s.instantiate(&scheme), s.instantiate(&scheme));
        s.unify(&one, &int(DataType::Int8)).unwrap();
        s.unify(&two, &int(DataType::Int64)).unwrap();
        assert_eq!(s.resolve(&one), Some(DataType::Int8));
        assert_eq!(s.resolve(&two), Some(DataType::Int64));
        assert_eq!(s.resolve(&scheme), Some(DataType::Int32));
    }

    /// `describe` shows the parts a diagnostic can name and borrows the
    /// rest from the type the term was compared with.
    #[test]
    fn describe_fills_unknown_parts_from_the_other_type() {
        let mut s = Solver::default();
        let t = TypeTerm::Tuple(vec![s.fresh(Kind::Any), int(DataType::Int32)]);
        let like = DataType::FixedTuple(vec![DataType::String, DataType::Int64]);
        assert_eq!(
            s.describe(&t, &like),
            DataType::FixedTuple(vec![DataType::String, DataType::Int32])
        );
    }
}
