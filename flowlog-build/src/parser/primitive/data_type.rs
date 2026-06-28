//! Data types and the per-program type registry.
//!
//! - [`DataType`]: the 12 runtime primitives. Codegen, planner, and
//!   storage work in these exclusively.
//! - [`TypeRegistry`]: interns every named type (primitives + user
//!   `.type` aliases and subtypes) under a [`TypeId`]. Used by the
//!   typechecker to enforce subtype rules; ignored everywhere below.
//!   Subtypes are zero-cost compile-time phantom types.

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use crate::parser::error::ParseError;
use flowlog_common::Span;

// =============================================================================
// DataType — the closed set of runtime primitives
// =============================================================================

/// Data types supported in FlowLog Datalog programs.
///
/// These types correspond to the primitive types in the FlowLog grammar:
/// - `"int8"` → [`DataType::Int8`]
/// - `"int16"` → [`DataType::Int16`]
/// - `"int32"` / `"number"` → [`DataType::Int32`]
/// - `"int64"` → [`DataType::Int64`]
/// - `"uint8"` → [`DataType::UInt8`]
/// - `"uint16"` → [`DataType::UInt16`]
/// - `"uint32"` / `"unsigned"` → [`DataType::UInt32`]
/// - `"uint64"` → [`DataType::UInt64`]
/// - `"f32"` / `"float"` → [`DataType::Float32`]
/// - `"f64"` → [`DataType::Float64`]
/// - `"string"` / `"symbol"` → [`DataType::String`]
/// - `"bool"` → [`DataType::Bool`]
///
/// They are used as attribute types in relations.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    /// 8-bit signed integer type.
    Int8,
    /// 16-bit signed integer type.
    Int16,
    /// 32-bit signed integer type.
    Int32,
    /// 64-bit signed integer type.
    Int64,
    /// 8-bit unsigned integer type.
    UInt8,
    /// 16-bit unsigned integer type.
    UInt16,
    /// 32-bit unsigned integer type.
    UInt32,
    /// 64-bit unsigned integer type.
    UInt64,
    /// 32-bit floating point type.
    Float32,
    /// 64-bit floating point type.
    Float64,
    /// UTF-8 string type.
    String,
    /// Boolean type.
    Bool,
    /// A fixed tuple type.
    FixedTuple(Vec<DataType>),
}

impl DataType {
    /// Returns `true` for all integer and floating-point types.
    pub(crate) fn is_numeric(&self) -> bool {
        self.is_integer() || self.is_float()
    }

    /// Returns `true` for integer types only (excludes floats, strings, bools).
    pub(crate) fn is_integer(&self) -> bool {
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

    /// Returns `true` for floating-point types (`Float32`, `Float64`).
    pub fn is_float(&self) -> bool {
        matches!(self, Self::Float32 | Self::Float64)
    }

    /// `true` if this column type is a fixed tuple.
    pub(crate) fn is_tuple(&self) -> bool {
        matches!(self, DataType::FixedTuple(_))
    }

    /// `true` if `pred` holds for any scalar leaf of this type, recursing
    /// through tuple (`FixedTuple`) columns.
    pub(crate) fn any_leaf(&self, pred: &impl Fn(&DataType) -> bool) -> bool {
        match self {
            DataType::FixedTuple(fields) => fields.iter().any(|f| f.any_leaf(pred)),
            other => pred(other),
        }
    }

    /// Returns the semiring type suffix, e.g. `"I32"`, `"U64"`, `"F32"`.
    ///
    /// Panics for non-numeric types (String, Bool).
    pub fn semiring_suffix(&self) -> &'static str {
        match self {
            Self::Int8 => "I8",
            Self::Int16 => "I16",
            Self::Int32 => "I32",
            Self::Int64 => "I64",
            Self::UInt8 => "U8",
            Self::UInt16 => "U16",
            Self::UInt32 => "U32",
            Self::UInt64 => "U64",
            Self::Float32 => "F32",
            Self::Float64 => "F64",
            _ => panic!("DataType::semiring_suffix() called on non-numeric type: {self}"),
        }
    }
}

impl FromStr for DataType {
    type Err = String;

    /// Parse a [`DataType`] from its grammar string representation.
    ///
    /// Accepted values: `"int8"`, `"int16"`, `"int32"` (alias `"number"`), `"int64"`,
    /// `"uint8"`, `"uint16"`, `"uint32"` (alias `"unsigned"`), `"uint64"`,
    /// `"f32"` (alias `"float"`), `"f64"`,
    /// `"string"` (alias `"symbol"`), `"bool"`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "int8" => Ok(Self::Int8),
            "int16" => Ok(Self::Int16),
            "int32" | "number" => Ok(Self::Int32),
            "int64" => Ok(Self::Int64),
            "uint8" => Ok(Self::UInt8),
            "uint16" => Ok(Self::UInt16),
            "uint32" | "unsigned" => Ok(Self::UInt32),
            "uint64" => Ok(Self::UInt64),
            "f32" | "float" => Ok(Self::Float32),
            "f64" => Ok(Self::Float64),
            "string" | "symbol" => Ok(Self::String),
            "bool" => Ok(Self::Bool),
            _ => Err(format!(
                "Parser error: '{s}'. Invalid data type. Expected one of: \
                int8, int16, int32 (number), int64, \
                uint8, uint16, uint32 (unsigned), uint64, \
                f32 (float), f64, string (symbol), bool."
            )),
        }
    }
}

impl fmt::Display for DataType {
    /// Returns the grammar string representation of this type.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let type_str = match self {
            Self::Int8 => "int8",
            Self::Int16 => "int16",
            Self::Int32 => "int32",
            Self::Int64 => "int64",
            Self::UInt8 => "uint8",
            Self::UInt16 => "uint16",
            Self::UInt32 => "uint32",
            Self::UInt64 => "uint64",
            Self::Float32 => "f32",
            Self::Float64 => "f64",
            Self::String => "string",
            Self::Bool => "bool",
            Self::FixedTuple(fields) => {
                let inner = fields
                    .iter()
                    .map(DataType::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                return write!(f, "({inner})");
            }
        };
        write!(f, "{type_str}")
    }
}

// =============================================================================
// TypeRegistry — every named type, primitive or user-declared
// =============================================================================

/// Stable handle for a named type — an index into the registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TypeId(usize);

/// One row in the registry.
///
/// Aliases (`.type X = Y`) don't get their own `TypeDef` — the name `X`
/// just maps to `Y`'s existing `TypeId` in `by_name`. Only primitives
/// and subtypes are distinct identities.
#[derive(Debug, Clone)]
struct TypeDef {
    name: String,
    /// `None` for primitives; `Some` for subtypes (the type they refine).
    parent: Option<TypeId>,
    /// Eagerly resolved so `root_primitive()` is O(1).
    root_primitive: DataType,
    /// `Span::DUMMY` for built-in primitives.
    span: Span,
}

/// Surface names for each primitive. First name is canonical; the rest
/// are accepted aliases. Position in the table determines the
/// primitive's `TypeId` (also indexed by [`PRIMITIVE_TO_ID`]).
const PRIM_NAMES: [(DataType, &[&str]); 12] = [
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

/// Definition of a tuple type, keyed by its [`TypeId`] in [`TypeRegistry`].
#[derive(Debug, Clone)]
pub(crate) struct TupleDef {
    /// Field types, as `TypeId`s.
    pub(crate) fields: Vec<TypeId>,
}

/// Interned table of every type the program can refer to.
#[derive(Debug, Clone)]
pub struct TypeRegistry {
    types: Vec<TypeDef>,
    by_name: HashMap<String, TypeId>,
    tuples: HashMap<TypeId, TupleDef>,
}

impl TypeRegistry {
    /// Pre-populated with the 12 built-in primitives.
    #[must_use]
    pub fn new() -> Self {
        let mut reg = Self {
            types: Vec::with_capacity(16),
            by_name: HashMap::with_capacity(20),
            tuples: HashMap::new(),
        };
        for (i, (prim, names)) in PRIM_NAMES.iter().enumerate() {
            reg.types.push(TypeDef {
                name: names[0].to_string(),
                parent: None,
                root_primitive: prim.clone(),
                span: Span::DUMMY,
            });
            for n in *names {
                reg.by_name.insert(n.to_lowercase(), TypeId(i));
            }
        }
        reg
    }

    /// Case-insensitive surface-name lookup. `None` until the matching
    /// `.type` declaration has been processed. Registration is
    /// define-before-use (source order, eager parent resolution) both at
    /// top level (`build_type_registry`) and per-instance
    /// (`inliner::collect_instance`) — there is no retry, so a forward
    /// reference yields `UnknownTypeParent`.
    #[must_use]
    pub(crate) fn lookup(&self, name: &str) -> Option<TypeId> {
        self.by_name.get(&name.to_lowercase()).copied()
    }

    #[must_use]
    #[inline]
    pub(crate) fn root_primitive(&self, id: TypeId) -> DataType {
        self.types[id.0].root_primitive.clone()
    }

    /// Canonical `TypeId` for a built-in primitive. O(1) — primitives
    /// are seeded at positions 0..12 in [`PRIM_NAMES`] order.
    #[must_use]
    pub(crate) fn primitive_id(&self, dt: DataType) -> TypeId {
        let idx = PRIM_NAMES
            .iter()
            .position(|(p, _)| p == &dt)
            .expect("every DataType variant is in PRIM_NAMES");
        TypeId(idx)
    }

    /// `.type X = Y` — `X` becomes a synonym for `Y`'s existing `TypeId`.
    /// No new entry is created; the alias is invisible after registration.
    pub(crate) fn register_alias(
        &mut self,
        name: &str,
        parent_name: &str,
        span: Span,
    ) -> Result<TypeId, ParseError> {
        let canonical = self.reject_duplicate(name, span)?;
        let parent_id = self.resolve_parent(name, parent_name, span)?;
        self.by_name.insert(canonical, parent_id);
        Ok(parent_id)
    }

    /// `.type X <: Y` — `X` gets a fresh `TypeId` that the typechecker
    /// treats as distinct from siblings.
    pub(crate) fn register_subtype(
        &mut self,
        name: &str,
        parent_name: &str,
        span: Span,
    ) -> Result<TypeId, ParseError> {
        let canonical = self.reject_duplicate(name, span)?;
        let parent_id = self.resolve_parent(name, parent_name, span)?;
        // A subtype of a tuple type is not allowed.
        if self.tuples.contains_key(&parent_id) {
            return Err(ParseError::SubtypeOfTuple {
                span,
                name: name.to_string(),
                parent: parent_name.to_string(),
            });
        }
        let root = self.types[parent_id.0].root_primitive.clone();
        let id = TypeId(self.types.len());
        self.types.push(TypeDef {
            name: canonical.clone(),
            parent: Some(parent_id),
            root_primitive: root,
            span,
        });
        self.by_name.insert(canonical, id);
        Ok(id)
    }

    /// `.type T = (f0:T0, …, fk:Tk)` — register a fixed tuple type. `fields` is
    /// the list of `(field_name, field_type_name)` in source order.
    ///
    /// Two-phase so a self-reference can be *detected*: the tuple's own `TypeId`
    /// and name are reserved *before* its field types are looked up, so a field
    /// typed as this very tuple resolves to it. **Rejects recursion** (a field
    /// transitively reaching this tuple).
    pub(crate) fn register_tuple(
        &mut self,
        name: &str,
        fields: &[(String, String)],
        span: Span,
    ) -> Result<TypeId, ParseError> {
        let canonical = self.reject_duplicate(name, span)?;

        // Phase 1: reserve id + name before resolving fields, so a self-typed
        // field resolves to this tuple and the recursion check below sees it.
        let id = TypeId(self.types.len());
        self.types.push(TypeDef {
            name: canonical.clone(),
            parent: None,
            // Overwritten below once fields are erased.
            root_primitive: DataType::FixedTuple(Vec::new()),
            span,
        });
        self.by_name.insert(canonical, id);

        // Phase 2: resolve field types (self-name now resolves to `id`).
        let mut field_ids = Vec::with_capacity(fields.len());
        for (fname, ftype) in fields {
            let fid = self
                .lookup(ftype)
                .ok_or_else(|| ParseError::TupleFieldUnknownType {
                    span,
                    tuple: name.to_string(),
                    field: fname.clone(),
                    field_type: ftype.clone(),
                })?;
            field_ids.push(fid);
        }

        // Recursive tuples are not supported — reject at definition with a clean
        // error rather than carrying a representation we can't lower.
        if self.tuple_reaches(id, &field_ids) {
            return Err(ParseError::RecursiveTuple {
                span,
                name: name.to_string(),
            });
        }

        // Non-recursive: erase each field to its primitive (a nested tuple field
        // erases to its own already-computed fixed tuple).
        let erased: Vec<DataType> = field_ids
            .iter()
            .map(|&fid| self.root_primitive(fid))
            .collect();
        self.types[id.0].root_primitive = DataType::FixedTuple(erased);
        self.tuples.insert(id, TupleDef { fields: field_ids });
        Ok(id)
    }

    /// Does any of `fields` transitively reach `target` (i.e. is the tuple being
    /// registered recursive)? Follows nested tuple fields through the `tuples`
    /// table. The target's own `TupleDef` is not yet inserted during
    /// registration, so a direct self-reference is caught by `t == target`.
    fn tuple_reaches(&self, target: TypeId, fields: &[TypeId]) -> bool {
        let mut stack: Vec<TypeId> = fields.to_vec();
        let mut seen: std::collections::HashSet<TypeId> = std::collections::HashSet::new();
        while let Some(t) = stack.pop() {
            if t == target {
                return true;
            }
            if !seen.insert(t) {
                continue;
            }
            if let Some(def) = self.tuples.get(&t) {
                stack.extend(def.fields.iter().copied());
            }
        }
        false
    }

    fn reject_duplicate(&self, name: &str, span: Span) -> Result<String, ParseError> {
        let canonical = name.to_lowercase();
        if let Some(prior) = self.by_name.get(&canonical) {
            return Err(ParseError::DuplicateTypeDecl {
                span,
                prior: self.types[prior.0].span,
                name: name.to_string(),
            });
        }
        Ok(canonical)
    }

    fn resolve_parent(
        &self,
        name: &str,
        parent_name: &str,
        span: Span,
    ) -> Result<TypeId, ParseError> {
        self.lookup(parent_name)
            .ok_or_else(|| ParseError::UnknownTypeParent {
                span,
                name: name.to_string(),
                parent: parent_name.to_string(),
            })
    }

    // ─── Compatibility predicates used by the typechecker ────────────

    /// `true` iff `sub` widens to `sup` by walking parent pointers.
    /// Aliases share their parent's `TypeId`, so they're transparent
    /// here without extra handling.
    #[must_use]
    pub(crate) fn is_widening(&self, sub: TypeId, sup: TypeId) -> bool {
        let mut cur = sub;
        loop {
            if cur == sup {
                return true;
            }
            match self.types[cur.0].parent {
                Some(p) => cur = p,
                None => return false,
            }
        }
    }

    /// Most-specific common descendant. `Some(t)` when one type widens
    /// to the other; `None` for siblings or mismatched primitive roots.
    /// Variable-binding uses this to reject `R(x: UserId), S(x: ProductId)`.
    #[must_use]
    pub(crate) fn meet(&self, a: TypeId, b: TypeId) -> Option<TypeId> {
        if self.is_widening(a, b) {
            Some(a)
        } else if self.is_widening(b, a) {
            Some(b)
        } else {
            None
        }
    }

    /// Canonical (lowercased) name for diagnostics.
    #[must_use]
    pub(crate) fn name_of(&self, id: TypeId) -> &str {
        &self.types[id.0].name
    }

    /// Field `TypeId`s of a tuple type, or `None` if `id` is not a tuple.
    #[must_use]
    pub(crate) fn tuple_field_ids(&self, id: TypeId) -> Option<&[TypeId]> {
        self.tuples.get(&id).map(|def| def.fields.as_slice())
    }
}

impl Default for TypeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for TypeRegistry {
    /// Structural equality on the type list (name + kind + parent),
    /// so `#[derive(PartialEq)]` on `Program` can compare two parses.
    fn eq(&self, other: &Self) -> bool {
        self.types.len() == other.types.len()
            && self
                .types
                .iter()
                .zip(other.types.iter())
                .all(|(a, b)| a.name == b.name && a.parent == b.parent)
    }
}

impl Eq for TypeRegistry {}

impl fmt::Display for TypeRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, td) in self.types.iter().enumerate() {
            match td.parent {
                None => writeln!(f, "  [{i}] {} (primitive)", td.name)?,
                Some(p) => writeln!(f, "  [{i}] {} <: {}", td.name, self.name_of(p))?,
            }
        }
        Ok(())
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    // ── DataType ───────────────────────────────────────────────────────

    /// Every `DataType` variant — the canonical iteration target for tests
    /// that check a property across the whole enum.
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

    #[test]
    fn number_alias_parses_as_int32() {
        let parsed = DataType::from_str("number").unwrap();
        assert_eq!(parsed, DataType::Int32);
    }

    #[test]
    fn symbol_alias_parses_as_string() {
        let parsed = DataType::from_str("symbol").unwrap();
        assert_eq!(parsed, DataType::String);
    }

    #[test]
    fn unsigned_alias_parses_as_uint32() {
        let parsed = DataType::from_str("unsigned").unwrap();
        assert_eq!(parsed, DataType::UInt32);
    }

    #[test]
    fn float_alias_parses_as_float32() {
        let parsed = DataType::from_str("float").unwrap();
        assert_eq!(parsed, DataType::Float32);
    }

    #[test]
    fn from_str_invalid_returns_err() {
        let err = DataType::from_str("invalid").unwrap_err();
        assert!(err.contains("Invalid data type"));
    }

    #[test]
    fn is_numeric_returns_true_for_numeric_types() {
        for dt in ALL {
            let expected = !matches!(dt, DataType::String | DataType::Bool);
            assert_eq!(dt.is_numeric(), expected, "{dt}");
        }
    }

    #[test]
    fn is_float_returns_true_only_for_floats() {
        for dt in ALL {
            let expected = matches!(dt, DataType::Float32 | DataType::Float64);
            assert_eq!(dt.is_float(), expected, "{dt}");
        }
    }

    #[test]
    fn semiring_suffix_matches_expected() {
        assert_eq!(DataType::Int32.semiring_suffix(), "I32");
        assert_eq!(DataType::UInt64.semiring_suffix(), "U64");
        assert_eq!(DataType::Float32.semiring_suffix(), "F32");
    }

    // ── TypeRegistry ───────────────────────────────────────────────────

    fn reg() -> TypeRegistry {
        TypeRegistry::new()
    }

    /// Surface aliases must resolve to the same canonical `TypeId`;
    /// otherwise `.decl R(x: number)` and `.decl S(x: int32)` would
    /// disagree on column identity.
    #[test]
    fn primitives_seeded_with_aliases() {
        let r = reg();
        assert_eq!(r.lookup("number"), r.lookup("int32"));
        assert_eq!(r.lookup("symbol"), r.lookup("string"));
        assert_eq!(r.lookup("unsigned"), r.lookup("uint32"));
        assert_eq!(r.lookup("float"), r.lookup("f32"));
    }

    /// Core invariant: sibling subtypes have no meet, so the typechecker
    /// rejects `R(x: UserId), S(x: ProductId)`.
    #[test]
    fn meet_sibling_subtypes_rejected() {
        let mut r = reg();
        let a = r.register_subtype("UserId", "number", Span::DUMMY).unwrap();
        let b = r
            .register_subtype("ProductId", "number", Span::DUMMY)
            .unwrap();
        assert_eq!(r.meet(a, b), None);
    }

    /// Most-specific-wins, order-independent.
    #[test]
    fn meet_subtype_with_parent_picks_more_specific() {
        let mut r = reg();
        let s = r.register_subtype("UserId", "number", Span::DUMMY).unwrap();
        let n = r.primitive_id(DataType::Int32);
        assert_eq!(r.meet(s, n), Some(s));
        assert_eq!(r.meet(n, s), Some(s));
    }

    /// Asymmetric: subtype → parent yes, parent → subtype no.
    #[test]
    fn is_widening_is_asymmetric() {
        let mut r = reg();
        let s = r.register_subtype("UserId", "number", Span::DUMMY).unwrap();
        let n = r.primitive_id(DataType::Int32);
        assert!(r.is_widening(s, n));
        assert!(!r.is_widening(n, s));
    }

    // ── Tuples ─────────────────────────────────────────────────────────

    fn fields(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(n, t)| (n.to_string(), t.to_string()))
            .collect()
    }

    #[test]
    fn flat_tuple_erases_to_fixed_tuple() {
        let mut r = reg();
        let id = r
            .register_tuple(
                "Pair",
                &fields(&[("a", "symbol"), ("b", "symbol")]),
                Span::DUMMY,
            )
            .unwrap();
        assert_eq!(
            r.root_primitive(id),
            DataType::FixedTuple(vec![DataType::String, DataType::String])
        );
        // A `.decl` column of this type resolves to one tuple column.
        assert_eq!(r.lookup("Pair"), Some(id));
    }

    #[test]
    fn tuple_fields_may_be_heterogeneous() {
        let mut r = reg();
        let id = r
            .register_tuple(
                "Mix",
                &fields(&[("a", "symbol"), ("b", "number")]),
                Span::DUMMY,
            )
            .unwrap();
        assert_eq!(
            r.root_primitive(id),
            DataType::FixedTuple(vec![DataType::String, DataType::Int32])
        );
    }

    #[test]
    fn tuple_fields_may_nest_non_recursively() {
        let mut r = reg();
        r.register_tuple(
            "Pair",
            &fields(&[("a", "symbol"), ("b", "symbol")]),
            Span::DUMMY,
        )
        .unwrap();
        let outer = r
            .register_tuple(
                "Outer",
                &fields(&[("p", "Pair"), ("n", "number")]),
                Span::DUMMY,
            )
            .unwrap();
        assert_eq!(
            r.root_primitive(outer),
            DataType::FixedTuple(vec![
                DataType::FixedTuple(vec![DataType::String, DataType::String]),
                DataType::Int32,
            ])
        );
    }

    #[test]
    fn tuple_arity_is_not_capped_at_registration() {
        // No arity cap yet (see `register_tuple`): a wide tuple registers fine.
        // A >12 tuple still fails later in the generated crate until the nested
        // representation lands — that's intentional for now.
        let mut r = reg();
        let wide: Vec<(String, String)> = (0..13)
            .map(|i| (format!("f{i}"), "symbol".to_string()))
            .collect();
        assert!(r.register_tuple("Wide", &wide, Span::DUMMY).is_ok());
    }

    #[test]
    fn unknown_field_type_is_rejected() {
        let mut r = reg();
        assert!(matches!(
            r.register_tuple("R", &fields(&[("a", "Nope")]), Span::DUMMY),
            Err(ParseError::TupleFieldUnknownType { .. })
        ));
    }

    #[test]
    fn self_referential_tuple_is_rejected() {
        let mut r = reg();
        // `.type List = ( head: symbol, tail: List )` — a tuple referencing its
        // own type is recursive and rejected at registration.
        assert!(matches!(
            r.register_tuple(
                "List",
                &fields(&[("head", "symbol"), ("tail", "List")]),
                Span::DUMMY,
            ),
            Err(ParseError::RecursiveTuple { .. })
        ));
    }

    #[test]
    fn subtyping_a_tuple_is_rejected() {
        let mut r = reg();
        r.register_tuple(
            "Pair",
            &fields(&[("a", "symbol"), ("b", "symbol")]),
            Span::DUMMY,
        )
        .unwrap();
        assert!(matches!(
            r.register_subtype("P2", "Pair", Span::DUMMY),
            Err(ParseError::SubtypeOfTuple { .. })
        ));
    }
}
