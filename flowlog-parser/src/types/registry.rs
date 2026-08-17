//! The per-program table of named types.
//!
//! - [`TypeRegistry`]: interns every named type (primitives plus user
//!   `.type` aliases, subtypes, and tuples) under a [`TypeId`]. Subtype
//!   identity exists only here; downstream stages see only root
//!   [`DataType`]s, so subtypes are zero-cost compile-time phantom
//!   types.

use std::collections::HashMap;
use std::collections::HashSet;

use flowlog_error::FileId;
use flowlog_error::Span;
use pest::iterators::Pair;

use super::DataType;
use super::data_type::PRIM_NAMES;
use crate::Node;
use crate::Rule;
use crate::declaration::RawTypeOp;
use crate::declaration::split_type_alias;
use crate::error::ParseError;

// =============================================================================
// TypeRegistry
// =============================================================================

/// Stable handle for a named type: an index into the registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TypeId(usize);

/// One row in the registry: a distinct type identity (primitive,
/// subtype, or tuple). Aliases add no row (see `by_name`).
#[derive(Debug, Clone)]
struct TypeDef {
    name: String,
    /// `None` for primitives; `Some` for subtypes (the type they refine).
    parent: Option<TypeId>,
    /// What this type erases to; cached so `root_primitive()` is O(1).
    root_primitive: DataType,
    /// `Span::DUMMY` for built-in primitives.
    span: Span,
}

/// Definition of a tuple type, keyed by its [`TypeId`] in [`TypeRegistry`].
#[derive(Debug, Clone)]
struct TupleDef {
    fields: Vec<TypeId>,
}

/// Interned table of every type the program can refer to.
///
/// Built in declaration order (define-before-use, no retry) during
/// parsing; read-only after.
#[derive(Debug, Clone)]
pub(crate) struct TypeRegistry {
    /// One `TypeDef` per distinct identity, append-only; a `TypeId` is
    /// an index here, so ids stay dense and stable.
    types: Vec<TypeDef>,
    /// Lowercased spelling to id, for `lookup`. Many-to-one: primitive
    /// aliases (`number`) and `.type X = Y` aliases insert extra names
    /// for an existing id instead of new `types` entries.
    by_name: HashMap<String, TypeId>,
    /// Field lists for the ids in `types` that are tuples.
    tuples: HashMap<TypeId, TupleDef>,
}

impl TypeRegistry {
    /// Pre-populated with the 12 built-in primitives.
    #[must_use]
    pub(crate) fn new() -> Self {
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

    /// Case-insensitive surface-name lookup; `None` for a name not yet
    /// registered (the registry is built define-before-use, so a forward
    /// reference does not resolve).
    #[must_use]
    pub(crate) fn lookup(&self, name: &str) -> Option<TypeId> {
        self.by_name.get(&name.to_lowercase()).copied()
    }

    /// The erased [`DataType`] this type lowers to: a primitive returns
    /// itself, a subtype its ancestor primitive, a tuple its fixed-tuple
    /// shape.
    #[must_use]
    #[inline]
    pub(crate) fn root_primitive(&self, id: TypeId) -> DataType {
        self.types[id.0].root_primitive.clone()
    }

    /// Canonical `TypeId` for a built-in primitive, or `None` for types
    /// with no registry row (tuples and the lit families are not seeded).
    #[must_use]
    pub(crate) fn primitive_id(&self, dt: DataType) -> Option<TypeId> {
        PRIM_NAMES.iter().position(|(p, _)| p == &dt).map(TypeId)
    }

    /// Build the registry for a program: register every top-level `.type`
    /// declaration in `parsed_rule`. Define-before-use: a `.type` may reference
    /// only primitives or types declared earlier in source, so a cycle
    /// (including `.type X = X`) surfaces as `UnknownTypeParent`.
    ///
    /// `.type` declarations inside `.comp` bodies are skipped: the inliner
    /// registers per-instance prefixed types during expansion.
    pub(crate) fn from_type_declarations(
        parsed_rule: Pair<Rule>,
        file: FileId,
    ) -> Result<Self, ParseError> {
        let mut registry = Self::new();
        for node in parsed_rule.into_inner() {
            if node.as_rule() != Rule::type_alias_decl {
                continue;
            }
            let (name, op, parent, span) = split_type_alias(Node::new(node, file))?;
            match op {
                RawTypeOp::Alias => registry.register_alias(&name, &parent, span)?,
                RawTypeOp::Subtype => registry.register_subtype(&name, &parent, span)?,
                RawTypeOp::Tuple(fields) => registry.register_tuple(&name, &fields, span)?,
            };
        }
        Ok(registry)
    }

    /// Registers `.type X = Y`: `X` becomes a synonym for `Y`'s existing
    /// `TypeId`. No new entry is created; the alias is invisible after
    /// registration.
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

    /// Registers `.type X <: Y`: `X` gets a fresh `TypeId` that the
    /// typechecker treats as distinct from siblings.
    pub(crate) fn register_subtype(
        &mut self,
        name: &str,
        parent_name: &str,
        span: Span,
    ) -> Result<TypeId, ParseError> {
        let canonical = self.reject_duplicate(name, span)?;
        let parent_id = self.resolve_parent(name, parent_name, span)?;
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

    /// Registers a fixed tuple type `.type T = (f0: T0, ..., fk: Tk)`.
    /// `fields` is the list of `(field_name, field_type_name)` in source
    /// order.
    ///
    /// Recursion is rejected as `RecursiveTuple`: a field naming this
    /// very tuple resolves (it is not an unknown type), as does any
    /// field reaching this tuple transitively.
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

        // Recursive tuples are not supported: reject at definition with a
        // clean error rather than carry a representation we can't lower.
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

    /// Returns `true` if any of `fields` transitively reaches `target`,
    /// i.e. the tuple being registered is recursive. Works before
    /// `target`'s own `TupleDef` is inserted: a direct self-reference is
    /// caught by id comparison, not the `tuples` table.
    fn tuple_reaches(&self, target: TypeId, fields: &[TypeId]) -> bool {
        let mut stack: Vec<TypeId> = fields.to_vec();
        let mut seen: HashSet<TypeId> = HashSet::new();
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

    // --- Compatibility predicates ---

    /// Returns `true` if `sub` is `sup` or refines it through any chain
    /// of subtype declarations. Aliases share their parent's `TypeId`,
    /// so they are transparent here without extra handling.
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

    /// Returns whichever of `a` and `b` is the more specific type when
    /// one widens to the other, or `None` when neither does (sibling
    /// subtypes, or types with different primitive roots).
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

impl PartialEq for TypeRegistry {
    /// Structural equality on the type list: same length, each row
    /// agreeing on name and parent. Spans and cached erasures are
    /// identity-irrelevant.
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

// =============================================================================
// Tests
// =============================================================================
#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::assert_err;

    /// Surface aliases must resolve to the same canonical `TypeId`;
    /// otherwise `.decl R(x: number)` and `.decl S(x: int32)` would
    /// disagree on column identity.
    #[test]
    fn primitives_seeded_with_aliases() {
        let r = TypeRegistry::new();
        assert_eq!(r.lookup("number"), r.lookup("int32"));
        assert_eq!(r.lookup("symbol"), r.lookup("string"));
        assert_eq!(r.lookup("unsigned"), r.lookup("uint32"));
        assert_eq!(r.lookup("float"), r.lookup("f32"));
    }

    /// Guards the seeding invariant: a primitive's `TypeId` is its
    /// `PRIM_NAMES` row position, so id-by-value and id-by-name agree.
    #[test]
    fn primitive_id_matches_seeded_lookup() {
        let r = TypeRegistry::new();
        for (dt, names) in PRIM_NAMES {
            assert_eq!(r.primitive_id(dt.clone()), r.lookup(names[0]), "{dt}");
        }
    }

    #[rstest]
    #[case(DataType::FixedTuple(vec![]))]
    #[case(DataType::IntLit)]
    #[case(DataType::FloatLit)]
    fn primitive_id_is_none_for_unseeded_types(#[case] dt: DataType) {
        let r = TypeRegistry::new();
        assert_eq!(r.primitive_id(dt), None);
    }

    #[test]
    fn lookup_is_case_insensitive() {
        let r = TypeRegistry::new();
        assert_eq!(r.lookup("NUMBER"), r.lookup("number"));
        assert!(r.lookup("Int32").is_some());
    }

    #[test]
    fn lookup_unknown_name_is_none() {
        let r = TypeRegistry::new();
        assert_eq!(r.lookup("nosuchtype"), None);
    }

    /// `.type X = Y` creates a synonym: the returned id is the parent's,
    /// and looking either name up agrees.
    #[test]
    fn alias_resolves_to_parent_id() {
        let mut r = TypeRegistry::new();
        let id = r.register_alias("Money", "number", Span::DUMMY).unwrap();
        assert_eq!(Some(id), r.lookup("number"));
        assert_eq!(r.lookup("money"), r.lookup("number"));
    }

    /// Alias chains resolve transitively: `A = B = C = number` all share the
    /// `number` id and bottom out at the `Int32` primitive.
    #[test]
    fn alias_chain_resolves_transitively() {
        let mut r = TypeRegistry::new();
        r.register_alias("C", "number", Span::DUMMY).unwrap();
        r.register_alias("B", "C", Span::DUMMY).unwrap();
        let a = r.register_alias("A", "B", Span::DUMMY).unwrap();
        assert_eq!(Some(a), r.lookup("number"));
        assert_eq!(r.root_primitive(a), DataType::Int32);
    }

    /// Core invariant: sibling subtypes have no meet, so the typechecker
    /// rejects `R(x: UserId), S(x: ProductId)`.
    #[test]
    fn meet_sibling_subtypes_rejected() {
        let mut r = TypeRegistry::new();
        let a = r.register_subtype("UserId", "number", Span::DUMMY).unwrap();
        let b = r
            .register_subtype("ProductId", "number", Span::DUMMY)
            .unwrap();
        assert_eq!(r.meet(a, b), None);
    }

    /// Most-specific-wins, order-independent.
    #[test]
    fn meet_subtype_with_parent_picks_more_specific() {
        let mut r = TypeRegistry::new();
        let s = r.register_subtype("UserId", "number", Span::DUMMY).unwrap();
        let n = r.primitive_id(DataType::Int32).unwrap();
        assert_eq!(r.meet(s, n), Some(s));
        assert_eq!(r.meet(n, s), Some(s));
    }

    #[test]
    fn meet_of_unrelated_primitives_is_none() {
        let r = TypeRegistry::new();
        let n = r.primitive_id(DataType::Int32).unwrap();
        let s = r.primitive_id(DataType::String).unwrap();
        assert_eq!(r.meet(n, s), None);
    }

    #[test]
    fn is_widening_walks_a_subtype_chain_transitively() {
        let mut r = TypeRegistry::new();
        let a = r.register_subtype("A", "number", Span::DUMMY).unwrap();
        let b = r.register_subtype("B", "A", Span::DUMMY).unwrap();
        let n = r.primitive_id(DataType::Int32).unwrap();
        assert!(r.is_widening(b, n));
        assert!(!r.is_widening(n, b));
        assert_eq!(r.meet(b, a), Some(b));
    }

    /// Asymmetric: a subtype widens to its parent, but not the reverse.
    #[test]
    fn is_widening_is_asymmetric() {
        let mut r = TypeRegistry::new();
        let s = r.register_subtype("UserId", "number", Span::DUMMY).unwrap();
        let n = r.primitive_id(DataType::Int32).unwrap();
        assert!(r.is_widening(s, n));
        assert!(!r.is_widening(n, s));
    }

    #[test]
    fn name_of_returns_canonical_lowercased_name() {
        let mut r = TypeRegistry::new();
        let user = r.register_subtype("UserId", "number", Span::DUMMY).unwrap();
        assert_eq!(r.name_of(user), "userid");
        assert_eq!(r.name_of(r.primitive_id(DataType::Int32).unwrap()), "int32");
    }

    /// `PartialEq` is structural: two registries are equal iff their type
    /// lists have the same length and agree on each row's name and parent.
    #[test]
    fn partial_eq_is_structural() {
        let sub = |name: &str, parent: &str| {
            let mut r = TypeRegistry::new();
            r.register_subtype(name, parent, Span::DUMMY).unwrap();
            r
        };

        // Same declarations compare equal.
        assert_eq!(sub("Foo", "number"), sub("Foo", "number"));

        // Differing only by name (same length, same parent) is not equal.
        assert_ne!(sub("Foo", "number"), sub("Bar", "number"));

        // Differing only by parent (same length, same name) is not equal.
        assert_ne!(sub("Foo", "int32"), sub("Foo", "int64"));

        // Differ by length, but the shorter list is a prefix of the longer.
        let mut longer = sub("Foo", "number");
        longer
            .register_subtype("Bar", "number", Span::DUMMY)
            .unwrap();
        assert_ne!(sub("Foo", "number"), longer);
    }

    // --- Tuples ---

    fn fields(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(n, t)| (n.to_string(), t.to_string()))
            .collect()
    }

    #[test]
    fn flat_tuple_erases_to_fixed_tuple() {
        let mut r = TypeRegistry::new();
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
    fn tuple_field_ids_reflects_tuple_shape() {
        let mut r = TypeRegistry::new();
        let int32 = r.primitive_id(DataType::Int32).unwrap();
        assert_eq!(r.tuple_field_ids(int32), None);

        let id = r
            .register_tuple(
                "Mix",
                &fields(&[("a", "symbol"), ("b", "number")]),
                Span::DUMMY,
            )
            .unwrap();
        let field_ids = r.tuple_field_ids(id).expect("Mix is a tuple");
        assert_eq!(
            field_ids,
            [
                r.primitive_id(DataType::String).unwrap(),
                r.primitive_id(DataType::Int32).unwrap()
            ]
            .as_slice()
        );
    }

    #[test]
    fn tuple_fields_may_be_heterogeneous() {
        let mut r = TypeRegistry::new();
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
        let mut r = TypeRegistry::new();
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
        // representation lands; that's intentional for now.
        let mut r = TypeRegistry::new();
        let wide: Vec<(String, String)> = (0..13)
            .map(|i| (format!("f{i}"), "symbol".to_string()))
            .collect();
        assert!(r.register_tuple("Wide", &wide, Span::DUMMY).is_ok());
    }

    #[test]
    fn unknown_field_type_is_rejected() {
        let mut r = TypeRegistry::new();
        assert_err!(
            r.register_tuple("R", &fields(&[("a", "Nope")]), Span::DUMMY),
            ParseError::TupleFieldUnknownType { .. }
        );
    }

    #[test]
    fn self_referential_tuple_is_rejected() {
        let mut r = TypeRegistry::new();
        // `.type List = ( head: symbol, tail: List )`: a tuple referencing its
        // own type is recursive and rejected at registration.
        assert_err!(
            r.register_tuple(
                "List",
                &fields(&[("head", "symbol"), ("tail", "List")]),
                Span::DUMMY,
            ),
            ParseError::RecursiveTuple { .. }
        );
    }

    #[test]
    fn subtyping_a_tuple_is_rejected() {
        let mut r = TypeRegistry::new();
        r.register_tuple(
            "Pair",
            &fields(&[("a", "symbol"), ("b", "symbol")]),
            Span::DUMMY,
        )
        .unwrap();
        assert_err!(
            r.register_subtype("P2", "Pair", Span::DUMMY),
            ParseError::SubtypeOfTuple { .. }
        );
    }

    #[test]
    fn register_subtype_of_unknown_parent_is_rejected() {
        let mut r = TypeRegistry::new();
        assert_err!(
            r.register_subtype("UserId", "NoSuchType", Span::DUMMY),
            ParseError::UnknownTypeParent { .. }
        );
    }

    #[test]
    fn register_alias_of_unknown_parent_is_rejected() {
        let mut r = TypeRegistry::new();
        assert_err!(
            r.register_alias("Money", "NoSuchType", Span::DUMMY),
            ParseError::UnknownTypeParent { .. }
        );
    }

    #[test]
    fn registering_a_duplicate_type_name_is_rejected() {
        let mut r = TypeRegistry::new();
        r.register_alias("Id", "number", Span::DUMMY).unwrap();
        assert_err!(
            r.register_subtype("Id", "number", Span::DUMMY),
            ParseError::DuplicateTypeDecl { .. }
        );
    }

    #[test]
    fn registering_a_name_that_shadows_a_primitive_is_rejected() {
        // `.type number = int64` collides with the built-in primitive `number`.
        let mut r = TypeRegistry::new();
        assert_err!(
            r.register_alias("number", "int64", Span::DUMMY),
            ParseError::DuplicateTypeDecl { .. }
        );
    }
}
