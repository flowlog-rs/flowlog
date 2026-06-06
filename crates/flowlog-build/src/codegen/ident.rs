//! Ident lookup for relation fingerprints.
//!
//! "Global" idents are stable across strata (one entry per EDB/IDB), while
//! "local" idents belong to a recursion scope — collections that entered
//! the scope get a fresh binding inside it.

use std::collections::HashMap;

use proc_macro2::Ident;
use quote::format_ident;

use crate::build::relation::rust_ident;
use crate::codegen::CodeGen;

/// Two distinct relations whose names lower to the *same* Rust binding ident.
#[derive(Debug)]
struct BindingCollision {
    ident: String,
    first: String,
    second: String,
}

/// Build the `fingerprint → binding ident` map, rejecting the case where two
/// distinct relations lower to the same Rust ident.
///
/// [`rust_ident`] escapes per-name, so it cannot see the other relations and
/// cannot guarantee global uniqueness: a keyword and an ordinary name can
/// collapse together (`crate` escapes to `crate_`, and a relation literally
/// named `crate_` already *is* `crate_`). Two equal binding idents would emit
/// two `let <ident> = …` statements, silently shadowing one relation — a
/// miscompile that is worse than a panic. We reject it loudly here instead.
///
/// (The collision is a structural limit of per-name escaping; a follow-up that
/// binds relations to synthetic idents removes it by construction.)
fn build_global_ident_map<'a>(
    relations: impl IntoIterator<Item = (u64, &'a str)>,
) -> Result<HashMap<u64, Ident>, BindingCollision> {
    let mut map: HashMap<u64, Ident> = HashMap::new();
    let mut owner: HashMap<String, (u64, &'a str)> = HashMap::new();
    for (fingerprint, name) in relations {
        let ident = rust_ident(name);
        let ident_str = ident.to_string();
        match owner.get(&ident_str).copied() {
            // A *different* relation already claimed this ident — collision.
            Some((prev_fp, prev_name)) if prev_fp != fingerprint => {
                return Err(BindingCollision {
                    ident: ident_str,
                    first: prev_name.to_string(),
                    second: name.to_string(),
                });
            }
            // Same relation seen twice (identical fingerprint) — harmless.
            Some(_) => {}
            None => {
                owner.insert(ident_str, (fingerprint, name));
            }
        }
        map.insert(fingerprint, ident);
    }
    Ok(map)
}

impl CodeGen {
    /// Seed the global `fingerprint → ident` map from every declared
    /// relation (EDB + IDB), using the relation's own name as the ident.
    ///
    /// Names are routed through [`rust_ident`] so a relation whose name
    /// collides with a Rust keyword (`type`, `match`, …) is escaped rather
    /// than emitted verbatim into an invalid binding. Because that escape is
    /// per-name it cannot guarantee uniqueness, so we also guard against two
    /// distinct relations lowering to the same binding (see
    /// [`build_global_ident_map`]).
    pub(super) fn make_global_ident_map(&mut self) {
        let relations = self
            .program
            .relations()
            .iter()
            .map(|rel| (rel.fingerprint(), rel.name()));
        self.global_fp_to_ident = build_global_ident_map(relations).unwrap_or_else(|c| {
            panic!(
                "codegen: relations `{}` and `{}` both lower to the Rust binding \
                 `{}`; per-name keyword escaping cannot guarantee unique \
                 collection idents. Rename one relation. (A follow-up that binds \
                 relations to synthetic idents will remove this limitation.)",
                c.first, c.second, c.ident,
            )
        });
    }

    /// Resolve a fingerprint to its global ident, falling back to `t_<fp>`
    /// for intermediate collections produced during codegen.
    pub(super) fn find_global_ident(&self, fp: u64) -> Ident {
        self.global_fp_to_ident
            .get(&fp)
            .cloned()
            .unwrap_or_else(|| format_ident!("t_{}", fp))
    }
}

/// Resolve a fingerprint to its local ident inside a recursion scope,
/// falling back to `t_<fp>` for intermediates.
pub(crate) fn find_local_ident(local_fp_to_ident: &HashMap<u64, Ident>, fp: u64) -> Ident {
    local_fp_to_ident
        .get(&fp)
        .cloned()
        .unwrap_or_else(|| format_ident!("t_{}", fp))
}

#[cfg(test)]
mod global_ident_map_tests {
    use super::build_global_ident_map;

    /// Distinct relations keep distinct bindings (the common case).
    #[test]
    fn distinct_relations_get_distinct_bindings() {
        let map = build_global_ident_map([(1u64, "Edge"), (2u64, "Path")]).unwrap();
        assert_eq!(map.len(), 2);
        assert_eq!(map[&1].to_string(), "Edge");
        assert_eq!(map[&2].to_string(), "Path");
    }

    /// A keyword and its underscore-escaped twin collapse to one ident — the
    /// silent-miscompile the guard must reject. `crate` escapes to `crate_`,
    /// and a relation literally named `crate_` is already `crate_`.
    #[test]
    fn keyword_and_underscore_twin_collide() {
        let err = build_global_ident_map([(1u64, "crate"), (2u64, "crate_")]).unwrap_err();
        assert_eq!(err.ident, "crate_");
        assert_eq!(err.first, "crate");
        assert_eq!(err.second, "crate_");
    }

    /// All four non-raw-able keywords (`self`/`Self`/`super`/`crate`) clash with
    /// their underscore twin the same way.
    #[test]
    fn every_non_rawable_keyword_clashes_with_its_twin() {
        for kw in ["self", "Self", "super", "crate"] {
            let twin = format!("{kw}_");
            let err = build_global_ident_map([(1u64, kw), (2u64, twin.as_str())]).unwrap_err();
            assert_eq!(err.ident, twin, "keyword `{kw}` should clash with `{twin}`");
        }
    }

    /// The same relation appearing twice (identical fingerprint) is not a
    /// collision — it legitimately reuses one binding.
    #[test]
    fn same_relation_repeated_is_not_a_collision() {
        let map = build_global_ident_map([(7u64, "Edge"), (7u64, "Edge")]).unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(map[&7].to_string(), "Edge");
    }

    /// Distinct keyword relations that do *not* share an escaped form coexist
    /// fine (`type` → `r#type`, `match` → `r#match`).
    #[test]
    fn distinct_keywords_do_not_collide() {
        let map = build_global_ident_map([(1u64, "type"), (2u64, "match")]).unwrap();
        assert_eq!(map[&1].to_string(), "r#type");
        assert_eq!(map[&2].to_string(), "r#match");
    }
}
