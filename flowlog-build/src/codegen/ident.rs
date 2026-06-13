//! Ident lookup for relation fingerprints.
//!
//! "Global" idents are stable across strata (one entry per EDB/IDB), while
//! "local" idents belong to a recursion scope — collections that entered
//! the scope get a fresh binding inside it.
//!
//! Global binding idents are *synthetic*: `rel_<N>_<name>`, where `<N>` is
//! the relation's declaration index. The `rel_<N>` prefix alone makes the
//! ident unique and keyword-proof no matter what the relation is called —
//! a relation may be named `type`, `crate_`, or anything else the grammar
//! accepts, with no escaping or collision handling. The name suffix is
//! purely cosmetic, kept (after sanitization) so generated code stays
//! greppable against the source program.
//!
//! Human-facing output (profiler labels, diagnostics) never shows these
//! idents; it goes through [`CodeGen::display_name`], which resolves the
//! fingerprint back to the declaration and uses the user's original
//! spelling ([`Relation::raw_name`]).
//!
//! [`Relation::raw_name`]: crate::parser::Relation::raw_name

use std::collections::HashMap;

use proc_macro2::Ident;
use quote::format_ident;

use crate::codegen::CodeGen;

/// Mint the synthetic binding ident for the `index`-th declared relation.
///
/// The name suffix is sanitized to ASCII alphanumerics/underscores (the
/// inliner's `·` separator and any other exotic character become `_`).
/// Lossy sanitization is harmless: uniqueness comes from `index`, never
/// from the suffix.
fn binding_ident(index: usize, name: &str) -> Ident {
    let sanitized: String = name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    format_ident!("rel_{}_{}", index, sanitized)
}

impl CodeGen {
    /// Seed the global `fingerprint → binding ident` map from every declared
    /// relation (EDB + IDB). Declaration order makes `<N>` deterministic,
    /// keeping generated code stable across runs.
    pub(super) fn make_global_ident_map(&mut self) {
        self.global_fp_to_ident = self
            .program
            .relations()
            .iter()
            .enumerate()
            .map(|(index, rel)| (rel.fingerprint(), binding_ident(index, rel.name())))
            .collect();
    }

    /// Resolve a fingerprint to its global ident, falling back to `t_<fp>`
    /// for intermediate collections produced during codegen.
    pub(super) fn find_global_ident(&self, fp: u64) -> Ident {
        self.global_fp_to_ident
            .get(&fp)
            .cloned()
            .unwrap_or_else(|| format_ident!("t_{}", fp))
    }

    /// Human-facing name for a fingerprint (profiler labels, diagnostics):
    /// the declared relation's original spelling
    /// ([`Relation::raw_name`](crate::parser::Relation::raw_name)), or the
    /// binding ident's text for synthesized intermediates.
    pub(super) fn display_name(&self, fp: u64) -> String {
        self.program
            .relation_by_fingerprint(fp)
            .map(|rel| rel.raw_name().to_string())
            .unwrap_or_else(|| self.find_global_ident(fp).to_string())
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
mod binding_ident_tests {
    use super::binding_ident;
    use quote::quote;

    /// Keyword-named relations get ordinary, valid bindings — the `rel_<N>`
    /// prefix means no name can produce a keyword ident.
    #[test]
    fn keyword_names_are_harmless() {
        for (i, kw) in ["type", "match", "crate", "self", "Self", "super"]
            .iter()
            .enumerate()
        {
            let id = binding_ident(i, kw);
            let ts = quote! { { let #id = 1; let _ = #id; } };
            syn::parse2::<syn::Block>(ts).unwrap_or_else(|e| {
                panic!("keyword {kw:?} -> `{id}` is not a usable binding: {e}")
            });
        }
        assert_eq!(binding_ident(0, "type").to_string(), "rel_0_type");
    }

    /// Uniqueness comes from the index alone — even *identical* names get
    /// distinct bindings, so the colliding pairs of per-name escaping
    /// (`crate` vs `crate_`) are trivially safe.
    #[test]
    fn identical_names_distinct_by_index() {
        assert_ne!(
            binding_ident(3, "edge").to_string(),
            binding_ident(4, "edge").to_string()
        );
        assert_eq!(binding_ident(1, "crate_").to_string(), "rel_1_crate_");
    }

    /// Non-ASCII characters (the inliner's `·` separator) sanitize to `_`,
    /// keeping generated code ASCII.
    #[test]
    fn inliner_separator_is_sanitized() {
        assert_eq!(binding_ident(2, "c·holdsat").to_string(), "rel_2_c_holdsat");
    }
}
