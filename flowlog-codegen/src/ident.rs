//! Ident lookup for relation fingerprints.
//!
//! "Global" idents are stable across strata (one entry per EDB/IDB), while
//! "local" idents belong to a recursion scope: collections that entered
//! the scope get a fresh binding inside it.
//!
//! Global binding idents are *synthetic*: `rel_<N>_<name>`, where `<N>` is
//! the relation's declaration index. The `rel_<N>` prefix alone makes the
//! ident unique and keyword-proof no matter what the relation is called:
//! a relation may be named `type`, `crate_`, or anything else the grammar
//! accepts, with no escaping or collision handling. The name suffix is
//! purely cosmetic, kept (after sanitization) so generated code stays
//! greppable against the source program.
//!
//! An intermediate collection, one no relation declares, binds as
//! `t_<fp>` ([`intermediate_ident`]).
//!
//! Human-facing output (profiler labels, diagnostics) never shows a
//! relation's binding ident; it goes through [`CodeGen::display_name`],
//! which resolves the fingerprint back to the declaration and uses the
//! user's original spelling ([`Relation::raw_name`]).
//!
//! [`Relation::raw_name`]: flowlog_parser::Relation::raw_name

use std::collections::HashMap;

use proc_macro2::Ident;
use quote::format_ident;

use crate::CodeGen;

// =============================================================================
// Naming scheme
// =============================================================================

/// Returns the synthetic binding ident for the `index`-th declared relation.
///
/// The name suffix is sanitized to ASCII alphanumerics and underscores: the
/// inliner's middle-dot separator, U+00B7, and any other exotic character
/// become `_`. Lossy sanitization is harmless: uniqueness comes from
/// `index`, never from the suffix.
fn binding_ident(index: usize, name: &str) -> Ident {
    let sanitized: String = name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    format_ident!("rel_{}_{}", index, sanitized)
}

/// Returns the binding ident of the intermediate collection `fp`.
pub(crate) fn intermediate_ident(fp: u64) -> Ident {
    format_ident!("t_{}", fp)
}

/// Returns the ident of the input `name`'s handle, `h<name>`: the input
/// session the dataflow returns and the `Inputs` constructor takes.
pub fn input_handle_ident(name: &str) -> Ident {
    format_ident!("h{}", name)
}

/// Returns the `Inputs` field `in_<name>` that holds the input `name`'s
/// loader. The prefix keeps a relation named like a Rust keyword a valid
/// field.
pub fn input_field_ident(name: &str) -> Ident {
    format_ident!("in_{}", name)
}

/// Returns the marker type `Rel<name>` that implements the runtime
/// `Relation` for `name`.
pub fn relation_marker_ident(name: &str) -> Ident {
    format_ident!("Rel{}", name)
}

/// Returns the ident `buf_<name>` of the output `name`'s emitter.
pub fn output_emitter_ident(name: &str) -> Ident {
    format_ident!("buf_{}", name)
}

// =============================================================================
// Global idents
// =============================================================================

impl CodeGen {
    /// Seeds the global `fingerprint -> binding ident` map from every
    /// declared relation (EDB + IDB). Declaration order makes `<N>`
    /// deterministic, keeping generated code stable across runs.
    pub(super) fn make_global_ident_map(&mut self) {
        self.global_fp_to_ident = self
            .program
            .relations()
            .iter()
            .enumerate()
            .map(|(index, rel)| (rel.fingerprint(), binding_ident(index, rel.name())))
            .collect();
    }

    /// Returns the global ident of fingerprint `fp`: its relation's binding,
    /// or [`intermediate_ident`] for a collection no relation declares.
    pub(super) fn find_global_ident(&self, fp: u64) -> Ident {
        self.global_fp_to_ident
            .get(&fp)
            .cloned()
            .unwrap_or_else(|| intermediate_ident(fp))
    }
}

// =============================================================================
// Local idents
// =============================================================================

/// Returns the ident of fingerprint `fp` in a scope whose bindings are
/// `local_fp_to_ident`: its entry there, or [`intermediate_ident`] for a
/// collection the scope does not rebind.
pub(crate) fn find_local_ident(local_fp_to_ident: &HashMap<u64, Ident>, fp: u64) -> Ident {
    local_fp_to_ident
        .get(&fp)
        .cloned()
        .unwrap_or_else(|| intermediate_ident(fp))
}

// =============================================================================
// Display names
// =============================================================================

impl CodeGen {
    /// Returns the human-facing name of fingerprint `fp` (profiler labels,
    /// diagnostics): the declared relation's original spelling
    /// ([`Relation::raw_name`](flowlog_parser::Relation::raw_name)), or the
    /// binding ident's text for an intermediate collection.
    pub(super) fn display_name(&self, fp: u64) -> String {
        self.program
            .relation_by_fingerprint(fp)
            .map(|rel| rel.raw_name().to_string())
            .unwrap_or_else(|| self.find_global_ident(fp).to_string())
    }
}

#[cfg(test)]
mod tests {
    use flowlog_common::Config;
    use flowlog_common::SourceMap;
    use quote::quote;
    use syn::Block;
    use syn::parse2;

    use super::*;

    /// Keyword-named relations get ordinary, valid bindings: the `rel_<N>`
    /// prefix means no name can produce a keyword ident.
    #[test]
    fn keyword_names_are_harmless() {
        for (i, kw) in ["type", "match", "crate", "self", "Self", "super"]
            .iter()
            .enumerate()
        {
            let id = binding_ident(i, kw);
            let ts = quote! { { let #id = 1; let _ = #id; } };
            parse2::<Block>(ts).unwrap_or_else(|e| {
                panic!("keyword {kw:?} -> `{id}` is not a usable binding: {e}")
            });
        }
        assert_eq!(binding_ident(0, "type").to_string(), "rel_0_type");
    }

    /// Uniqueness comes from the index alone: even *identical* names get
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

    /// Non-ASCII characters (the inliner's U+00B7 separator) sanitize to `_`,
    /// keeping generated code ASCII.
    #[test]
    fn inliner_separator_is_sanitized() {
        assert_eq!(
            binding_ident(2, "c\u{b7}holdsat").to_string(),
            "rel_2_c_holdsat"
        );
    }

    /// A scope's own binding wins; a collection it does not rebind keeps its
    /// intermediate ident.
    #[test]
    fn a_local_lookup_falls_back_to_the_intermediate_ident() {
        let local = HashMap::from([(7, format_ident!("in_rel_0_edge"))]);
        assert_eq!(find_local_ident(&local, 7).to_string(), "in_rel_0_edge");
        assert_eq!(find_local_ident(&local, 8).to_string(), "t_8");
    }

    /// A relation displays as its source spelling, never its binding; an
    /// intermediate collection displays as its ident.
    #[test]
    fn display_names_use_the_source_spelling_of_a_relation() {
        let mut file = tempfile::NamedTempFile::new().expect("tempfile");
        std::io::Write::write_all(&mut file, b".decl Edge(x: int32)\n.input Edge\n")
            .expect("write");
        let mut config = Config::default();
        let program = flowlog_parser::parse(
            &file.path().to_string_lossy(),
            &[],
            &mut SourceMap::default(),
            &mut config,
        )
        .expect("program");
        let edge = program.relations()[0].fingerprint();
        let mut codegen = CodeGen::new(config, program);
        codegen.make_global_ident_map();

        assert_eq!(codegen.find_global_ident(edge).to_string(), "rel_0_edge");
        assert_eq!(codegen.display_name(edge), "Edge");
        assert_eq!(codegen.display_name(9), "t_9");
    }
}
