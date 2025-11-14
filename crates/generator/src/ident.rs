use proc_macro2::Ident;
use quote::format_ident;
use std::collections::HashMap;

use parser::Relation;

/// =========================================================================
/// Ident Mapping Utilities
/// =========================================================================

/// Map input and output fingerprints to collection identifiers.
pub(super) fn make_ident_map(
    input_rels: Vec<&Relation>,
    output_rels: Vec<&Relation>,
) -> HashMap<u64, Ident> {
    input_rels
        .into_iter()
        .map(|rel| (rel.fingerprint(), format_ident!("{}", rel.name())))
        .chain(
            output_rels
                .into_iter()
                .map(|rel| (rel.fingerprint(), format_ident!("{}", rel.name()))),
        )
        .collect()
}

/// Lookup the variable name for a given collection fp, or fall back to `t_<fp>`.
pub(super) fn find_ident(fp_to_ident: &HashMap<u64, Ident>, fp: u64) -> Ident {
    fp_to_ident
        .get(&fp)
        .cloned()
        .unwrap_or_else(|| format_ident!("t_{}", fp))
}
