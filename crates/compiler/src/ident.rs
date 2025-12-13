//! Ident system for FlowLog compiler.
//!
//! This module builds and maintains a global mapping from relation fingerprints
//! to their identifiers. It also provides utilities to lookup local identifiers
//! within recursion strata.

use proc_macro2::Ident;
use quote::format_ident;
use std::collections::HashMap;

use super::Compiler;

// =========================================================================
// Ident Mapping Utilities
// =========================================================================

impl Compiler {
    /// Map input and output fingerprints to global collection identifiers.
    /// Here global means across all strata, so we include both EDBs and IDBs.
    /// Relateively, local means within a recursion stratum, collections are
    /// entering recursion scope and thus get new local identifiers.
    pub(super) fn make_global_ident_map(&mut self) {
        self.global_fp_to_ident = self
            .program
            .edbs()
            .into_iter()
            .map(|rel| (rel.fingerprint(), format_ident!("{}", rel.name())))
            .chain(
                self.program
                    .idbs()
                    .into_iter()
                    .map(|rel| (rel.fingerprint(), format_ident!("{}", rel.name()))),
            )
            .collect();
    }

    /// Lookup the global variable name for a given collection fp, or fall back to `t_<fp>`.
    pub(super) fn find_global_ident(&self, fp: u64) -> Ident {
        self.global_fp_to_ident
            .get(&fp)
            .cloned()
            .unwrap_or_else(|| format_ident!("t_{}", fp))
    }
}

/// Lookup the local variable name (inside a stratum recursion scope) for a given collection fp,
/// or fall back to `t_<fp>`.
pub(crate) fn find_local_ident(local_fp_to_ident: &HashMap<u64, Ident>, fp: u64) -> Ident {
    local_fp_to_ident
        .get(&fp)
        .cloned()
        .unwrap_or_else(|| format_ident!("t_{}", fp))
}
