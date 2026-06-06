//! Ident lookup for relation fingerprints.
//!
//! "Global" idents are stable across strata (one entry per EDB/IDB), while
//! "local" idents belong to a recursion scope — collections that entered
//! the scope get a fresh binding inside it.

use std::collections::HashMap;

use proc_macro2::Ident;
use quote::format_ident;

use crate::codegen::CodeGen;

impl CodeGen {
    /// Seed the global `fingerprint → ident` map from every declared
    /// relation (EDB + IDB), using the relation's own name as the ident.
    ///
    /// Names are routed through [`rust_ident`](crate::build::relation::rust_ident)
    /// so a relation whose name collides with a Rust keyword (`type`, `match`,
    /// …) is escaped rather than emitted verbatim into an invalid binding.
    pub(super) fn make_global_ident_map(&mut self) {
        self.global_fp_to_ident = self
            .program
            .relations()
            .iter()
            .map(|rel| {
                (
                    rel.fingerprint(),
                    crate::build::relation::rust_ident(rel.name()),
                )
            })
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
}

/// Resolve a fingerprint to its local ident inside a recursion scope,
/// falling back to `t_<fp>` for intermediates.
pub(crate) fn find_local_ident(local_fp_to_ident: &HashMap<u64, Ident>, fp: u64) -> Ident {
    local_fp_to_ident
        .get(&fp)
        .cloned()
        .unwrap_or_else(|| format_ident!("t_{}", fp))
}
