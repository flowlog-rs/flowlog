//! FlowLog program representation and manipulation.
//!
//! A [`Program`] is the central data structure produced by the parser.  It holds
//! everything needed to evaluate a FlowLog Datalog program:
//!
//! | Component | Description |
//! |-----------|-------------|
//! | [`Relation`] declarations | Schema: name, attribute types, EDB/IDB role |
//! | [`Segment`]s | Rules and loop blocks in source order |
//! | UDF declarations | External scalar functions (`.extern fn`) |
//! | Inline facts | Ground tuples written directly in source (`rel(1, 2).`) |
//!
//! # File loading and include resolution
//!
//! [`Program::parse`] is the entry point for file-based loading.  It first
//! resolves all `.include "path"` directives at the text level (see
//! [`resolve_includes`]), producing a single combined source string, then parses
//! that string in one pass.  This keeps the parser itself simple — it never has
//! to merge two partially-parsed programs.
//!
//! # Segment model
//!
//! Rules are grouped into [`Segment::Plain`] segments separated by
//! [`Segment::Loop`] / [`Segment::Fixpoint`] barriers.  The stratifier
//! processes segments in source order and treats each loop or fixpoint block
//! as a hard boundary between strata groups.

mod display;
mod include;
mod parse;
mod prune;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::collections::HashSet;

use flowlog_common::Span;

use crate::ConstType;
use crate::declaration::ExternFn;
use crate::declaration::Relation;
use crate::logic::FlowLogRule;
use crate::primitive::TypeRegistry;
use crate::segment::Segment;

// =============================================================================
// Program
// =============================================================================

/// A fully-parsed FlowLog program.
///
/// Construct one with [`Program::parse`] (file path) or, in tests, via the
/// [`Lexeme`] impl on an already-parsed pest node.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Program {
    /// All relation declarations (`.decl`), in source order.
    relations: Vec<Relation>,
    /// Ordered sequence of [`Segment`]s (plain rule groups and loop blocks).
    ///
    /// This is the primary representation consumed by the stratifier.
    /// Source order is preserved exactly, including across included files.
    segments: Vec<Segment>,
    /// External scalar UDF declarations (`.extern fn`).
    udfs: Vec<ExternFn>,
    /// Inline ground facts, keyed by relation name. Each entry is a list
    /// of `(span, tuple)` — the head span of the source `rel(c1, ...).`
    /// fact plus its constant columns.
    facts: HashMap<String, Vec<(Span, Vec<ConstType>)>>,
    /// Built during parsing, consulted by the typechecker for subtype
    /// rules, ignored downstream (subtypes are compile-time-only).
    type_registry: TypeRegistry,
}

// =============================================================================
// Public API
// =============================================================================

impl Program {
    // --- Relation declarations ---

    /// All relation declarations.
    #[must_use]
    #[inline]
    pub fn relations(&self) -> &[Relation] {
        &self.relations
    }

    /// Look up a declared relation by fingerprint.
    ///
    /// This is the bridge from codegen's fingerprint world back to the
    /// declaration — primarily so human-facing output (profiler labels,
    /// diagnostics) can show the user's original spelling
    /// ([`Relation::raw_name`]) instead of the canonical internal name.
    /// Linear scan: callers are codegen-time, never on the data path.
    #[must_use]
    pub fn relation_by_fingerprint(&self, fp: u64) -> Option<&Relation> {
        self.relations.iter().find(|rel| rel.fingerprint() == fp)
    }

    // --- EDB inputs (file-backed `.input` + inline facts) ---

    /// EDB relations available before rule evaluation starts.
    ///
    /// This is the union of:
    /// - file-backed relations declared with `.input`
    /// - relations with inline ground facts such as `rel(1, 2).`
    ///
    /// A relation may belong to both subsets.
    #[must_use]
    pub fn edbs(&self) -> Vec<&Relation> {
        self.relations
            .iter()
            .filter(|rel| self.is_edb_relation(rel))
            .collect()
    }

    /// Ordered EDB relation names (sorted lexicographically).
    #[must_use]
    pub fn edb_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .edbs()
            .iter()
            .map(|rel| rel.name().to_string())
            .collect();
        names.sort_unstable();
        names
    }

    /// Deduplicated EDB relation fingerprints.
    #[must_use]
    pub fn edb_fingerprints(&self) -> HashSet<u64> {
        self.edbs().iter().map(|rel| rel.fingerprint()).collect()
    }

    #[cfg(test)]
    #[must_use]
    #[inline]
    pub fn file_backed_relations(&self) -> Vec<&Relation> {
        self.relations
            .iter()
            .filter(|rel| rel.is_file_backed())
            .collect()
    }

    #[cfg(test)]
    #[must_use]
    pub fn inline_fact_relations(&self) -> Vec<&Relation> {
        self.relations
            .iter()
            .filter(|rel| self.has_inline_facts(rel.name()))
            .collect()
    }

    // --- IDB outputs (`.output` / `.printsize`) ---

    /// IDB relations (those annotated with `.output` or `.printsize`).
    ///
    /// Returned in declaration order.
    #[must_use]
    #[inline]
    pub fn idbs(&self) -> Vec<&Relation> {
        self.relations
            .iter()
            .filter(|rel| rel.is_output_printsize())
            .collect()
    }

    /// IDB relations annotated with `.output`, in declaration order.
    #[must_use]
    #[inline]
    pub fn output_idbs(&self) -> Vec<&Relation> {
        self.relations.iter().filter(|rel| rel.output()).collect()
    }

    /// IDB relations annotated with `.printsize`, in declaration order.
    #[must_use]
    #[inline]
    pub fn printsize_idbs(&self) -> Vec<&Relation> {
        self.relations
            .iter()
            .filter(|rel| rel.printsize())
            .collect()
    }

    // --- Segments & rules ---

    /// Ordered program items (rule segments and loop blocks in source order).
    ///
    /// This is the primary representation for the stratifier.  It processes
    /// items in order and treats each `Segment::Loop` as a hard barrier.
    #[must_use]
    #[inline]
    pub fn segments(&self) -> &[Segment] {
        &self.segments
    }

    /// Mutable version of [`segments`](Self::segments).
    pub fn segments_mut(&mut self) -> &mut [Segment] {
        &mut self.segments
    }

    /// All top-level rules, flattened across all `Segment::Plain` segments.
    /// Does NOT include rules inside loop blocks.
    ///
    /// This is provided for backward-compatible access by callers that do not
    /// yet understand loop blocks.  Prefer [`segments`] for loop-aware processing.
    #[must_use]
    pub fn rules(&self) -> Vec<&FlowLogRule> {
        self.segments
            .iter()
            .flat_map(|item| item.as_rules())
            .collect()
    }

    /// Look up a rule by its global source-order ID.
    ///
    /// # Panics
    /// Panics if `rid` is out of bounds.
    #[must_use]
    pub fn rule(&self, rid: usize) -> &FlowLogRule {
        let mut offset = 0;
        for seg in &self.segments {
            let rules: &[FlowLogRule] = match seg {
                Segment::Plain(rules) => rules,
                Segment::Loop(block) | Segment::Fixpoint(block) => block.rules(),
            };
            if rid < offset + rules.len() {
                return &rules[rid - offset];
            }
            offset += rules.len();
        }
        panic!("Parser error: rule ID {rid} out of bounds");
    }

    // --- Inline facts ---

    /// Inline facts (ground tuples).
    #[must_use]
    #[inline]
    pub fn facts(&self) -> &HashMap<String, Vec<(Span, Vec<ConstType>)>> {
        &self.facts
    }

    /// Mutable access to inline ground facts — only used by the typechecker's
    /// lowering pass to rewrite polymorphic literals to their concrete
    /// declared types.
    pub fn facts_mut(&mut self) -> &mut HashMap<String, Vec<(Span, Vec<ConstType>)>> {
        &mut self.facts
    }

    /// Whether the named relation has any inline ground facts.
    #[must_use]
    #[inline]
    pub fn has_inline_facts(&self, relation_name: &str) -> bool {
        self.facts.contains_key(relation_name)
    }

    // --- UDFs ---

    /// External UDF declarations.
    #[must_use]
    #[inline]
    pub fn udfs(&self) -> &[ExternFn] {
        &self.udfs
    }

    // --- Internal ---

    /// Split-borrow used by the subtype pass: registry is read while
    /// segments are mutated in place. Going through a method lets the
    /// borrow checker see the two fields are disjoint.
    #[inline]
    pub fn registry_and_segments_mut(&mut self) -> (&TypeRegistry, &mut [Segment]) {
        (&self.type_registry, &mut self.segments)
    }

    #[inline]
    fn is_edb_relation(&self, rel: &Relation) -> bool {
        rel.has_input() || self.has_inline_facts(rel.name())
    }
}
