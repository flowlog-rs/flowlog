//! FlowLog program representation.
//!
//! A [`Program`] is the parser's output: everything needed to evaluate a
//! FlowLog Datalog program. Build one with [`parse`](crate::parse).
//!
//! | Component | Description |
//! |-----------|-------------|
//! | [`Relation`] declarations | Schema: name, attribute types, EDB/IDB role |
//! | [`Segment`]s | Rules and loop blocks in source order |
//! | UDF declarations | External scalar functions (`.extern fn`) |
//! | Inline facts | Ground tuples written directly in source (`rel(1, 2).`) |

mod display;
mod fact;

use std::collections::HashMap;
use std::collections::HashSet;

pub use fact::InlineFact;

use crate::ast::FlowLogRule;
use crate::declaration::ExternFn;
use crate::declaration::Relation;
use crate::segment::Segment;
use crate::types::TypeRegistry;

// =============================================================================
// Program
// =============================================================================

/// A fully-parsed FlowLog program.
///
/// Construct one with [`parse`](crate::parse); there is deliberately no
/// `Default`, so parsing is the only way to build a `Program`, and the
/// library otherwise exposes it read-only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Program {
    /// All relation declarations (`.decl`), in source order.
    pub(crate) relations: Vec<Relation>,
    /// Ordered sequence of [`Segment`]s (plain rule groups and loop blocks),
    /// in source order, preserved exactly across included files.
    pub(crate) segments: Vec<Segment>,
    /// External scalar UDF declarations (`.extern fn`).
    pub(crate) udfs: Vec<ExternFn>,
    /// Inline ground facts, keyed by canonical relation name.
    pub(crate) facts: HashMap<String, Vec<InlineFact>>,
    /// Type declarations (aliases, subtypes, tuples). Compile-time only;
    /// not needed to evaluate the program.
    pub(crate) type_registry: TypeRegistry,
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

    /// Look up a declared relation by fingerprint, or `None` if none matches.
    ///
    /// Recovers a relation's original spelling ([`Relation::raw_name`]) from
    /// its canonical fingerprint. Linear scan; not for the data path.
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

    /// Ordered program items (rule segments and loop blocks) in source order.
    #[must_use]
    #[inline]
    pub fn segments(&self) -> &[Segment] {
        &self.segments
    }

    /// Mutable version of [`segments`](Self::segments).
    pub(crate) fn segments_mut(&mut self) -> &mut [Segment] {
        &mut self.segments
    }

    /// All top-level rules, flattened across `Segment::Plain` segments;
    /// excludes rules inside loop blocks. Prefer [`segments`](Self::segments)
    /// for loop-aware processing.
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
    pub fn facts(&self) -> &HashMap<String, Vec<InlineFact>> {
        &self.facts
    }

    /// Mutable access to inline ground facts, for rewriting polymorphic
    /// literals to their concrete declared types.
    pub(crate) fn facts_mut(&mut self) -> &mut HashMap<String, Vec<InlineFact>> {
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

    /// Split-borrow of the registry (shared) and segments (mutable): going
    /// through a method lets the borrow checker see the two fields are
    /// disjoint.
    #[inline]
    pub(crate) fn registry_and_segments_mut(&mut self) -> (&TypeRegistry, &mut [Segment]) {
        (&self.type_registry, &mut self.segments)
    }

    #[inline]
    fn is_edb_relation(&self, rel: &Relation) -> bool {
        rel.has_input() || self.has_inline_facts(rel.name())
    }
}

#[cfg(test)]
mod tests {
    use crate::Relation;
    use crate::test_util::assembled;

    /// `rules()` flattens the rules of every `Segment::Plain` in source order
    /// and excludes rules nested inside loop blocks.
    #[test]
    fn rules_flattens_plain_segments_and_excludes_loop_bodies() {
        let program = assembled(
            "
            .decl a(x: number)
            .decl b(x: number)
            .output a
            a(X) :- b(X).
            fixpoint { }
            a(1) :- b(1).
            ",
        )
        .expect("assembles");
        assert_eq!(program.rules().len(), 2);
    }

    /// `edbs()` is the union of file-backed (`.input`) relations and relations
    /// with inline facts; `file_backed_relations()` and `inline_fact_relations()`
    /// are the individual subsets, and a relation may belong to both.
    #[test]
    fn edb_subsets_track_file_backed_inline_and_overlap_relations() {
        let program = assembled(
            "
            .decl file_only(x: number)
            .decl fact_only(x: number)
            .decl both(x: number)
            .decl out(x: number)
            .input file_only(IO=\"file\", filename=\"file_only.csv\", delimiter=\",\")
            .input both(IO=\"file\", filename=\"both.csv\", delimiter=\",\")
            .output out

            fact_only(1).
            both(2).

            out(X) :- file_only(X).
            out(X) :- fact_only(X).
            out(X) :- both(X).
            ",
        )
        .expect("assembles");

        let names = |rels: Vec<&Relation>| {
            let mut v: Vec<String> = rels.iter().map(|r| r.name().to_string()).collect();
            v.sort_unstable();
            v
        };

        assert_eq!(
            names(program.edbs()),
            vec!["both", "fact_only", "file_only"]
        );
        assert_eq!(
            names(program.file_backed_relations()),
            vec!["both", "file_only"]
        );
        assert_eq!(
            names(program.inline_fact_relations()),
            vec!["both", "fact_only"]
        );
    }
}
