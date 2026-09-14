//! FlowLog program representation.
//!
//! A [`Program`] is the parser's output: everything needed to evaluate a
//! FlowLog Datalog program. Build one with [`parse`](crate::parse).
//!
//! | Component | Description |
//! |-----------|-------------|
//! | [`Relation`] declarations | Schema: name, attribute types, EDB/IDB role |
//! | Rules | Every rule, in source order |
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
    /// Every rule in source order, preserved exactly across included files.
    pub(crate) rules: Vec<FlowLogRule>,
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
        self.relations
            .iter()
            .filter(|rel| rel.has_output())
            .collect()
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

    // --- Rules ---

    /// Returns every rule in source order.
    #[must_use]
    #[inline]
    pub fn rules(&self) -> &[FlowLogRule] {
        &self.rules
    }

    /// Mutable version of [`rules`](Self::rules). Owned so a stage can add
    /// or drop rules, not only rewrite them in place.
    #[inline]
    pub(crate) fn rules_mut(&mut self) -> &mut Vec<FlowLogRule> {
        &mut self.rules
    }

    /// Returns the rule with global source-order ID `rule_id`.
    #[must_use]
    #[inline]
    pub fn rule(&self, rule_id: usize) -> Option<&FlowLogRule> {
        self.rules.get(rule_id)
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

    /// Split-borrow of the registry (shared) and rules (mutable): going
    /// through a method lets the borrow checker see the two fields are
    /// disjoint.
    #[inline]
    pub(crate) fn registry_and_rules_mut(&mut self) -> (&TypeRegistry, &mut [FlowLogRule]) {
        (&self.type_registry, &mut self.rules)
    }

    #[inline]
    fn is_edb_relation(&self, rel: &Relation) -> bool {
        rel.has_input() || self.has_inline_facts(rel.name())
    }
}

#[cfg(test)]
mod tests {
    use crate::InputSource;
    use crate::Relation;
    use crate::test_util::assembled;

    #[test]
    fn rules_are_returned_in_source_order() {
        let program = assembled(
            "
            .decl a(x: number)
            .decl b(x: number)
            .output a
            a(X) :- b(X).
            a(2) :- b(2).
            a(1) :- b(1).
            ",
        )
        .expect("assembles");
        let rules = program.rules();
        assert_eq!(rules.len(), 3);
        assert!(rules[1].to_string().contains("2"));
    }

    #[test]
    fn rule_returns_none_for_out_of_bounds_id() {
        let program = assembled(
            "
            .decl a(x: number)
            .decl b(x: number)
            .output a
            a(X) :- b(X).
            ",
        )
        .expect("assembles");
        assert!(program.rule(1).is_none());
    }

    /// `edbs()` is the union of relations carrying an `.input` and relations
    /// carrying inline facts, and the two overlap. Neither holds a `put`-fed
    /// `IO="command"` source, which reads no file and declares no facts yet
    /// is available before evaluation all the same.
    #[test]
    fn edbs_unions_file_backed_inline_and_put_fed_relations() {
        let program = assembled(
            "
            .decl file_only(x: number)
            .decl fact_only(x: number)
            .decl cmd_only(x: number)
            .decl both(x: number)
            .decl out(x: number)
            .input file_only(IO=\"file\", filename=\"file_only.csv\", delimiter=\",\")
            .input both(IO=\"file\", filename=\"both.csv\", delimiter=\",\")
            .input cmd_only(IO=\"command\", delimiter=\",\")
            .output out

            fact_only(1).
            both(2).

            out(X) :- file_only(X).
            out(X) :- fact_only(X).
            out(X) :- cmd_only(X).
            out(X) :- both(X).
            ",
        )
        .expect("assembles");

        let names = |rels: Vec<&Relation>| {
            let mut v: Vec<String> = rels.iter().map(|r| r.name().to_string()).collect();
            v.sort_unstable();
            v
        };
        let file_backed: Vec<&Relation> = program
            .relations()
            .iter()
            .filter(|rel| rel.input().is_some_and(InputSource::is_file_backed))
            .collect();
        let inline_facts: Vec<&Relation> = program
            .relations()
            .iter()
            .filter(|rel| program.has_inline_facts(rel.name()))
            .collect();

        assert_eq!(
            names(program.edbs()),
            vec!["both", "cmd_only", "fact_only", "file_only"]
        );
        assert_eq!(names(file_backed), vec!["both", "file_only"]);
        assert_eq!(names(inline_facts), vec!["both", "fact_only"]);
    }
}
