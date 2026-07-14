//! Atom types for FlowLog Datalog programs.
//!
//! - [`AtomArg`]: variable / constant / placeholder (`_`)
//! - [`Atom`]: `name(arg1, ..., argN)`

use std::fmt;

use educe::Educe;
use flowlog_common::Span;
use flowlog_common::compute_fp;

use super::Constant;
use crate::Lexeme;
use crate::Node;
use crate::Rule;
use crate::error::ParseError;
use crate::error::grammar_bug;

/// An argument to an atom: variable, constant, or `_`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AtomArg {
    Var(String),
    Const(Constant),
    Placeholder,
}

impl fmt::Display for AtomArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Var(v) => write!(f, "{v}"),
            Self::Const(c) => write!(f, "{c}"),
            Self::Placeholder => write!(f, "_"),
        }
    }
}

impl Lexeme for AtomArg {
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        let inner = node.children().next_any("argument value")?;
        Ok(match inner.rule() {
            Rule::variable => Self::Var(inner.text().to_string()),
            Rule::constant => Self::Const(inner.lower()?),
            Rule::placeholder => Self::Placeholder,
            other => {
                return Err(grammar_bug(format!(
                    "invalid atom argument rule: {other:?}"
                )));
            }
        })
    }
}

/// `name(arg1, ..., argN)` predicate.
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct Atom {
    /// Canonical (lowercased) relation name.
    name: String,
    /// Surface spelling as written; excluded from identity.
    #[educe(PartialEq(ignore), Hash(ignore))]
    raw_name: String,
    arguments: Vec<AtomArg>,
    /// `compute_fp(name)`; cached relation identity for downstream stages.
    fingerprint: u64,
    #[educe(PartialEq(ignore), Hash(ignore))]
    span: Span,
}

impl Atom {
    /// Creates a synthesized atom with no source location (`Span::DUMMY`).
    /// `fingerprint` must be `compute_fp` of the lowercased `name`.
    // TODO: compute the fingerprint here and go pub(crate) once
    // flowlog-build's catalog stops hand-constructing atoms.
    #[must_use]
    pub fn new(name: &str, arguments: Vec<AtomArg>, fingerprint: u64) -> Self {
        Self {
            name: name.to_lowercase(),
            raw_name: name.to_string(),
            arguments,
            fingerprint,
            span: Span::DUMMY,
        }
    }

    /// Source location this atom was parsed from.
    #[must_use]
    #[inline]
    pub fn span(&self) -> Span {
        self.span
    }

    /// Canonical (lowercased) relation name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Original surface spelling of the relation name as the user wrote it.
    #[must_use]
    pub fn raw_name(&self) -> &str {
        &self.raw_name
    }

    /// Rename in-place. Lowercases and refreshes the cached fingerprint.
    pub(crate) fn set_name(&mut self, name: String) {
        let lname = name.to_lowercase();
        self.fingerprint = compute_fp(&lname);
        self.name = lname;
    }

    /// Arguments in source order.
    #[must_use]
    pub fn arguments(&self) -> &[AtomArg] {
        &self.arguments
    }

    pub(crate) fn arguments_mut(&mut self) -> &mut [AtomArg] {
        &mut self.arguments
    }

    /// Number of arguments.
    #[must_use]
    pub fn arity(&self) -> usize {
        self.arguments.len()
    }

    /// Hash of the canonical name (`compute_fp`).
    #[must_use]
    pub fn fingerprint(&self) -> u64 {
        self.fingerprint
    }
}

impl fmt::Display for Atom {
    /// Formats as the user wrote it: the surface spelling of the name,
    /// always including parentheses (`Edge(a, b, _)`).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.raw_name)?;
        for (i, arg) in self.arguments.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{arg}")?;
        }
        write!(f, ")")
    }
}

impl Lexeme for Atom {
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        let span = node.span();
        let mut children = node.children();

        let raw_name = children.next_any("relation name")?.text().to_string();
        let name = raw_name.to_lowercase();
        let fingerprint = compute_fp(&name);

        let arguments = children
            .filter(|c| c.rule() == Rule::atom_arg)
            .map(|c| c.lower::<AtomArg>())
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            name,
            raw_name,
            arguments,
            fingerprint,
            span,
        })
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::test_util::parse_node;
    use crate::types::DataType;

    #[rstest]
    #[case("x", AtomArg::Var("x".into()))]
    #[case("_", AtomArg::Placeholder)]
    #[case("42", AtomArg::Const(Constant::new(DataType::IntLit, "42")))]
    fn atom_arg_parses_each_variant(#[case] src: &str, #[case] expected: AtomArg) {
        assert_eq!(parse_node::<AtomArg>(Rule::atom_arg, src), expected);
    }

    #[test]
    fn atom_parses_arguments_in_source_order() {
        let atom = parse_node::<Atom>(Rule::atom, "edge(x, 1, _)");
        assert_eq!(atom.name(), "edge");
        assert_eq!(atom.arity(), 3);
        assert_eq!(
            atom.arguments(),
            &[
                AtomArg::Var("x".into()),
                AtomArg::Const(Constant::new(DataType::IntLit, "1")),
                AtomArg::Placeholder,
            ]
        );
    }

    #[test]
    fn nullary_atom_has_zero_arity() {
        let atom = parse_node::<Atom>(Rule::atom, "done()");
        assert_eq!(atom.name(), "done");
        assert_eq!(atom.arity(), 0);
    }

    #[test]
    fn atom_name_is_lowercased_and_raw_name_is_preserved() {
        let atom = parse_node::<Atom>(Rule::atom, "Edge(x)");
        assert_eq!(atom.name(), "edge", "canonical name is lowercased");
        assert_eq!(
            atom.raw_name(),
            "Edge",
            "raw name keeps the surface spelling"
        );
    }

    #[test]
    fn atom_fingerprint_is_derived_from_canonical_name() {
        let upper = parse_node::<Atom>(Rule::atom, "Edge(x)");
        // Fingerprint follows the canonical (lowercased) name, so `Edge` and
        // `edge` are the same relation.
        assert_eq!(upper.fingerprint(), compute_fp("edge"));
        // Distinct relation names get distinct fingerprints.
        let other = parse_node::<Atom>(Rule::atom, "path(x)");
        assert_ne!(upper.fingerprint(), other.fingerprint());
    }

    /// Identity ignores the surface spelling (and span): `Edge(x)` and
    /// `edge(x)` are the same atom to equality and hashing.
    #[test]
    fn atoms_differing_only_in_surface_case_are_equal() {
        let upper = parse_node::<Atom>(Rule::atom, "Edge(x)");
        let lower = parse_node::<Atom>(Rule::atom, "edge(x)");
        assert_eq!(upper, lower);
        assert_ne!(upper.raw_name(), lower.raw_name());
    }

    #[test]
    fn set_name_lowercases_and_refreshes_fingerprint() {
        let mut atom = parse_node::<Atom>(Rule::atom, "edge(x)");
        atom.set_name("Path".to_string());
        assert_eq!(atom.name(), "path");
        assert_eq!(atom.fingerprint(), compute_fp("path"));
    }

    /// Display round-trips the source: the surface spelling is kept and
    /// parentheses are always included (a nullary atom prints as `done()`).
    #[rstest]
    #[case("done()")]
    #[case("Edge(x, 1, _)")]
    fn display_round_trips_source(#[case] src: &str) {
        assert_eq!(parse_node::<Atom>(Rule::atom, src).to_string(), src);
    }
}
