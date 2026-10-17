//! Rule heads for FlowLog Datalog programs.
//!
//! - [`HeadArg`]: `Var | Arith`
//! - [`Head`]: `rel(arg1, ..., argN)`

use std::fmt;

use educe::Educe;
use flowlog_common::Span;
use flowlog_common::compute_fp;

use super::Arithmetic;
use super::Factor;
use crate::Lexeme;
use crate::Node;
use crate::Rule;
use crate::error::ParseError;

/// Argument in a rule head.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum HeadArg {
    /// Pass-through variable.
    Var(String),
    /// Arithmetic expression (includes UDF calls).
    Arith(Arithmetic),
}

impl HeadArg {
    /// Variables referenced by this argument (order preserved, duplicates kept).
    #[must_use]
    pub fn vars(&self) -> Vec<&String> {
        match self {
            Self::Var(v) => vec![v],
            Self::Arith(a) => a.vars(),
        }
    }
}

impl fmt::Display for HeadArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Var(v) => write!(f, "{v}"),
            Self::Arith(a) => write!(f, "{a}"),
        }
    }
}

impl Lexeme for HeadArg {
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        let arith: Arithmetic = node.lower()?;
        Ok(
            if arith.rest().is_empty()
                && let Factor::Var(name) = arith.init()
            {
                Self::Var(name.clone())
            } else {
                Self::Arith(arith)
            },
        )
    }
}

/// `rel(arg1, ..., argN)`
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct Head {
    name: String,
    #[educe(PartialEq(ignore), Hash(ignore))]
    raw_name: String,
    head_fingerprint: u64,
    head_arguments: Vec<HeadArg>,
    #[educe(PartialEq(ignore), Hash(ignore))]
    span: Span,
}

impl Head {
    /// Creates a synthesized head with no source location (`Span::DUMMY`).
    /// The planner builds one for the body it plans an aggregate's braces
    /// as.
    #[must_use]
    pub fn new(name: String, head_arguments: Vec<HeadArg>) -> Self {
        let raw_name = name.clone();
        let name = name.to_lowercase();
        let head_fingerprint = compute_fp(&name);
        Self {
            name,
            raw_name,
            head_fingerprint,
            head_arguments,
            span: Span::DUMMY,
        }
    }

    /// Rename in-place. Lowercases and refreshes the cached fingerprint.
    /// Leaves `raw_name` untouched.
    pub fn set_name(&mut self, name: String) {
        let lname = name.to_lowercase();
        self.head_fingerprint = compute_fp(&lname);
        self.name = lname;
    }

    /// Source location this head was parsed from.
    #[must_use]
    #[inline]
    pub fn span(&self) -> Span {
        self.span
    }

    /// Canonical relation name.
    #[must_use]
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Original surface spelling of the relation name as the user wrote it.
    #[must_use]
    #[inline]
    pub fn raw_name(&self) -> &str {
        &self.raw_name
    }

    /// Head fingerprint.
    #[must_use]
    #[inline]
    pub fn head_fingerprint(&self) -> u64 {
        self.head_fingerprint
    }

    /// Arguments.
    #[must_use]
    #[inline]
    pub fn head_arguments(&self) -> &[HeadArg] {
        &self.head_arguments
    }

    #[inline]
    pub fn head_arguments_mut(&mut self) -> &mut [HeadArg] {
        &mut self.head_arguments
    }

    /// Arity (number of arguments).
    #[must_use]
    #[inline]
    pub fn arity(&self) -> usize {
        self.head_arguments.len()
    }
}

impl fmt::Display for Head {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.name)?;
        for (i, arg) in self.head_arguments.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{arg}")?;
        }
        write!(f, ")")
    }
}

impl Lexeme for Head {
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        let span = node.span();
        let mut children = node.children();
        let name = children.require(Rule::relation_ref)?;
        let raw_name = name.text().to_string();
        let name = raw_name.to_lowercase();
        let head_fingerprint = compute_fp(&name);
        let head_arguments = children.map(Node::lower).collect::<Result<_, _>>()?;
        Ok(Self {
            name,
            raw_name,
            head_fingerprint,
            head_arguments,
            span,
        })
    }
}

#[cfg(test)]
mod tests {
    use pest::Parser as _;
    use pest::error::ErrorVariant;
    use rstest::rstest;

    use super::*;
    use crate::FlowLogParser;
    use crate::test_harness::parse_node;

    #[test]
    fn cast_keyword_cannot_name_a_head_relation() {
        let err = FlowLogParser::parse(Rule::head, "as(x, T)").unwrap_err();
        assert!(matches!(err.variant, ErrorVariant::ParsingError { .. }));
    }

    #[test]
    fn head_arguments_do_not_admit_negation() {
        let err = FlowLogParser::parse(Rule::head, "H(!f(x))").unwrap_err();
        assert!(matches!(err.variant, ErrorVariant::ParsingError { .. }));
    }

    /// A lone variable, grouped or not, is a pass-through; anything else is
    /// a value to compute.
    #[rstest]
    #[case("x", "variable")]
    #[case("(x)", "variable")]
    #[case("x + 1", "value")]
    #[case("f(x)", "value")]
    #[case("as(x, T)", "value")]
    fn head_arguments_lower_to_variables_or_values(#[case] source: &str, #[case] expected: &str) {
        let arg: HeadArg = parse_node(Rule::arithmetic_expr, source);
        let kind = match arg {
            HeadArg::Var(_) => "variable",
            HeadArg::Arith(_) => "value",
        };
        assert_eq!(kind, expected);
    }

    /// `HeadArg::vars` must return the *real* referenced variables for each
    /// variant: a constant/empty/"xyzzy" stand-in return would break every
    /// downstream binding pass. Covers `Var` (itself) and `Arith`.
    #[test]
    fn head_arg_vars_returns_real_variables() {
        let x = "X".to_string();
        assert_eq!(HeadArg::Var("X".into()).vars(), vec![&x]);

        let y = "Y".to_string();
        assert_eq!(HeadArg::Arith(Arithmetic::var("Y")).vars(), vec![&y]);
    }

    /// `HeadArg`'s `Display` renders each variant exactly; an empty (default)
    /// rendering would corrupt every `.dl` round-trip and diagnostic.
    #[test]
    fn head_arg_display_renders_each_variant() {
        assert_eq!(HeadArg::Var("X".into()).to_string(), "X");
        assert_eq!(HeadArg::Arith(Arithmetic::var("Y")).to_string(), "Y");
    }

    /// `head_fingerprint` returns the cached `compute_fp(name)` (lowercased),
    /// not a constant: distinct names yield distinct fingerprints, and the
    /// value matches `compute_fp` exactly.
    #[test]
    fn head_fingerprint_is_name_hash_not_constant() {
        let foo = Head::new("Foo".into(), vec![]);
        let bar = Head::new("Bar".into(), vec![]);
        assert_eq!(foo.head_fingerprint(), compute_fp("foo"));
        assert_ne!(foo.head_fingerprint(), bar.head_fingerprint());
    }

    /// `arity` is the argument count, not a constant 0 or 1. Table spans 0..=3
    /// so both the `-> 0` and `-> 1` mutants die.
    #[test]
    fn head_arity_counts_arguments() {
        for n in 0..=3usize {
            let args: Vec<HeadArg> = (0..n).map(|i| HeadArg::Var(format!("v{i}"))).collect();
            let head = Head::new("r".into(), args);
            assert_eq!(head.arity(), n, "arity for {n} args");
        }
    }

    /// `Head`'s `Display` writes `name(a, b, c)`: commas separate arguments
    /// (the `i > 0` guard) with none before the first. Three args pin the
    /// boundary: `==`/`<`/`>=` each produce a different string, and an empty
    /// (default) rendering is caught too.
    #[test]
    fn head_display_comma_separates_arguments() {
        let head = Head::new(
            "r".into(),
            vec![
                HeadArg::Var("a".into()),
                HeadArg::Var("b".into()),
                HeadArg::Var("c".into()),
            ],
        );
        assert_eq!(head.to_string(), "r(a, b, c)");
    }
}
