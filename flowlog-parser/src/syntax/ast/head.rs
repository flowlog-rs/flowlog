//! Rule heads for FlowLog Datalog programs.
//!
//! - [`HeadArg`]: `Var | Arith | Aggregation`
//! - [`Head`]: `rel(arg1, ..., argN)`

use std::fmt;

use educe::Educe;
use flowlog_common::Span;
use flowlog_common::compute_fp;

use super::Aggregation;
use super::Arithmetic;
use crate::Lexeme;
use crate::Node;
use crate::Rule;
use crate::error::ParseError;
use crate::error::grammar_bug;

/// Argument in a rule head.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum HeadArg {
    /// Pass-through variable.
    Var(String),
    /// Arithmetic expression (includes UDF calls).
    Arith(Arithmetic),
    /// Aggregation (e.g., `count(X)`).
    Aggregation(Aggregation),
}

impl HeadArg {
    /// Variables referenced by this argument (order preserved, duplicates kept).
    #[must_use]
    pub fn vars(&self) -> Vec<&String> {
        match self {
            Self::Var(v) => vec![v],
            Self::Arith(a) => a.vars(),
            Self::Aggregation(agg) => agg.vars(),
        }
    }
}

impl fmt::Display for HeadArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Var(v) => write!(f, "{v}"),
            Self::Arith(a) => write!(f, "{a}"),
            Self::Aggregation(agg) => write!(f, "{agg}"),
        }
    }
}

impl Lexeme for HeadArg {
    /// Parse a head argument from the grammar.
    ///
    /// Optimization: if the arithmetic is a single variable (`is_var()`), emit `Var` instead of `Arith`.
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        let inner = node.children().next_any("head argument value")?;
        Ok(match inner.rule() {
            Rule::arithmetic_expr => {
                let arith = inner.lower::<Arithmetic>()?;
                if arith.is_var() {
                    let name = arith
                        .init()
                        .vars()
                        .into_iter()
                        .next()
                        .ok_or_else(|| grammar_bug("is_var() but no variable in init"))?
                        .clone();
                    Self::Var(name)
                } else {
                    Self::Arith(arith)
                }
            }
            Rule::aggregate_expr => Self::Aggregation(inner.lower()?),
            other => {
                return Err(grammar_bug(format!(
                    "unexpected rule for HeadArg: {other:?}"
                )));
            }
        })
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
    #[cfg(test)]
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
    /// Parse `relation_name "(" (head_arg ("," head_arg)*)? ")"`.
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        let span = node.span();
        let mut children = node.children();

        let raw_name = children.next_any("relation name")?.text().to_string();
        let name = raw_name.to_lowercase();
        let head_fingerprint = compute_fp(&name);

        let head_arguments: Vec<HeadArg> = children
            .filter(|c| c.rule() == Rule::head_arg)
            .map(|c| c.lower::<HeadArg>())
            .collect::<Result<_, _>>()?;

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
    use super::*;
    use crate::AggregationOperator;

    /// `HeadArg::vars` must return the *real* referenced variables for each
    /// variant: a constant/empty/"xyzzy" stand-in return would break every
    /// downstream binding pass. Covers `Var` (itself), `Arith`, `Aggregation`.
    #[test]
    fn head_arg_vars_returns_real_variables() {
        let x = "X".to_string();
        assert_eq!(HeadArg::Var("X".into()).vars(), vec![&x]);

        let y = "Y".to_string();
        assert_eq!(HeadArg::Arith(Arithmetic::var("Y")).vars(), vec![&y]);

        let z = "Z".to_string();
        let agg = Aggregation::new(AggregationOperator::Sum, Arithmetic::var("Z"));
        assert_eq!(HeadArg::Aggregation(agg).vars(), vec![&z]);
    }

    /// `HeadArg`'s `Display` renders each variant exactly; an empty (default)
    /// rendering would corrupt every `.dl` round-trip and diagnostic.
    #[test]
    fn head_arg_display_renders_each_variant() {
        assert_eq!(HeadArg::Var("X".into()).to_string(), "X");
        assert_eq!(HeadArg::Arith(Arithmetic::var("Y")).to_string(), "Y");
        let agg = Aggregation::new(AggregationOperator::Sum, Arithmetic::var("Z"));
        assert_eq!(HeadArg::Aggregation(agg).to_string(), "sum(Z)");
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
