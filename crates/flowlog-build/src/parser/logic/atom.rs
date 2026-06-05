//! Atom types for FlowLog Datalog programs.
//!
//! - [`AtomArg`]: variable / constant / placeholder (`_`) / expression
//! - [`Atom`]: `name(arg1, ..., argN)`

use std::fmt;

use pest::iterators::Pair;

use super::{Arithmetic, Factor};
use crate::common::{FileId, Ignored, Span, compute_fp};
use crate::parser::error::{ParseError, grammar_bug};
use crate::parser::primitive::ConstType;
use crate::parser::{Lexeme, Rule, span_of};

/// An argument to an atom: variable, constant, `_`, or a Soufflé-style
/// arithmetic expression (`idx - 1`, `ord(h)`, `as(x, T)`).
///
/// [`Self::Expr`] is a transient surface form: the `desugar` pass lifts
/// every expression argument to a fresh variable bound by an equality in
/// the rule body, so no stage past parsing/desugaring ever observes it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum AtomArg {
    Var(String),
    Const(ConstType),
    Placeholder,
    Expr(Arithmetic),
}

impl fmt::Display for AtomArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Var(v) => write!(f, "{v}"),
            Self::Const(c) => write!(f, "{c}"),
            Self::Placeholder => write!(f, "_"),
            Self::Expr(e) => write!(f, "{e}"),
        }
    }
}

impl Lexeme for AtomArg {
    /// Parse an atom argument from the grammar.
    ///
    /// `atom_arg = { placeholder | arithmetic_expr }`. A single-factor
    /// arithmetic expression that is a bare variable or constant collapses
    /// to [`Self::Var`] / [`Self::Const`] so the common case is unchanged;
    /// anything else (operators, builtins, UDF calls, casts) becomes
    /// [`Self::Expr`].
    fn from_parsed_rule(parsed_rule: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
        let inner = parsed_rule
            .into_inner()
            .next()
            .ok_or_else(|| grammar_bug("atom_arg missing inner token"))?;

        Ok(match inner.as_rule() {
            Rule::placeholder => Self::Placeholder,
            Rule::arithmetic_expr => {
                let arith = Arithmetic::from_parsed_rule(inner, file)?;
                if arith.rest().is_empty() {
                    match arith.init() {
                        Factor::Var(v) => Self::Var(v.clone()),
                        Factor::Const(c) => Self::Const(c.clone()),
                        _ => Self::Expr(arith),
                    }
                } else {
                    Self::Expr(arith)
                }
            }
            other => {
                return Err(grammar_bug(format!(
                    "invalid atom argument rule: {other:?}"
                )));
            }
        })
    }
}

/// `name(arg1, ..., argN)` predicate.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct Atom {
    name: String,
    arguments: Vec<AtomArg>,
    fingerprint: u64,
    span: Ignored<Span>,
}

impl Atom {
    /// Create a new atom.
    ///
    /// Converts the name to lowercase.
    #[must_use]
    pub(crate) fn new(name: &str, arguments: Vec<AtomArg>, fingerprint: u64) -> Self {
        Self {
            name: name.to_lowercase(),
            arguments,
            fingerprint,
            span: Ignored(Span::DUMMY),
        }
    }

    /// Source location this atom was parsed from.
    #[must_use]
    #[inline]
    pub(crate) fn span(&self) -> Span {
        self.span.0
    }

    /// Relation name.
    #[must_use]
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// Rename in-place. Lowercases and refreshes the cached fingerprint.
    pub(crate) fn set_name(&mut self, name: String) {
        let lname = name.to_lowercase();
        self.fingerprint = compute_fp(&lname);
        self.name = lname;
    }

    /// Arguments (as a slice).
    #[must_use]
    pub(crate) fn arguments(&self) -> &[AtomArg] {
        &self.arguments
    }

    pub(crate) fn arguments_mut(&mut self) -> &mut [AtomArg] {
        &mut self.arguments
    }

    /// Number of arguments.
    #[must_use]
    pub(crate) fn arity(&self) -> usize {
        self.arguments.len()
    }

    /// Get the fingerprint.
    #[must_use]
    pub(crate) fn fingerprint(&self) -> u64 {
        self.fingerprint
    }
}

impl fmt::Display for Atom {
    /// Formats as `name(a, b, _)`, always including parentheses.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.name)?;
        for (i, arg) in self.arguments.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{arg}")?;
        }
        write!(f, ")")
    }
}

impl fmt::Debug for Atom {
    /// Same as [`Display`](fmt::Display), with the fingerprint appended as
    /// ` [fp: 0x...]` for debugging.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)?;
        write!(f, " [fp: 0x{:016x}]", self.fingerprint)
    }
}

impl Lexeme for Atom {
    /// Parse `name("(" (atom_arg ("," atom_arg)*)? ")")`.
    fn from_parsed_rule(parsed_rule: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
        let span = span_of(&parsed_rule, file);
        let mut inner = parsed_rule.into_inner();

        let name = inner
            .next()
            .ok_or_else(|| grammar_bug("atom missing relation name"))?
            .as_str()
            .to_lowercase();
        let fingerprint = compute_fp(&name);

        let arguments = inner
            .filter(|p| p.as_rule() == Rule::atom_arg)
            .map(|p| AtomArg::from_parsed_rule(p, file))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            name,
            arguments,
            fingerprint,
            span: Ignored(span),
        })
    }
}
