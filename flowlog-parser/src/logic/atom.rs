//! Atom types for FlowLog Datalog programs.
//!
//! - [`AtomArg`]: variable / constant / placeholder (`_`)
//! - [`Atom`]: `name(arg1, ..., argN)`

use std::fmt;

use educe::Educe;
use flowlog_common::FileId;
use flowlog_common::Span;
use flowlog_common::compute_fp;
use pest::iterators::Pair;

use crate::Lexeme;
use crate::Rule;
use crate::error::ParseError;
use crate::error::grammar_bug;
use crate::primitive::ConstType;
use crate::span_of;

/// An argument to an atom: variable, constant, or `_`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AtomArg {
    Var(String),
    Const(ConstType),
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
    /// Parse an atom argument from the grammar.
    fn from_parsed_rule(parsed_rule: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
        let inner = parsed_rule
            .into_inner()
            .next()
            .ok_or_else(|| grammar_bug("atom_arg missing inner token"))?;

        Ok(match inner.as_rule() {
            Rule::variable => Self::Var(inner.as_str().to_string()),
            Rule::constant => Self::Const(ConstType::from_parsed_rule(inner, file)?),
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
#[derive(Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct Atom {
    name: String,
    #[educe(PartialEq(ignore), Hash(ignore))]
    raw_name: String,
    arguments: Vec<AtomArg>,
    fingerprint: u64,
    #[educe(PartialEq(ignore), Hash(ignore))]
    span: Span,
}

impl Atom {
    /// Create a new atom.
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

    /// Canonical relation name.
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
    pub fn set_name(&mut self, name: String) {
        let lname = name.to_lowercase();
        self.fingerprint = compute_fp(&lname);
        self.name = lname;
    }

    /// Arguments (as a slice).
    #[must_use]
    pub fn arguments(&self) -> &[AtomArg] {
        &self.arguments
    }

    pub fn arguments_mut(&mut self) -> &mut [AtomArg] {
        &mut self.arguments
    }

    /// Number of arguments.
    #[must_use]
    pub fn arity(&self) -> usize {
        self.arguments.len()
    }

    /// Get the fingerprint.
    #[must_use]
    pub fn fingerprint(&self) -> u64 {
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

        let raw_name = inner
            .next()
            .ok_or_else(|| grammar_bug("atom missing relation name"))?
            .as_str()
            .to_string();
        let name = raw_name.to_lowercase();
        let fingerprint = compute_fp(&name);

        let arguments = inner
            .filter(|p| p.as_rule() == Rule::atom_arg)
            .map(|p| AtomArg::from_parsed_rule(p, file))
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
