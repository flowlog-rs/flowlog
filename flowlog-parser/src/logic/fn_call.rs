//! Function call expressions for FlowLog rule heads and bodies.
//!
//! A [`FnCall`] represents a user-defined (`.extern fn`) function applied to
//! arguments in a value position (e.g., `my_udf(x, y + 1)`). UDFs are
//! value-only, so a `FnCall` only ever appears inside an expression, never as
//! a bare body predicate. Parsing goes through `parse_call_expr` (in
//! `arithmetic.rs`), which resolves the unified `call_expr` name to either a
//! built-in or, failing that, a `FnCall`.

use std::fmt;

use educe::Educe;

use super::Arithmetic;
use flowlog_common::Span;

/// A user-defined function call in a value position.
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct FnCall {
    /// Function name.
    name: String,
    /// Arguments.
    args: Vec<Arithmetic>,
    #[educe(PartialEq(ignore), Hash(ignore))]
    span: Span,
}

impl FnCall {
    /// Create a new function call.
    #[must_use]
    pub fn new(name: String, args: Vec<Arithmetic>, span: Span) -> Self {
        Self { name, args, span }
    }

    /// Source location this call was parsed from.
    #[must_use]
    #[inline]
    pub fn span(&self) -> Span {
        self.span
    }

    /// Function name.
    #[must_use]
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Arguments.
    #[must_use]
    #[inline]
    pub fn args(&self) -> &[Arithmetic] {
        &self.args
    }

    #[inline]
    pub fn args_mut(&mut self) -> &mut [Arithmetic] {
        &mut self.args
    }

    /// Variables referenced by this function call.
    #[must_use]
    pub fn vars(&self) -> Vec<&String> {
        self.args.iter().flat_map(|a| a.vars()).collect()
    }
}

impl fmt::Display for FnCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let args = self
            .args
            .iter()
            .map(|a| a.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "{}({})", self.name, args)
    }
}

#[cfg(test)]
mod tests {
    use crate::logic::Factor;
    use crate::{FlowLogParser, Lexeme, Rule};
    use flowlog_common::FileId;
    use pest::Parser;

    /// A non-built-in `call_expr` resolves to a `FnCall` through the real
    /// factor-parsing path (`parse_call_expr`).
    #[test]
    fn call_expr_resolves_to_fn_call() {
        let mut pairs = FlowLogParser::parse(Rule::factor, "my_udf(x, y)").unwrap();
        let factor = Factor::from_parsed_rule(pairs.next().unwrap(), FileId::new(0)).unwrap();
        let Factor::FnCall(fc) = factor else {
            panic!("expected Factor::FnCall, got {factor:?}");
        };
        assert_eq!(fc.name(), "my_udf");
        assert_eq!(fc.args().len(), 2);
    }
}
