//! Function call expressions for FlowLog Datalog programs.
//!
//! - [`FnCall`]: a user-defined (`.extern fn`) function applied to
//!   arguments in a value position (`my_udf(x, y + 1)`).

use std::fmt;

use educe::Educe;
use flowlog_common::Span;

use super::Arithmetic;

/// A user-defined function call in a value position.
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct FnCall {
    name: String,
    args: Vec<Arithmetic>,
    #[educe(PartialEq(ignore), Hash(ignore))]
    span: Span,
}

impl FnCall {
    /// Builds a call to the user-defined function `name`.
    #[must_use]
    pub(crate) fn new(name: String, args: Vec<Arithmetic>, span: Span) -> Self {
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

    /// Argument expressions, in source order.
    #[must_use]
    #[inline]
    pub fn args(&self) -> &[Arithmetic] {
        &self.args
    }

    #[inline]
    pub(crate) fn args_mut(&mut self) -> &mut [Arithmetic] {
        &mut self.args
    }

    /// Variables appearing in the argument expressions.
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
    use pest::Parser as _;
    use pest::error::ErrorVariant;
    use rstest::rstest;

    use super::*;
    use crate::FlowLogParser;
    use crate::Rule;

    #[rstest]
    #[case("as(x, T)")]
    #[case("as(x, 5)")]
    #[case("f(x, cfg.Type)")]
    #[case("f(x, (field: number))")]
    fn call_syntax_excludes_casts_and_type_arguments(#[case] source: &str) {
        let err = FlowLogParser::parse(Rule::call_expr, source).unwrap_err();
        assert!(matches!(err.variant, ErrorVariant::ParsingError { .. }));
    }

    #[rstest]
    #[case::complete(true)]
    #[case::unclosed(false)]
    fn call_syntax_handles_nested_arguments(#[case] closed: bool) {
        let close = if closed {
            ")".repeat(256)
        } else {
            String::new()
        };
        let source = format!("{}x{close}", "f(".repeat(256));
        let result = FlowLogParser::parse(Rule::call_expr, &source);
        if closed {
            assert_eq!(result.unwrap().as_str(), source);
        } else {
            assert!(matches!(
                result.unwrap_err().variant,
                ErrorVariant::ParsingError { .. }
            ));
        }
    }

    /// `my_udf(x, y)` built directly.
    fn call() -> FnCall {
        FnCall::new(
            "my_udf".to_string(),
            vec![Arithmetic::var("x"), Arithmetic::var("y")],
            Span::DUMMY,
        )
    }

    #[test]
    fn accessors_expose_name_args_and_vars() {
        let fc = call();
        assert_eq!(fc.name(), "my_udf");
        assert_eq!(fc.args().len(), 2);
        assert_eq!(fc.vars(), [&"x".to_string(), &"y".to_string()]);
    }

    #[test]
    fn display_renders_call_syntax() {
        assert_eq!(call().to_string(), "my_udf(x, y)");
    }
}
