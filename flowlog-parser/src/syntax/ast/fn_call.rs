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
    use super::*;

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
