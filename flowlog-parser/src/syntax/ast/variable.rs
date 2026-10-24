//! A variable of a rule: one the user named, or one a stage made up. Two
//! variables are the same only when they are the same variant with the
//! same content, so a made-up one never collides with a user's, and each
//! made-up one displays as a name no identifier can spell.

use std::fmt;

/// A variable of a rule.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Variable {
    /// Named by the user.
    Named(String),
    /// Stands for the answer of the `n`th aggregate hoisted out of its
    /// rule's expressions. Displays as `@agg_n`, which no identifier can
    /// spell.
    Aggregate(usize),
    /// The column the planner adds for the computed target of the `n`th
    /// aggregate. Displays as `@agg_n.target`.
    AggregateTarget(usize),
    /// The shadow column the planner adds when it fuses the `n`th
    /// comparison into a join key. Displays as `__eqk_n`.
    EquijoinKey(usize),
}

impl From<&str> for Variable {
    fn from(name: &str) -> Self {
        Self::Named(name.to_string())
    }
}

impl From<String> for Variable {
    fn from(name: String) -> Self {
        Self::Named(name)
    }
}

/// A user-named variable is the name; a made-up one equals no spelling.
impl PartialEq<str> for Variable {
    fn eq(&self, name: &str) -> bool {
        match self {
            Self::Named(own) => own == name,
            Self::Aggregate(_) | Self::AggregateTarget(_) | Self::EquijoinKey(_) => false,
        }
    }
}

impl fmt::Display for Variable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Named(name) => f.write_str(name),
            Self::Aggregate(n) => write!(f, "@agg_{n}"),
            Self::AggregateTarget(n) => write!(f, "@agg_{n}.target"),
            Self::EquijoinKey(n) => write!(f, "__eqk_{n}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A user's variable displays as its name, a made-up one as a name no
    /// identifier can spell.
    #[test]
    fn display_shows_each_variable_by_its_spelling() {
        assert_eq!(Variable::from("x").to_string(), "x");
        assert_eq!(Variable::Aggregate(2).to_string(), "@agg_2");
        assert_eq!(Variable::AggregateTarget(2).to_string(), "@agg_2.target");
        assert_eq!(Variable::EquijoinKey(2).to_string(), "__eqk_2");
    }

    /// Only a user-named variable equals a spelling: a made-up variable's
    /// display is not a name anyone wrote.
    #[test]
    fn a_spelling_equals_only_the_user_variable_of_that_name() {
        let x = Variable::from("x");
        assert!(x == *"x");
        assert!(x != *"y");
        assert!(Variable::Aggregate(0) != *"@agg_0");
        assert!(Variable::EquijoinKey(0) != *"__eqk_0");
    }
}
