//! Typed cursor over the pest parse tree.
//!
//! A [`Node`] is one node of the raw parse tree (a pest pair), not an AST
//! node: it is the input a [`Lexeme`] impl lowers into an AST type.
//! [`Children`] walks a node's children, reporting a missing or wrong
//! child as a uniform internal error.

// A cursor, rather than raw pest `Pair` walking, so the ~25 Lexeme impls
// don't each repeat `into_inner().next().ok_or_else(|| grammar_bug(...))`:
// the accessors centralize that lookup and its error text.

use flowlog_common::FileId;
use flowlog_common::Span;
use pest::iterators::Pair;
use pest::iterators::Pairs;

use crate::Lexeme;
use crate::Rule;
use crate::error::ParseError;
use crate::error::grammar_bug;

/// One node of the pest parse tree: a parsed pair plus the [`FileId`] its
/// spans anchor to. The input to lowering, not an AST node;
/// [`lower`](Self::lower) turns it into one. Rule and span are snapshotted
/// at construction, so they stay readable after
/// [`children`](Self::children) consumes the underlying pair.
#[derive(Debug)]
pub(crate) struct Node<'a> {
    rule: Rule,
    span: Span,
    file: FileId,
    pair: Pair<'a, Rule>,
}

impl<'a> Node<'a> {
    pub(crate) fn new(pair: Pair<'a, Rule>, file: FileId) -> Self {
        let r = pair.as_rule();
        let s = pair.as_span();
        Self {
            rule: r,
            span: Span::new(file, s.start() as u32, s.end() as u32),
            file,
            pair,
        }
    }

    pub(crate) fn rule(&self) -> Rule {
        self.rule
    }

    pub(crate) fn span(&self) -> Span {
        self.span
    }

    /// Surface text of the node.
    pub(crate) fn text(&self) -> &str {
        self.pair.as_str()
    }

    /// Walk the node's children; consumes the node (rule/span stay
    /// available on the returned cursor's parent snapshot).
    pub(crate) fn children(self) -> Children<'a> {
        Children {
            parent: self.rule,
            file: self.file,
            iter: self.pair.into_inner(),
        }
    }

    /// Lower this node via its [`Lexeme`] impl.
    pub(crate) fn lower<T: Lexeme>(self) -> Result<T, ParseError> {
        T::from_parsed_rule(self)
    }

    /// Migration bridge to the raw pest API; new code walks via
    /// [`children`](Self::children) instead.
    pub(crate) fn into_parts(self) -> (Pair<'a, Rule>, FileId) {
        (self.pair, self.file)
    }
}

/// Cursor over a node's child parse nodes. Every miss reports the parent
/// rule and the unmet expectation through [`grammar_bug`], so extraction
/// errors read uniformly: the grammar promised a child that isn't there.
pub(crate) struct Children<'a> {
    parent: Rule,
    file: FileId,
    iter: Pairs<'a, Rule>,
}

impl<'a> Children<'a> {
    /// Next child, whatever its rule; `what` names the expectation for
    /// the internal error when the grammar contract is violated.
    pub(crate) fn next_any(&mut self, what: &str) -> Result<Node<'a>, ParseError> {
        match self.iter.next() {
            Some(pair) => Ok(Node::new(pair, self.file)),
            None => Err(grammar_bug(format!("{:?}: missing {what}", self.parent))),
        }
    }

    /// Next child, required to be `want`.
    pub(crate) fn require(&mut self, want: Rule) -> Result<Node<'a>, ParseError> {
        let node = self.next_any(&format!("{want:?}"))?;
        if node.rule() != want {
            return Err(grammar_bug(format!(
                "{:?}: expected {want:?}, found {:?}",
                self.parent,
                node.rule()
            )));
        }
        Ok(node)
    }

    /// Consume and return the next child only if it is `want`.
    // Named `take_if`, not `take`: `Children` is an `Iterator`, and
    // `Iterator::take(usize)` would shadow-compete with a `take` here.
    pub(crate) fn take_if(&mut self, want: Rule) -> Option<Node<'a>> {
        if self.iter.peek().map(|p| p.as_rule()) == Some(want) {
            return self.iter.next().map(|p| Node::new(p, self.file));
        }
        None
    }

    /// Lower the next child via `T`'s [`Lexeme`] impl.
    pub(crate) fn lower_next<T: Lexeme>(&mut self, what: &str) -> Result<T, ParseError> {
        self.next_any(what)?.lower()
    }
}

impl<'a> Iterator for Children<'a> {
    type Item = Node<'a>;

    fn next(&mut self) -> Option<Node<'a>> {
        let file = self.file;
        self.iter.next().map(|p| Node::new(p, file))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_err;
    use crate::ast::Constant;
    use crate::test_util::parse_pair;
    use crate::types::DataType;

    fn node(start: Rule, src: &'static str) -> Node<'static> {
        Node::new(parse_pair(start, src), FileId::new(0))
    }

    /// Rule and span survive consuming the pair: both are snapshotted at
    /// construction, so a builder can read them after walking children.
    #[test]
    fn rule_and_span_survive_children() {
        let n = node(Rule::constant, "42");
        let (seen_rule, span) = (n.rule(), n.span());
        let _ = n.children();
        assert_eq!(seen_rule, Rule::constant);
        assert_eq!((span.start(), span.end()), (0, 2));
    }

    /// `next_any` yields children in order and reports the parent rule
    /// plus the caller's expectation when the grammar runs dry.
    #[test]
    fn next_any_walks_then_reports_exhaustion() {
        let mut ch = node(Rule::constant, "42").children();
        assert_eq!(ch.next_any("value").unwrap().rule(), Rule::integer);
        assert_err!(ch.next_any("value"), ParseError::Internal(_));
    }

    /// `expect` enforces the rule; `take_if` consumes only on a match and
    /// leaves the cursor intact otherwise.
    #[test]
    fn expect_and_take_gate_on_rule() {
        let mut ch = node(Rule::constant, "42").children();
        assert!(ch.take_if(Rule::string).is_none());
        assert!(ch.require(Rule::integer).is_ok());

        let mut ch = node(Rule::constant, "1.5").children();
        assert_err!(ch.require(Rule::integer), ParseError::Internal(_));
    }

    /// `lower_next` drives a child's `Lexeme` impl directly.
    #[test]
    fn lower_next_builds_the_child() {
        // `arithmetic_expr` wraps a `factor` whose child is a `constant`.
        let mut ch = node(Rule::factor, "42").children();
        let c: Constant = ch.lower_next("constant").unwrap();
        assert_eq!(c, Constant::new(DataType::IntLit, "42"));
    }
}
