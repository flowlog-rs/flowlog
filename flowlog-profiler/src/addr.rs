//! The operator address: the path of scope indices that locates one timely
//! operator in a dataflow.

use std::fmt;
use std::str::FromStr;

use serde::Deserialize;
use serde::Serialize;

use crate::ProfilerError;

/// Address of a timely operator, as the path of nested scope indices from
/// the dataflow root (`[0, 8, 10]`). Ordered so address sets and maps
/// follow dataflow construction order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Addr(pub Vec<u32>);

/// The root scope path `[0]`, where plan-side address tracking starts.
impl Default for Addr {
    fn default() -> Self {
        Self(vec![0])
    }
}

/// The wire form of an address in the metrics tables: `[0, 8, 10]`.
/// `FromStr` is the reader's end of that format; `Display` renders the
/// identical shape, so the two round-trip.
impl fmt::Display for Addr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        for (i, x) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{x}")?;
        }
        write!(f, "]")
    }
}

impl FromStr for Addr {
    type Err = ProfilerError;

    /// Parses the wire form `[0, 8, 10]`; the empty path `[]` is valid.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let inner = s
            .trim()
            .strip_prefix('[')
            .and_then(|s| s.strip_suffix(']'))
            .ok_or_else(|| ProfilerError::ParseAddr(format!("addr must be bracketed: {s}")))?
            .trim();
        if inner.is_empty() {
            return Ok(Addr(vec![]));
        }
        let mut v = Vec::new();
        for part in inner.split(',') {
            let p = part.trim();
            if p.is_empty() {
                continue;
            }
            v.push(
                p.parse::<u32>()
                    .map_err(|_| ProfilerError::ParseAddr(format!("bad addr element {p}")))?,
            );
        }
        Ok(Addr(v))
    }
}

impl Addr {
    /// Descend into a subscope. Children start at index `1`: index `0` is
    /// the scope boundary, never a leaf operator.
    pub(crate) fn enter_scope(&mut self) {
        self.0.push(1);
    }

    /// Pop back to the parent scope, returning `false` if already at the
    /// root. A root leave is an unbalanced enter/leave; the caller reports
    /// it with the recording context it holds, which this path lacks.
    pub(crate) fn leave_scope(&mut self) -> bool {
        if self.0.len() > 1 {
            self.0.pop();
            true
        } else {
            false
        }
    }

    /// Advance the last position by `steps`, returning the addresses
    /// consumed: the range this allocation assigns.
    pub(crate) fn advance(&mut self, steps: u32) -> Vec<Addr> {
        (0..steps).map(|_| self.advance_one()).collect()
    }

    /// Advance the last position by one, returning the previous address.
    fn advance_one(&mut self) -> Addr {
        let prev = self.clone();
        if let Some(last) = self.0.last_mut() {
            *last += 1;
        }
        prev
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case(vec![0])]
    #[case(vec![0, 8, 10])]
    #[case(vec![])]
    fn display_and_from_str_round_trip(#[case] path: Vec<u32>) {
        let addr = Addr(path);
        assert_eq!(addr.to_string().parse::<Addr>().unwrap(), addr);
    }

    #[test]
    fn unbracketed_input_is_rejected() {
        let Err(ProfilerError::ParseAddr(msg)) = "0, 8]".parse::<Addr>() else {
            panic!("expected ParseAddr");
        };
        assert_eq!(msg, "addr must be bracketed: 0, 8]");
    }

    #[test]
    fn non_numeric_element_is_rejected() {
        let Err(ProfilerError::ParseAddr(msg)) = "[0, x]".parse::<Addr>() else {
            panic!("expected ParseAddr");
        };
        assert_eq!(msg, "bad addr element x");
    }

    #[test]
    fn default_is_the_root_scope_path() {
        assert_eq!(Addr::default(), Addr(vec![0]));
    }

    /// Subscope children start at index 1; index 0 is the scope boundary.
    #[test]
    fn entering_a_scope_starts_children_at_index_one() {
        let mut addr = Addr::default();
        addr.enter_scope();
        assert_eq!(addr, Addr(vec![0, 1]));
        assert!(addr.leave_scope());
        assert_eq!(addr, Addr(vec![0]));
    }

    /// Leaving the root reports failure (an unbalanced enter/leave) and
    /// leaves the path untouched rather than popping past the root.
    #[test]
    fn leaving_the_root_scope_is_rejected() {
        let mut addr = Addr::default();
        assert!(!addr.leave_scope());
        assert_eq!(addr, Addr(vec![0]));
    }

    #[test]
    fn advance_returns_the_consumed_addresses() {
        let mut addr = Addr(vec![0, 3]);
        let consumed = addr.advance(2);
        assert_eq!(consumed, vec![Addr(vec![0, 3]), Addr(vec![0, 4])]);
        assert_eq!(addr, Addr(vec![0, 5]));
    }
}
