//! Address type used in Timely Dataflow logs.
//!
//! Example log addr: [0, 8, 10]  =>  Addr(vec![0, 8, 10])
//!
//! We store it as a Vec<u32> and derive ordering so it can be used in BTreeSet/Map.

use std::fmt;
use std::str::FromStr;

use serde::Deserialize;
use serde::Serialize;
use tracing::error;

/// Address in a Timely Dataflow log, representing nested scopes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Addr(pub Vec<u32>);

impl Default for Addr {
    fn default() -> Self {
        Self(vec![0])
    }
}

/// The textual form an `Addr` takes in the metrics log: `[0, 8, 10]`. `Display`
/// and `FromStr` are the paired writer/reader so the two ends can't drift — the
/// engine emits this shape and the visualizer parses it back.
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
    type Err = String;

    /// Parse `[0, 8, 10]` into `Addr(vec![0, 8, 10])`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let inner = s
            .trim()
            .strip_prefix('[')
            .and_then(|s| s.strip_suffix(']'))
            .ok_or_else(|| format!("addr must be bracketed: {s}"))?
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
                    .map_err(|_| format!("bad addr element {p}"))?,
            );
        }
        Ok(Addr(v))
    }
}

impl Addr {
    /// Build an address from an explicit scope path (read side, used by the visualizer).
    pub fn new(path: Vec<u32>) -> Self {
        Self(path)
    }

    /// Enter a new scope.
    pub(crate) fn enter_scope(&mut self) {
        self.0.push(1);
    }

    /// Leave the current scope.
    pub(crate) fn leave_scope(&mut self) {
        if self.0.len() > 1 {
            self.0.pop();
        } else {
            error!("Profiler error: attempted to leave root address scope");
        }
    }

    /// Advance the last position by `steps`, returning previous addresses.
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
