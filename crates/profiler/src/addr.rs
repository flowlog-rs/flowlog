//! Address type used in Timely Dataflow logs.
//!
//! Example log addr: [0, 8, 10]  =>  Addr(vec![0, 8, 10])
//!
//! We store it as a Vec<u32> and derive ordering so it can be used in BTreeSet/Map.

use serde::{Deserialize, Serialize};

/// Address in a Timely Dataflow log, representing nested scopes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Addr(pub Vec<u32>);

impl Default for Addr {
    fn default() -> Self {
        Self(vec![0])
    }
}

impl Addr {
    /// Enter a new scope.
    pub fn enter_scope(&mut self) {
        self.0.push(1);
    }

    /// Leave the current scope.
    pub fn leave_scope(&mut self) {
        self.0.pop();
    }

    /// Advance the last position by `steps`, returning previous addresses.
    pub fn advance(&mut self, steps: u32) -> Vec<Addr> {
        (0..steps).map(|_| self.advance_one()).collect()
    }

    /// Advance the last position by one, returning the previous address.
    pub fn advance_one(&mut self) -> Addr {
        let prev = self.clone();
        if let Some(last) = self.0.last_mut() {
            *last += 1;
        }
        prev
    }
}
