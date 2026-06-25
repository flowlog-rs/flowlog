//! Shared statistics type used by both log parsing and view aggregation.

use serde::Serialize;
use std::ops::Add;

/// Per-field statistics across workers: mean, variance, min, max.
#[derive(Debug, Clone, Serialize, Default)]
pub struct Stats {
    pub mean: f64,
    pub var: f64,
    pub min: f64,
    pub max: f64,
}

impl Stats {
    pub fn new(mean: f64, var: f64, min: f64, max: f64) -> Self {
        Self {
            mean,
            var,
            min,
            max,
        }
    }

    /// Compute stats from a slice of per-worker values.
    pub fn from_values(values: &[f64]) -> Self {
        let n = values.len() as f64;
        if n == 0.0 {
            return Self::default();
        }
        let mean = values.iter().sum::<f64>() / n;
        let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let var = if n > 1.0 {
            values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n
        } else {
            0.0
        };
        Self {
            mean,
            var,
            min,
            max,
        }
    }
}

/// Adding two Stats sums means and variances (valid for independent variables).
/// Min/max are also summed, providing conservative bounds.
impl Add for &Stats {
    type Output = Stats;
    fn add(self, rhs: &Stats) -> Stats {
        Stats {
            mean: self.mean + rhs.mean,
            var: self.var + rhs.var,
            min: self.min + rhs.min,
            max: self.max + rhs.max,
        }
    }
}
