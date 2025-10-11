use tracing::info;

use planner::Transformation;

/// Simple placeholder executor that logs planner transformations.
/// In future iterations, this will evaluate transformations against a runtime/engine.
pub struct Executor {}

impl Executor {
    pub fn new() -> Self {
        Self {}
    }

    /// Execute all transformations in a stratum in order.
    /// Currently logs the IO collections and basic flow metadata per transformation.
    pub fn execute(&self, transformations: &[Transformation]) {
        info!(target: "executor", "Executing {} transformation(s)", transformations.len());
        for (i, t) in transformations.iter().enumerate() {
            if t.is_unary() {
                let inp = t.unary_input();
                let out = t.output();
                let (ik, iv) = inp.arity();
                let (ok, ov) = out.arity();
                info!(target: "executor", 
                      "[{:>3}] Unary  input=({:016x}, k={}, v={}) -> output=({:016x}, k={}, v={})",
                      i, inp.fingerprint(), ik, iv, out.fingerprint(), ok, ov);
            } else {
                let (l, r) = t.binary_input();
                let out = t.output();
                let (lk, lv) = l.arity();
                let (rk, rv) = r.arity();
                let (ok, ov) = out.arity();
                info!(target: "executor", 
                      "[{:>3}] Binary left=({:016x}, k={}, v={}) ⋈ right=({:016x}, k={}, v={}) -> output=({:016x}, k={}, v={})",
                      i, l.fingerprint(), lk, lv, r.fingerprint(), rk, rv, out.fingerprint(), ok, ov);
            }
        }
    }
}
