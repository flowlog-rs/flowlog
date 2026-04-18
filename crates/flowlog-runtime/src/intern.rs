//! Thread-safe string interning via `lasso::ThreadedRodeo`.

use lasso::{Spur, ThreadedRodeo};
use std::sync::LazyLock;

/// Global string interner shared across all FlowLog engines in the process.
///
/// **Limitation**: this is a process-local pool. In a distributed DD
/// deployment (multiple machines), each process gets its own independent
/// `INTERNER`, so `Spur` values are NOT comparable across machines.
/// Distributed support would require a global interning protocol or
/// switching back to `String`-keyed collections.
pub static INTERNER: LazyLock<ThreadedRodeo> = LazyLock::new(ThreadedRodeo::default);

const MAX_RETRIES: usize = 1024;

/// Intern a string, returning its [`Spur`] handle.
#[inline(always)]
pub fn intern(s: &str) -> Spur {
    for _ in 0..MAX_RETRIES {
        match INTERNER.try_get_or_intern(s) {
            Ok(key) => return key,
            Err(_) => std::thread::yield_now(),
        }
    }
    panic!("string interner failed after {MAX_RETRIES} attempts for {s:?}");
}

/// Resolve a [`Spur`] handle back to a `&'static str`.
#[inline(always)]
pub fn resolve(key: Spur) -> &'static str {
    INTERNER.resolve(&key)
}
