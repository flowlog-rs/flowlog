//! Shared macros for FlowLog.

/// Maximum number of retries for transient string-interner allocation failures.
pub const INTERN_MAX_RETRIES: usize = 1024;
