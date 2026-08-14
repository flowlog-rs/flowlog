//! [`FlowlogError`], the trait every error type implements, and
//! [`InternalError`], what a stage reports when its own invariant breaks.

use std::error::Error as StdError;
use std::fmt;

/// Canonical bug-report URL, so every stage's "please file a bug" note
/// points at the same tracker.
pub const BUG_URL: &str = "https://github.com/flowlog-rs/flowlog/issues/new";

/// What every FlowLog error answers, whichever stage raised it.
pub trait FlowlogError: StdError + Send + Sync + 'static {
    /// Returns `true` if the failure is a bug in FlowLog, rather than a
    /// mistake in the user's program, data, or environment.
    ///
    /// Drives the exit code a front-end picks, and whether asking for a
    /// report is worthwhile.
    fn is_internal(&self) -> bool {
        false
    }
}

/// An invariant violation inside FlowLog, as opposed to a user error.
#[derive(Debug)]
pub struct InternalError {
    stage: &'static str,
    detail: String,
    bug_url: &'static str,
}

impl InternalError {
    /// `stage` names the pass that caught the violation and appears in the
    /// message, so a report says where to start looking.
    pub fn new(stage: &'static str, detail: impl Into<String>, bug_url: &'static str) -> Self {
        Self {
            stage,
            detail: detail.into(),
            bug_url,
        }
    }

    /// Where to send a report for this failure.
    #[must_use]
    pub fn bug_url(&self) -> &'static str {
        self.bug_url
    }
}

impl fmt::Display for InternalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "internal compiler error at stage `{}`: {}",
            self.stage, self.detail
        )
    }
}

impl StdError for InternalError {}

impl FlowlogError for InternalError {
    fn is_internal(&self) -> bool {
        true
    }
}
