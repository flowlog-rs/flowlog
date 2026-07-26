//! [`ProfilerError`], the crate's single error currency for failed
//! operations on profiling artifacts.

use std::fmt;
use std::io;
use std::path::PathBuf;

use flowlog_common::BUG_URL;
use flowlog_common::InternalError;

/// A failed operation on profiling artifacts.
#[derive(Debug)]
pub enum ProfilerError {
    /// Filesystem failure on the named artifact.
    Io { path: PathBuf, source: io::Error },
    /// An `ops.json` plan graph that fails structural validation (a
    /// malformed or hand-edited file; a well-formed compiler emit passes).
    InvalidPlan(String),
    /// A malformed operator address literal in a wire table.
    ParseAddr(String),
    /// A malformed block label in a wire table.
    ParseBlock(String),
    /// The plan predicts an operator the run's metrics never recorded:
    /// address prediction drifted, or the ops.json and metrics directory
    /// are from different runs. Cannot arise for a matching run's metrics.
    MetricsMismatch(String),
    /// Serializing a plan graph to JSON failed. Unreachable in practice
    /// (`PlanGraph` derives `Serialize`), surfaced rather than panicked.
    Serialize(serde_json::Error),
    /// An invariant an earlier stage should have upheld was violated: a
    /// FlowLog bug, not a bad artifact. Carries a stage tag and bug-report
    /// note for whoever hits it.
    Internal(InternalError),
}

impl ProfilerError {
    /// Build an [`Internal`](ProfilerError::Internal) error for a violated
    /// profiler invariant, tagged for a bug report.
    pub(crate) fn internal(detail: impl Into<String>) -> Self {
        ProfilerError::Internal(InternalError::new("profiler", detail, BUG_URL))
    }
}

impl fmt::Display for ProfilerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProfilerError::Io { path, source } => write!(f, "{}: {source}", path.display()),
            ProfilerError::InvalidPlan(reason) => write!(f, "invalid plan graph: {reason}"),
            ProfilerError::ParseAddr(reason) => write!(f, "invalid operator address: {reason}"),
            ProfilerError::ParseBlock(reason) => write!(f, "invalid block label: {reason}"),
            ProfilerError::MetricsMismatch(reason) => {
                write!(f, "plan does not match metrics: {reason}")
            }
            ProfilerError::Serialize(source) => write!(f, "serializing plan graph: {source}"),
            ProfilerError::Internal(source) => write!(f, "{source}"),
        }
    }
}

impl std::error::Error for ProfilerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ProfilerError::Io { source, .. } => Some(source),
            ProfilerError::Serialize(source) => Some(source),
            ProfilerError::Internal(source) => Some(source),
            ProfilerError::InvalidPlan(_)
            | ProfilerError::ParseAddr(_)
            | ProfilerError::ParseBlock(_)
            | ProfilerError::MetricsMismatch(_) => None,
        }
    }
}
