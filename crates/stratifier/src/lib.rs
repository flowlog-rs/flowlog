//! FlowLog Stratifier Library
//!
//! This crate provides analysis utilities that operate on a parsed
//! FlowLog `Program` and help plan evaluation order:
//!
//! - [`DependencyGraph`]: builds the rule dependency map
//!   identifying which rules reference predicates defined by other rules.
//! - [`Stratifier`]: partitions rules into strata (layers) so every rule’s
//!   dependencies are evaluated earlier; detects recursive strata (SCCs).
//!
//! # Example
//! ```rust,no_run
//! use parser::Program;
//! use stratifier::Stratifier;
//!
//! // Parse a FlowLog program (example reach.dl, adjust path as needed)
//! let program = Program::parse("../../example/reach.dl");
//!
//! // Compute stratum
//! let stratum = Stratifier::from_program(&program);
//! println!("{}", stratum);
//! ```

mod dependency_graph;
pub mod stratifier;

pub use stratifier::Stratifier;
