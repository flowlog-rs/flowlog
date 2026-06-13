//! Aggregation code generation utilities for FlowLog compiler.
//!
//! This module provides helper functions to generate Rust code for aggregation
//! operators in the codegen pipeline. It covers row chopping, reduction
//! selection, key/value reconstruction, and semiring-optimized pipelines.

mod avg;
mod common;
mod count;
mod max;
mod min;
mod sum;

pub(super) use self::common::{
    aggregation_merge_kv, aggregation_opt_post_leave, aggregation_reduce_stmt, aggregation_row_chop,
};
pub(super) use avg::{
    aggregation_avg_optimize, aggregation_avg_post_leave, aggregation_avg_pre_leave,
};
pub(super) use count::{aggregation_count_optimize, aggregation_count_pre_leave};
pub(super) use max::{aggregation_max_optimize, aggregation_max_pre_leave};
pub(super) use min::{aggregation_min_optimize, aggregation_min_pre_leave};
pub(super) use sum::{aggregation_sum_optimize, aggregation_sum_pre_leave};
