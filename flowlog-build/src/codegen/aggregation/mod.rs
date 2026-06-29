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

pub(super) use avg::aggregation_avg_optimize;
pub(super) use avg::aggregation_avg_post_leave;
pub(super) use avg::aggregation_avg_pre_leave;
pub(super) use count::aggregation_count_optimize;
pub(super) use count::aggregation_count_pre_leave;
pub(super) use max::aggregation_max_optimize;
pub(super) use max::aggregation_max_pre_leave;
pub(super) use min::aggregation_min_optimize;
pub(super) use min::aggregation_min_pre_leave;
pub(super) use sum::aggregation_sum_optimize;
pub(super) use sum::aggregation_sum_pre_leave;

pub(super) use self::common::aggregation_merge_kv;
pub(super) use self::common::aggregation_opt_post_leave;
pub(super) use self::common::aggregation_reduce_stmt;
pub(super) use self::common::aggregation_row_chop;
