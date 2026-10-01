//! The shared codegen core. Turns a parsed FlowLog program plus a
//! stratified execution plan into a [`Skeleton`] that each frontend
//! (library mode in `flowlog-build`, binary mode in `flowlog-compiler`)
//! assembles into its own final Rust source.
//!
//! The modules follow where their output lands in the generated engine:
//!
//! - `codegen`: the [`Codegen`] state every pass reads and extends.
//! - `skeleton`: the [`Skeleton`] itself, and the order it is filled in.
//! - `io`: the program's boundary, from relation declarations and input
//!   collections to the emitters its outputs publish through.
//! - `stratum`: the dataflow's strata in order, each non-recursive or
//!   recursive.
//! - `rule`: a rule's head and body steps, shared by both kinds of stratum.
//! - `expr`: the expressions inside operator closures.
//! - `ty`: the `(Data, Diff, Time)` types every collection carries.
//! - `ident`, `error`, `profile`: binding idents, internal errors, and the
//!   profiler's side of the engine.

mod codegen;
mod error;
mod expr;
mod ident;
mod io;
mod profile;
mod rule;
mod skeleton;
mod stratum;
#[cfg(test)]
mod test_harness;
mod ty;

pub use codegen::Codegen;
pub use error::CodegenError;
pub(crate) use expr::term::constant::const_to_token;
pub use ident::input_field_ident;
pub use ident::input_handle_ident;
pub use ident::output_emitter_ident;
pub use ident::relation_marker_ident;
pub use io::gen_relations;
pub use skeleton::Skeleton;
pub(crate) use ty::data::internal_tuple_tokens;
pub(crate) use ty::data::row_is_copy;
pub(crate) use ty::data::tuple_tokens;
pub use ty::data::user_tuple_tokens;
pub use ty::diff::weight_tokens;
