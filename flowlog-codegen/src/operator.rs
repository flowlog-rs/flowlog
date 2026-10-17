//! Operators: [`Codegen::gen_operator`] lowers one planned step to
//! the runtime operator that computes it, dispatching on the step's shape
//! to [`map`] for a map, filter or arrangement, [`join`] for a join or an
//! antijoin, and [`aggregate`] for the reduce of an aggregate's matches;
//! [`union`] unions a relation's heads and [`lattice`] merges a lattice
//! relation's. The closures' pieces come from [`expr`](crate::expr), and
//! the strata order the operators.

use std::collections::HashMap;

use flowlog_planner::planner::Collection;
use flowlog_planner::planner::StratumPlanner;
use flowlog_planner::planner::Transformation;
use flowlog_profiler::PlanGraph;
use proc_macro2::Ident;
use proc_macro2::Span;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;
use syn::LitStr;

use crate::Codegen;
use crate::CodegenError;
use crate::ident::find_local_ident;

mod aggregate;
mod join;
mod lattice;
mod map;
mod union;

/// A planned step's operator as its emitter sees it: its names, where it
/// runs, and the bindings in scope.
struct Operator<'a> {
    /// The operator's name.
    name: String,
    /// The name as the literal the dataflow passes the operator.
    literal: LitStr,
    /// The name of the output's arrangement when the output is keyed, and
    /// `None` when it is rows. The name is the collection's, which other
    /// steps share by fingerprint, not the step's.
    arrange: Option<String>,
    /// Whether the step sits inside a loop.
    recursive: bool,
    /// Whether string columns are interned.
    si: bool,
    /// The binding of every collection in scope: the global ones outside
    /// a loop, the loop's own inside one.
    local_fp_to_ident: &'a HashMap<u64, Ident>,
}

impl Codegen {
    /// Returns the operator that computes `transformation`, followed by its
    /// output's arrangement when the output is keyed.
    ///
    /// `local_fp_to_ident` names every collection in scope: the global
    /// bindings outside a loop, the loop's own inside one. `arranged_map`
    /// holds the arrangement of every keyed collection built so far; a join
    /// reads its inputs from it and a keyed output is added to it. Also
    /// records the output's types and mutability by fingerprint, and its
    /// profiler node.
    pub(crate) fn gen_operator(
        &mut self,
        local_fp_to_ident: &HashMap<u64, Ident>,
        transformation: &Transformation,
        arranged_map: &mut HashMap<u64, Ident>,
        stratum: &StratumPlanner,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        let recursive = stratum.is_recursive_transformation(transformation);

        // `atom_fps` decides *which* inputs are named atoms; the label
        // text comes from `display_name` (the user's spelling).
        let atom_fps = stratum.atom_fps();
        let edb_names = transformation
            .input_fingerprints()
            .into_iter()
            .filter(|fp| atom_fps.contains(fp))
            .map(|fp| self.display_name(fp))
            .collect::<Vec<_>>();
        let edb_suffix = if edb_names.is_empty() {
            String::new()
        } else {
            format!(" <- {}", edb_names.join(", "))
        };
        let name = format!(
            "{}: {}{}",
            transformation.profile_operation_name(),
            transformation.flow(),
            edb_suffix,
        );
        let keyed = matches!(
            transformation,
            Transformation::RowToKv { .. }
                | Transformation::KvToKv { .. }
                | Transformation::JnToKv { .. }
                | Transformation::NJnToKv { .. }
                | Transformation::AggToKv { .. }
        );
        let operator = Operator {
            literal: LitStr::new(&name, Span::call_site()),
            arrange: keyed.then(|| format!("Arrange: {}", transformation.output())),
            name,
            recursive,
            si: self.config.str_intern_enabled(),
            local_fp_to_ident,
        };

        // Cache the planner's value for this collection by fingerprint:
        // unions and outputs downstream know their parts only by fingerprint.
        let output_fp = transformation.output().fingerprint();
        self.global_fp_to_mutability
            .insert(output_fp, transformation.output().mutability());
        let inputs = transformation.input_fingerprints();
        self.record_output_type(
            inputs[0],
            inputs.get(1).copied(),
            output_fp,
            transformation.flow(),
        )?;

        match transformation {
            Transformation::RowToRow {
                input,
                output,
                flow,
            } => self.gen_row_to_row(operator, input, output, flow, plan_graph),
            Transformation::RowToKv {
                input,
                output,
                flow,
            } => self.gen_row_to_kv(operator, input, output, flow, arranged_map, plan_graph),
            Transformation::KvToRow {
                input,
                output,
                flow,
            }
            | Transformation::KvToKv {
                input,
                output,
                flow,
            } => self.gen_kv_map(operator, input, output, flow, arranged_map, plan_graph),
            Transformation::JnToRow {
                input: (left, right),
                output,
                flow,
            }
            | Transformation::JnToKv {
                input: (left, right),
                output,
                flow,
            } => self.gen_join(
                operator,
                (left, right),
                output,
                flow,
                arranged_map,
                plan_graph,
            ),
            Transformation::NJnToRow {
                input: (left, right),
                output,
                flow,
            }
            | Transformation::NJnToKv {
                input: (left, right),
                output,
                flow,
            } => self.gen_antijoin(
                operator,
                (left, right),
                output,
                flow,
                arranged_map,
                plan_graph,
            ),
            Transformation::AggToRow {
                input: (matches, groups),
                output,
                flow,
            }
            | Transformation::AggToKv {
                input: (matches, groups),
                output,
                flow,
            } => self.gen_aggregate(
                operator,
                (matches, groups.as_deref()),
                output,
                flow,
                arranged_map,
                plan_graph,
            ),
        }
    }
}

// =============================================================================
// Shared pieces
// =============================================================================

/// Returns the statement arranging the keyed `output`, bound as
/// `<collection>_arr` under `name`: by itself when it is key-only, else by
/// key. Records the arrangement in `arranged_map` for the joins that read
/// it. Returns nothing for an output that is rows, which has no `name`.
fn register_arrangement(
    arranged_map: &mut HashMap<u64, Ident>,
    output: &Collection,
    collection: &Ident,
    name: Option<&str>,
) -> TokenStream {
    let Some(name) = name else {
        return quote! {};
    };
    let arrangement = format_ident!("{}_arr", collection);
    arranged_map.insert(output.fingerprint(), arrangement.clone());
    let arrange = if output.is_k_only() {
        quote! { flowlog_arrange_self }
    } else {
        quote! { flowlog_arrange }
    };
    quote! {
        let #arrangement = ::flowlog_runtime::operators::#arrange(#collection.clone(), #name);
    }
}

/// Returns the arrangement of `input`, or an internal error when it is
/// not arranged yet: the planner orders every arrangement before the
/// steps that read it.
fn arranged_input(
    local_fp_to_ident: &HashMap<u64, Ident>,
    arranged_map: &HashMap<u64, Ident>,
    input: &Collection,
) -> Result<Ident, CodegenError> {
    let fingerprint = input.fingerprint();
    arranged_map.get(&fingerprint).cloned().ok_or_else(|| {
        let base = find_local_ident(local_fp_to_ident, fingerprint);
        CodegenError::internal(format!(
            "collection `{base}` (fingerprint 0x{fingerprint:016x}) must be arranged before use"
        ))
    })
}

/// Returns a keyed output's tuple: the key alone when `output` is key-only,
/// else `(key, value)`.
fn keyed_output(output: &Collection, key: TokenStream, value: TokenStream) -> TokenStream {
    if output.is_k_only() {
        quote! { #key }
    } else {
        quote! { ( #key, #value ) }
    }
}
