//! Transformation code generation for FlowLog compiler.
//!
//! This module generates Rust code for various data transformations
//! such as Row-to-Row, Row-to-KV, KV-to-KV, KV-to-Row, Joins, and Antijoins
//! in the differential dataflow pipelines.

use std::cell::RefCell;
use std::collections::HashMap;

use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};

use crate::codegen::arg::{
    build_kv_constraints_predicate, build_row_constraints_predicate, combine_predicates,
    compute_join_param_tokens, compute_kv_param_tokens, kv_use_counts, row_pattern_and_fields,
    row_use_counts,
};
use crate::codegen::ident::find_local_ident;
use crate::codegen::{CodeGen, CodegenError, data_type_tokens};
use crate::planner::{StratumPlanner, Transformation};
use crate::profiler::{Profiler, with_profiler};

impl CodeGen {
    /// Generate differential dataflow pipelines for a single transformation.
    ///
    /// For non-recursive transformations, we use the global fingerprint-to-ident map.
    /// For recursive transformations, we use the local fingerprint-to-ident map.
    /// This function accepts the map as `local_fp_to_ident` to keep the interface unified.
    pub(super) fn gen_transformation(
        &mut self,
        local_fp_to_ident: &HashMap<u64, Ident>,
        transformation: &Transformation,
        arranged_map: &mut HashMap<u64, Ident>,
        stratum: &StratumPlanner,
        profiler: &mut Option<Profiler>,
    ) -> Result<TokenStream, CodegenError> {
        // Arrangement sharing is decided in the planner. If this output's
        // arrangement was marked a duplicate of an earlier one in this scope,
        // resolve the canonical collection ident to clone instead of
        // rebuilding the projection + sorted trace (see
        // `ProgramPlanner::mark_shared_arrangements`).
        let arr_alias = stratum
            .arrangement_alias_of(transformation.output().fingerprint())
            .map(|canon_fp| find_local_ident(local_fp_to_ident, canon_fp));

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
            format!(" ← {}", edb_names.join(", "))
        };
        let transformation_name = format!(
            "{}: {}{}",
            transformation.profile_operation_name(),
            transformation.flow(),
            edb_suffix,
        );
        let si = self.features.string_intern();

        // Mark UDF import needed if any fn_call predicate is present
        if !transformation.flow().fn_call_preds().is_empty() {
            self.features.mark_udf();
        }

        match transformation {
            // Row -> Row
            Transformation::RowToRow {
                input,
                output,
                flow,
            } => {
                // Inputs / outputs
                let inp = find_local_ident(local_fp_to_ident, input.fingerprint());
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                // Profiler hook (optional)
                with_profiler(profiler, |profiler| {
                    profiler.map_join_operator(
                        transformation_name,
                        vec![inp.to_string()],
                        out.to_string(),
                        output.fingerprint(),
                    );
                });

                // Type inference + row pattern
                let input_arity = input.arity().1;
                let (row_pat, row_fields) = row_pattern_and_fields(
                    input_arity,
                    flow.key(),
                    flow.value(),
                    flow.compares(),
                    flow.fn_call_preds(),
                    flow.constraints(),
                );
                self.record_transformation_output_type(
                    input.fingerprint(),
                    None,
                    output.fingerprint(),
                    flow,
                    stratum,
                )?;
                let input_type = self.find_global_data_type(input.fingerprint())?.clone();
                let itype = input_type.1.clone();

                // Output expression + predicates
                let row_ty = data_type_tokens(&itype, si);
                let remaining = RefCell::new(row_use_counts(&[flow.value()]));
                let out_val = self.build_key_val_from_row_args(
                    flow.value(),
                    &row_fields,
                    si,
                    Some(&remaining),
                )?;
                let cmp_pred = self.build_row_compare_predicate(
                    flow.compares(),
                    &row_fields,
                    si,
                    &input_type,
                )?;
                let cst_pred =
                    build_row_constraints_predicate(flow.constraints(), &row_fields, si)?;
                let fc_pred =
                    self.build_row_fn_call_predicate(flow.fn_call_preds(), &row_fields, si)?;
                let pred = combine_predicates(vec![cmp_pred, cst_pred, fc_pred]);

                let flat_map_body = flat_map_body_tokens(pred, out_val);

                Ok(quote! {
                    let #out = #inp.clone()
                        .flat_map(|#row_pat: #row_ty| { #flat_map_body });
                })
            }

            // Row -> KV
            Transformation::RowToKv {
                input,
                output,
                flow,
            } => {
                // Inputs / outputs
                let inp = find_local_ident(local_fp_to_ident, input.fingerprint());
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                // Profiler hook (optional)
                with_profiler(profiler, |profiler| {
                    profiler.map_join_arrange_operator(
                        transformation_name,
                        vec![inp.to_string()],
                        format!("{}_arr", out),
                        output.fingerprint(),
                        output.is_k_only(),
                    );
                });

                // Type inference + row pattern
                let input_arity = input.arity().1;
                let (row_pat, row_fields) = row_pattern_and_fields(
                    input_arity,
                    flow.key(),
                    flow.value(),
                    flow.compares(),
                    flow.fn_call_preds(),
                    flow.constraints(),
                );
                self.record_transformation_output_type(
                    input.fingerprint(),
                    None,
                    output.fingerprint(),
                    flow,
                    stratum,
                )?;
                let input_type = self.find_global_data_type(input.fingerprint())?.clone();
                let itype = input_type.1.clone();

                // Output expression + predicates
                let row_ty = data_type_tokens(&itype, si);
                let remaining = RefCell::new(row_use_counts(&[flow.key(), flow.value()]));
                let out_key = self.build_key_val_from_row_args(
                    flow.key(),
                    &row_fields,
                    si,
                    Some(&remaining),
                )?;
                let out_val = self.build_key_val_from_row_args(
                    flow.value(),
                    &row_fields,
                    si,
                    Some(&remaining),
                )?;
                let out_expr = if output.is_k_only() {
                    quote! { #out_key }
                } else {
                    quote! { ( #out_key, #out_val ) }
                };

                let cmp_pred = self.build_row_compare_predicate(
                    flow.compares(),
                    &row_fields,
                    si,
                    &input_type,
                )?;
                let cst_pred =
                    build_row_constraints_predicate(flow.constraints(), &row_fields, si)?;
                let fc_pred =
                    self.build_row_fn_call_predicate(flow.fn_call_preds(), &row_fields, si)?;
                let pred = combine_predicates(vec![cmp_pred, cst_pred, fc_pred]);

                // Transformation logic
                let flat_map_body = flat_map_body_tokens(pred, out_expr);

                let transformation = quote! {
                    let #out = #inp.clone()
                        .flat_map(|#row_pat: #row_ty| { #flat_map_body });
                };

                // Arrangement registration (shares a byte-identical arrangement
                // already built in this scope, if any).
                Ok(self.register_arrangement(
                    arranged_map,
                    arr_alias.as_ref(),
                    output.fingerprint(),
                    &out,
                    transformation,
                    output.is_k_only(),
                ))
            }

            // KV -> Row
            Transformation::KvToRow {
                input,
                output,
                flow,
            } => {
                // Inputs / outputs
                let inp = find_local_ident(local_fp_to_ident, input.fingerprint());
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                // Profiler hook (optional)
                with_profiler(profiler, |profiler| {
                    profiler.map_join_operator(
                        transformation_name,
                        vec![inp.to_string()],
                        out.to_string(),
                        output.fingerprint(),
                    );
                });

                // Type inference
                self.record_transformation_output_type(
                    input.fingerprint(),
                    None,
                    output.fingerprint(),
                    flow,
                    stratum,
                )?;

                // Output value + predicates
                let input_type = self.find_global_data_type(input.fingerprint())?.clone();
                let remaining = RefCell::new(kv_use_counts(&[flow.value()]));
                let out_val =
                    self.build_key_val_from_kv_args(flow.value(), si, Some(&remaining))?;
                let cmp_pred = self.build_kv_compare_predicate(flow.compares(), si, &input_type)?;
                let cst_pred = build_kv_constraints_predicate(flow.constraints(), si)?;
                let fc_pred = self.build_kv_fn_call_predicate(flow.fn_call_preds(), si)?;
                let pred = combine_predicates(vec![cmp_pred, cst_pred, fc_pred]);
                let (kv_param_k, kv_param_v) = compute_kv_param_tokens(
                    flow.key(),
                    flow.value(),
                    flow.compares(),
                    flow.fn_call_preds(),
                    Some(flow.constraints()),
                );

                // Transformation logic
                let flat_map_body = flat_map_body_tokens(pred, out_val);

                Ok(quote! {
                    let #out = #inp.clone()
                        .flat_map(|( #kv_param_k, #kv_param_v )| { #flat_map_body });
                })
            }

            // KV -> KV
            Transformation::KvToKv {
                input,
                output,
                flow,
            } => {
                // Inputs / outputs
                let inp = find_local_ident(local_fp_to_ident, input.fingerprint());
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                // Profiler hook (optional)
                with_profiler(profiler, |profiler| {
                    profiler.map_join_arrange_operator(
                        transformation_name,
                        vec![inp.to_string()],
                        format!("{}_arr", out),
                        output.fingerprint(),
                        output.is_k_only(),
                    );
                });

                // Type inference
                self.record_transformation_output_type(
                    input.fingerprint(),
                    None,
                    output.fingerprint(),
                    flow,
                    stratum,
                )?;

                // Output expression + predicates
                let input_type = self.find_global_data_type(input.fingerprint())?.clone();
                let remaining = RefCell::new(kv_use_counts(&[flow.key(), flow.value()]));
                let out_key = self.build_key_val_from_kv_args(flow.key(), si, Some(&remaining))?;
                let out_val =
                    self.build_key_val_from_kv_args(flow.value(), si, Some(&remaining))?;
                let out_expr = if output.is_k_only() {
                    quote! { #out_key }
                } else {
                    quote! { ( #out_key, #out_val ) }
                };
                let cmp_pred = self.build_kv_compare_predicate(flow.compares(), si, &input_type)?;
                let cst_pred = build_kv_constraints_predicate(flow.constraints(), si)?;
                let fc_pred = self.build_kv_fn_call_predicate(flow.fn_call_preds(), si)?;
                let pred = combine_predicates(vec![cmp_pred, cst_pred, fc_pred]);
                let (kv_param_k, kv_param_v) = compute_kv_param_tokens(
                    flow.key(),
                    flow.value(),
                    flow.compares(),
                    flow.fn_call_preds(),
                    Some(flow.constraints()),
                );

                // Closure parameter depends on whether input is key-only
                let closure_param = if input.is_k_only() {
                    quote! { |#kv_param_k| }
                } else {
                    quote! { |( #kv_param_k, #kv_param_v )| }
                };

                // Ideally, in system design, projection (to key) in SIP optimization may introduce duplicates,
                // we have to apply deduplication to avoid incorrect Yannakakis computation bounds.
                // Dedup only applies when there is no predicate (predicate paths already filter).
                let dedup_call = self.dedup_nonrecursive();
                let out_dedup_expr = if pred.is_none() && output.is_k_only() {
                    quote! { let #out = #out #dedup_call; }
                } else {
                    quote! {}
                };

                // Flat_map body depends on whether there is a predicate
                let flat_map_body = flat_map_body_tokens(pred, out_expr);

                let transformation = quote! {
                    let #out = #inp.clone()
                        .flat_map(#closure_param { #flat_map_body });
                    #out_dedup_expr
                };

                // Arrangement registration
                Ok(self.register_arrangement(
                    arranged_map,
                    arr_alias.as_ref(),
                    output.fingerprint(),
                    &out,
                    transformation,
                    output.is_k_only(),
                ))
            }

            // Join: Key-value ⋈ Key-value -> Row
            Transformation::JnToRow {
                input,
                output,
                flow,
            } => {
                // Inputs / outputs
                let (left, right) = input;
                let l_base = find_local_ident(local_fp_to_ident, left.fingerprint());
                let r_base = find_local_ident(local_fp_to_ident, right.fingerprint());
                let l = expect_arranged(arranged_map, left.fingerprint(), &l_base)?;
                let r = expect_arranged(arranged_map, right.fingerprint(), &r_base)?;
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                // Profiler hook (optional)
                with_profiler(profiler, |profiler| {
                    profiler.map_join_operator(
                        transformation_name,
                        vec![l.to_string(), r.to_string()],
                        out.to_string(),
                        output.fingerprint(),
                    );
                });

                // Type inference
                self.record_transformation_output_type(
                    left.fingerprint(),
                    Some(right.fingerprint()),
                    output.fingerprint(),
                    flow,
                    stratum,
                )?;

                // Output expression + predicates
                let (jn_k, jn_lv, jn_rv) = compute_join_param_tokens(
                    flow.key(),
                    flow.value(),
                    flow.compares(),
                    flow.fn_call_preds(),
                );
                let out_val = self.build_key_val_from_join_args(flow.value(), si)?;

                let left_type = self.find_global_data_type(left.fingerprint())?.clone();
                let right_type = self.find_global_data_type(right.fingerprint())?.clone();
                let cmp_pred = self.build_join_compare_predicate(
                    flow.compares(),
                    si,
                    &left_type,
                    &right_type,
                )?;
                let fc_pred = self.build_join_fn_call_predicate(flow.fn_call_preds(), si)?;
                let pred = combine_predicates(vec![cmp_pred, fc_pred]);
                let join_body = join_body_tokens(pred, out_val);

                Ok(quote! {
                    let #out = #l.clone()
                        .join_core(#r.clone(), |#jn_k, #jn_lv, #jn_rv| { #join_body });
                })
            }

            // Join: Key-value ⋈ Key-value -> key-value
            Transformation::JnToKv {
                input,
                output,
                flow,
            } => {
                // Inputs / outputs
                let (left, right) = input;
                let l_base = find_local_ident(local_fp_to_ident, left.fingerprint());
                let r_base = find_local_ident(local_fp_to_ident, right.fingerprint());
                let l = expect_arranged(arranged_map, left.fingerprint(), &l_base)?;
                let r = expect_arranged(arranged_map, right.fingerprint(), &r_base)?;
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                // Profiler hook (optional)
                with_profiler(profiler, |profiler| {
                    profiler.map_join_arrange_operator(
                        transformation_name,
                        vec![l.to_string(), r.to_string()],
                        format!("{}_arr", out),
                        output.fingerprint(),
                        output.is_k_only(),
                    );
                });

                // Type inference
                self.record_transformation_output_type(
                    left.fingerprint(),
                    Some(right.fingerprint()),
                    output.fingerprint(),
                    flow,
                    stratum,
                )?;

                // Output expression + predicates
                let (jn_k, jn_lv, jn_rv) = compute_join_param_tokens(
                    flow.key(),
                    flow.value(),
                    flow.compares(),
                    flow.fn_call_preds(),
                );
                let out_key = self.build_key_val_from_join_args(flow.key(), si)?;
                let out_val = self.build_key_val_from_join_args(flow.value(), si)?;
                let out_expr = if output.is_k_only() {
                    quote! { #out_key }
                } else {
                    quote! { ( #out_key, #out_val ) }
                };

                let left_type = self.find_global_data_type(left.fingerprint())?.clone();
                let right_type = self.find_global_data_type(right.fingerprint())?.clone();
                let cmp_pred = self.build_join_compare_predicate(
                    flow.compares(),
                    si,
                    &left_type,
                    &right_type,
                )?;
                let fc_pred = self.build_join_fn_call_predicate(flow.fn_call_preds(), si)?;
                let pred = combine_predicates(vec![cmp_pred, fc_pred]);
                let join_body = join_body_tokens(pred, out_expr);

                let transformation = quote! {
                    let #out = #l.clone()
                        .join_core(#r.clone(), |#jn_k, #jn_lv, #jn_rv| { #join_body });
                };

                Ok(self.register_arrangement(
                    arranged_map,
                    arr_alias.as_ref(),
                    output.fingerprint(),
                    &out,
                    transformation,
                    output.is_k_only(),
                ))
            }

            // Antijoin: Key-value ¬ Key-only to Row
            Transformation::NJnToRow {
                input,
                output,
                flow,
            } => {
                self.features.mark_as_collection();
                self.features.mark_timely_map();

                // Inputs / outputs
                let (left, right) = input;
                let l_base = find_local_ident(local_fp_to_ident, left.fingerprint());
                let r_base = find_local_ident(local_fp_to_ident, right.fingerprint());
                let l = expect_arranged(arranged_map, left.fingerprint(), &l_base)?;
                let r = expect_arranged(arranged_map, right.fingerprint(), &r_base)?;
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                // Profiler hook (optional)
                with_profiler(profiler, |profiler| {
                    profiler.anti_join_operator(
                        transformation_name,
                        vec![l.to_string(), r.to_string()],
                        out.to_string(),
                        output.fingerprint(),
                    );
                });

                // Type inference
                self.record_transformation_output_type(
                    left.fingerprint(),
                    Some(right.fingerprint()),
                    output.fingerprint(),
                    flow,
                    stratum,
                )?;

                let (pos_weight_concat, neg_weight_concat) = self.weight_concat_tokens();

                // Output expression
                let (anti_param_k, anti_param_v) =
                    compute_kv_param_tokens(flow.key(), flow.value(), flow.compares(), &[], None);
                let remaining = RefCell::new(kv_use_counts(&[flow.value()]));
                let out_map_value =
                    self.build_key_val_from_kv_args(flow.value(), si, Some(&remaining))?;
                let inter_dedup = self.dedup_antijoin();
                let final_normalize = self.dedup_recursive();

                Ok(quote! {
                    let #out =
                        #r.clone()
                            .flat_map_ref(|#anti_param_k, #anti_param_v| std::iter::once(( #anti_param_k.clone(), #anti_param_v.clone() )))
                            #inter_dedup
                            #pos_weight_concat
                            .concat(
                                {
                                    #l.clone()
                                    .join_core(#r.clone(), |aj_k, _, aj_rv| {
                                        Some((aj_k.clone(), aj_rv.clone()))
                                    })
                                    #inter_dedup
                                    #neg_weight_concat
                                }
                            )
                            .flat_map(|( #anti_param_k, #anti_param_v )| std::iter::once( #out_map_value ))
                            #final_normalize;
                })
            }

            // Antijoin: Key-only ¬ Key-only to key-value
            Transformation::NJnToKv {
                input,
                output,
                flow,
            } => {
                self.features.mark_as_collection();
                self.features.mark_timely_map();

                // Inputs / outputs
                let (left, right) = input;
                let l_base = find_local_ident(local_fp_to_ident, left.fingerprint());
                let r_base = find_local_ident(local_fp_to_ident, right.fingerprint());
                let l = expect_arranged(arranged_map, left.fingerprint(), &l_base)?;
                let r = expect_arranged(arranged_map, right.fingerprint(), &r_base)?;
                let out = find_local_ident(local_fp_to_ident, output.fingerprint());

                // Profiler hook (optional)
                with_profiler(profiler, |profiler| {
                    profiler.anti_join_arrange_operator(
                        transformation_name,
                        vec![l.to_string(), r.to_string()],
                        format!("{}_arr", out),
                        output.fingerprint(),
                        output.is_k_only(),
                    );
                });

                // Type inference
                self.record_transformation_output_type(
                    left.fingerprint(),
                    Some(right.fingerprint()),
                    output.fingerprint(),
                    flow,
                    stratum,
                )?;

                let (pos_weight_concat, neg_weight_concat) = self.weight_concat_tokens();

                // Output expression
                let (anti_param_k, anti_param_v) =
                    compute_kv_param_tokens(flow.key(), flow.value(), flow.compares(), &[], None);
                let remaining = RefCell::new(kv_use_counts(&[flow.key(), flow.value()]));
                let out_map_key =
                    self.build_key_val_from_kv_args(flow.key(), si, Some(&remaining))?;
                let out_map_value =
                    self.build_key_val_from_kv_args(flow.value(), si, Some(&remaining))?;
                let out_map_expr = if output.is_k_only() {
                    quote! { #out_map_key }
                } else {
                    quote! { ( #out_map_key, #out_map_value ) }
                };
                let inter_dedup = self.dedup_antijoin();
                let final_normalize = self.dedup_recursive();

                let transformation = quote! {
                    let #out =
                        #r.clone()
                            .flat_map_ref(|#anti_param_k, #anti_param_v | std::iter::once( ( #anti_param_k.clone(), #anti_param_v.clone() ) ))
                            #inter_dedup
                            #pos_weight_concat
                            .concat(
                                {
                                    #l.clone()
                                        .join_core(#r.clone(), |aj_k, _, aj_rv| {
                                            Some((aj_k.clone(), aj_rv.clone()))
                                        })
                                        #inter_dedup
                                        #neg_weight_concat
                                }
                            )
                            .flat_map(|( #anti_param_k, #anti_param_v )| std::iter::once( #out_map_expr ))
                            #final_normalize;
                };

                let arrange_stmt = self.register_arrangement(
                    arranged_map,
                    arr_alias.as_ref(),
                    output.fingerprint(),
                    &out,
                    transformation,
                    output.is_k_only(),
                );

                Ok(arrange_stmt)
            }
        }
    }
}

// =========================================================================
// Arrangement Management Utilities
// =========================================================================
impl CodeGen {
    /// Weight-conversion tokens for antijoin arithmetic (`pos` / `neg`).
    ///
    /// - `DatalogBatch` (`Present` diff): convert to `1i32` / `-1i32`.
    /// - `ExtendBatch` (`i32` diff, always `1`): `pos` is no-op, `neg` uses fixed `-1`.
    /// - Incremental (`i32` diff, variable): `pos` is no-op, `neg` negates actual diff (`-d`).
    pub(crate) fn weight_concat_tokens(&self) -> (TokenStream, TokenStream) {
        let pos = if self.config.is_datalog_batch() {
            // Convert Present diff → 1i32
            quote! {
                .inner
                .flat_map(move |(x, t, _)| std::iter::once((x, t.clone(), 1i32)))
                .as_collection()
            }
        } else {
            // i32 diff — no conversion needed
            quote! {}
        };
        let neg = if self.config.is_batch() {
            // Batch: fixed -1 weight (no retractions possible)
            quote! {
                .inner
                .flat_map(move |(x, t, _)| std::iter::once((x, t.clone(), -1i32)))
                .as_collection()
            }
        } else {
            // Incremental: negate the actual diff
            quote! {
                .inner
                .flat_map(move |(x, t, d)| std::iter::once((x, t.clone(), -d)))
                .as_collection()
            }
        };
        (pos, neg)
    }

    /// Emit a collection plus its key/self arrangement, honoring the planner's
    /// arrangement-sharing decision.
    ///
    /// `alias_canonical` is `Some(canonical_collection)` when the planner marked
    /// this output's arrangement a duplicate of an earlier one in this scope
    /// (see [`ProgramPlanner::mark_shared_arrangements`](crate::planner::ProgramPlanner)).
    /// On an alias, the projection/join is **not** rebuilt: this ident is bound
    /// to a cheap `clone()` of the canonical collection (so any direct,
    /// non-arranged consumer still resolves) and its fingerprint is pointed at
    /// the canonical's already-built sorted trace. Otherwise the collection is
    /// built and arranged as the canonical for its key.
    fn register_arrangement(
        &mut self,
        arranged_map: &mut HashMap<u64, Ident>,
        alias_canonical: Option<&Ident>,
        fingerprint: u64,
        collection_ident: &Ident,
        transformation: TokenStream,
        only_key: bool,
    ) -> TokenStream {
        if let Some(canonical) = alias_canonical {
            // Planner marked this a duplicate: reuse the canonical collection's
            // trace; bind this ident to a cheap clone of that collection.
            let canonical_arrangement = format_ident!("{}_arr", canonical);
            arranged_map.insert(fingerprint, canonical_arrangement);
            return quote! { let #collection_ident = #canonical.clone(); };
        }

        let arrangement_ident = format_ident!("{}_arr", collection_ident);
        arranged_map.insert(fingerprint, arrangement_ident.clone());

        let arrange = if only_key {
            quote! { let #arrangement_ident = #collection_ident.clone().arrange_by_self(); }
        } else {
            quote! { let #arrangement_ident = #collection_ident.clone().arrange_by_key(); }
        };
        quote! { #transformation #arrange }
    }
}

fn expect_arranged(
    arranged_map: &HashMap<u64, Ident>,
    fingerprint: u64,
    base_ident: &Ident,
) -> Result<Ident, CodegenError> {
    arranged_map.get(&fingerprint).cloned().ok_or_else(|| {
        CodegenError::internal(format!(
            "collection `{base_ident}` (fingerprint 0x{fingerprint:016x}) \
             must be arranged before use"
        ))
    })
}

/// Build the body of a `flat_map` closure that yields `out` either
/// conditionally (when `pred` is `Some`) or unconditionally.
///
/// The unconditional branch uses `std::iter::once` because `flat_map`
/// expects an iterator.
fn flat_map_body_tokens(pred: Option<TokenStream>, out: TokenStream) -> TokenStream {
    match pred {
        Some(pred) => quote! { if #pred { Some( #out ) } else { None } },
        None => quote! { std::iter::once( #out ) },
    }
}

/// Build the body of a `join_core` closure that yields `out` either
/// conditionally (when `pred` is `Some`) or unconditionally.
///
/// The unconditional branch returns a bare `Some(...)` because
/// `join_core` expects an `Option`, not an iterator.
fn join_body_tokens(pred: Option<TokenStream>, out: TokenStream) -> TokenStream {
    match pred {
        Some(pred) => quote! { if #pred { Some( #out ) } else { None } },
        None => quote! { Some( #out ) },
    }
}
