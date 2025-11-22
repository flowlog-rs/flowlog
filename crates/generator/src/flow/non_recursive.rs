use std::collections::{HashMap, HashSet};

use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use tracing::trace;

use crate::data_type::{find_type, insert_or_verify_type};
use crate::ident::find_ident;
use crate::transformation::gen_transformation;

use parser::DataType;
use planner::Transformation;

// =========================================================================
// Non-Recursive Flow Generation
// =========================================================================

/// Generate the non-recursive core differential dataflow pipelines, returning the assembled
/// statements along with the arrangement map populated while building them.
pub(crate) fn gen_non_recursive_core_flows(
    fp_to_ident: &HashMap<u64, Ident>,
    fp_to_type: &mut HashMap<u64, DataType>,
    transformations: &[Transformation],
) -> (Vec<TokenStream>, HashMap<u64, Ident>) {
    let mut flows = Vec::new();
    // Stratum-scoped cache of arrangements; emit arrange_by_key just before first use.
    let mut non_recursive_arranged_map: HashMap<u64, Ident> = HashMap::new();

    for transformation in transformations {
        flows.push(gen_transformation(
            fp_to_ident,
            fp_to_type,
            transformation,
            &mut non_recursive_arranged_map,
        ));
    }

    trace!("Generated static flows:\n{}\n", quote! { #(#flows)* });
    (flows, non_recursive_arranged_map)
}

/// Generate non recursive post-processing flows
pub(crate) fn gen_non_recursive_post_flows(
    fp_to_ident: &HashMap<u64, Ident>,
    fp_to_type: &mut HashMap<u64, DataType>,
    calculated_output_fps: &HashSet<u64>,
    output2idbs: &HashMap<u64, Vec<u64>>,
) -> Vec<TokenStream> {
    let mut flows = Vec::new();

    for (output_fp, idb_fps) in output2idbs {
        let output = find_ident(fp_to_ident, *output_fp);
        let mut outs: Vec<Ident> = Vec::new();

        for idb_fp in idb_fps {
            outs.push(format_ident!("t_{}", idb_fp));
        }

        insert_or_verify_type(fp_to_type, *output_fp, find_type(fp_to_type, idb_fps[0]));

        let head = outs[0].clone();
        let tail = &outs[1..];
        let mut expr: TokenStream = quote! { #head };
        for t in tail {
            expr = quote! { #expr.concat(&#t) };
        }

        // If this output was already computed in a previous stratum, union the previous
        // collection with the newly produced tuples before applying distinct.
        if calculated_output_fps.contains(output_fp) {
            flows.push(quote! {
                let #output = #output
                    .concat(&#expr)
                    .distinct();
            });
        } else {
            flows.push(quote! {
                let #output = #expr
                    .distinct();
            });
        }
    }

    trace!(
        "Generated post-processing flows:\n{}\n",
        quote! { #(#flows)* }
    );
    flows
}
