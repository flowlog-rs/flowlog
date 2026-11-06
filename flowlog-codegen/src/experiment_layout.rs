//! Applies a prevalidated experimental layout without changing operators.

use std::collections::HashMap;

use flowlog_planner::planner::Transformation;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;
use serde::Deserialize;

use crate::Codegen;

#[derive(Deserialize)]
pub(super) struct Step {
    inputs: Vec<Vec<usize>>,
    pub(super) output: Vec<usize>,
    swap: bool,
}

#[derive(Deserialize)]
pub(super) struct Layouts {
    pub(super) scopes: Vec<HashMap<String, Step>>,
    relations: HashMap<String, Vec<usize>>,
}

impl Layouts {
    pub(super) fn load() -> Option<Self> {
        std::env::var_os("FLOWLOG_EXPERIMENT_LAYOUT_PLAN").map(|path| {
            let text = std::fs::read_to_string(path).expect("read experimental layout");
            serde_json::from_str(&text).expect("parse experimental layout")
        })
    }
}

fn reordered(source: TokenStream, current: &[usize], desired: &[usize]) -> TokenStream {
    if current == desired {
        return source;
    }
    let fields: Vec<_> = (0..current.len())
        .map(|i| format_ident!("field_{i}"))
        .collect();
    let output: Vec<_> = desired
        .iter()
        .map(|column| {
            &fields[current
                .iter()
                .position(|c| c == column)
                .expect("field in layout")]
        })
        .collect();
    quote! { (#source).map(|(#(#fields,)*)| (#(#output,)*)) }
}

impl Codegen {
    pub(super) fn experiment_transformation(&self, tx: &Transformation) -> Transformation {
        match &self.experiment_layouts {
            None => tx.clone(),
            Some(layouts) => {
                let step = &layouts.scopes[self.experiment_current_scope]
                    [&format!("{:016x}", tx.output().fingerprint())];
                tx.experiment_permute(&step.inputs, &step.output, step.swap)
            }
        }
    }

    pub(super) fn experiment_edb(
        &self,
        relation: u64,
        source: &Ident,
        arity: usize,
    ) -> Option<TokenStream> {
        let layouts = self.experiment_layouts.as_ref()?;
        let logical: Vec<_> = (0..arity).collect();
        let physical = &layouts.relations[&format!("{relation:016x}")];
        let input = reordered(quote! { #source }, &logical, physical);
        let dedup = quote! { ::flowlog_runtime::operators::flowlog_input_dedup(#input) };
        let restored = reordered(dedup, physical, &logical);
        Some(quote! { let #source = #restored; })
    }

    pub(super) fn experiment_before_union(
        &self,
        relation: u64,
        source: &Ident,
        lifted: TokenStream,
    ) -> TokenStream {
        let Some(layouts) = &self.experiment_layouts else {
            return lifted;
        };
        let physical = &layouts.relations[&format!("{relation:016x}")];
        let logical: Vec<_> = (0..physical.len()).collect();
        let current = self
            .experiment_binding_orders
            .get(&source.to_string())
            .unwrap_or(&logical);
        reordered(lifted, current, physical)
    }

    pub(super) fn experiment_after_union(&self, relation: u64, dedup: TokenStream) -> TokenStream {
        let Some(layouts) = &self.experiment_layouts else {
            return dedup;
        };
        let physical = &layouts.relations[&format!("{relation:016x}")];
        let logical: Vec<_> = (0..physical.len()).collect();
        reordered(dedup, physical, &logical)
    }
}
