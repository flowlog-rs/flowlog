use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct PlanTreeNodeProfile {
    fingerprints: String,
    parents: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(super) struct RuleProfile {
    text: String,
    plan_tree: Vec<PlanTreeNodeProfile>,
}

impl RuleProfile {
    pub(super) fn new(text: String, plan_tree_info: Vec<((u64, Option<u64>), u64)>) -> Self {
        let plan_tree = Self::render_plan_tree(&plan_tree_info);
        Self { text, plan_tree }
    }

    fn render_plan_tree(plan_tree_info: &[((u64, Option<u64>), u64)]) -> Vec<PlanTreeNodeProfile> {
        let fp_hex = |fp: u64| format!("0x{:016x}", fp);
        let outputs: HashSet<_> = plan_tree_info.iter().map(|(_, fp)| *fp).collect();

        plan_tree_info
            .iter()
            .map(|((fp1, fp2), output_fp)| {
                let parents = std::iter::once(*fp1)
                    .chain(fp2.iter().copied())
                    .filter(|fp| outputs.contains(fp))
                    .map(fp_hex)
                    .collect();
                PlanTreeNodeProfile {
                    fingerprints: fp_hex(*output_fp),
                    parents,
                }
            })
            .collect()
    }
}
