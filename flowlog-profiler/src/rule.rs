//! Rule profiling data and plan tree rendering helpers.

use std::collections::HashSet;
use std::iter;

use serde::Deserialize;
use serde::Serialize;

type PlanTreeInfo = [((u64, Option<u64>), u64)];

/// A node entry in the rendered plan tree for a rule.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PlanTreeNodeProfile {
    /// The output fingerprint for this plan node.
    pub fingerprint: String,
    /// Parent output fingerprints (only if they appear in the plan tree outputs).
    pub parents: Vec<String>,
}

/// A rule profile including the original rule text and a rendered plan tree.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RuleProfile {
    pub text: String,
    pub plan_tree: Vec<PlanTreeNodeProfile>,
}

impl RuleProfile {
    pub(crate) fn new(text: String, plan_tree_info: Vec<((u64, Option<u64>), u64)>) -> Self {
        let plan_tree = Self::render_plan_tree(&plan_tree_info);
        Self { text, plan_tree }
    }

    fn render_plan_tree(plan_tree_info: &PlanTreeInfo) -> Vec<PlanTreeNodeProfile> {
        let outputs: HashSet<u64> = plan_tree_info.iter().map(|(_, fp)| *fp).collect();

        plan_tree_info
            .iter()
            .map(|((fp1, fp2), output_fp)| {
                let parents = iter::once(*fp1)
                    .chain(fp2.iter().copied())
                    .filter(|fp| outputs.contains(fp))
                    .map(Self::format_fingerprint)
                    .collect();
                PlanTreeNodeProfile {
                    fingerprint: Self::format_fingerprint(*output_fp),
                    parents,
                }
            })
            .collect()
    }

    #[inline]
    fn format_fingerprint(fp: u64) -> String {
        format!("0x{:016x}", fp)
    }
}
