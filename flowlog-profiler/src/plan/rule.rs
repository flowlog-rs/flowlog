//! A rule's source text paired with the graph of transformations
//! its plan compiles to.
//!
//! Each transformation is a [`TreeNode`] keyed by the fingerprint of the
//! collection it produces; that same fingerprint tags the operator-level
//! [`crate::plan::node::Node`] the transformation became, which is how a rule's
//! structure and its measured metrics are joined. Despite the `plan_tree`
//! name the shape is a DAG, since a binary join has two parents.

use std::collections::HashSet;
use std::iter;

use serde::Deserialize;
use serde::Serialize;

use crate::plan::graph::format_fingerprint;

/// The planner's raw records, one per transformation:
/// `((left input fingerprint, optional right input fingerprint), output
/// fingerprint)`.
type PlanTreeInfo = [((u64, Option<u64>), u64)];

/// One transformation of a rule's plan, identified by the fingerprint of
/// the collection it produces.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TreeNode {
    /// Fingerprint of this transformation's output; matches the
    /// [`crate::plan::node::Node`] carrying the same value.
    pub fingerprint: String,
    /// Fingerprints of the transformations feeding this one. Only inputs
    /// produced within this rule appear; leaf EDB inputs are omitted.
    pub parents: Vec<String>,
}

/// A rule: its source text and the transformation DAG its plan compiles to.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Rule {
    /// The rule as written.
    pub text: String,
    /// One [`TreeNode`] per transformation, in plan order.
    pub plan_tree: Vec<TreeNode>,
}

impl Rule {
    pub(crate) fn new(text: String, plan_tree_info: Vec<((u64, Option<u64>), u64)>) -> Self {
        let plan_tree = Self::render_plan_tree(&plan_tree_info);
        Self { text, plan_tree }
    }

    /// Turns the planner's raw records into the stored DAG, keeping on each
    /// transformation only the parent fingerprints produced within this
    /// rule, so `parents` never dangles on a leaf EDB input.
    fn render_plan_tree(plan_tree_info: &PlanTreeInfo) -> Vec<TreeNode> {
        let outputs: HashSet<u64> = plan_tree_info.iter().map(|(_, fp)| *fp).collect();

        plan_tree_info
            .iter()
            .map(|((fp1, fp2), output_fp)| {
                let parents = iter::once(*fp1)
                    .chain(fp2.iter().copied())
                    .filter(|fp| outputs.contains(fp))
                    .map(format_fingerprint)
                    .collect();
                TreeNode {
                    fingerprint: format_fingerprint(*output_fp),
                    parents,
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Leaf inputs (EDB fingerprints) are not tree outputs, so they must
    /// not appear as dangling parent references.
    #[test]
    fn parents_link_only_fingerprints_that_are_outputs() {
        let rule = Rule::new("r".into(), vec![((1, None), 2), ((2, Some(99)), 3)]);
        assert!(rule.plan_tree[0].parents.is_empty());
        assert_eq!(rule.plan_tree[1].parents, vec![format_fingerprint(2)]);
    }

    #[test]
    fn binary_transformation_records_both_parents() {
        let rule = Rule::new(
            "r".into(),
            vec![((1, None), 2), ((5, None), 6), ((2, Some(6)), 7)],
        );
        assert_eq!(
            rule.plan_tree[2].parents,
            vec![format_fingerprint(2), format_fingerprint(6)]
        );
    }
}
