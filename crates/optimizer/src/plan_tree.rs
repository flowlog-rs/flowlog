//! Plan tree (left-to-right chain) over core atoms for Macaron Datalog programs.
use catalog::rule::Catalog;
use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Clone)]
pub struct PlanTree {
    root: usize,
    tree: HashMap<usize, Vec<usize>>, // parent -> child; leaf -> []
    sub_trees: HashMap<usize, Vec<usize>>, // cached preorder traversal per subtree root
}

impl PlanTree {
    /// Returns the root node index of the plan tree.
    pub fn root(&self) -> usize {
        self.root
    }

    /// Returns the parent -> children mapping.
    pub fn tree(&self) -> &HashMap<usize, Vec<usize>> {
        &self.tree
    }

    /// Returns cached preorder traversals per subtree root.
    pub fn sub_trees(&self) -> &HashMap<usize, Vec<usize>> {
        &self.sub_trees
    }

    /// Returns true if the node has no children.
    pub fn is_leaf(&self, x: usize) -> bool {
        self.tree
            .get(&x)
            .unwrap_or_else(|| {
                panic!(
                    "Optimizer error: node {} not found in plan tree (is_leaf)",
                    x
                )
            })
            .is_empty()
    }

    /// Returns the children list for a node.
    pub fn children(&self, x: usize) -> &Vec<usize> {
        self.tree.get(&x).unwrap_or_else(|| {
            panic!(
                "Optimizer error: node {} not found in plan tree (children)",
                x
            )
        })
    }

    /// Build a plan tree from a Catalog.
    ///
    /// For now, this function simply keeps the join order from left to right
    /// as the atoms appear in the rule, without any reordering or optimization.
    /// Future versions may implement cost-based or heuristic join reordering.
    pub fn from_catalog(catalog: &Catalog) -> Self {
        // Collect indices of core atoms in the rule.
        let core_atoms: Vec<usize> = catalog
            .is_core_atom_bitmap()
            .iter()
            .enumerate()
            .filter_map(|(i, &is_core)| if is_core { Some(i) } else { None })
            .collect();

        // Panic if there are no core atoms (should not happen for valid rules).
        if core_atoms.is_empty() {
            panic!(
                "Optimizer error: No core atoms for the rule {}",
                catalog.rule()
            );
        }

        // The root of the plan tree is the last core atom (rightmost in join order).
        let root = *core_atoms.last().unwrap();
        let mut tree: HashMap<usize, Vec<usize>> = HashMap::new();

        // Build a left-deep tree: each parent points to its left neighbor as child.
        for pair in core_atoms.windows(2).rev() {
            let parent = pair[1];
            let child = pair[0];
            tree.insert(parent, vec![child]);
        }

        // The leftmost atom has no children.
        if let Some(&first) = core_atoms.first() {
            tree.insert(first, vec![]);
        }

        // Precompute preorder traversals for each subtree root.
        let mut sub_trees: HashMap<usize, Vec<usize>> = HashMap::new();
        for &n in tree.keys() {
            Self::populate_subtree(n, &tree, &mut sub_trees);
        }

        // Return the constructed plan tree.
        Self {
            root,
            tree,
            sub_trees,
        }
    }

    /// Populate preorder cache for subtree rooted at node.
    fn populate_subtree(
        node: usize,
        tree: &HashMap<usize, Vec<usize>>,
        cache: &mut HashMap<usize, Vec<usize>>,
    ) -> Vec<usize> {
        // If we've already computed the subtree for this node, return it from cache.
        if let Some(existing) = cache.get(&node) {
            return existing.clone();
        }

        // Start the preorder traversal with the current node.
        let mut acc = vec![node];

        // Recursively add all children and their subtrees.
        if let Some(children) = tree.get(&node) {
            for &c in children {
                acc.extend(Self::populate_subtree(c, tree, cache));
            }
        }

        // Cache the result for future lookups.
        cache.insert(node, acc.clone());
        acc
    }
}

impl fmt::Display for PlanTree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn print_tree(
            f: &mut fmt::Formatter<'_>,
            tree: &HashMap<usize, Vec<usize>>,
            current: usize,
            prefix: &str,
            last: bool,
        ) -> fmt::Result {
            writeln!(
                f,
                "{}{}{}",
                prefix,
                if last { "└── " } else { "├── " },
                current
            )?;
            let new_prefix = format!("{}{}", prefix, if last { "    " } else { "│   " });
            if let Some(children) = tree.get(&current) {
                let len = children.len();
                for (i, child) in children.iter().enumerate() {
                    print_tree(f, tree, *child, &new_prefix, i == len - 1)?;
                }
            }
            Ok(())
        }

        print_tree(f, &self.tree, self.root, "", true)
    }
}
