//! Plan tree (left-to-right chain) over core atoms for Macaron Datalog programs.
use catalog::rule::Catalog;
use std::collections::HashMap;
use std::fmt;
use tracing::trace;

#[derive(Debug, Clone)]
pub struct PlanTree {
    root: usize,
    tree: HashMap<usize, Vec<usize>>, // parent -> child; leaf -> []
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
    /// The catalog is assumed to have already been processed by GYO optimization,
    /// so all positive atoms in the catalog are core atoms. This function builds
    /// a left-deep join tree from left to right as the atoms appear in the rule,
    /// without any reordering or optimization.
    /// Future versions may implement cost-based or heuristic join reordering.
    pub fn from_catalog(catalog: &Catalog) -> Self {
        let core_atoms: Vec<usize> = (0..catalog.core_atom_number()).collect();

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

        let plan_tree = Self { root, tree };
        trace!("Plan tree:\n{}", plan_tree);

        // Return the constructed plan tree.
        plan_tree
    }

    pub fn get_first_join_tuple_index(&self) -> (usize, usize) {
        (self.root, self.children(self.root)[0])
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
