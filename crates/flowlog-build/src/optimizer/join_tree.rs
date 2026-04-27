//! Yannakakis+ join tree over the positive atoms of an α-acyclic rule.
//!
//! Public API: [`JoinTree::from_catalog`] returns a single rooted join
//! tree consumed downstream by the planner.
//!
//! # Algorithm
//!
//! 1. **Enumerate** every labeled tree on `n = positive_atom_number()` via
//!    Prüfer sequences (`n^(n-2)` candidates).
//! 2. **Filter** for join-tree validity:
//!    - every tree edge connects atoms with ≥1 shared variable, and
//!    - the running intersection property holds (for each variable, the
//!      atoms containing it form a connected subtree).
//! 3. **Score** each surviving topology against every candidate root by
//!    `(width, depth)`:
//!    - **width** = VLDB26 recursion: at each internal node, split children
//!      into a *planning* tail (the last child) and a *leftover* prefix
//!      (parent + earlier children); the local width is the count of
//!      variables that have to escape across that cut. Computed under a
//!      canonical child ordering (sorted by atom index) — children are not
//!      permuted.
//!    - **depth** = standard max distance from root to leaf, used as a
//!      lexicographic tiebreaker after width.
//! 4. Return the `argmin (width, depth)`.
//!
//! # Complexity
//!
//! Search size is `n^(n-2) × n` (trees × candidate roots), pruned hard by
//! the validity filter. Practical for `n ≲ 8` — typical FlowLog rule
//! sizes. Beyond that, the Prüfer enumeration becomes expensive and a
//! heuristic should replace it.
//!
//! # Future work
//!
//! Replace Prüfer with the output-linear join-tree enumeration of
//! Zheng et al. (`https://starai.cs.ucla.edu/papers/ZhengICDT26.pdf`),
//! ideally combined with a cardinality-estimation cost model.
//!
//! # Assumption
//!
//! The catalog is α-acyclic at this point — either the rule was acyclic
//! to begin with, or its cyclic core has already been broken by Phase 3.
//! Violating that assumption surfaces as a [`PlanError::Internal`] ICE
//! ("no valid join tree exists for a cyclic hypergraph"), not a panic —
//! every invariant violation is reported as an error so the compiler
//! never aborts mid-stratum.

use crate::catalog::Catalog;
use crate::planner::PlanError;
use std::collections::{HashMap, HashSet};
use std::fmt;
use tracing::debug;

/// Undirected edge list of a labeled tree.
type Edges = Vec<(usize, usize)>;
/// Parent → children adjacency for a rooted tree. Every atom appears as a
/// key; leaves map to an empty vector.
type RootedTree = HashMap<usize, Vec<usize>>;

// ===========================================================================
// Public API
// ===========================================================================

#[derive(Debug, Clone)]
pub struct JoinTree {
    root: usize,
    tree: RootedTree,
}

impl JoinTree {
    /// Build the optimal join tree for `catalog` by enumerating every
    /// valid join tree and picking the `(width, depth)`-minimum.
    ///
    /// # Errors
    ///
    /// Returns [`PlanError::Internal`] if any invariant is violated:
    /// - empty catalog (`positive_atom_number() == 0`);
    /// - no valid join tree exists (catalog still cyclic, or the
    ///   positive-atom hypergraph is disconnected).
    pub fn from_catalog(catalog: &Catalog) -> Result<Self, PlanError> {
        let n = catalog.positive_atom_number();
        if n == 0 {
            return Err(PlanError::internal(
                "JoinTree::from_catalog called on an empty catalog",
            ));
        }

        let head_vars: HashSet<String> = catalog.head_arguments_strs().into_iter().collect();
        let atom_vars: Vec<HashSet<String>> = (0..n)
            .map(|i| catalog.positive_atom_argument_vars_str_set(i).clone())
            .collect();

        let result = if n == 1 {
            singleton(0)
        } else {
            search_min_width_depth(n, &atom_vars, &head_vars).map_err(|_| {
                PlanError::internal(format!(
                    "no valid join tree exists for rule:\n  {:?}\n  (catalog still cyclic, or the positive-atom hypergraph is disconnected)",
                    catalog.rule()
                ))
            })?
        };

        debug!("\nRule:\n  {:?}\nJoin tree:\n{}", catalog.rule(), result);
        Ok(result)
    }

    pub fn root(&self) -> usize {
        self.root
    }

    pub fn children(&self, atom_id: usize) -> &[usize] {
        self.tree.get(&atom_id).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Flatten the rooted tree into an undirected `node → neighbors` adjacency
    /// list (length = number of atoms). Every parent-child edge becomes two
    /// entries — one in each direction.
    pub fn undirected_adjacency(&self) -> Vec<Vec<usize>> {
        let n = self.tree.len();
        let mut adj = vec![Vec::new(); n];
        let mut stack = vec![self.root];
        while let Some(parent) = stack.pop() {
            for &child in self.children(parent) {
                adj[parent].push(child);
                adj[child].push(parent);
                stack.push(child);
            }
        }
        adj
    }
}

fn singleton(root: usize) -> JoinTree {
    let mut tree = RootedTree::new();
    tree.insert(root, vec![]);
    JoinTree { root, tree }
}

// ===========================================================================
// Search
// ===========================================================================

/// Enumerate every valid join tree of the hypergraph and return the one
/// minimizing `(width, depth)` lexicographically. Width strictly dominates;
/// depth only breaks ties.
fn search_min_width_depth(
    n: usize,
    atom_vars: &[HashSet<String>],
    head_vars: &HashSet<String>,
) -> Result<JoinTree, PlanError> {
    let mut best: Option<((usize, usize), JoinTree)> = None;

    for edges in enumerate_labeled_trees(n)? {
        // Build adjacency once per candidate topology — both validity check
        // and the per-root rooting need it; rebuilding inside each was
        // doing O(n) work O(n) times per topology.
        let adj = adjacency(&edges, n);
        if !satisfies_running_intersection(&adj, atom_vars, n) {
            continue;
        }
        for root in 0..n {
            let tree = root_at_with_adj(&adj, root, n);
            let depth = tree_depth(&tree, root);
            let width = tree_width(&tree, root, atom_vars, head_vars);
            let key = (width, depth);
            if best.as_ref().is_none_or(|(b, _)| key < *b) {
                best = Some((key, JoinTree { root, tree }));
            }
        }
    }

    best.map(|(_, tree)| tree).ok_or_else(|| {
        PlanError::internal(
            "no valid join tree exists for this catalog (cyclic or disconnected hypergraph)",
        )
    })
}

// ===========================================================================
// Tree enumeration — Prüfer sequences
// ===========================================================================

/// Enumerate every labeled tree on `n` nodes (`n ≥ 2`) as an undirected
/// edge list. Each tree corresponds to a unique Prüfer sequence in
/// `[0, n)^(n-2)`.
///
/// Boxed because the `n == 2` case yields a different concrete iterator
/// shape than the general case. Returns an error rather than panicking
/// when called with `n < 2`.
fn enumerate_labeled_trees(n: usize) -> Result<Box<dyn Iterator<Item = Edges>>, PlanError> {
    if n < 2 {
        return Err(PlanError::internal(format!(
            "enumerate_labeled_trees called with n={n}, expected n ≥ 2"
        )));
    }
    if n == 2 {
        return Ok(Box::new(std::iter::once(vec![(0, 1)])));
    }
    let len = n - 2;
    let total = (n as u64).pow(len as u32);
    Ok(Box::new((0..total).map(move |idx| {
        let mut seq = Vec::with_capacity(len);
        let mut x = idx;
        for _ in 0..len {
            seq.push((x % n as u64) as usize);
            x /= n as u64;
        }
        // Prüfer decode is invariant-by-construction here (seq drawn from
        // [0, n)^(n-2) with n≥3); decode panics would represent ICEs but
        // are unreachable on valid inputs. We absorb any decode failure
        // into an empty edge list, which validity filtering then drops.
        prufer_decode(&seq, n).unwrap_or_default()
    })))
}

/// Standard O(n²) Prüfer decode. Returns `None` if the input violates
/// Prüfer invariants (which shouldn't happen for sequences generated by
/// [`enumerate_labeled_trees`]).
fn prufer_decode(seq: &[usize], n: usize) -> Option<Edges> {
    let mut degree = vec![1usize; n];
    for &x in seq {
        degree[x] += 1;
    }
    let mut edges = Vec::with_capacity(n - 1);
    for &v in seq {
        let leaf = (0..n).find(|&j| degree[j] == 1)?;
        edges.push((leaf.min(v), leaf.max(v)));
        degree[leaf] -= 1;
        degree[v] -= 1;
    }
    let remaining: Vec<usize> = (0..n).filter(|&i| degree[i] == 1).collect();
    if remaining.len() != 2 {
        return None;
    }
    edges.push((remaining[0], remaining[1]));
    Some(edges)
}

// ===========================================================================
// Validity
// ===========================================================================

/// `true` iff the tree (given as adjacency over `n` atoms) is an admissible
/// join plan: for every variable, the atoms carrying it induce a connected
/// subtree (running intersection property).
///
/// RIP alone is the right check. Cartesian edges within a connected
/// component, and excess inter-component bridges, are both rejected by
/// RIP itself — no separate up-front guard needed. The remaining
/// permissive case is **genuinely disconnected hypergraphs** (e.g.
/// `Out(s, t) :- A(s), B(t).` from a `var = const` head pin): no strict
/// join tree exists, but the bridge-spanning trees we admit are correct
/// plans (`core()` handles empty-key joins as Cartesian products), and
/// width scoring penalizes such bridges so Yannakakis-style topologies
/// win whenever they exist.
fn satisfies_running_intersection(
    adj: &[Vec<usize>],
    atom_vars: &[HashSet<String>],
    n: usize,
) -> bool {
    let all_vars: HashSet<&String> = atom_vars.iter().flatten().collect();
    all_vars
        .into_iter()
        .all(|v| is_var_subtree_connected(v, atom_vars, adj, n))
}

fn is_var_subtree_connected(
    var: &String,
    atom_vars: &[HashSet<String>],
    adj: &[Vec<usize>],
    n: usize,
) -> bool {
    let containing: Vec<usize> = (0..n).filter(|&i| atom_vars[i].contains(var)).collect();
    if containing.len() <= 1 {
        return true;
    }
    let containing_set: HashSet<usize> = containing.iter().copied().collect();
    let mut visited = HashSet::new();
    let mut stack = vec![containing[0]];
    while let Some(node) = stack.pop() {
        if !visited.insert(node) {
            continue;
        }
        for &neighbor in &adj[node] {
            if containing_set.contains(&neighbor) && !visited.contains(&neighbor) {
                stack.push(neighbor);
            }
        }
    }
    visited.len() == containing.len()
}

fn adjacency(edges: &[(usize, usize)], n: usize) -> Vec<Vec<usize>> {
    let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n];
    for &(a, b) in edges {
        adj[a].push(b);
        adj[b].push(a);
    }
    adj
}

// ===========================================================================
// Rooting + structural metrics
// ===========================================================================

/// Root an undirected tree at `root` using a precomputed adjacency. Children
/// at each node are sorted by atom index so the width function operates under
/// a canonical, deterministic child order.
fn root_at_with_adj(adj: &[Vec<usize>], root: usize, n: usize) -> RootedTree {
    let mut tree: RootedTree = (0..n).map(|i| (i, Vec::new())).collect();
    let mut visited = vec![false; n];
    visited[root] = true;
    let mut stack = vec![root];
    while let Some(parent) = stack.pop() {
        let mut children: Vec<usize> = adj[parent]
            .iter()
            .copied()
            .filter(|&c| !visited[c])
            .collect();
        children.sort_unstable();
        for &c in &children {
            visited[c] = true;
            stack.push(c);
        }
        tree.get_mut(&parent).unwrap().extend(children);
    }
    tree
}

fn tree_depth(tree: &RootedTree, root: usize) -> usize {
    fn dfs(tree: &RootedTree, node: usize) -> usize {
        let children = tree.get(&node).map(Vec::as_slice).unwrap_or(&[]);
        if children.is_empty() {
            return 0;
        }
        1 + children.iter().map(|&c| dfs(tree, c)).max().unwrap()
    }
    dfs(tree, root)
}

// ===========================================================================
// Width — VLDB26 recursive formula under canonical child ordering
// ===========================================================================

fn tree_width(
    tree: &RootedTree,
    root: usize,
    atom_vars: &[HashSet<String>],
    head_vars: &HashSet<String>,
) -> usize {
    let subtree_atoms = collect_subtree_atoms(tree, root);
    let root_children = tree.get(&root).map(Vec::as_slice).unwrap_or(&[]);
    width_recurse(
        root,
        root_children,
        tree,
        &subtree_atoms,
        atom_vars,
        head_vars,
    )
}

/// For each node in the rooted tree, the set of atoms in its subtree
/// (inclusive of itself), in DFS order. Computed once per `tree_width`
/// invocation and reused by the recursion.
fn collect_subtree_atoms(tree: &RootedTree, root: usize) -> HashMap<usize, Vec<usize>> {
    fn dfs(node: usize, tree: &RootedTree, cache: &mut HashMap<usize, Vec<usize>>) {
        let mut atoms = vec![node];
        if let Some(children) = tree.get(&node) {
            for &c in children {
                dfs(c, tree, cache);
                atoms.extend(cache.get(&c).unwrap().iter().copied());
            }
        }
        cache.insert(node, atoms);
    }
    let mut cache = HashMap::new();
    dfs(root, tree, &mut cache);
    cache
}

/// Recursive width: at each internal node, split children into a *planning*
/// tail (last child) and a *leftover* prefix (parent + earlier children).
/// The local width contribution is the count of variables that have to
/// escape across that cut, recursing into both sides.
///
/// `active_children` is `parent`'s children slice for *this* recursion frame
/// — passing it explicitly (instead of mutating the tree) lets the leftover
/// recursion drop the planning child by simply slicing one element off the
/// tail, avoiding a full `RootedTree` clone per frame.
fn width_recurse(
    parent: usize,
    active_children: &[usize],
    tree: &RootedTree,
    subtree_atoms: &HashMap<usize, Vec<usize>>,
    atom_vars: &[HashSet<String>],
    head_vars: &HashSet<String>,
) -> usize {
    if active_children.is_empty() {
        return 0;
    }

    let planning_child = *active_children.last().unwrap();
    let leftover_children = &active_children[..active_children.len() - 1];

    let planning_atoms = subtree_atoms.get(&planning_child).unwrap();
    let leftover_atoms: Vec<usize> = std::iter::once(parent)
        .chain(
            leftover_children
                .iter()
                .flat_map(|c| subtree_atoms.get(c).unwrap().iter().copied()),
        )
        .collect();

    let planning_vars = vars_of(planning_atoms, atom_vars);
    let leftover_vars = vars_of(&leftover_atoms, atom_vars);

    let planning_head = escape_set(&planning_vars, &leftover_vars, head_vars);
    let leftover_head = escape_set(&leftover_vars, &planning_vars, head_vars);

    let planning_width = width_recurse(
        planning_child,
        tree.get(&planning_child).map(Vec::as_slice).unwrap_or(&[]),
        tree,
        subtree_atoms,
        atom_vars,
        &planning_head,
    );
    let leftover_width = width_recurse(
        parent,
        leftover_children,
        tree,
        subtree_atoms,
        atom_vars,
        &leftover_head,
    );
    let intermediate = planning_head.union(&leftover_head).count();

    intermediate.max(planning_width).max(leftover_width)
}

/// Union of `atom_vars[a]` over all atoms `a` in `atoms`.
fn vars_of(atoms: &[usize], atom_vars: &[HashSet<String>]) -> HashSet<String> {
    atoms
        .iter()
        .flat_map(|&a| atom_vars[a].iter().cloned())
        .collect()
}

/// Variables in `here` that have to be carried across a cut — i.e. that
/// appear on the `other` side of the cut or in the rule's `head`.
fn escape_set(
    here: &HashSet<String>,
    other: &HashSet<String>,
    head: &HashSet<String>,
) -> HashSet<String> {
    here.iter()
        .filter(|v| other.contains(*v) || head.contains(*v))
        .cloned()
        .collect()
}

// ===========================================================================
// Display
// ===========================================================================

impl fmt::Display for JoinTree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn print_subtree(
            f: &mut fmt::Formatter<'_>,
            tree: &RootedTree,
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
                    print_subtree(f, tree, *child, &new_prefix, i == len - 1)?;
                }
            }
            Ok(())
        }
        print_subtree(f, &self.tree, self.root, "", true)
    }
}
