//! Demand pushdown: restrict each derived relation to the rows the
//! program can read of it.
//!
//! A body atom naming a constant, `Path(1, y)`, reads only the rows of
//! `Path` whose first column is `1`. [`push_demand`] computes, for every
//! relation column, the values the program can read there (the column's
//! *demand*), closes it under the rules (a rule asked for rows with
//! `x = 1` asks its body for `x = 1` too), and filters each rule to its
//! head relation's demand. An output is read everywhere, so it keeps every
//! row; every other relation keeps exactly the rows inside its demand plus
//! its input and inline facts, which is all the program reads of it, so no
//! output changes.
//!
//! The filter is a comparison added to a rule body, so it runs where the
//! rule runs: a base rule of a recursive stratum is filtered before its
//! rows enter the loop, and a recursive rule that copies a demanded column
//! from a recursive atom needs no filter at all. A derived row outside its
//! relation's demand is never produced, so the filtered program derives no
//! row the original does not.
//!
//! `docs/design/demand.md` states the closure condition, the proof that it
//! preserves the outputs, and what the pass leaves alone (programs that
//! are not stratified, lattice components, columns of uncomparable types).

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;

use flowlog_common::Span;
use tracing::debug;

use crate::Arithmetic;
use crate::Atom;
use crate::AtomArg;
use crate::ComparisonExpr;
use crate::ComparisonOperator;
use crate::Constant;
use crate::DataType;
use crate::Factor;
use crate::FlowLogRule;
use crate::HeadArg;
use crate::Predicate;
use crate::Variable;
use crate::program::Program;

/// Most constants one column's demand lists before it widens to every
/// value. A filtered variable splits its rule into one copy per value, so
/// the cap bounds the copies; picked by hand, nothing measured.
const MAX_VALUES: usize = 4;

/// Most copies one rule is split into, over all its filtered variables.
/// Past it the widest filters are given up; picked by hand, nothing
/// measured.
const MAX_COPIES: usize = 16;

/// Filters every rule to the rows of its head relation the program can
/// read and drops the rules that can derive none of them. Every `.output`
/// and `.printsize` relation keeps exactly its rows. Runs on a validated
/// program; one that is not stratified is left as written.
pub(crate) fn push_demand(program: &mut Program) {
    if !is_stratified(program) {
        debug!("Demand pushdown: skipped, a recursion runs through negation or an aggregate.");
        return;
    }
    let (_, actions) = plan(program);
    let before = program.rules.len();
    let rules = std::mem::take(&mut program.rules);
    program.rules = rules
        .into_iter()
        .zip(actions)
        .flat_map(|(rule, action)| match action {
            Action::Keep => vec![rule],
            Action::Drop => vec![],
            Action::Filter(filters) => copies(&rule, &filters),
        })
        .collect();
    debug!(
        "Demand pushdown: {} rules in, {} rules out.",
        before,
        program.rules.len()
    );
}

// =============================================================================
// Demand
// =============================================================================

/// The values a column can be read at.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Demand {
    /// Every value.
    Any,
    /// Only these constants; none at all when the column is never read.
    Values(BTreeSet<Constant>),
}

/// Demand of a column the program does not declare.
static ANY: Demand = Demand::Any;

impl Demand {
    /// The demand of a column nothing reads.
    fn none() -> Self {
        Self::Values(BTreeSet::new())
    }

    /// Returns `true` if no value is demanded.
    fn is_empty(&self) -> bool {
        match self {
            Self::Any => false,
            Self::Values(values) => values.is_empty(),
        }
    }

    /// The demanded constants, or `None` for every value.
    fn values(&self) -> Option<&BTreeSet<Constant>> {
        match self {
            Self::Any => None,
            Self::Values(values) => Some(values),
        }
    }

    /// Grows `self` to admit `other` too, widening to [`Demand::Any`] past
    /// [`MAX_VALUES`]. Returns `true` if `self` changed.
    fn absorb(&mut self, other: &Demand) -> bool {
        let Self::Values(values) = self else {
            return false;
        };
        let Self::Values(more) = other else {
            *self = Self::Any;
            return true;
        };
        let before = values.len();
        values.extend(more.iter().cloned());
        if values.len() > MAX_VALUES {
            *self = Self::Any;
            return true;
        }
        values.len() != before
    }

    /// The values both `self` and `other` admit.
    fn meet(&self, other: &Demand) -> Demand {
        match (self, other) {
            (Self::Any, demand) | (demand, Self::Any) => demand.clone(),
            (Self::Values(left), Self::Values(right)) => {
                Self::Values(left.intersection(right).cloned().collect())
            }
        }
    }

    /// Whether a head constant can land in this column. A constant whose
    /// type does not compare exactly is assumed to; [`plan_rule`] then
    /// widens the column.
    fn admits(&self, constant: &Constant, ty: &DataType) -> bool {
        match self {
            Self::Any => true,
            Self::Values(values) if values.is_empty() => false,
            Self::Values(values) => !is_exact_in(constant, ty) || values.contains(constant),
        }
    }
}

/// Returns `true` if two values of `ty` are equal exactly when their
/// constants are: type-check gives an integer its canonical decimal
/// spelling and a string its decoded content, and a boolean has two
/// spellings. A float keeps the spelling it was written with (`1.0` and
/// `1.00`), and a tuple column holds no constant, so neither is demanded by
/// value.
fn is_exact(ty: &DataType) -> bool {
    match ty {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::String
        | DataType::Bool => true,
        DataType::IntLit
        | DataType::FloatLit
        | DataType::Float32
        | DataType::Float64
        | DataType::FixedTuple(_) => false,
    }
}

/// Returns `true` if `constant` is a value of the exactly compared column
/// type `ty`.
fn is_exact_in(constant: &Constant, ty: &DataType) -> bool {
    is_exact(ty) && constant.ty() == ty
}

/// What reading a column of type `ty` at the values `needed` demands of
/// it: those values when the column compares them exactly, else every
/// value.
fn read_at(needed: &Demand, ty: &DataType) -> Demand {
    match needed {
        Demand::Values(values) if values.iter().all(|value| is_exact_in(value, ty)) => {
            needed.clone()
        }
        Demand::Values(_) | Demand::Any => Demand::Any,
    }
}

/// Every relation column's demand, by canonical relation name.
#[derive(Debug, Clone)]
struct Demands(HashMap<String, Vec<Demand>>);

impl Demands {
    /// The demand of `relation`'s `column`; [`Demand::Any`] for one the
    /// program does not declare.
    fn get(&self, relation: &str, column: usize) -> &Demand {
        self.0
            .get(relation)
            .and_then(|columns| columns.get(column))
            .unwrap_or(&ANY)
    }

    /// Grows a column's demand to admit `read`; returns `true` if it grew.
    fn absorb(&mut self, relation: &str, column: usize, read: &Demand) -> bool {
        self.0
            .get_mut(relation)
            .and_then(|columns| columns.get_mut(column))
            .is_some_and(|demand| demand.absorb(read))
    }
}

/// Every relation's column types, by canonical name.
type Types = HashMap<String, Vec<DataType>>;

/// The type of `relation`'s `column`, or `None` if the program does not
/// declare it.
fn column_type<'a>(types: &'a Types, relation: &str, column: usize) -> Option<&'a DataType> {
    types.get(relation).and_then(|columns| columns.get(column))
}

// =============================================================================
// Rule shape
// =============================================================================

/// One head argument, as the analysis sees it.
#[derive(Debug)]
enum HeadColumn<'a> {
    Var(&'a Variable),
    Const(&'a Constant),
    /// Any other expression: its values are not known before running.
    Expr,
}

impl<'a> HeadColumn<'a> {
    fn of(arg: &'a HeadArg) -> Self {
        match arg {
            HeadArg::Var(var) => Self::Var(var),
            HeadArg::Arith(arith) if arith.rest().is_empty() => match arith.init() {
                Factor::Var(var) => Self::Var(var),
                Factor::Const(constant) => Self::Const(constant),
                Factor::FnCall(_)
                | Factor::Builtin(_)
                | Factor::Cast(_)
                | Factor::Group(_)
                | Factor::Tuple(_)
                | Factor::TupleProj { .. }
                | Factor::Placeholder
                | Factor::Aggregate(_) => Self::Expr,
            },
            HeadArg::Arith(_) => Self::Expr,
        }
    }
}

/// What the analysis reads of one rule.
#[derive(Debug)]
struct Shape<'a> {
    rule: &'a FlowLogRule,
    head: &'a str,
    columns: Vec<HeadColumn<'a>>,
    /// The head columns each plain head variable fills.
    head_vars: BTreeMap<&'a Variable, Vec<usize>>,
    /// The exactly compared constants a top-level `v = c` (or `c = v`)
    /// pins each variable to. Two different ones mean the rule never
    /// fires; the analysis needs the variable at either, which keeps a
    /// rule that derives nothing.
    pins: BTreeMap<&'a Variable, BTreeSet<Constant>>,
}

impl<'a> Shape<'a> {
    fn of(rule: &'a FlowLogRule) -> Self {
        let columns: Vec<HeadColumn<'a>> = rule
            .head()
            .head_arguments()
            .iter()
            .map(HeadColumn::of)
            .collect();
        let mut head_vars: BTreeMap<&Variable, Vec<usize>> = BTreeMap::new();
        for (index, column) in columns.iter().enumerate() {
            if let HeadColumn::Var(var) = column {
                head_vars.entry(var).or_default().push(index);
            }
        }
        let mut pins: BTreeMap<&Variable, BTreeSet<Constant>> = BTreeMap::new();
        for predicate in rule.rhs() {
            if let Some((var, constant)) = pin(predicate) {
                pins.entry(var).or_default().insert(constant.clone());
            }
        }
        Self {
            rule,
            head: rule.head().name(),
            columns,
            head_vars,
            pins,
        }
    }

    /// The values the rule can need of each variable it constrains: a head
    /// variable the values its head columns are demanded at, a pinned one
    /// its pin. A variable missing from the map is needed at every value.
    fn needed(&self, demands: &Demands) -> BTreeMap<&'a Variable, Demand> {
        let mut needed: BTreeMap<&Variable, Demand> = self
            .head_vars
            .iter()
            .map(|(&var, columns)| {
                let demand = columns.iter().fold(Demand::Any, |demand, &column| {
                    demand.meet(demands.get(self.head, column))
                });
                (var, demand)
            })
            .collect();
        for (&var, pinned) in &self.pins {
            let demand = needed.entry(var).or_insert(Demand::Any);
            *demand = demand.meet(&Demand::Values(pinned.clone()));
        }
        needed
    }

    /// Whether the rule can derive a row inside its head relation's demand:
    /// every head column admits its argument and every constrained
    /// variable keeps a value. A rule that cannot is never read.
    fn is_live(
        &self,
        needed: &BTreeMap<&Variable, Demand>,
        demands: &Demands,
        types: &Types,
    ) -> bool {
        let columns_admit = self.columns.iter().enumerate().all(|(index, column)| {
            let demand = demands.get(self.head, index);
            match column {
                HeadColumn::Const(constant) => match column_type(types, self.head, index) {
                    Some(ty) => demand.admits(constant, ty),
                    None => true,
                },
                HeadColumn::Var(_) | HeadColumn::Expr => !demand.is_empty(),
            }
        });
        columns_admit && needed.values().all(|demand| !demand.is_empty())
    }
}

/// The variable and the exactly compared constant a top-level `v = c` or
/// `c = v` pins together, if `predicate` is one.
fn pin(predicate: &Predicate) -> Option<(&Variable, &Constant)> {
    let Predicate::Compare(comparison) = predicate else {
        return None;
    };
    if !matches!(comparison.operator(), ComparisonOperator::Equal) {
        return None;
    }
    let ((Factor::Var(var), Factor::Const(constant)) | (Factor::Const(constant), Factor::Var(var))) =
        (bare(comparison.left())?, bare(comparison.right())?)
    else {
        return None;
    };
    is_exact(constant.ty()).then_some((var, constant))
}

/// The lone factor of an expression with no operators.
fn bare(arith: &Arithmetic) -> Option<&Factor> {
    arith.rest().is_empty().then_some(arith.init())
}

// =============================================================================
// Analysis
// =============================================================================

/// What the rewrite does to one rule.
#[derive(Debug, PartialEq, Eq)]
enum Action {
    /// Leave the rule as it is.
    Keep,
    /// Remove the rule: it derives no row the program reads.
    Drop,
    /// Split the rule into one copy per combination of values, each
    /// filtered by `var = value` for its variables.
    Filter(Vec<(Variable, Vec<Constant>)>),
}

/// The final demand of every column and the action for every rule, in
/// rule order.
fn plan(program: &Program) -> (Demands, Vec<Action>) {
    let types: Types = program
        .relations()
        .iter()
        .map(|relation| (relation.name().to_string(), relation.data_type()))
        .collect();
    let edbs: HashSet<&str> = program
        .relations()
        .iter()
        .filter(|relation| program.is_edb_relation(relation))
        .map(|relation| relation.name())
        .collect();
    let shapes: Vec<Shape> = program.rules().iter().map(Shape::of).collect();
    let mut seeds = seeds(program);
    // A column some rule cannot keep inside its demand is demanded
    // everywhere instead, and the analysis reruns. So is every column of
    // a relation the plan would leave underived, which makes all its
    // rules live. Each column widened is finite in `demands`, so the
    // seeds grow, and this stops.
    loop {
        let demands = solve(&shapes, &types, seeds.clone());
        let mut unbounded = Vec::new();
        let actions: Vec<Action> = shapes
            .iter()
            .map(|shape| plan_rule(shape, &demands, &types, &edbs, &mut unbounded))
            .collect();
        if unbounded.is_empty() {
            for relation in stranded(&shapes, &actions, &edbs) {
                let arity = types.get(relation).map_or(0, Vec::len);
                unbounded.extend(
                    (0..arity)
                        .filter(|&column| demands.get(relation, column).values().is_some())
                        .map(|column| (relation, column)),
                );
            }
        }
        if unbounded.is_empty() {
            log_demands(&demands);
            return (demands, actions);
        }
        for (relation, column) in unbounded {
            seeds.absorb(relation, column, &Demand::Any);
        }
    }
}

/// The starting demand: every column of an output, a `.printsize`
/// relation, or a lattice component is demanded everywhere, and so is a
/// head column computed by an expression; every other column starts
/// unread.
fn seeds(program: &Program) -> Demands {
    let lattice = lattice_components(program);
    let mut demands = Demands(
        program
            .relations()
            .iter()
            .map(|relation| {
                let everywhere =
                    relation.is_output_printsize() || lattice.contains(relation.name());
                let demand = if everywhere {
                    Demand::Any
                } else {
                    Demand::none()
                };
                (relation.name().to_string(), vec![demand; relation.arity()])
            })
            .collect(),
    );
    for rule in program.rules() {
        let head = rule.head();
        for (index, arg) in head.head_arguments().iter().enumerate() {
            if matches!(HeadColumn::of(arg), HeadColumn::Expr) {
                demands.absorb(head.name(), index, &Demand::Any);
            }
        }
    }
    demands
}

/// The relations sharing a recursive component with a lattice relation.
/// A lattice rule merges rows instead of adding them, which the filter's
/// argument does not cover, so the whole component is kept as written.
fn lattice_components(program: &Program) -> HashSet<&str> {
    let mut reads: HashMap<&str, HashSet<&str>> = HashMap::new();
    let mut readers: HashMap<&str, HashSet<&str>> = HashMap::new();
    for rule in program.rules() {
        let head = rule.head().name();
        for atom in rule.body_atoms() {
            reads.entry(head).or_default().insert(atom.name());
            readers.entry(atom.name()).or_default().insert(head);
        }
    }
    let mut component = HashSet::new();
    for relation in program.relations() {
        if relation.lattice().is_none() {
            continue;
        }
        let below = reachable(relation.name(), &reads);
        let above = reachable(relation.name(), &readers);
        component.extend(below.intersection(&above).copied());
    }
    component
}

/// Every node `edges` reaches from `start`, `start` included.
fn reachable<'a>(start: &'a str, edges: &HashMap<&'a str, HashSet<&'a str>>) -> HashSet<&'a str> {
    let mut seen = HashSet::from([start]);
    let mut stack = vec![start];
    while let Some(node) = stack.pop() {
        for &next in edges.get(node).into_iter().flatten() {
            if seen.insert(next) {
                stack.push(next);
            }
        }
    }
    seen
}

/// Whether no rule negates, or aggregates over, a relation that reads the
/// rule's own head. A recursion through negation, which the planner runs
/// with a warning, or through an aggregate, which it rejects, has no
/// stratified meaning for the filter's argument to keep.
fn is_stratified(program: &Program) -> bool {
    let mut reads: HashMap<&str, HashSet<&str>> = HashMap::new();
    for rule in program.rules() {
        let head = reads.entry(rule.head().name()).or_default();
        head.extend(rule.body_atoms().into_iter().map(Atom::name));
    }
    !program.rules().iter().any(|rule| {
        rule.rhs().iter().any(|predicate| {
            let lower = match predicate {
                Predicate::NegativeAtom(atom) => vec![atom],
                Predicate::Aggregate(aggregate) => aggregate.body_atoms(),
                Predicate::PositiveAtom(_) | Predicate::Compare(_) => vec![],
            };
            lower
                .into_iter()
                .any(|atom| reachable(atom.name(), &reads).contains(rule.head().name()))
        })
    })
}

/// The relations a rule that `actions` keeps reads, in any atom, but that
/// no kept rule derives and that have no input and no inline facts: every
/// rule deriving them falls outside their demand. Dropping all of those
/// rules would leave the relation underived, which `prune` would then
/// make an empty input, so the caller demands it everywhere instead.
fn stranded<'a>(
    shapes: &[Shape<'a>],
    actions: &[Action],
    edbs: &HashSet<&str>,
) -> BTreeSet<&'a str> {
    let kept: Vec<&Shape<'a>> = shapes
        .iter()
        .zip(actions)
        .filter(|(_, action)| **action != Action::Drop)
        .map(|(shape, _)| shape)
        .collect();
    let derived: HashSet<&str> = kept.iter().map(|shape| shape.head).collect();
    kept.iter()
        .flat_map(|shape| shape.rule.body_atoms())
        .map(Atom::name)
        .filter(|name| !derived.contains(name) && !edbs.contains(name))
        .collect()
}

/// The least demand above `seeds` that every live rule's body respects:
/// iterates until no column grows. Columns only grow and each widens to
/// [`Demand::Any`] past [`MAX_VALUES`], so this stops.
fn solve(shapes: &[Shape], types: &Types, seeds: Demands) -> Demands {
    let mut demands = seeds;
    loop {
        let mut grew = false;
        for shape in shapes {
            let needed = shape.needed(&demands);
            if !shape.is_live(&needed, &demands, types) {
                continue;
            }
            for atom in shape.rule.body_atoms() {
                for (column, read) in reads(atom, &needed, types) {
                    grew |= demands.absorb(atom.name(), column, &read);
                }
            }
        }
        if !grew {
            return demands;
        }
    }
}

/// What a live rule demands of each column of `atom`: the constant it
/// names, the values the rule needs of the variable there, or every value.
/// An atom inside an aggregate is read the same way: a variable there that
/// also occurs outside the braces is the same variable, and one that does
/// not is needed at every value.
fn reads(atom: &Atom, needed: &BTreeMap<&Variable, Demand>, types: &Types) -> Vec<(usize, Demand)> {
    atom.arguments()
        .iter()
        .enumerate()
        .map(|(column, arg)| {
            let Some(ty) = column_type(types, atom.name(), column) else {
                return (column, Demand::Any);
            };
            let read = match arg {
                AtomArg::Const(constant) => {
                    read_at(&Demand::Values(BTreeSet::from([constant.clone()])), ty)
                }
                AtomArg::Var(var) => needed
                    .get(var)
                    .map_or(Demand::Any, |values| read_at(values, ty)),
                AtomArg::Placeholder => Demand::Any,
            };
            (column, read)
        })
        .collect()
}

/// The action for one rule under the final demand. A head column the rule
/// cannot keep inside its demand is pushed onto `unbounded`, and the
/// action is then discarded: the caller widens the column and reruns.
///
/// Every head column with a finite demand ends up inside it: a constant
/// is checked against it, a pinned variable is equal to an admitted
/// constant, a variable copied from a bounded column already lies inside,
/// and any other variable is filtered.
fn plan_rule<'a>(
    shape: &Shape<'a>,
    demands: &Demands,
    types: &Types,
    edbs: &HashSet<&str>,
    unbounded: &mut Vec<(&'a str, usize)>,
) -> Action {
    let needed = shape.needed(demands);
    if !shape.is_live(&needed, demands, types) {
        return Action::Drop;
    }
    for (index, column) in shape.columns.iter().enumerate() {
        if demands.get(shape.head, index).values().is_none() {
            continue;
        }
        let checked = match column {
            HeadColumn::Const(constant) => {
                column_type(types, shape.head, index).is_some_and(|ty| is_exact_in(constant, ty))
            }
            HeadColumn::Var(_) => true,
            HeadColumn::Expr => false,
        };
        if !checked {
            unbounded.push((shape.head, index));
        }
    }

    let mut filters: Vec<(&Variable, &BTreeSet<Constant>)> = shape
        .head_vars
        .keys()
        .filter(|var| !shape.pins.contains_key(*var))
        .filter_map(|&var| Some((var, needed.get(var)?.values()?)))
        .filter(|&(var, values)| !carried(shape.rule, var, values, demands, edbs))
        .collect();

    // Copies multiply across variables: give up the widest filters first,
    // widening their columns, until the product fits.
    filters.sort_by_key(|(_, values)| values.len());
    while filters
        .iter()
        .map(|(_, values)| values.len())
        .fold(1usize, usize::saturating_mul)
        > MAX_COPIES
    {
        let (var, _) = filters.pop().expect("a product over the cap has a factor");
        unbounded.extend(
            shape.head_vars[var]
                .iter()
                .filter(|&&index| demands.get(shape.head, index).values().is_some())
                .map(|&index| (shape.head, index)),
        );
    }

    if filters.is_empty() {
        return Action::Keep;
    }
    Action::Filter(
        filters
            .into_iter()
            .map(|(var, values)| (var.clone(), values.iter().cloned().collect()))
            .collect(),
    )
}

/// Whether a top-level positive atom of `rule` copies `var` from a column
/// that holds only values inside `values`: a column of a relation with no
/// input and no inline facts, whose demand is no wider than `values`. Every
/// rule deriving that relation keeps the column inside its demand, so the
/// copy needs no filter.
fn carried(
    rule: &FlowLogRule,
    var: &Variable,
    values: &BTreeSet<Constant>,
    demands: &Demands,
    edbs: &HashSet<&str>,
) -> bool {
    rule.rhs().iter().any(|predicate| match predicate {
        Predicate::PositiveAtom(atom) => {
            !edbs.contains(atom.name())
                && atom.arguments().iter().enumerate().any(|(column, arg)| {
                    matches!(arg, AtomArg::Var(other) if other == var)
                        && demands
                            .get(atom.name(), column)
                            .values()
                            .is_some_and(|demand| demand.is_subset(values))
                })
        }
        Predicate::NegativeAtom(_) | Predicate::Compare(_) | Predicate::Aggregate(_) => false,
    })
}

/// Logs every column with a finite demand, a string quoted and escaped.
fn log_demands(demands: &Demands) {
    let mut finite: Vec<String> = demands
        .0
        .iter()
        .flat_map(|(relation, columns)| {
            columns
                .iter()
                .enumerate()
                .filter_map(move |(index, demand)| {
                    let values = demand.values()?;
                    let shown: Vec<String> = values
                        .iter()
                        .map(|value| match value.ty() {
                            DataType::String => format!("{:?}", value.text()),
                            _ => value.text().to_string(),
                        })
                        .collect();
                    Some(format!("{relation}.{index} in {{{}}}", shown.join(", ")))
                })
        })
        .collect();
    finite.sort_unstable();
    if !finite.is_empty() {
        debug!("Demand pushdown: {}", finite.join("; "));
    }
}

// =============================================================================
// Rewrite
// =============================================================================

/// `rule` split into one copy per combination of `filters`' values, each
/// filtered by `var = value` for every variable, in filter then value
/// order.
fn copies(rule: &FlowLogRule, filters: &[(Variable, Vec<Constant>)]) -> Vec<FlowLogRule> {
    filters
        .iter()
        .fold(vec![rule.clone()], |copies, (var, values)| {
            copies
                .iter()
                .flat_map(|copy| {
                    values
                        .iter()
                        .map(move |value| with_filter(copy, var, value))
                })
                .collect()
        })
}

/// `rule` with `var = value` appended to its body. The comparison follows
/// every atom, so a `.plan` order, which counts positive atoms only, still
/// applies.
fn with_filter(rule: &FlowLogRule, var: &Variable, value: &Constant) -> FlowLogRule {
    let mut rhs = rule.rhs().to_vec();
    rhs.push(Predicate::Compare(ComparisonExpr::new(
        Arithmetic::var(var.clone()),
        ComparisonOperator::Equal,
        Arithmetic::new(Factor::Const(value.clone()), vec![]),
        Span::DUMMY,
    )));
    let mut copy = rule.clone();
    copy.set_rhs(rhs);
    copy
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::fmt::Write as _;

    use flowlog_common::Config;
    use flowlog_common::SourceMap;
    use rstest::rstest;

    use super::Action;
    use super::Demand;
    use super::Demands;
    use super::plan;
    use crate::AggregateExpr;
    use crate::AggregationOperator;
    use crate::Arithmetic;
    use crate::ArithmeticOperator;
    use crate::Atom;
    use crate::AtomArg;
    use crate::ComparisonExpr;
    use crate::ComparisonOperator;
    use crate::Constant;
    use crate::DataType;
    use crate::Factor;
    use crate::FlowLogRule;
    use crate::HeadArg;
    use crate::Predicate;
    use crate::Program;
    use crate::Variable;
    use crate::test_harness::parse;

    /// `source` after the whole pipeline, with demand pushdown on or off.
    fn program(source: &str, demand: bool) -> Program {
        let mut config = Config {
            no_demand: !demand,
            ..Config::default()
        };
        parse(source, &mut SourceMap::new(), &mut config).expect("test program parses")
    }

    /// The rules of `source` after the whole pipeline, as written out, with
    /// demand pushdown on or off.
    fn rules(source: &str, demand: bool) -> Vec<String> {
        program(source, demand)
            .rules()
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    const REACH: &str = "
        .decl edge(x: int32, y: int32)
        .input edge
        .decl path(x: int32, y: int32)
        path(x, y) :- edge(x, y).
        path(x, z) :- path(x, y), edge(y, z).
        .decl out(y: int32)
        .output out
        out(y) :- path(1, y).
    ";

    /// The base rule is filtered before its rows enter the loop, and the
    /// recursive rule, which copies the demanded column from `path`
    /// itself, is left alone.
    #[test]
    fn base_rule_is_filtered_and_recursive_rule_carries_the_demand() {
        assert_eq!(
            rules(REACH, true),
            [
                "path(x, y) :- edge(x, y), x = 1.",
                "path(x, z) :- path(x, y), edge(y, z).",
                "out(y) :- path(1, y).",
            ]
        );
    }

    #[test]
    fn no_demand_keeps_the_rules_as_written() {
        assert_eq!(
            rules(REACH, false),
            [
                "path(x, y) :- edge(x, y).",
                "path(x, z) :- path(x, y), edge(y, z).",
                "out(y) :- path(1, y).",
            ]
        );
    }

    #[test]
    fn each_demanded_value_gets_its_own_copy() {
        let src = "
            .decl edge(x: int32, y: int32)
            .input edge
            .decl path(x: int32, y: int32)
            path(x, y) :- edge(x, y).
            path(x, z) :- path(x, y), edge(y, z).
            .decl out(y: int32)
            .output out
            out(y) :- path(2, y).
            out(y) :- path(1, y).
        ";
        assert_eq!(
            rules(src, true),
            [
                "path(x, y) :- edge(x, y), x = 1.",
                "path(x, y) :- edge(x, y), x = 2.",
                "path(x, z) :- path(x, y), edge(y, z).",
                "out(y) :- path(2, y).",
                "out(y) :- path(1, y).",
            ]
        );
    }

    /// An output is read in full, so a constant another rule reads it at
    /// restricts nothing.
    #[test]
    fn output_keeps_every_row() {
        let src = "
            .decl edge(x: int32, y: int32)
            .input edge
            .decl path(x: int32, y: int32)
            .output path
            path(x, y) :- edge(x, y).
            .decl out(y: int32)
            .output out
            out(y) :- path(1, y).
        ";
        assert_eq!(
            rules(src, true),
            ["path(x, y) :- edge(x, y).", "out(y) :- path(1, y)."]
        );
    }

    #[test]
    fn constants_in_two_columns_filter_both() {
        let src = "
            .decl a(x: int32, y: int32, z: int32)
            .input a
            .decl q(x: int32, y: int32, z: int32)
            q(x, y, z) :- a(x, y, z).
            .decl out(z: int32)
            .output out
            out(z) :- q(1, 2, z).
        ";
        assert_eq!(
            rules(src, true),
            [
                "q(x, y, z) :- a(x, y, z), x = 1, y = 2.",
                "out(z) :- q(1, 2, z).",
            ]
        );
    }

    #[test]
    fn string_column_is_filtered_by_value() {
        let src = "
            .decl edge(x: string, y: string)
            .input edge
            .decl path(x: string, y: string)
            path(x, y) :- edge(x, y).
            .decl out(y: string)
            .output out
            out(y) :- path(\"a\", y).
        ";
        assert_eq!(
            rules(src, true),
            [
                "path(x, y) :- edge(x, y), x = \"a\".",
                "out(y) :- path(\"a\", y).",
            ]
        );
    }

    /// A rule whose head constant lies outside its relation's demand
    /// derives nothing the program reads.
    #[test]
    fn head_constant_outside_the_demand_drops_the_rule() {
        let src = "
            .decl a(y: int32)
            .input a
            .decl b(y: int32)
            .input b
            .decl q(x: int32, y: int32)
            q(1, y) :- a(y).
            q(2, y) :- b(y).
            .decl out(y: int32)
            .output out
            out(y) :- q(1, y).
        ";
        assert_eq!(rules(src, true), ["q(1, y) :- a(y).", "out(y) :- q(1, y)."]);
    }

    /// A head variable pinned by `x = c` is judged by its pin: outside the
    /// demand the rule goes, inside it needs no filter.
    #[test]
    fn pinned_head_variable_is_judged_by_its_pin() {
        let src = "
            .decl a(x: int32, y: int32)
            .input a
            .decl q(x: int32, y: int32)
            q(x, y) :- a(x, y), x = 3.
            q(x, y) :- a(x, y), x = 1.
            .decl out(y: int32)
            .output out
            out(y) :- q(1, y).
        ";
        assert_eq!(
            rules(src, true),
            ["q(x, y) :- a(x, y), x = 1.", "out(y) :- q(1, y)."]
        );
    }

    /// A variable filling two head columns needs a value both admit. When
    /// none does, the rule derives nothing `out` reads; but `out` still
    /// reads `q`, so `q` keeps its rule, unfiltered, rather than be left
    /// underived.
    // Cases: what `out` reads of `q`, `q`'s rules after the pass.
    #[rstest]
    #[case("q(1, 1)", &["q(x, x) :- a(x), x = 1."])]
    #[case("q(1, 2)", &["q(x, x) :- a(x)."])]
    fn repeated_head_variable_meets_both_demands(#[case] read: &str, #[case] expected: &[&str]) {
        let src = format!(
            "
            .decl a(x: int32)
            .input a
            .decl q(x: int32, y: int32)
            q(x, x) :- a(x).
            .decl out(x: int32)
            .output out
            out(0) :- {read}.
        "
        );
        let rules = rules(&src, true);
        let of_q: Vec<&str> = rules
            .iter()
            .map(String::as_str)
            .filter(|rule| rule.starts_with("q("))
            .collect();
        assert_eq!(of_q, expected);
    }

    /// `path` has an inline fact outside the demand, so its rows are not all
    /// filtered ones and the recursive rule cannot rely on them.
    #[test]
    fn relation_with_facts_does_not_carry_the_demand() {
        let src = "
            .decl edge(x: int32, y: int32)
            .input edge
            .decl path(x: int32, y: int32)
            path(7, 7).
            path(x, y) :- edge(x, y).
            path(x, z) :- path(x, y), edge(y, z).
            .decl out(y: int32)
            .output out
            out(y) :- path(1, y).
        ";
        assert_eq!(
            rules(src, true),
            [
                "path(x, y) :- edge(x, y), x = 1.",
                "path(x, z) :- path(x, y), edge(y, z), x = 1.",
                "out(y) :- path(1, y).",
            ]
        );
    }

    /// A rule reading `!b(x, y)` at `x = 1` demands `b` there: its rows
    /// at `x = 1` are the ones the negation tests.
    #[test]
    fn negated_atom_is_demanded_where_the_rule_reads_it() {
        let src = "
            .decl a(x: int32, y: int32)
            .input a
            .decl c(x: int32, y: int32)
            .input c
            .decl b(x: int32, y: int32)
            b(x, y) :- c(x, y).
            .decl q(x: int32, y: int32)
            q(x, y) :- a(x, y), !b(x, y).
            .decl out(y: int32)
            .output out
            out(y) :- q(1, y).
        ";
        assert_eq!(
            rules(src, true),
            [
                "b(x, y) :- c(x, y), x = 1.",
                "q(x, y) :- a(x, y), !b(x, y), x = 1.",
                "out(y) :- q(1, y).",
            ]
        );
    }

    /// The braces of an aggregate grouped by `x` read only `x`'s group, so
    /// the group key's demand reaches the relation inside them.
    #[test]
    fn aggregate_reads_its_group_at_the_rule_demand() {
        let src = "
            .decl node(x: int32)
            .input node
            .decl edge(x: int32, y: int32)
            .input edge
            .decl e(x: int32, y: int32)
            e(x, y) :- edge(x, y).
            .decl deg(x: int32, n: int32)
            deg(x, n) :- node(x), n = count : { e(x, _) }.
            .decl out(n: int32)
            .output out
            out(n) :- deg(1, n).
        ";
        assert_eq!(
            rules(src, true),
            [
                "e(x, y) :- edge(x, y), x = 1.",
                "deg(x, @agg_0) :- node(x), @agg_0 = count : { e(x, _) }, x = 1.",
                "out(n) :- deg(1, n).",
            ]
        );
    }

    /// Demand on an aggregate's answer filters the answer, after it is
    /// computed; the braces still see every row of the group.
    #[test]
    fn aggregate_answer_is_filtered_after_aggregation() {
        let src = "
            .decl edge(x: int32, y: int32)
            .input edge
            .decl e(x: int32, y: int32)
            e(x, y) :- edge(x, y).
            .decl deg(x: int32, n: int32)
            deg(x, n) :- e(x, _), n = count : { e(x, _) }.
            .decl out(x: int32)
            .output out
            out(x) :- deg(x, 2).
        ";
        assert_eq!(
            rules(src, true),
            [
                "e(x, y) :- edge(x, y).",
                "deg(x, @agg_0) :- e(x, _), @agg_0 = count : { e(x, _) }, @agg_0 = 2.",
                "out(x) :- deg(x, 2).",
            ]
        );
    }

    /// `path` is read at `x = 1`, but its recursive rule reads it at an
    /// unknown `y`: the demand grows to every value and nothing is
    /// filtered. Magic Sets would pass the bindings sideways instead.
    #[test]
    fn demand_that_changes_across_recursion_is_every_value() {
        let src = "
            .decl edge(x: int32, y: int32)
            .input edge
            .decl path(x: int32, y: int32)
            path(x, y) :- edge(x, y).
            path(x, z) :- edge(x, y), path(y, z).
            .decl out(y: int32)
            .output out
            out(y) :- path(1, y).
        ";
        assert_eq!(
            rules(src, true),
            [
                "path(x, y) :- edge(x, y).",
                "path(x, z) :- edge(x, y), path(y, z).",
                "out(y) :- path(1, y).",
            ]
        );
    }

    /// Columns the pass cannot compare by value, or whose head argument is
    /// computed, are read everywhere.
    // Cases: column type, head argument, the constant `out` reads.
    #[rstest]
    #[case("f64", "x", "1.5")]
    #[case("int32", "x + 1", "1")]
    fn uncomparable_or_computed_column_is_read_everywhere(
        #[case] ty: &str,
        #[case] head: &str,
        #[case] constant: &str,
    ) {
        let src = format!(
            "
            .decl a(x: {ty}, y: int32)
            .input a
            .decl q(x: {ty}, y: int32)
            q({head}, y) :- a(x, y).
            .decl out(y: int32)
            .output out
            out(y) :- q({constant}, y).
        "
        );
        assert_eq!(rules(&src, true), rules(&src, false));
    }

    /// A lattice rule merges rows rather than adding them, so its whole
    /// recursive component is kept as written.
    #[test]
    fn lattice_component_is_kept_as_written() {
        let src = "
            .decl edge(x: int32, y: int32, w: int32)
            .input edge
            .decl source(x: int32)
            .input source
            .decl dist(x: int32, d: min<int32>)
            dist(x, 0) :- source(x).
            dist(y, d + w) :- dist(x, d), edge(x, y, w).
            .decl out(d: int32)
            .output out
            out(d) :- dist(1, d).
        ";
        assert_eq!(rules(src, true), rules(src, false));
    }

    #[test]
    fn demand_past_the_value_cap_is_every_value() {
        let src = "
            .decl edge(x: int32, y: int32)
            .input edge
            .decl path(x: int32, y: int32)
            path(x, y) :- edge(x, y).
            .decl out(y: int32)
            .output out
            out(y) :- path(1, y).
            out(y) :- path(2, y).
            out(y) :- path(3, y).
            out(y) :- path(4, y).
            out(y) :- path(5, y).
        ";
        assert_eq!(rules(src, true), rules(src, false));
    }

    /// `x`, `y` and `z` are demanded at 3, 3 and 2 values: 18 copies, past
    /// the cap of 16. The widest filter, `y` (last of the two widest in
    /// variable order), is given up and its column read everywhere,
    /// leaving 6 copies.
    #[test]
    fn copies_past_the_cap_give_up_the_widest_filter() {
        let src = "
            .decl a(x: int32, y: int32, z: int32, w: int32)
            .input a
            .decl q(x: int32, y: int32, z: int32, w: int32)
            q(x, y, z, w) :- a(x, y, z, w).
            .decl out(w: int32)
            .output out
            out(w) :- q(1, 1, 1, w).
            out(w) :- q(2, 2, 2, w).
            out(w) :- q(3, 3, 1, w).
        ";
        let rules = rules(src, true);
        assert_eq!(
            rules[..6],
            [
                "q(x, y, z, w) :- a(x, y, z, w), z = 1, x = 1.",
                "q(x, y, z, w) :- a(x, y, z, w), z = 1, x = 2.",
                "q(x, y, z, w) :- a(x, y, z, w), z = 1, x = 3.",
                "q(x, y, z, w) :- a(x, y, z, w), z = 2, x = 1.",
                "q(x, y, z, w) :- a(x, y, z, w), z = 2, x = 2.",
                "q(x, y, z, w) :- a(x, y, z, w), z = 2, x = 3.",
            ]
        );
        assert_eq!(rules.len(), 9);
    }

    /// A relation that a kept rule still reads, but whose every rule falls
    /// outside its demand, keeps its rules: dropping them all would leave
    /// it underived, and `prune` would make it an empty input, one more in
    /// the program's interface.
    #[test]
    fn relation_still_read_keeps_its_rules() {
        let src = "
            .decl a(y: int32)
            .input a
            .decl q(x: int32, y: int32)
            q(2, y) :- a(y).
            .decl out(y: int32)
            .output out
            out(y) :- q(1, y).
        ";
        assert_eq!(rules(src, true), ["q(2, y) :- a(y).", "out(y) :- q(1, y)."]);
        assert_eq!(program(src, true).edb_names(), ["a"]);
    }

    /// The rules the pass drops may be the only ones reading an input. The
    /// input stays, so the program loads the same inputs, accepts the same
    /// ones in a transaction, and runs incrementally if it did.
    #[rstest]
    #[case::from_a_file(".input e(IO=\"file\", filename=\"e.csv\")", "")]
    #[case::mutable(".input e(IO=\"command\", delimiter=\",\")", "mutable")]
    #[case::append(".input e(IO=\"command\", delimiter=\",\")", "append")]
    fn inputs_stay_when_their_readers_are_dropped(#[case] input: &str, #[case] mode: &str) {
        let src = format!(
            "
            .decl e(a: int32, b: int32) {mode}
            {input}
            .decl base(a: int32)
            base(7).
            .decl q(k: int32, a: int32)
            q(2, a) :- e(a, _).
            q(1, a) :- base(a).
            .decl out(a: int32)
            .output out
            out(a) :- q(1, a).
            "
        );
        let (on, off) = (program(&src, true), program(&src, false));
        assert_eq!(on.rules().len(), 2, "{on}");
        assert_eq!(on.edb_names(), off.edb_names());
        assert_eq!(on.file_inputs().count(), off.file_inputs().count());
        assert_eq!(on.is_incremental(), off.is_incremental());
    }

    /// A recursion through negation has no stratified meaning. The planner
    /// runs it as one loop, with a warning, and the rows the loop settles on
    /// depend on its rules: dropping `blocked(2, a) :- reach(a)`, outside
    /// the demand `blocked.0 in {1}`, would end the loop at other rows. The
    /// program is left as written.
    #[test]
    fn recursion_through_negation_is_left_as_written() {
        let src = "
            .decl e(a: int32, b: int32)
            .input e
            .decl blocked(k: int32, a: int32)
            blocked(1, a) :- e(a, _).
            blocked(2, a) :- reach(a).
            .decl start(a: int32)
            start(a) :- e(a, _), !blocked(1, a).
            .decl reach(a: int32)
            reach(a) :- start(a).
            reach(b) :- reach(a), e(a, b).
            .decl out(a: int32)
            .output out
            out(a) :- reach(a).
        ";
        assert_eq!(rules(src, true), rules(src, false));
    }

    /// Likewise through an aggregate, which the planner rejects: it must
    /// still see the rule that closes the recursion.
    #[test]
    fn recursion_through_an_aggregate_is_left_as_written() {
        let src = "
            .decl e(a: int32, b: int32)
            .input e
            .decl q(k: int32, a: int32)
            q(1, b) :- e(_, b).
            q(2, a) :- p(a, _).
            .decl p(a: int32, n: int32)
            .output p
            p(a, n) :- e(a, _), n = count : { q(1, a) }.
        ";
        assert_eq!(rules(src, true), rules(src, false));
    }

    // =========================================================================
    // Property: random programs keep their outputs
    // =========================================================================

    /// Random stratified programs over `int32` values `0..DOMAIN`, each
    /// evaluated by [`evaluate`] as written and after the pass. Seeds are
    /// fixed, so a failure replays; `DEMAND_CASES` runs more of them.
    #[test]
    fn random_programs_keep_their_outputs() {
        let cases = std::env::var("DEMAND_CASES")
            .ok()
            .and_then(|cases| cases.parse().ok())
            .unwrap_or(400u64);
        let mut tally = Tally::default();
        for seed in 1..=cases {
            check_random_program(seed, &mut tally);
        }
        println!("{tally:?}");
        // The generator must mostly write valid programs, and the pass
        // must act on enough of them, in each way, for the property to
        // say something.
        assert!(
            tally.rejected * 10 <= cases,
            "{} of {cases} programs rejected; first: {}",
            tally.rejected,
            tally.first_rejection
        );
        let percent = |count: u64| count * 100 / cases;
        assert!(
            percent(tally.filtered) >= 20
                && percent(tally.dropped) >= 8
                && percent(tally.looped) >= 8
                && percent(tally.carried) >= 4
                && percent(tally.negated) >= 4
                && percent(tally.aggregated) >= 4,
            "too little coverage: {tally:?}"
        );
    }

    /// What the random programs exercised.
    #[derive(Debug, Default)]
    struct Tally {
        rejected: u64,
        first_rejection: String,
        /// Programs with a rule the pass filtered.
        filtered: u64,
        /// Programs with a rule the pass dropped.
        dropped: u64,
        /// Programs with a filtered rule deriving a recursive relation.
        looped: u64,
        /// Programs whose recursive rule carries a demand that another
        /// rule of its relation filters: the loop runs unfiltered.
        carried: u64,
        /// Programs with a filtered rule that negates an atom.
        negated: u64,
        /// Programs with a filtered rule that aggregates.
        aggregated: u64,
        /// Rows outside their relation's demand that the pass saved.
        rows_saved: usize,
    }

    /// Generates program `seed`, runs the pipeline on it with and without
    /// the pass, and checks the pass's guarantees on the two models:
    /// - the inputs are unchanged: the same relations have inline facts,
    ///   and the same facts;
    /// - every output is unchanged;
    /// - every relation keeps a subset of its rows, and exactly its rows
    ///   inside its demand (its *box*); outside it, only inline facts;
    /// - the demand is closed: a rule firing with its head inside the box
    ///   reads only rows inside their boxes, negated and aggregated ones
    ///   included.
    fn check_random_program(seed: u64, tally: &mut Tally) {
        let (source, levels) = random_program(&mut Rng(seed));
        let run = |demand: bool| {
            let mut config = Config {
                no_demand: !demand,
                ..Config::default()
            };
            parse(&source, &mut SourceMap::new(), &mut config)
        };
        let before = match run(false) {
            Ok(program) => program,
            Err(error) => {
                if tally.rejected == 0 {
                    tally.first_rejection = format!("{error:?}\n{source}");
                }
                tally.rejected += 1;
                return;
            }
        };
        let after = run(true).expect("the pass keeps a program valid");
        let (demands, actions) = plan(&before);
        count_coverage(&before, &actions, tally);

        let original = evaluate(&before, &levels);
        let filtered = evaluate(&after, &levels);
        let none = BTreeSet::new();
        let context = || format!("seed {seed}:\n{source}\nafter:\n{after}");

        assert_eq!(
            (before.edb_names(), before.facts()),
            (after.edb_names(), after.facts()),
            "the inputs changed; {}",
            context()
        );
        for relation in before.idbs() {
            assert_eq!(
                original.get(relation.name()).unwrap_or(&none),
                filtered.get(relation.name()).unwrap_or(&none),
                "output {} changed; {}",
                relation.name(),
                context()
            );
        }
        for (name, rows) in &filtered {
            let was = original.get(name).unwrap_or(&none);
            let facts = facts_of(&after, name);
            for row in rows {
                assert!(was.contains(row), "{name}{row:?} is new; {}", context());
                assert!(
                    in_box(&demands, name, row) || facts.contains(row),
                    "{name}{row:?} is outside the demand; {}",
                    context()
                );
            }
            for row in was {
                if in_box(&demands, name, row) {
                    assert!(rows.contains(row), "{name}{row:?} is lost; {}", context());
                } else {
                    tally.rows_saved += usize::from(!rows.contains(row));
                }
            }
        }
        for rule in before.rules() {
            for candidate in candidates(rule, &original) {
                if !in_box(&demands, rule.head().name(), &candidate.head) {
                    continue;
                }
                for (name, row) in &candidate.reads {
                    assert!(
                        in_box(&demands, name, row),
                        "`{rule}` reads {name}{row:?} outside the demand; {}",
                        context()
                    );
                }
            }
        }
    }

    fn count_coverage(program: &Program, actions: &[Action], tally: &mut Tally) {
        let recursive = recursive_relations(program);
        let filtered: HashSet<&str> = program
            .rules()
            .iter()
            .zip(actions)
            .filter(|(_, action)| matches!(action, Action::Filter(_)))
            .map(|(rule, _)| rule.head().name())
            .collect();
        let has =
            |rule: &FlowLogRule, wanted: fn(&Predicate) -> bool| rule.rhs().iter().any(wanted);
        let (mut carried, mut negated, mut aggregated) = (false, false, false);
        for (rule, action) in program.rules().iter().zip(actions) {
            let head = rule.head().name();
            match action {
                Action::Keep => {
                    carried |= filtered.contains(head)
                        && has(rule, |p| matches!(p, Predicate::PositiveAtom(_)))
                        && rule.rhs().iter().any(
                            |p| matches!(p, Predicate::PositiveAtom(atom) if atom.name() == head),
                        );
                }
                Action::Filter(_) => {
                    negated |= has(rule, |p| matches!(p, Predicate::NegativeAtom(_)));
                    aggregated |= has(rule, |p| matches!(p, Predicate::Aggregate(_)));
                }
                Action::Drop => {}
            }
        }
        tally.filtered += u64::from(!filtered.is_empty());
        tally.dropped += u64::from(actions.contains(&Action::Drop));
        tally.looped += u64::from(filtered.iter().any(|head| recursive.contains(*head)));
        tally.carried += u64::from(carried);
        tally.negated += u64::from(negated);
        tally.aggregated += u64::from(aggregated);
    }

    /// The relations that read themselves through positive atoms.
    fn recursive_relations(program: &Program) -> HashSet<&str> {
        let mut reads: HashMap<&str, HashSet<&str>> = HashMap::new();
        for rule in program.rules() {
            let entry = reads.entry(rule.head().name()).or_default();
            for predicate in rule.rhs() {
                if let Predicate::PositiveAtom(atom) = predicate {
                    entry.insert(atom.name());
                }
            }
        }
        reads
            .keys()
            .copied()
            .filter(|&start| {
                let mut seen = HashSet::new();
                let mut stack: Vec<&str> = reads[start].iter().copied().collect();
                while let Some(next) = stack.pop() {
                    if next == start {
                        return true;
                    }
                    if seen.insert(next) {
                        stack.extend(reads.get(next).into_iter().flatten().copied());
                    }
                }
                false
            })
            .collect()
    }

    /// Whether `row` of `relation` lies inside the relation's demand.
    fn in_box(demands: &Demands, relation: &str, row: &[i64]) -> bool {
        row.iter()
            .enumerate()
            .all(|(column, value)| match demands.get(relation, column) {
                Demand::Any => true,
                Demand::Values(values) => {
                    values.contains(&Constant::new(DataType::Int32, value.to_string()))
                }
            })
    }

    fn facts_of(program: &Program, relation: &str) -> BTreeSet<Vec<i64>> {
        program
            .facts()
            .get(relation)
            .map_or_else(BTreeSet::new, |facts| {
                facts
                    .iter()
                    .map(|fact| fact.columns.iter().map(value).collect())
                    .collect()
            })
    }

    // -------------------------------------------------------------------------
    // Generator
    // -------------------------------------------------------------------------

    /// Values range over `0..DOMAIN`.
    const DOMAIN: u64 = 4;

    /// xorshift64*: deterministic, so a seed replays its program.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }

        fn percent(&mut self, chance: u64) -> bool {
            self.below(100) < chance
        }

        fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
            let index = usize::try_from(self.below(items.len() as u64)).expect("small index");
            &items[index]
        }
    }

    /// A relation of a random program: its name, arity, and level. A rule
    /// reads positive atoms at its head's level or below, and negated or
    /// aggregated ones strictly below, so the levels stratify it.
    struct Rel {
        name: String,
        arity: usize,
        level: usize,
    }

    /// A random program and the level of each of its relations. Level 0
    /// holds three inline-fact relations, levels 1 to 3 two to four
    /// derived ones (some recursive, some also with facts or `.output`),
    /// and level 4 the outputs `o0` and `o1`, which read the others at
    /// constants so that there is demand to push.
    fn random_program(rng: &mut Rng) -> (String, HashMap<String, usize>) {
        let mut source = String::new();
        let mut rels: Vec<Rel> = Vec::new();
        for (name, arity) in [("e0", 2), ("e1", 2), ("e2", 1)] {
            declare(&mut source, name, arity);
            facts(rng, &mut source, name, arity, 30);
            rels.push(Rel {
                name: name.to_string(),
                arity,
                level: 0,
            });
        }
        for index in 0..2 + rng.below(3) {
            let name = format!("r{index}");
            let arity = 1 + usize::try_from(rng.below(2)).expect("small arity");
            declare(&mut source, &name, arity);
            if rng.percent(10) {
                facts(rng, &mut source, &name, arity, 10);
            }
            if rng.percent(10) {
                writeln!(source, ".output {name}").expect("write to a String");
            }
            let level = 1 + usize::try_from(rng.below(3)).expect("small level");
            rels.push(Rel { name, arity, level });
        }
        for name in ["o0", "o1"] {
            declare(&mut source, name, 1);
            writeln!(source, ".output {name}").expect("write to a String");
            rels.push(Rel {
                name: name.to_string(),
                arity: 1,
                level: 4,
            });
        }
        for head in rels.iter().filter(|rel| rel.level > 0) {
            for index in 0..1 + rng.below(3) {
                let rule = random_rule(rng, head, &rels, index == 0);
                source.push_str(&rule);
            }
        }
        let levels = rels
            .iter()
            .map(|rel| (rel.name.clone(), rel.level))
            .collect();
        (source, levels)
    }

    fn declare(source: &mut String, name: &str, arity: usize) {
        let columns: Vec<String> = (0..arity)
            .map(|column| format!("c{column}: int32"))
            .collect();
        writeln!(source, ".decl {name}({})", columns.join(", ")).expect("write to a String");
    }

    /// Writes each row of `name` as an inline fact with chance `percent`.
    fn facts(rng: &mut Rng, source: &mut String, name: &str, arity: usize, percent: u64) {
        let rows = DOMAIN.pow(u32::try_from(arity).expect("small arity"));
        for mut row in 0..rows {
            let mut columns = Vec::new();
            for _ in 0..arity {
                columns.push((row % DOMAIN).to_string());
                row /= DOMAIN;
            }
            if rng.percent(percent) {
                writeln!(source, "{name}({}).", columns.join(", ")).expect("write to a String");
            }
        }
    }

    /// A random safe rule deriving `head`: one to three positive atoms,
    /// maybe a negated atom, a comparison, and a `count` grouped by a
    /// bound variable, then a head of bound variables, constants, and
    /// `(v + 1) % DOMAIN`. Outputs read at constants more often. A rule
    /// after a relation's `first` often reads the relation itself first,
    /// and a head column often takes the variable at that column of the
    /// first atom, so that loops carry their columns.
    fn random_rule(rng: &mut Rng, head: &Rel, rels: &[Rel], first: bool) -> String {
        let outputs = head.level == 4;
        let same_or_below: Vec<&Rel> = rels
            .iter()
            .filter(|rel| rel.level <= head.level && (rel.level < 4))
            .collect();
        let below: Vec<&Rel> = rels.iter().filter(|rel| rel.level < head.level).collect();
        let constant_chance = if outputs { 55 } else { 20 };
        let recursive = !first && !outputs && rng.percent(50);
        let mut body: Vec<String> = Vec::new();
        let mut bound: Vec<&str> = Vec::new();
        let mut first_args: Vec<String> = Vec::new();
        for index in 0..1 + rng.below(3) {
            let rel = if index == 0 && recursive {
                head
            } else {
                *rng.pick(&same_or_below)
            };
            let args: Vec<String> = (0..rel.arity)
                .map(|_| {
                    if rng.percent(constant_chance) {
                        rng.below(DOMAIN).to_string()
                    } else if rng.percent(10) {
                        "_".to_string()
                    } else {
                        let var = *rng.pick(&["a", "b", "c"]);
                        if !bound.contains(&var) {
                            bound.push(var);
                        }
                        var.to_string()
                    }
                })
                .collect();
            body.push(format!("{}({})", rel.name, args.join(", ")));
            if index == 0 {
                first_args = args;
            }
        }
        let term = |rng: &mut Rng, bound: &[&str]| {
            if bound.is_empty() || rng.percent(30) {
                rng.below(DOMAIN).to_string()
            } else {
                (*rng.pick(bound)).to_string()
            }
        };
        if !below.is_empty() && rng.percent(25) {
            let rel = *rng.pick(&below);
            let args: Vec<String> = (0..rel.arity).map(|_| term(rng, &bound)).collect();
            body.push(format!("!{}({})", rel.name, args.join(", ")));
        }
        if !bound.is_empty() && rng.percent(35) {
            let op = *rng.pick(&["=", "!=", "<", ">"]);
            body.push(format!("{} {op} {}", rng.pick(&bound), rng.below(DOMAIN)));
        }
        let groups: Vec<&Rel> = below.iter().copied().filter(|rel| rel.arity == 2).collect();
        if !groups.is_empty() && !bound.is_empty() && rng.percent(20) {
            let rel = *rng.pick(&groups);
            let key = *rng.pick(&bound);
            let other = if rng.percent(50) { "_" } else { "l" };
            body.push(format!("n = count : {{ {}({key}, {other}) }}", rel.name));
            bound.push("n");
        }
        let args: Vec<String> = (0..head.arity)
            .map(|column| {
                let carry = first_args
                    .get(column)
                    .filter(|arg| ["a", "b", "c"].contains(&arg.as_str()));
                if !bound.is_empty() && rng.percent(10) {
                    format!("({} + 1) % {DOMAIN}", rng.pick(&bound))
                } else if let Some(var) = carry
                    && rng.percent(50)
                {
                    var.clone()
                } else {
                    term(rng, &bound)
                }
            })
            .collect();
        format!(
            "{}({}) :- {}.\n",
            head.name,
            args.join(", "),
            body.join(", ")
        )
    }

    // -------------------------------------------------------------------------
    // Evaluator
    // -------------------------------------------------------------------------

    /// Every relation's rows.
    type Model = HashMap<String, BTreeSet<Vec<i64>>>;

    /// A rule's variables, bound to values.
    type Binding = BTreeMap<Variable, i64>;

    /// Rows read, each with its relation.
    type Reads = Vec<(String, Vec<i64>)>;

    /// One way a rule's positive atoms, aggregates, and comparisons hold:
    /// the head row it would derive, the rows it reads (negated rows
    /// probed, and every row an aggregate's braces could match), and
    /// whether its negations hold too, so that it fires.
    struct Candidate {
        head: Vec<i64>,
        reads: Reads,
        fires: bool,
    }

    /// The least model of `program`, a level at a time: each level's rules
    /// fire until nothing new is derived, so a negation or an aggregate
    /// only reads finished levels.
    fn evaluate(program: &Program, levels: &HashMap<String, usize>) -> Model {
        let mut model: Model = program
            .facts()
            .keys()
            .map(|name| (name.clone(), facts_of(program, name)))
            .collect();
        for level in 0..=4 {
            let rules: Vec<&FlowLogRule> = program
                .rules()
                .iter()
                .filter(|rule| levels[rule.head().name()] == level)
                .collect();
            loop {
                let derived: Reads = rules
                    .iter()
                    .flat_map(|rule| {
                        candidates(rule, &model)
                            .into_iter()
                            .filter(|candidate| candidate.fires)
                            .map(|candidate| (rule.head().name().to_string(), candidate.head))
                    })
                    .collect();
                let mut grew = false;
                for (name, row) in derived {
                    grew |= model.entry(name).or_default().insert(row);
                }
                if !grew {
                    break;
                }
            }
        }
        model
    }

    fn candidates(rule: &FlowLogRule, model: &Model) -> Vec<Candidate> {
        let mut partial: Vec<(Binding, Reads)> = vec![(Binding::new(), vec![])];
        for predicate in rule.rhs() {
            if let Predicate::PositiveAtom(atom) = predicate {
                partial = partial
                    .into_iter()
                    .flat_map(|(binding, reads)| {
                        matches(atom, model, &binding)
                            .into_iter()
                            .map(move |(binding, row)| {
                                let mut reads = reads.clone();
                                reads.push((atom.name().to_string(), row));
                                (binding, reads)
                            })
                    })
                    .collect();
            }
        }
        let mut candidates = Vec::new();
        'binding: for (mut binding, mut reads) in partial {
            for predicate in rule.rhs() {
                if let Predicate::Aggregate(aggregate) = predicate {
                    let (answer, read) = count(aggregate, model, &binding);
                    reads.extend(read);
                    let var = aggregate.var().expect("hoisting names every aggregate");
                    binding.insert(var.clone(), answer);
                }
            }
            for predicate in rule.rhs() {
                if let Predicate::Compare(comparison) = predicate
                    && !compare(comparison, &binding)
                {
                    continue 'binding;
                }
            }
            let mut fires = true;
            for predicate in rule.rhs() {
                if let Predicate::NegativeAtom(atom) = predicate {
                    let row: Vec<i64> = atom
                        .arguments()
                        .iter()
                        .map(|arg| match arg {
                            AtomArg::Var(var) => binding[var],
                            AtomArg::Const(constant) => value(constant),
                            AtomArg::Placeholder => panic!("the generator negates no `_`"),
                        })
                        .collect();
                    fires &= !model
                        .get(atom.name())
                        .is_some_and(|rows| rows.contains(&row));
                    reads.push((atom.name().to_string(), row));
                }
            }
            let head = rule
                .head()
                .head_arguments()
                .iter()
                .map(|arg| match arg {
                    HeadArg::Var(var) => binding[var],
                    HeadArg::Arith(arith) => evaluate_arith(arith, &binding),
                })
                .collect();
            candidates.push(Candidate { head, reads, fires });
        }
        candidates
    }

    /// The bindings extending `binding` under which `atom` matches a row,
    /// with that row.
    fn matches(atom: &Atom, model: &Model, binding: &Binding) -> Vec<(Binding, Vec<i64>)> {
        let Some(rows) = model.get(atom.name()) else {
            return Vec::new();
        };
        rows.iter()
            .filter_map(|row| {
                let mut extended = binding.clone();
                for (arg, &value_there) in atom.arguments().iter().zip(row) {
                    match arg {
                        AtomArg::Var(var) => {
                            if *extended.entry(var.clone()).or_insert(value_there) != value_there {
                                return None;
                            }
                        }
                        AtomArg::Const(constant) => {
                            if value(constant) != value_there {
                                return None;
                            }
                        }
                        AtomArg::Placeholder => {}
                    }
                }
                Some((extended, row.clone()))
            })
            .collect()
    }

    /// `count` of `aggregate`'s matches under `outer`, the distinct
    /// bindings of its body's variables, and every row its atoms could
    /// match there.
    fn count(aggregate: &AggregateExpr, model: &Model, outer: &Binding) -> (i64, Reads) {
        assert!(matches!(aggregate.operator(), AggregationOperator::Count));
        let mut found = vec![outer.clone()];
        let mut reads = Vec::new();
        for predicate in aggregate.body() {
            let Predicate::PositiveAtom(atom) = predicate else {
                panic!("the generator writes only atoms in braces");
            };
            for (_, row) in matches(atom, model, outer) {
                reads.push((atom.name().to_string(), row));
            }
            found = found
                .iter()
                .flat_map(|binding| matches(atom, model, binding))
                .map(|(binding, _)| binding)
                .collect();
        }
        let distinct: BTreeSet<Binding> = found.into_iter().collect();
        (i64::try_from(distinct.len()).expect("small count"), reads)
    }

    fn compare(comparison: &ComparisonExpr, binding: &Binding) -> bool {
        let left = evaluate_arith(comparison.left(), binding);
        let right = evaluate_arith(comparison.right(), binding);
        match comparison.operator() {
            ComparisonOperator::Equal => left == right,
            ComparisonOperator::NotEqual => left != right,
            ComparisonOperator::GreaterThan => left > right,
            ComparisonOperator::GreaterEqualThan => left >= right,
            ComparisonOperator::LessThan => left < right,
            ComparisonOperator::LessEqualThan => left <= right,
            ComparisonOperator::Match { .. } | ComparisonOperator::Contains { .. } => {
                panic!("the generator writes no string comparison")
            }
        }
    }

    /// Evaluates left to right: the parser has already grouped by
    /// precedence.
    fn evaluate_arith(arith: &Arithmetic, binding: &Binding) -> i64 {
        let factor = |factor: &Factor| match factor {
            Factor::Var(var) => binding[var],
            Factor::Const(constant) => value(constant),
            Factor::Group(inner) => evaluate_arith(inner, binding),
            other => panic!("the generator writes no {other:?}"),
        };
        arith
            .rest()
            .iter()
            .fold(factor(arith.init()), |left, (op, right)| match op {
                ArithmeticOperator::Plus => left + factor(right),
                ArithmeticOperator::Modulo => left % factor(right),
                other => panic!("the generator writes no {other:?}"),
            })
    }

    fn value(constant: &Constant) -> i64 {
        constant.text().parse().expect("an int32 constant")
    }
}
