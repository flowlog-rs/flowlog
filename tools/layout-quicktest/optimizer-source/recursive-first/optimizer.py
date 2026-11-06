"""Optimize recursive layouts before fixing their non-recursive producers."""

from collections import Counter
import hashlib
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parent
JOINT = ROOT.parent
sys.path.insert(0, str(JOINT / 'binary-full'))
from prepare import AggregateModel, RawModel


class TwoPhase:
    def __init__(self, plan, metric='raw'):
        super().__init__(plan, metric)
        categories = []
        for stratum in self.plan['strata']:
            local = {tx['output']['id']: tx for tx in stratum['transformations']}
            for tx in stratum['transformations']:
                if tx['arranged'] or len(tx['inputs']) == 2:
                    categories.append((f"s{stratum['index']}:{tx['output']['id']}",
                                       tx['recursive']))
            for relation, heads in sorted(stratum['heads'].items()):
                for head in heads:
                    if head not in local:
                        raise ValueError('Expected a local head producer: ' + head)
                    categories.append(('r:' + relation, local[head]['recursive']))
        if len(categories) != len(self.edges) or any(
                target != edge[1] for (target, _), edge in zip(categories, self.edges)):
            raise ValueError('Recursive classification disagrees with sort boundaries')
        self.recursive_edges = {i for i, (_, recursive) in enumerate(categories) if recursive}
        self.non_recursive_edges = set(range(len(self.edges))) - self.recursive_edges
        self.recursive_interface = set()
        for index in self.recursive_edges:
            feed, target, _ = self.edges[index]
            self.recursive_interface.update(self.base_variables(feed.base))
            self.recursive_interface.update(self.node_variables(target))

    def scores(self, state):
        costs = self.costs(state)
        return (sum(costs[i] for i in self.recursive_edges),
                sum(costs[i] for i in self.non_recursive_edges))

    def greedy_phase(self, state, recursive):
        objective = self.recursive_edges if recursive else self.non_recursive_edges
        frozen = set() if recursive else self.recursive_interface
        costs = self.costs(state)
        history = [self.scores(state)]
        evaluations = 0
        while True:
            best = None
            best_gain = 0
            for changes in self.candidates(state, True):
                if any(variable in frozen for variable, _ in changes):
                    continue
                touched = set().union(*(self.incident[v] for v, _ in changes))
                relevant = touched & objective
                if not relevant:
                    continue
                temporary = list(state)
                for variable, order in changes:
                    temporary[variable] = order
                new_costs = {i: self.edge_cost(i, temporary) for i in relevant}
                evaluations += 1
                gain = sum(costs[i] - value for i, value in new_costs.items())
                if gain > best_gain:
                    best_gain = gain
                    best = (tuple(temporary), touched, new_costs)
            if best is None:
                self.validate(state)
                if self.scores(state) != history[-1]:
                    raise ValueError('Two-phase incremental scores disagree')
                return state, history, evaluations
            state, touched, new_costs = best
            for index in touched:
                costs[index] = new_costs[index] if index in new_costs else self.edge_cost(index, state)
            history.append((sum(costs[i] for i in self.recursive_edges),
                            sum(costs[i] for i in self.non_recursive_edges)))

    def restore_non_recursive_ties(self, state):
        current = self.scores(state)
        state = list(state)
        changed = True
        while changed:
            changed = False
            for variable in sorted(self.active - self.recursive_interface):
                if state[variable] == self.initial[variable]:
                    continue
                previous = state[variable]
                state[variable] = self.initial[variable]
                if self.scores(state) == current:
                    changed = True
                else:
                    state[variable] = previous
        self.validate(state)
        return tuple(state)


class RecursiveFirstModel(TwoPhase, RawModel):
    pass


class RecursiveFirstAggregateModel(TwoPhase, AggregateModel):
    pass


def optimize(plan, seeds=8):
    aggregate = any(tx.get('aggregate') for s in plan['strata'] for tx in s['transformations'])
    model = (RecursiveFirstAggregateModel if aggregate else RecursiveFirstModel)(plan)
    best = None
    trials = []
    for seed in range(seeds):
        recursive, first, first_evals = model.greedy_phase(model.seeded(seed), True)
        state, second, second_evals = model.greedy_phase(recursive, False)
        state = model.restore_non_recursive_ties(state)
        if any(recursive[v] != state[v] for v in model.recursive_interface):
            raise ValueError('Non-recursive pass changed a recursive interface')
        score = model.scores(state)
        changes = sum(a != b for a, b in zip(state, model.initial))
        key = (*score, changes)
        trials.append({'seed': seed, 'recursive_history': first,
            'non_recursive_history': second, 'recursive_state': recursive, 'final_state': state,
            'recursive_cost': score[0], 'non_recursive_cost': score[1],
            'changed_variables': changes, 'evaluations': first_evals + second_evals})
        if best is None or key < best[0]:
            best = (key, seed, state)
    if best is None:
        raise ValueError('Expected at least one search seed')
    key, seed, state = best
    before = model.scores(model.initial)
    details = []
    for index, (feed, target, kind) in enumerate(model.edges):
        details.append({'kind': kind, 'source': feed.base, 'target': target,
            'recursive': index in model.recursive_edges,
            'baseline_cost': model.edge_cost(index, model.initial),
            'cost': model.edge_cost(index, state),
            'source_order': model.base_order(feed.base, state),
            'completed_target': model.completed_target(feed, target, state)})
    return {'summary': {'policy': 'recursive-first; freeze interfaces; non-recursive second',
        'metric': 'raw', 'integer_scale': 1, 'normalization': 'none; raw inversions',
        'aggregate_cost': 'projection onto non-aggregate columns' if aggregate else None,
        'baseline_cost': sum(before), 'selected_cost': key[0] + key[1],
        'baseline_recursive_cost': before[0], 'selected_recursive_cost': key[0],
        'baseline_non_recursive_cost': before[1], 'selected_non_recursive_cost': key[1],
        'recursive_boundaries': len(model.recursive_edges),
        'non_recursive_boundaries': len(model.non_recursive_edges),
        'seed': seed, 'changed_variables': key[2],
        'changed_layouts': sum(model.node_order(n, state) != model.node_order(n, model.initial)
                               for n in model.nodes),
        'swapped_joins': sum(state[j['side']] != (0,) for j in model.joins.values() if not j['anti']),
        'source_arities': dict(Counter(len(a) for a in model.alphabets)),
        'unscored_boundaries': len(model.opaque_edges)},
        'recursive_interface_variables': sorted(model.recursive_interface),
        'selected': model.describe(state), 'trials': trials, 'edges': details}


def main():
    destination = ROOT / 'selected'
    destination.mkdir(exist_ok=True)
    for plan_path in sorted((JOINT / 'binary-full/plans').glob('*.json')):
        path = destination / plan_path.name
        if path.exists():
            continue
        started = time.perf_counter()
        result = optimize(json.loads(plan_path.read_text()))
        result['summary'].update(program=plan_path.stem,
            plan_sha256=hashlib.sha256(plan_path.read_bytes()).hexdigest(),
            seconds=time.perf_counter() - started)
        path.write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps(result['summary']), flush=True)


if __name__ == '__main__':
    main()
