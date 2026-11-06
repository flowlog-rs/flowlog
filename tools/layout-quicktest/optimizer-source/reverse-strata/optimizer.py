"""Optimize later strata first and freeze their complete input interfaces."""

import argparse
import importlib.util
import json
from pathlib import Path
import time

ROOT = Path(__file__).resolve().parent
JOINT = ROOT.parent
spec = importlib.util.spec_from_file_location('recursive_first_optimizer', JOINT / 'recursive-first/optimizer.py')
previous = importlib.util.module_from_spec(spec)
spec.loader.exec_module(previous)
RecursiveFirstModel = previous.RecursiveFirstModel
RecursiveFirstAggregateModel = previous.RecursiveFirstAggregateModel


class ReverseStrata:
    def __init__(self, plan, recursive_first=True):
        super().__init__(plan)
        ownership = []
        for stratum in plan['strata']:
            local = {tx['output']['id']: tx for tx in stratum['transformations']}
            for tx in stratum['transformations']:
                if tx['arranged'] or len(tx['inputs']) == 2:
                    ownership.append((stratum['index'], tx['recursive'],
                                      f"s{stratum['index']}:{tx['output']['id']}"))
            for relation, heads in sorted(stratum['heads'].items()):
                for head in heads:
                    ownership.append((stratum['index'], local[head]['recursive'], 'r:' + relation))
        if len(ownership) != len(self.edges) or any(owner[2] != edge[1] for owner, edge in zip(ownership, self.edges)):
            raise ValueError('Stratum ownership disagrees with materialization boundaries')
        self.edge_strata = [owner[0] for owner in ownership]
        self.phases = []
        for index in sorted({s['index'] for s in plan['strata']}, reverse=True):
            for recursive in ([True, False] if recursive_first else [None]):
                edges = {i for i, owner in enumerate(ownership)
                         if owner[0] == index and (recursive is None or owner[1] == recursive)}
                if edges:
                    self.phases.append((index, recursive, edges))

    def priority(self, state):
        costs = self.costs(state)
        return tuple(sum(costs[i] for i in edges) for _, _, edges in self.phases)

    def interface(self, edges):
        result = set()
        for i in edges:
            feed, target, _ = self.edges[i]
            result.update(self.base_variables(feed.base))
            result.update(self.node_variables(target))
        return result

    def optimize_phase(self, state, objective, frozen):
        costs = self.costs(state)
        history = [sum(costs[i] for i in objective)]
        while True:
            best = None
            gain = 0
            for changes in self.candidates(state, True):
                if any(v in frozen for v, _ in changes):
                    continue
                relevant = set().union(*(self.incident[v] for v, _ in changes)) & objective
                if not relevant:
                    continue
                candidate = list(state)
                for v, order in changes:
                    candidate[v] = order
                updates = {i: self.edge_cost(i, candidate) for i in relevant}
                improvement = sum(costs[i] - value for i, value in updates.items())
                if improvement > gain:
                    gain, best = improvement, (tuple(candidate), updates)
            if best is None:
                self.validate(state)
                return state, history
            state, updates = best
            for i, value in updates.items():
                costs[i] = value
            history.append(history[-1] - gain)

    def search_reverse(self, state):
        frozen = set()
        phases = []
        for index, recursive, edges in self.phases:
            before = state
            state, history = self.optimize_phase(state, edges, frozen)
            if any(state[v] != before[v] for v in frozen):
                raise ValueError('Earlier optimization changed a later stratum interface')
            frozen.update(self.interface(edges))
            phases.append({'stratum': index, 'recursive': recursive, 'history': history,
                           'frozen_variables': sorted(frozen), 'state': state})
        priority = self.priority(state)
        state = list(state)
        for v in sorted(self.active - frozen):
            previous = state[v]
            state[v] = self.initial[v]
            if self.priority(state) != priority:
                state[v] = previous
        self.validate(state)
        return tuple(state), phases


class ReverseModel(ReverseStrata, RecursiveFirstModel):
    pass


class ReverseAggregateModel(ReverseStrata, RecursiveFirstAggregateModel):
    pass


def optimize(plan, recursive_first=True, seeds=8):
    aggregate = any(tx.get('aggregate') for s in plan['strata'] for tx in s['transformations'])
    model = (ReverseAggregateModel if aggregate else ReverseModel)(plan, recursive_first)
    trials = []
    for seed in range(seeds):
        state, phases = model.search_reverse(model.seeded(seed))
        changes = sum(a != b for a, b in zip(state, model.initial))
        trials.append({'seed': seed, 'priority': model.priority(state),
                       'changed_variables': changes, 'phases': phases, 'state': state})
    winner = min(trials, key=lambda t: (*t['priority'], t['changed_variables']))
    selected = winner['state']
    return {'policy': 'reverse-stratum; freeze all incident interfaces', 'recursive_first_within_stratum': recursive_first,
        'seed': winner['seed'], 'priority_phases': [(i, r) for i, r, _ in model.phases],
        'baseline_priority': model.priority(model.initial), 'selected_priority': winner['priority'],
        'baseline_global_scores': model.scores(model.initial), 'selected_global_scores': model.scores(selected),
        'selected': model.describe(selected), 'trials': trials}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--programs', nargs='+', default=['doop', 'tc', 'z3'])
    args = parser.parse_args()
    for name in args.programs:
        plan = json.loads((JOINT / 'binary-full/plans' / (name + '.json')).read_text())
        for mode, recursive_first in [('recursive-first', True), ('combined', False)]:
            destination = ROOT / 'selected' / mode / (name + '.json')
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists():
                continue
            started = time.time()
            result = optimize(plan, recursive_first)
            result['seconds'] = time.time() - started
            destination.write_text(json.dumps(result, indent=2) + '\n')
            vpt = result['selected']['relations'].get('e6cf50bbe008df61')
            print(name, mode, 'scores', result['selected_global_scores'], 'VPT', vpt,
                  'priority', result['selected_priority'], flush=True)


if __name__ == '__main__':
    main()
