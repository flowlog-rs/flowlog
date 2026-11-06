"""Apply alternative layout costs to the same frozen main plans."""

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parent))
from real_plan_search import PlanModel
from costs import distance


class CostModel(PlanModel):
    def __init__(self, plan, metric):
        super().__init__(plan)
        self.metric = metric
        self.alphabets = [{field: i for i, field in enumerate(self.base_order(feed.base, self.initial))}
                          for feed, _, _ in self.edges]

    def edge_cost(self, edge, state):
        if edge in self.opaque_edges:
            return 0
        feed, target, _ = self.edges[edge]
        alphabet = self.alphabets[edge]
        source = tuple(alphabet[field] for field in self.base_order(feed.base, state))
        output = tuple(alphabet[field] for field in self.completed_target(feed, target, state))
        return distance(source, output, self.metric)

    def restore_ties(self, state):
        state = list(state)
        current = sum(self.costs(state))
        changed = True
        while changed:
            changed = False
            for variable in sorted(self.active):
                if state[variable] == self.initial[variable]:
                    continue
                old = state[variable]
                state[variable] = self.initial[variable]
                if sum(self.costs(state)) == current:
                    changed = True
                else:
                    state[variable] = old
        self.validate(state)
        return tuple(state)


def optimize(plan, metric, seeds=8):
    model = CostModel(plan, metric)
    trials = []
    best = None
    for seed in range(seeds):
        state, history, evaluations = model.search(model.seeded(seed), True)
        state = model.restore_ties(state)
        changes = sum(a != b for a, b in zip(state, model.initial))
        key = (history[-1], changes)
        trials.append({'seed': seed, 'cost': history[-1], 'changed_variables': changes,
                       'history': history, 'evaluations': evaluations})
        if best is None or key < best[0]:
            best = (key, seed, state)
    state = best[2]
    details = []
    for index, (feed, target, kind) in enumerate(model.edges):
        details.append({'kind': kind, 'source': feed.base, 'target': target,
                        'baseline_cost': model.edge_cost(index, model.initial),
                        'cost': model.edge_cost(index, state),
                        'source_order': model.base_order(feed.base, state),
                        'completed_target': model.completed_target(feed, target, state)})
    return {
        'summary': {'metric': metric, 'baseline_cost': sum(model.costs(model.initial)),
                    'selected_cost': best[0][0], 'seed': best[1],
                    'changed_variables': best[0][1],
                    'changed_layouts': sum(model.node_order(n, state) != model.node_order(n, model.initial)
                                           for n in model.nodes),
                    'swapped_joins': sum(state[j['side']] != (0,) for j in model.joins.values() if not j['anti']),
                    'source_arities': dict(Counter(len(a) for a in model.alphabets)),
                    'unscored_boundaries': len(model.opaque_edges)},
        'selected': model.describe(state), 'trials': trials, 'edges': details,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--metrics', nargs='+', default=['inversion', 'lex_steps', 'relative_lex', 'prefix'])
    parser.add_argument('--programs', nargs='+', default=['doop', 'cvc5', 'polonius_str', 'andersen', 'sg'])
    parser.add_argument('--seeds', type=int, default=8)
    args = parser.parse_args()
    for metric in args.metrics:
        directory = ROOT / 'selected' / metric
        directory.mkdir(parents=True, exist_ok=True)
        for program in args.programs:
            start = time.perf_counter()
            path = ROOT.parent / 'main-comparison/plans' / (program + '.json')
            result = optimize(json.loads(path.read_text()), metric, args.seeds)
            result['summary'].update(program=program, seconds=time.perf_counter()-start,
                                     plan_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
            (directory / (program + '.json')).write_text(json.dumps(result, indent=2) + '\n')
            print(json.dumps(result['summary']), flush=True)


if __name__ == '__main__':
    main()
