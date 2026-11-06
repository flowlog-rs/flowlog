"""Select layouts by normalized Boolean inversions times upstream width."""

import argparse
from copy import deepcopy
from fractions import Fraction
from functools import lru_cache
import importlib.util
import json
from math import lcm
from pathlib import Path
import time

ROOT = Path(__file__).resolve().parent
JOINT = ROOT.parent
spec = importlib.util.spec_from_file_location(
    'structural_score', JOINT / 'scoring-exploration/optimizer.py')
previous = importlib.util.module_from_spec(spec)
spec.loader.exec_module(previous)
from binary_cost import inversions


@lru_cache(maxsize=None)
def maximum_inversions(source_fields, retained_fields):
    """Return the maximum over field orders before imposing K/V constraints.

    Projection keeps duplicate witnesses. Moving the retained fields to the
    front in reverse source order maximizes strict inversions; omitted fields
    keep their source order as a virtual suffix.
    """
    if not 0 <= retained_fields <= source_fields:
        raise ValueError('Retained field count exceeds source field count')
    return inversions(tuple(range(source_fields)),
                      tuple(range(source_fields - 1,
                                  source_fields - retained_fields - 1, -1)))


class WidthCost:
    def __init__(self, plan, width_basis='source'):
        if width_basis not in {'source', 'output'}:
            raise ValueError('Width basis must be source or output')
        self.width_basis = width_basis
        super().__init__(plan)
        self.widths = []
        self.denominators = []
        for index, (feed, _, _) in enumerate(self.edges):
            source = self.base_order(feed.base, self.initial)
            columns = self.sort_columns(index, self.initial)
            retained = {feed.references[c] for c in columns
                        if feed.references[c] is not None}
            self.widths.append(len(source) if width_basis == 'source' else len(columns))
            self.denominators.append(maximum_inversions(len(source), len(retained)))
        # A common integer scale preserves exact ties during greedy selection.
        self.scale = lcm(*(d for d in self.denominators if d))

    def sort_columns(self, index, state):
        target = self.edges[index][1]
        reduction = getattr(self, 'reductions', {}).get(target)
        return tuple(c for c in self.node_order(target, state) if c != reduction)

    def edge_cost(self, index, state):
        raw = super().edge_cost(index, state)
        denominator = self.denominators[index]
        if not denominator:
            return 0
        return raw * self.widths[index] * (self.scale // denominator)

    def edge_details(self, index, state):
        feed, target, kind = self.edges[index]
        source = self.base_order(feed.base, state)
        retained = tuple(feed.references[c] for c in self.sort_columns(index, state)
                         if feed.references[c] is not None)
        raw = inversions(source, retained)
        denominator = self.denominators[index]
        disorder = Fraction(raw, denominator) if denominator else Fraction(0)
        unique = tuple(dict.fromkeys(retained))
        virtual = unique + tuple(c for c in source if c not in unique)
        return {'source': feed.base, 'target': target, 'kind': kind,
                'source_order': source, 'retained_order': retained,
                'virtual_target': virtual,
                'source_fields': len(source), 'sort_width': self.widths[index],
                'inversions': raw, 'maximum_inversions': denominator,
                'sortedness': float(1 - disorder),
                'cost': float(Fraction(self.edge_cost(index, state), self.scale)),
                'unscored': index in self.opaque_edges}


class WidthModel(WidthCost, previous.reverse.ReverseModel):
    pass


class AggregateWidthModel(WidthCost, previous.reverse.ReverseAggregateModel):
    def __init__(self, plan, width_basis='source'):
        grouped = deepcopy(plan)
        # Both phase ownership passes must see the grouping sort boundary.
        for stratum in grouped['strata']:
            for tx in stratum['transformations']:
                if tx.get('aggregate'):
                    tx['arranged'] = True
        super().__init__(grouped, width_basis=width_basis)


def model_for(plan, width_basis='source'):
    aggregate = any(tx.get('aggregate') for s in plan['strata']
                    for tx in s['transformations'])
    return (AggregateWidthModel if aggregate else WidthModel)(plan, width_basis=width_basis)


def optimize(plan, width_basis='source'):
    model = model_for(plan, width_basis=width_basis)
    seeds = [(str(seed), model.seeded(seed)) for seed in range(8)]
    seeds.append(('producer', previous.producer_seed(model)))
    trials = []
    for seed, initial in seeds:
        state, phases = model.search_reverse(initial)
        trials.append({'seed': seed, 'priority': model.priority(state),
                       'changed_variables': sum(a != b for a, b in zip(state, model.initial)),
                       'state': state, 'phases': phases})
    winner = min(trials, key=lambda t: (*t['priority'], t['changed_variables']))
    state = winner['state']
    return {'metric': width_basis + '_fields * inversions / maximum_inversions',
            'width_basis': width_basis,
            'sortedness': '1 - inversions / maximum_inversions',
            'consumer_weight': 1, 'integer_scale': model.scale,
            'normalization': 'maximum over all field orders with the same source and retained arities',
            'seed': winner['seed'], 'selected_priority': winner['priority'],
            'baseline_scores': [float(Fraction(v, model.scale))
                                for v in model.priority(model.initial)],
            'selected_scores': [float(Fraction(v, model.scale)) for v in winner['priority']],
            'selected': model.describe(state), 'trials': trials,
            'edges': [model.edge_details(i, state) for i in range(len(model.edges))]}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--programs', nargs='+', default=['doop', 'z3', 'tc'])
    parser.add_argument('--width-basis', choices=['source', 'output'], default='source')
    parser.add_argument('--data', type=Path,
                        default=Path('layout-output'))
    args = parser.parse_args()
    for program in args.programs:
        path = JOINT / 'binary-full/plans' / (program + '.json')
        output = args.data / 'selected/normalized-width' / path.name
        if output.exists():
            saved = json.loads(output.read_text())
            if saved.get('width_basis', 'output') != args.width_basis:
                raise ValueError('Saved selection uses a different width basis')
            print('Already selected', program, flush=True)
            continue
        print('Search', program, flush=True)
        started = time.monotonic()
        result = optimize(json.loads(path.read_text()), width_basis=args.width_basis)
        result.update(seconds=time.monotonic() - started,
                      plan_sha256=previous.verifier.sha(path))
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, indent=2) + '\n')
        print(program, 'seed', result['seed'], 'seconds', round(result['seconds'], 2),
              'scores', result['selected_scores'], flush=True)


if __name__ == '__main__':
    main()
