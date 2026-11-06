"""Score grouping as projection onto the non-aggregated columns."""

from copy import deepcopy
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parent / 'binary-raw'))
from search_raw import RawModel, inversions, search


class AggregateModel(RawModel):
    def __init__(self, plan, metric='raw'):
        self.reductions = {}
        lowered = deepcopy(plan)
        for stratum in lowered['strata']:
            stratum['aggregates'] = []
            for tx in stratum['transformations']:
                aggregate = tx.get('aggregate')
                if aggregate is None:
                    continue
                name = f"s{stratum['index']}:{tx['output']['id']}"
                self.reductions[name] = aggregate['position']
                arity = sum(tx['output']['arity'])
                tx['key'] = []
                tx['value'] = [{'slot': [0, False, i]} for i in range(arity)]
                tx['arranged'] = True
        super().__init__(lowered, metric)

    def base_order(self, base, state):
        order = super().base_order(base, state)
        if base in self.reductions:
            position = self.reductions[base]
            return tuple(c for c in order if c != position) + (position,)
        return order

    def edge_cost(self, edge, state):
        if self.edges[edge][1] not in self.reductions:
            return super().edge_cost(edge, state)
        feed, target, _ = self.edges[edge]
        position = self.reductions[target]
        source = self.base_order(feed.base, state)
        retained = tuple(feed.references[c] for c in self.node_order(target, state)
                         if c != position and feed.references[c] is not None)
        return inversions(source, retained)


def optimize(plan, seeds=8):
    original = search.CostModel
    search.CostModel = AggregateModel
    try:
        result = search.optimize(plan, 'raw', seeds)
    finally:
        search.CostModel = original
    result['summary'].update(integer_scale=1, normalization='none; raw inversions',
                              aggregate_cost='projection onto non-aggregate columns',
                              baseline_score=result['summary']['baseline_cost'],
                              selected_score=result['summary']['selected_cost'])
    return result


if __name__ == '__main__':
    for program in ['cc', 'sssp']:
        path = ROOT / 'plans' / (program + '.json')
        started = time.perf_counter()
        result = optimize(json.loads(path.read_text()))
        result['summary']['seconds'] = time.perf_counter() - started
        destination = ROOT / 'selected/raw' / path.name
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(result, indent=2) + '\n')
        print(program, result['summary'], flush=True)
