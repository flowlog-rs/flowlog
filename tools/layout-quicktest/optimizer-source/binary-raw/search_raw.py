"""Select layouts using unnormalized Boolean-record inversion counts."""

import argparse
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parent
MODEL = ROOT.parent / 'binary-layout'
sys.path.insert(0, str(MODEL))
from binary_cost import inversions
from search_binary import BinaryModel, search


class RawModel(BinaryModel):
    def __init__(self, plan, metric='raw'):
        super().__init__(plan, metric)
        self.scale = 1

    def edge_cost(self, edge, state):
        if edge in self.opaque_edges:
            return 0
        feed, target, _ = self.edges[edge]
        source = self.base_order(feed.base, state)
        retained = tuple(feed.references[c] for c in self.node_order(target, state)
                         if feed.references[c] is not None)
        return inversions(source, retained)


def optimize(plan, seeds=8):
    original = search.CostModel
    search.CostModel = RawModel
    try:
        result = search.optimize(plan, 'raw', seeds)
    finally:
        search.CostModel = original
    summary = result['summary']
    summary.update(integer_scale=1, normalization='none; raw inversions',
                   baseline_score=summary['baseline_cost'],
                   selected_score=summary['selected_cost'])
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--programs', nargs='+', default=[
        'sg', 'andersen', 'polonius_str', 'cspa', 'csda', 'cvc5', 'doop', 'z3'])
    args = parser.parse_args()
    for program in args.programs:
        path = ROOT.parent / 'main-comparison/plans' / (program + '.json')
        destination = ROOT / 'selected/raw' / (program + '.json')
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            continue
        started = time.perf_counter()
        result = optimize(json.loads(path.read_text()))
        import hashlib
        result['summary'].update(program=program, seconds=time.perf_counter()-started,
                                 plan_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
        destination.write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps(result['summary']), flush=True)


if __name__ == '__main__':
    main()
