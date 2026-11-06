"""Select legal physical layouts by Boolean-record inversions per input row."""

import argparse
import hashlib
import json
from pathlib import Path
import sys
import time

from binary_cost import inversions

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parent / 'prefix-comparison'))
import search


class BinaryModel(search.CostModel):
    def __init__(self, plan, metric='binary'):
        super().__init__(plan, metric)
        self.scale = 1 << max(map(len, self.alphabets), default=0)

    def edge_cost(self, edge, state):
        if edge in self.opaque_edges:
            return 0
        feed, target, _ = self.edges[edge]
        source = self.base_order(feed.base, state)
        retained = tuple(feed.references[c] for c in self.node_order(target, state)
                         if feed.references[c] is not None)
        return inversions(source, retained) * (self.scale >> len(source))


def optimize(plan, seeds=8):
    original = search.CostModel
    search.CostModel = BinaryModel
    try:
        result = search.optimize(plan, 'binary', seeds)
    finally:
        search.CostModel = original
    summary = result['summary']
    scale = 1 << max(map(int, summary['source_arities']), default=0)
    summary.update(integer_scale=scale, normalization='inversions / 2**source_arity',
                   baseline_score=summary['baseline_cost'] / scale,
                   selected_score=summary['selected_cost'] / scale)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--programs', nargs='+', default=['doop', 'z3', 'csda'])
    parser.add_argument('--plan', type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--seeds', type=int, default=8)
    args = parser.parse_args()
    if args.plan is not None and args.output is None:
        parser.error('--plan requires --output')
    if args.plan is None and args.output is not None:
        parser.error('--output requires --plan')
    jobs = [(args.plan, args.output)] if args.plan else [
        (ROOT.parent / 'main-comparison/plans' / (program + '.json'),
         ROOT / 'selected/binary' / (program + '.json')) for program in args.programs]
    for path, output in jobs:
        started = time.perf_counter()
        result = optimize(json.loads(path.read_text()), args.seeds)
        result['summary'].update(program=path.stem, seconds=time.perf_counter()-started,
                                 plan_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps(result['summary']), flush=True)


if __name__ == '__main__':
    main()
