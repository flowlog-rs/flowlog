"""Compare bounded structural penalties and index-consumer weights."""

import argparse
from collections import Counter
import importlib.util
import json
from pathlib import Path
import time

ROOT = Path(__file__).resolve().parent
JOINT = ROOT.parent
spec = importlib.util.spec_from_file_location('reverse_optimizer', JOINT / 'reverse-strata/optimizer.py')
reverse = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reverse)
spec = importlib.util.spec_from_file_location('layout_verifier', JOINT / 'prefix-comparison/verify.py')
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)
SCALE = 1_000_000


class StructuralScore(reverse.ReverseModel):
    def __init__(self, plan, policy):
        self.policy = policy
        super().__init__(plan)
        uses = Counter(inp for join in self.joins.values() for inp in set(join['inputs']))
        # Consumer count is an importance proxy, not a count of trace builds.
        # It can overvalue a small index shared by many inexpensive joins.
        self.weights = [max(1, uses[target]) for _, target, _ in self.edges]
        self.primary_scale = SCALE * (sum(self.weights) + 1)

    def edge_cost(self, index, state):
        raw = super().edge_cost(index, state)
        if self.policy == 'raw':
            return raw
        # Saturation limits the influence of an already disordered wide edge.
        # This sacrifices distinctions between large inversion counts.
        bounded = round(SCALE * raw / (raw + 1))
        if self.policy == 'bounded':
            return bounded
        weighted = self.weights[index] * bounded
        if self.policy == 'bounded-reuse':
            return weighted
        if self.policy == 'nonzero-reuse':
            return self.weights[index] * int(raw != 0) * self.primary_scale + weighted
        raise ValueError('Unknown structural score: ' + self.policy)


def producer_seed(model):
    state = list(model.initial)
    choices = {}
    consumed = {inp['id'] for stratum in model.plan['strata']
                for tx in stratum['transformations'] for inp in tx['inputs']}
    for stratum in model.plan['strata']:
        local = {tx['output']['id']: tx for tx in stratum['transformations']}
        for relation, heads in stratum['heads'].items():
            candidates = []
            for head in heads:
                tx = local[head]
                if tx['kind'] not in ['Join', 'SemiJoin']:
                    continue
                slots = [value['slot'] for value in tx['value']]
                if not slots or any(slot is None for slot in slots):
                    continue
                order = tuple(sorted(range(len(slots)), key=lambda i:
                    (0 if slots[i][1] else 1 + slots[i][0], slots[i][2], i)))
                mixed = len({s[0] for s in slots if not s[1]}) == 2
                candidates.append((int(mixed), order))
            if candidates:
                priority = max(rank for rank, _ in candidates)
                choices[relation] = next(order for rank, order in candidates if rank == priority)

    def assign(name, order):
        node = model.nodes[name]
        keys = node['arity'][0]
        for variable, block in zip(node['variables'],
                [tuple(order[:keys]), tuple(c-keys for c in order[keys:])]):
            if variable not in model.pinned:
                state[variable] = block

    for relation, order in choices.items():
        assign('r:' + relation, order)
    for stratum in model.plan['strata']:
        heads = {}
        for relation, inputs in stratum['heads'].items():
            for head in inputs:
                heads.setdefault(head, []).append(relation)
        for head, relations in heads.items():
            name = f"s{stratum['index']}:{head}"
            desired = {choices[r] for r in relations if r in choices}
            if name in model.nodes and head not in consumed and len(desired) == 1 and \
               all(r in choices for r in relations):
                assign(name, next(iter(desired)))
    model.validate(state)
    return tuple(state)


def optimize(plan, policy):
    model = StructuralScore(plan, policy)
    trials = []
    initial_states = [(str(seed), model.seeded(seed)) for seed in range(8)]
    initial_states.append(('producer', producer_seed(model)))
    for seed, initial in initial_states:
        state, phases = model.search_reverse(initial)
        trials.append({'seed': seed, 'priority': model.priority(state),
                       'changed_variables': sum(a != b for a, b in zip(state, model.initial)),
                       'state': state, 'phases': phases})
    winner = min(trials, key=lambda t: (*t['priority'], t['changed_variables']))
    return {'policy': policy, 'integer_scale': SCALE, 'weights': model.weights,
            'primary_scale': model.primary_scale, 'seed': winner['seed'],
            'selected_priority': winner['priority'], 'trials': trials,
            'selected': model.describe(winner['state']),
            'producer_seed': model.describe(initial_states[-1][1])}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--programs', nargs='+', default=['doop'])
    parser.add_argument('--policies', nargs='+', default=['raw', 'bounded-reuse', 'nonzero-reuse'])
    args = parser.parse_args()
    for program in args.programs:
        plan = json.loads((JOINT / 'binary-full/plans' / (program + '.json')).read_text())
        for policy in args.policies:
            destination = ROOT / 'selected' / policy / (program + '.json')
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists():
                continue
            started = time.monotonic()
            result = optimize(plan, policy)
            result['seconds'] = time.monotonic() - started
            result['plan_sha256'] = verifier.sha(JOINT / 'binary-full/plans' / (program + '.json'))
            destination.write_text(json.dumps(result, indent=2) + '\n')
            print(program, policy, 'seed', result['seed'], 'seconds', round(result['seconds'], 2),
                  'VPT', result['selected']['relations'].get('e6cf50bbe008df61'), flush=True)


if __name__ == '__main__':
    main()
