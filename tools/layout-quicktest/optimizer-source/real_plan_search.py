"""Search complete benchmark plans with unit-weight inversion costs."""

import argparse
from collections import defaultdict
import csv
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import time

from greedy_search import distance, insertions


ROOT = Path(__file__).resolve().parent


@dataclass(frozen=True)
class Feed:
    """References into an ordering origin, including projected-away columns.

    Pure maps compose references and retain the origin. Completion occurs
    once at the following sort, so consecutive projections share one V'.
    """

    base: str
    references: tuple


class PlanModel:
    def __init__(self, plan):
        self.plan = plan
        self.nodes = {}
        self.joins = {}
        self.edges = []
        self.feeds = {}
        self.parent = {}
        self.blocks = {}
        self.constants = []
        self.opaque = []
        self.relations = {r['id']: r for r in plan['relations']}
        self.owners = {}
        self.transactions = {}
        self.inputs = {}
        for relation in plan['relations']:
            self.node('r:' + relation['id'], (0, len(relation['attributes'])),
                      relation['name'])
        for stratum in plan['strata']:
            if stratum['aggregates']:
                raise ValueError('Aggregate layouts need a separate contract')
            local = {tx['output']['id']: f"s{stratum['index']}:{tx['output']['id']}"
                     for tx in stratum['transformations']}
            previous = dict(self.owners)

            def resolve(identifier):
                if identifier in local:
                    return local[identifier]
                if identifier in previous:
                    return previous[identifier]
                if identifier in self.relations:
                    return 'r:' + identifier
                raise ValueError('Unresolved collection: ' + identifier)

            for tx in stratum['transformations']:
                name = local[tx['output']['id']]
                self.transactions[name] = tx
                inputs = [resolve(inp['id']) for inp in tx['inputs']]
                self.inputs[name] = inputs
                outputs = tx['key'] + tx['value']
                if len(inputs) == 1:
                    feed = self.feeds[inputs[0]]
                    references = []
                    for expression in outputs:
                        slot = expression['slot']
                        if slot is None:
                            if not expression['constant']:
                                raise ValueError('Computed projection needs a cost contract')
                            self.constants.append((name, expression['expression']))
                            references.append(None)
                        else:
                            _, key, index = slot
                            offset = 0 if key else tx['inputs'][0]['arity'][0]
                            references.append(feed.references[offset + index])
                    source = Feed(feed.base, tuple(references))
                else:
                    left, right = inputs
                    for inp, description in zip(inputs, tx['inputs']):
                        if inp not in self.nodes:
                            raise ValueError('Join input is not arranged: ' + inp)
                        if self.nodes[inp]['arity'] != tuple(description['arity']):
                            raise ValueError('Join input shape disagrees')
                    if tx['inputs'][0]['arity'][0] != tx['inputs'][1]['arity'][0]:
                        raise ValueError('Join key arities disagree')
                    self.unite((left, 0), (right, 0))
                    anti = tx['kind'].startswith('Anti')
                    join = {'inputs': inputs, 'output': name, 'anti': anti,
                            'label': tx['output']['label']}
                    self.joins[name] = join
                    references = []
                    for expression in outputs:
                        slot = expression['slot']
                        if slot is None:
                            if not expression['constant']:
                                self.opaque.append((name, expression['expression']))
                            else:
                                self.constants.append((name, expression['expression']))
                            references.append(None)
                            continue
                        side, key, index = slot
                        if anti:
                            side = 1
                        references.append(('k', index) if key else (side, index))
                    source = Feed('j:' + name, tuple(references))
                if tx['arranged'] or len(inputs) == 2:
                    self.node(name, tuple(tx['output']['arity']), tx['output']['label'])
                    self.edges.append((source, name, 'join-output' if len(inputs) == 2
                                       else 'arrangement'))
                else:
                    self.feeds[name] = source
                if not tx['recursive']:
                    self.owners[tx['output']['id']] = name
            for relation, heads in sorted(stratum['heads'].items()):
                for head in heads:
                    source = self.feeds[resolve(head)]
                    target = 'r:' + relation
                    if len(source.references) != self.nodes[target]['arity'][1]:
                        raise ValueError('Head and relation arities disagree')
                    self.edges.append((source, target, 'head-union'))
        roots = sorted({self.find(block) for block in self.blocks})
        self.variable_ids = {root: i for i, root in enumerate(roots)}
        self.initial = [tuple(range(self.blocks[root])) for root in roots]
        self.variable_blocks = defaultdict(list)
        self.labels = []
        for root in roots:
            self.labels.append(str(root))
        for block, arity in self.blocks.items():
            variable = self.variable_ids[self.find(block)]
            if len(self.initial[variable]) != arity:
                raise ValueError('Linked blocks have different arities')
            self.variable_blocks[variable].append(block)
            self.nodes[block[0]]['variables'][block[1]] = variable
        self.side_variables = set()
        for name, join in self.joins.items():
            if not join['anti']:
                join['side'] = len(self.initial)
                self.labels.append('orientation:' + name)
                self.side_variables.add(len(self.initial))
                self.initial.append((0,))
        self.initial = tuple(self.initial)
        self.incident = defaultdict(set)
        for index, (source, target, _) in enumerate(self.edges):
            for variable in self.base_variables(source.base) | self.node_variables(target):
                self.incident[variable].add(index)
        self.pinned = set()
        self.opaque_edges = set()
        opaque_nodes = {name for name, _ in self.opaque}
        for index, (source, target, _) in enumerate(self.edges):
            if target in opaque_nodes:
                self.pinned.update(self.base_variables(source.base) | self.node_variables(target))
                self.opaque_edges.add(index)
        for identifier, relation in self.relations.items():
            if relation.get('lattice'):
                self.pinned.update(self.node_variables('r:' + identifier))
        self.active = {variable for variable in self.incident
                       if (variable in self.side_variables or len(self.initial[variable]) > 1)
                       and variable not in self.pinned}

    def node(self, name, arity, label):
        if name in self.nodes:
            raise ValueError('Duplicate materialized collection')
        self.nodes[name] = {'arity': arity, 'label': label, 'variables': [None, None]}
        for index, size in enumerate(arity):
            block = (name, index)
            self.parent[block] = block
            self.blocks[block] = size
        self.feeds[name] = Feed(name, tuple(range(sum(arity))))

    def find(self, block):
        if self.parent[block] != block:
            self.parent[block] = self.find(self.parent[block])
        return self.parent[block]

    def unite(self, left, right):
        a, b = self.find(left), self.find(right)
        if a != b:
            self.parent[max(a, b)] = min(a, b)

    def node_variables(self, name):
        return set(self.nodes[name]['variables'])

    def base_variables(self, base):
        if base.startswith('j:'):
            join = self.joins[base[2:]]
            result = set().union(*(self.node_variables(inp) for inp in join['inputs']))
            if not join['anti']:
                result.add(join['side'])
            return result
        return self.node_variables(base)

    def node_order(self, name, state):
        node = self.nodes[name]
        key, value = node['variables']
        return state[key] + tuple(node['arity'][0] + c for c in state[value])

    def base_order(self, base, state):
        if not base.startswith('j:'):
            return self.node_order(base, state)
        join = self.joins[base[2:]]
        left, right = [self.nodes[inp] for inp in join['inputs']]
        key = tuple(('k', c) for c in state[left['variables'][0]])
        if join['anti']:
            return key + tuple((1, c) for c in state[right['variables'][1]])
        blocks = [tuple((side, c) for c in state[node['variables'][1]])
                  for side, node in enumerate([left, right])]
        if state[join['side']] == (1,):
            blocks.reverse()
        return key + blocks[0] + blocks[1]

    def completed_target(self, feed, target, state):
        """Append all omitted origin columns in the origin's selected order."""
        source = self.base_order(feed.base, state)
        retained = list(dict.fromkeys(feed.references[c] for c in self.node_order(target, state)
                                      if feed.references[c] is not None))
        if not set(retained) <= set(source):
            raise ValueError('Projection references an absent source column')
        return tuple(retained) + tuple(c for c in source if c not in retained)

    def edge_cost(self, edge, state):
        if edge in self.opaque_edges:
            return 0
        feed, target, _ = self.edges[edge]
        return distance(self.base_order(feed.base, state),
                        self.completed_target(feed, target, state))

    def costs(self, state):
        return [self.edge_cost(index, state) for index in range(len(self.edges))]

    def validate(self, state):
        for variable, original in enumerate(self.initial):
            if variable in self.pinned and state[variable] != original:
                raise ValueError('Opaque expression boundary was changed')
            if variable in self.side_variables:
                if state[variable] not in [(0,), (1,)]:
                    raise ValueError('Invalid join orientation')
            elif sorted(state[variable]) != sorted(original):
                raise ValueError('Invalid block permutation')
        for join in self.joins.values():
            left, right = [self.nodes[inp] for inp in join['inputs']]
            if state[left['variables'][0]] != state[right['variables'][0]]:
                raise ValueError('Join key order mismatch')
        for name in self.nodes:
            keys = self.nodes[name]['arity'][0]
            order = self.node_order(name, state)
            if set(order[:keys]) != set(range(keys)):
                raise ValueError('Arrangement key is not a prefix')
        for feed, target, _ in self.edges:
            completed = self.completed_target(feed, target, state)
            source = self.base_order(feed.base, state)
            if set(completed) != set(source) or len(completed) != len(source):
                raise ValueError('Virtual completion is not a permutation')
            retained = {reference for reference in feed.references if reference is not None}
            suffix = tuple(c for c in completed if c not in retained)
            if suffix != tuple(c for c in source if c not in retained):
                raise ValueError('Virtual suffix lost source-relative order')

    def follow(self, variable, desired, state):
        current = state[variable]
        ranks = {column: rank for rank, column in enumerate(desired)}
        return tuple(sorted(current, key=lambda column: ranks.get(column, len(ranks))))

    def candidates(self, state, joint):
        yielded = set()

        def candidate(changes):
            if any(variable in self.pinned and order != state[variable]
                   for variable, order in changes.items()):
                return None
            result = tuple(sorted((variable, tuple(order)) for variable, order in changes.items()
                                  if tuple(order) != state[variable]))
            if result and result not in yielded:
                yielded.add(result)
                return result
            return None

        for variable in sorted(self.active):
            orders = [(1 - state[variable][0],)] if variable in self.side_variables else sorted(insertions(state[variable]))
            for order in orders:
                result = candidate({variable: order})
                if result:
                    yield result
        if not joint:
            return
        for name, join in sorted(self.joins.items()):
            if join['anti']:
                continue
            side_variable = join['side']
            incident = self.node_variables(name) | self.base_variables('j:' + name)
            for variable in sorted((incident & self.active) - {side_variable}):
                for order in sorted(insertions(state[variable])):
                    result = candidate({variable: order,
                                        side_variable: (1 - state[side_variable][0],)})
                    if result:
                        yield result
            feed = Feed('j:' + name, tuple(self.join_references(name)))
            for side in [state[side_variable], (1 - state[side_variable][0],)]:
                temporary = list(state)
                temporary[side_variable] = side
                generated = self.base_order(feed.base, temporary)
                changes = {side_variable: side}
                output = self.nodes[name]
                for block, variable in enumerate(output['variables']):
                    offset = 0 if block == 0 else output['arity'][0]
                    desired = [column - offset for reference in generated
                               for column, source in enumerate(feed.references)
                               if source == reference and column - offset in state[variable]]
                    changes[variable] = self.follow(variable, desired, temporary)
                result = candidate(changes)
                if result:
                    yield result
                completed = self.completed_target(feed, name, temporary)
                changes = {side_variable: side}
                for input_side, inp in enumerate(join['inputs']):
                    for block, variable in enumerate(self.nodes[inp]['variables']):
                        tag = 'k' if block == 0 else input_side
                        desired = [column for source_side, column in completed if source_side == tag]
                        changes[variable] = self.follow(variable, desired, temporary)
                result = candidate(changes)
                if result:
                    yield result

    def join_references(self, name):
        tx = self.transactions[name]
        for expression in tx['key'] + tx['value']:
            if expression['slot'] is None:
                yield None
            else:
                side, key, index = expression['slot']
                yield ('k', index) if key else (side, index)

    def search(self, state, joint):
        values = self.costs(state)
        history = [sum(values)]
        evaluations = 0
        while True:
            best = None
            best_gain = 0
            for changes in self.candidates(state, joint):
                evaluations += 1
                temporary = list(state)
                touched = set()
                for variable, order in changes:
                    temporary[variable] = order
                    touched.update(self.incident[variable])
                new_values = {edge: self.edge_cost(edge, temporary) for edge in touched}
                gain = sum(values[edge] - value for edge, value in new_values.items())
                if gain > best_gain:
                    best_gain = gain
                    best = (tuple(temporary), new_values)
            if best is None:
                self.validate(state)
                if sum(self.costs(state)) != history[-1]:
                    raise ValueError('Incremental and full costs disagree')
                return state, history, evaluations
            state, changed_values = best
            for edge, value in changed_values.items():
                values[edge] = value
            history.append(history[-1] - best_gain)

    def seeded(self, seed):
        state = list(self.initial)
        for variable in sorted(self.active):
            if variable in self.side_variables:
                state[variable] = ((seed >> (variable % 3)) & 1,)
            else:
                order = state[variable]
                if seed & 1:
                    order = order[::-1]
                shift = (seed // 2) % len(order)
                state[variable] = order[shift:] + order[:shift]
        return tuple(state)

    def describe(self, state):
        layouts = {name: {'order': self.node_order(name, state),
                          'arity': node['arity'], 'label': node['label']}
                   for name, node in self.nodes.items()}
        sides = {name: state[join['side']][0] for name, join in self.joins.items()
                 if not join['anti']}
        scopes = []
        for stratum in self.plan['strata']:
            scope = {}
            for tx in stratum['transformations']:
                name = f"s{stratum['index']}:{tx['output']['id']}"
                input_orders = []
                for inp, description in zip(self.inputs[name], tx['inputs']):
                    order = self.node_order(inp, state) if inp in self.nodes and not inp.startswith('r:') else tuple(range(sum(description['arity'])))
                    input_orders.append(order)
                scope[tx['output']['id']] = {
                    'inputs': input_orders,
                    'output': self.node_order(name, state) if name in self.nodes else tuple(range(sum(tx['output']['arity']))),
                    'swap': bool(sides.get(name, 0)),
                }
            scopes.append(scope)
        relations = {identifier: self.node_order('r:' + identifier, state)
                     for identifier in self.relations}
        return {'layouts': layouts, 'swapped': sides,
                'scopes': scopes, 'relations': relations}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--programs', nargs='+', default=[
        'doop', 'cvc5', 'z3', 'polonius_str', 'cspa', 'csda', 'andersen',
        'tc', 'sg', 'dyck', 'bipartite', 'galen'])
    parser.add_argument('--seeds', type=int, default=8)
    parser.add_argument('--plans', type=Path, default=ROOT / 'real-plans')
    parser.add_argument('--output', type=Path, default=ROOT / 'real-results-fresh')
    args = parser.parse_args()
    args.output.mkdir(exist_ok=True)
    summaries = []
    for name in args.programs:
        start = time.perf_counter()
        source = args.plans / (name + '.json')
        plan = json.loads(source.read_text())
        model = PlanModel(plan)
        model.validate(model.initial)
        baseline_costs = model.costs(model.initial)
        baseline = sum(baseline_costs)
        separate, separate_history, separate_evaluations = model.search(model.initial, False)
        trials = []
        best = None
        for seed in range(args.seeds):
            initial = model.seeded(seed)
            result, history, evaluations = model.search(initial, True)
            trials.append({'seed': seed, 'cost': history[-1], 'history': history,
                           'evaluations': evaluations})
            if best is None or history[-1] < best[0]:
                best = (history[-1], seed, result)
        cost, seed, result = best
        final_costs = model.costs(result)
        details = []
        for index, (feed, target, kind) in enumerate(model.edges):
            details.append({'kind': kind, 'source': feed.base, 'target': target,
                            'baseline_cost': baseline_costs[index],
                            'cost': final_costs[index],
                            'source_order': model.base_order(feed.base, result),
                            'completed_target': model.completed_target(feed, target, result)})
        changed = sum(model.node_order(node, result) != model.node_order(node, model.initial)
                      for node in model.nodes)
        swaps = sum(result[join['side']] != (0,) for join in model.joins.values()
                    if not join['anti'])
        summary = {'program': name, 'rules': plan['rules'],
                   'materialized_layouts': len(model.nodes), 'sort_boundaries': len(model.edges),
                   'unscored_boundaries': len(model.opaque_edges),
                   'joins': len(model.joins), 'variables': len(model.active),
                   'baseline_cost': baseline, 'separate_cost': separate_history[-1],
                   'joint_first_seed_cost': trials[0]['cost'], 'joint_best_cost': cost,
                   'changed_layouts': changed, 'swapped_joins': swaps,
                   'search_seconds': time.perf_counter() - start, 'best_seed': seed,
                   'candidate_evaluations': separate_evaluations + sum(t['evaluations'] for t in trials),
                   'constant_expressions': model.constants,
                   'opaque_expressions': model.opaque,
                   'plan_sha256': hashlib.sha256(source.read_bytes()).hexdigest()}
        summaries.append(summary)
        output = {'summary': summary, 'trials': trials, 'edges': details,
                  'selected': model.describe(result),
                  'baseline': model.describe(model.initial)}
        (args.output / (name + '.json')).write_text(json.dumps(output, indent=2) + '\n')
        print(json.dumps(summary), flush=True)
        with (args.output / 'summary.csv').open('w') as handle:
            writer = csv.DictWriter(handle, fieldnames=list(summary))
            writer.writeheader()
            writer.writerows(summaries)


if __name__ == '__main__':
    main()
