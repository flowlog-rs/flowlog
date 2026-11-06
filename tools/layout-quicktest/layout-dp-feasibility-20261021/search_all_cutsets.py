"""Find automatic layout-variable cycle cutsets without running data workloads.

Factor identities remain distinct throughout conditioning. Domains count layouts;
this script never constructs their permutations or evaluates runtime plans.
"""

from collections import Counter, defaultdict
import argparse
import heapq
import importlib.util
import itertools
import json
import math
from pathlib import Path
import time


ROOT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('cutset_decoupling', ROOT / 'explore_decoupling.py')
DEC = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(DEC)


def bits(mask):
    while mask:
        low = mask & -mask
        yield low.bit_length() - 1
        mask ^= low


def make_factors(model, edges, domains):
    active = set(domains)
    factors = []
    for edge in edges:
        target = {v for v, _ in edge['targets']} & active
        retained = set(edge['feed'].references) - {None}
        changing = [i for i, (v, refs) in enumerate(edge['sources'])
                    if v in active and retained.intersection(refs)]
        if changing:
            for index, block in enumerate(changing):
                variable = edge['sources'][block][0]
                factors.append(dict(scope=sorted(target | {variable}), m=edge['m'],
                                    edge_index=edge['index'],
                                    mode='first' if index == 0 else 'difference', block=block))
        else:
            factors.append(dict(scope=sorted(target), m=edge['m'],
                                edge_index=edge['index'], mode='anchor', block=None))
    return factors


def connected_components(factors):
    adjacent = defaultdict(set)
    for i, factor in enumerate(factors):
        f = ('f', i)
        adjacent[f]
        for variable in factor['scope']:
            v = ('v', variable)
            adjacent[f].add(v)
            adjacent[v].add(f)
    remaining = set(adjacent)
    result = []
    while remaining:
        seed = min(remaining)
        todo = [seed]
        nodes = set()
        while todo:
            node = todo.pop()
            if node in nodes:
                continue
            nodes.add(node)
            todo.extend(adjacent[node] - nodes)
        remaining -= nodes
        result.append(dict(variable_ids=sorted(v for kind, v in nodes if kind == 'v'),
                           factor_ids=sorted(v for kind, v in nodes if kind == 'f')))
    result.sort(key=lambda item: (item['variable_ids'] or [10**9], item['factor_ids']))
    return result


class Graph:
    def __init__(self, variables, domains, factors):
        self.variables = variables
        self.sizes = [domains[v] for v in variables]
        self.n = len(variables)
        positions = {v: i for i, v in enumerate(variables)}
        self.scopes = [sum(1 << positions[v] for v in factor['scope']) for factor in factors]
        self.cycle_cache = {}

    def beta(self, cut):
        parent = {}
        edges = 0

        def find(v):
            parent.setdefault(v, v)
            while parent[v] != v:
                parent[v] = parent[parent[v]]
                v = parent[v]
            return v

        for i, scope in enumerate(self.scopes):
            factor = self.n + i
            find(factor)
            for v in bits(scope & ~cut):
                a, b = find(v), find(factor)
                edges += 1
                if a != b:
                    parent[a] = b
        return edges - len(parent) + len({find(v) for v in parent})

    def cycle(self, cut, cache=True):
        if cache and cut in self.cycle_cache:
            return self.cycle_cache[cut]
        pairs = {}
        best = None
        adjacent = {}
        for i, scope in enumerate(self.scopes):
            values = list(bits(scope & ~cut))
            if len(values) < 2:
                continue
            factor = self.n + i
            adjacent[factor] = values
            for v in values:
                adjacent.setdefault(v, []).append(factor)
            for a, b in itertools.combinations(values, 2):
                pair = (a, b)
                if pair in pairs:
                    rank = (self.sizes[a] * self.sizes[b], pair)
                    if best is None or rank < best[0]:
                        best = (rank, (1 << a) | (1 << b))
                pairs[pair] = i
        found = None if best is None else best[1]
        if found is None:
            colors = {}
            parents = {}

            def visit(v, parent):
                colors[v] = 1
                parents[v] = parent
                for u in adjacent[v]:
                    if u == parent:
                        continue
                    if colors.get(u, 0) == 0:
                        answer = visit(u, v)
                        if answer is not None:
                            return answer
                    elif colors[u] == 1:
                        answer = (1 << u) if u < self.n else 0
                        node = v
                        while node != u:
                            if node < self.n:
                                answer |= 1 << node
                            node = parents[node]
                        return answer
                colors[v] = 2
                return None

            for node in adjacent:
                if colors.get(node, 0) == 0:
                    found = visit(node, None)
                    if found is not None:
                        break
        if cache:
            if len(self.cycle_cache) > 50000:
                self.cycle_cache.clear()
            self.cycle_cache[cut] = found
        return found

    def packing_bound(self, cut):
        first = self.cycle(cut)
        cycle = first
        product = 1
        while cycle is not None:
            product *= min(self.sizes[v] for v in bits(cycle))
            cut |= cycle
            cycle = self.cycle(cut, cache=False)
        return first, product

    def greedy(self):
        cut = 0
        beta = self.beta(cut)
        steps = []
        while beta:
            choices = []
            for v in range(self.n):
                if cut & (1 << v):
                    continue
                after = self.beta(cut | (1 << v))
                gain = beta - after
                if gain:
                    choices.append((math.log(self.sizes[v]) / gain,
                                    self.sizes[v], -gain, self.variables[v], v, after))
            if not choices:
                raise ValueError('A surviving cycle must contain a deletable variable')
            _, _, _, _, v, after = min(choices)
            steps.append(dict(variable=self.variables[v], domain=self.sizes[v],
                              rank_before=beta, rank_drop=beta-after, rank_after=after))
            cut |= 1 << v
            beta = after
        removed = []
        for step in reversed(steps):
            v = self.variables.index(step['variable'])
            candidate = cut & ~(1 << v)
            if self.beta(candidate) == 0:
                cut = candidate
                removed.append(self.variables[v])
        return cut, steps, removed

    def product(self, cut):
        return math.prod(self.sizes[v] for v in bits(cut))

    def named(self, cut):
        return [self.variables[v] for v in bits(cut)]

    def search(self, max_nodes, deadline):
        rank = self.beta(0)
        if rank == 0:
            return dict(cycle_rank=0, cut=[], outer_product=1, lower_bound=1,
                        optimality_proven=True, search_nodes=0, stop_reason='forest',
                        greedy_cut=[], greedy_product=1, greedy_steps=[], redundant_removed=[])
        best, steps, removed = self.greedy()
        upper = self.product(best)
        greedy_cut, greedy_product = self.named(best), upper
        cycle, bound = self.packing_bound(0)
        queue = [(bound, 0, 1, cycle)]
        seen = {0}
        nodes = 0
        reason = 'exhausted'
        while queue:
            if queue[0][0] >= upper:
                queue.clear()
                break
            if nodes >= max_nodes:
                reason = 'node_limit'
                break
            if time.monotonic() >= deadline:
                reason = 'time_limit'
                break
            _, cut, product, cycle = heapq.heappop(queue)
            nodes += 1
            degree = Counter(v for scope in self.scopes for v in bits(scope & ~cut))
            candidates = sorted(bits(cycle),
                                key=lambda v: (math.log(self.sizes[v]) / max(1, degree[v]-1),
                                               self.sizes[v], self.variables[v]))
            for v in candidates:
                new_cut = cut | (1 << v)
                new_product = product * self.sizes[v]
                if new_product >= upper or new_cut in seen:
                    continue
                seen.add(new_cut)
                new_cycle, extra = self.packing_bound(new_cut)
                lower = new_product * extra
                if new_cycle is None:
                    best, upper = new_cut, new_product
                elif lower < upper:
                    heapq.heappush(queue, (lower, new_cut, new_product, new_cycle))
        lower = min(upper, queue[0][0]) if queue else upper
        if self.beta(best) != 0:
            raise ValueError('Returned cutset leaves a factor-graph cycle')
        return dict(cycle_rank=rank, cut=self.named(best), outer_product=upper,
                    lower_bound=lower, optimality_proven=lower == upper,
                    search_nodes=nodes, search_states=len(seen), stop_reason=reason,
                    greedy_cut=greedy_cut, greedy_product=greedy_product,
                    greedy_steps=steps, redundant_removed=removed,
                    residual_cycle_rank=0)


def analyze(path, max_nodes, seconds, cache):
    model = DEC.CONTRACTION.MODEL(json.loads(path.read_text()))
    all_edges = DEC.CONTRACTION.describe_edges(model)
    rows = []
    for stratum in model.plan['strata']:
        original = [e for e in all_edges if e['stratum'] == stratum['index']]
        edges, fusions = DEC.fusion(model, original)
        partitions, checks = DEC.CONTRACTION.contract(model, original)
        for mode in ['original', 'stratum-contracted']:
            sizes = {v: math.factorial(len(partitions[v]) if mode == 'stratum-contracted'
                                      else len(model.initial[v]))
                     for v in model.active - model.side_variables}
            sizes = {v: size for v, size in sizes.items() if size > 1}
            factors = make_factors(model, edges, sizes)
            touched = {v for factor in factors for v in factor['scope']}
            domains = {v: sizes[v] for v in sorted(touched)}
            components = connected_components(factors)
            remaining_nodes = max_nodes
            deadline = time.monotonic() + seconds
            for component in components:
                selected = [factors[i] for i in component['factor_ids']]
                graph = Graph(component['variable_ids'], domains, selected)
                signature = (tuple(graph.variables), tuple(graph.sizes), tuple(graph.scopes))
                if signature in cache:
                    solution = dict(cache[signature])
                    solution['reused_search'] = True
                else:
                    solution = graph.search(remaining_nodes, deadline)
                    cache[signature] = dict(solution)
                    solution['reused_search'] = False
                remaining_nodes -= solution['search_nodes'] if not solution['reused_search'] else 0
                component.update(solution)
            cut = sorted(v for component in components for v in component['cut'])
            row = dict(query=path.stem, stratum=stratum['index'], mode=mode,
                       recursive=stratum['recursive'], original_transitions=len(original),
                       fused_transitions=len(edges), fusions=fusions,
                       variables=len(domains), factors=factors, domains=domains,
                       components=components, component_count=len(components),
                       cycle_rank=sum(c['cycle_rank'] for c in components),
                       cut=cut, outer_product=math.prod(c['outer_product'] for c in components),
                       lower_bound=math.prod(c['lower_bound'] for c in components),
                       optimality_proven=all(c['optimality_proven'] for c in components),
                       search_nodes=sum(c['search_nodes'] for c in components
                                        if not c['reused_search']),
                       labels={v: model.labels[v] for v in domains},
                       variable_blocks={v: model.variable_blocks[v] for v in domains},
                       contraction_checks=checks if mode == 'stratum-contracted' else None)
            rows.append(row)
            print(path.stem, stratum['index'], mode, 'variables', len(domains),
                  'beta', row['cycle_rank'], 'cut', len(cut), 'P', row['outer_product'],
                  'LB', row['lower_bound'], 'exact', row['optimality_proven'],
                  'nodes', row['search_nodes'], flush=True)
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--nodes', type=int, default=100000)
    parser.add_argument('--seconds', type=float, default=30)
    parser.add_argument('--query', action='append')
    args = parser.parse_args()
    result = dict(model='fixed main join orientation, free stratum interfaces',
                  factorization='bijective full-row fusion and additive source-block split',
                  objective='minimum product of conditioned layout-variable domains',
                  factor_identities='preserved throughout conditioning',
                  max_search_nodes_per_stratum_mode=args.nodes,
                  seconds_per_stratum_mode=args.seconds, rows=[])
    cache = {}
    for path in sorted(DEC.CONTRACTION.PLANS.glob('*.json')):
        if args.query and path.stem not in args.query:
            continue
        result['rows'].extend(analyze(path, args.nodes, args.seconds, cache))
        (ROOT / 'all-cutsets.json').write_text(json.dumps(result, indent=2) + '\n')
    result['summary'] = dict(stratum_modes=len(result['rows']),
                            proven=sum(row['optimality_proven'] for row in result['rows']),
                            unproven=sum(not row['optimality_proven'] for row in result['rows']))
    (ROOT / 'all-cutsets.json').write_text(json.dumps(result, indent=2) + '\n')
    print(result['summary'], flush=True)


if __name__ == '__main__':
    main()
