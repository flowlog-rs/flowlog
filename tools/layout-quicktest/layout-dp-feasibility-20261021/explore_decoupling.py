"""Audit exact layout-factor reductions and elimination on CVC5 stratum 2.

This is an analysis prototype. It does not change plans or benchmark executables.
"""

from collections import defaultdict
import importlib.util
import itertools
import json
import math
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('contraction_analysis', ROOT / 'count_contracted_dp.py')
CONTRACTION = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CONTRACTION)
INVERSION_SPEC = importlib.util.spec_from_file_location(
    'decoupling_inversions', CONTRACTION.PLANS.parent / 'optimizer-source/binary-layout/binary_cost.py')
INVERSION = importlib.util.module_from_spec(INVERSION_SPEC)
INVERSION_SPEC.loader.exec_module(INVERSION)
I = INVERSION.inversions


def product(domains, scope):
    return math.prod(domains[v] for v in scope)


def fusion(model, edges):
    removed = set()
    replacements = []
    details = []
    for edge in edges:
        if edge['index'] in removed:
            continue
        node = model.nodes[edge['target']]
        variable = node['variables'][1]
        if node['arity'][0] or variable not in model.active:
            continue
        origin = model.base_order(edge['feed'].base, model.initial)
        refs = edge['feed'].references
        if len(refs) != len(origin) or set(refs) != set(origin):
            continue
        outgoing = [e for e in edges if e['feed'].base == edge['target']]
        if len(outgoing) != 1 or len(model.incident[variable]) != 2:
            continue
        other = outgoing[0]
        target = model.nodes[other['target']]
        if (target['arity'][0] or len(other['feed'].references) != len(origin)
                or set(other['feed'].references) != set(range(len(origin)))):
            continue
        combined = dict(edge)
        feed_type = type(edge['feed'])
        combined['feed'] = feed_type(edge['feed'].base,
                                     tuple(refs[c] for c in other['feed'].references))
        combined['target'] = other['target']
        combined['targets'] = other['targets']
        combined['index'] = str(edge['index']) + '+' + str(other['index'])
        removed.update([edge['index'], other['index']])
        replacements.append(combined)
        details.append(dict(first=edge['index'], second=other['index'],
                            variable=variable, width=len(origin),
                            target=other['target'], head_refs=other['feed'].references))
    return [e for e in edges if e['index'] not in removed] + replacements, details


def components(model, edges, orientations, separate):
    active = model.active if orientations else model.active - model.side_variables
    domains = {v: 2 if v in model.side_variables else math.factorial(len(model.initial[v]))
               for v in active}
    orders = {v: [(0,), (1,)] if v in model.side_variables
              else list(itertools.permutations(model.initial[v])) for v in active}
    result = []
    for edge in edges:
        target_vars = {v for v, _ in edge['targets']} & active
        orientation = None
        if edge['feed'].base.startswith('j:'):
            join = model.joins[edge['feed'].base[2:]]
            if orientations and not join['anti']:
                orientation = join['side']
        base_scope = target_vars | ({orientation} if orientation is not None else set())
        if not separate:
            scope = base_scope | ({v for v, _ in edge['sources']} & active)
            result.append(dict(scope=tuple(sorted(scope)), m=edge['m'], edge=edge,
                               mode='whole', block=None))
            continue
        retained = set(edge['feed'].references) - {None}
        changing = [i for i, (v, refs) in enumerate(edge['sources'])
                    if v in active and retained.intersection(refs)]
        if not changing:
            result.append(dict(scope=tuple(sorted(base_scope)), m=edge['m'], edge=edge,
                               mode='anchor', block=None))
        else:
            for n, block in enumerate(changing):
                variable = edge['sources'][block][0]
                result.append(dict(scope=tuple(sorted(base_scope | {variable})),
                                   m=edge['m'], edge=edge,
                                   mode='first' if n == 0 else 'difference', block=block))
    return domains, orders, result


def value(model, orders, component, assignment):
    edge = component['edge']

    def order(variable):
        return orders[variable][assignment[variable]] if variable in assignment else model.initial[variable]

    def cost(changing):
        blocks = []
        for index, (variable, refs) in enumerate(edge['sources']):
            permutation = order(variable) if changing is None or index == changing else model.initial[variable]
            blocks.append(tuple(refs[i] for i in permutation))
        if edge['feed'].base.startswith('j:'):
            join = model.joins[edge['feed'].base[2:]]
            if not join['anti'] and order(join['side']) == (1,):
                blocks[1], blocks[2] = blocks[2], blocks[1]
        source = sum(blocks, ())
        node = model.nodes[edge['target']]
        key, val = node['variables']
        columns = order(key) + tuple(node['arity'][0] + c for c in order(val))
        retained = tuple(edge['feed'].references[c] for c in columns
                         if edge['feed'].references[c] is not None)
        return I(source, retained)

    mode = component['mode']
    if mode == 'whole':
        return cost(None)
    if mode == 'anchor':
        return cost(-1)
    if mode == 'first':
        return cost(component['block'])
    return cost(component['block']) - cost(-1)


def greedy(factors, domains, fixed=frozenset(), rule='message'):
    scopes = [set(f['scope']) - set(fixed) for f in factors]
    initial = sum(f['m'] * product(domains, scope) for f, scope in zip(factors, scopes))
    initial_peak = max((product(domains, scope) for scope in scopes), default=1)
    graph = {v: set() for scope in scopes for v in scope}
    for scope in scopes:
        for v in scope:
            graph[v].update(scope - {v})
    work = 0
    peak = 1
    order = []
    while graph:
        ranks = []
        for v, adjacent in graph.items():
            joint = product(domains, adjacent | {v})
            msg = joint // domains[v]
            missing = [(a, b) for a in adjacent for b in adjacent if a < b and b not in graph[a]]
            fill = sum(domains[a] * domains[b] for a, b in missing)
            rank = {'state': (joint, fill, len(missing), len(adjacent), v),
                    'fill': (fill, joint, len(missing), len(adjacent), v),
                    'message': (msg, joint, fill, len(adjacent), v)}[rule]
            ranks.append((rank, v, joint))
        _, v, joint = min(ranks)
        adjacent = graph[v]
        work += joint
        peak = max(peak, joint)
        order.append(v)
        for u in adjacent:
            graph[u].update(adjacent - {u})
            graph[u].discard(v)
        del graph[v]
    return dict(rule=rule, initial_m_work=initial, elimination_assignments=work,
                total=initial + work, peak=max(initial_peak, peak), order=order)


def beam(factors, domains, width=32):
    variables = sorted({v for f in factors for v in f['scope']})
    positions = {v: i for i, v in enumerate(variables)}
    sizes = [domains[v] for v in variables]
    n = len(variables)
    full = (1 << n) - 1
    graph = [0] * n
    cache = {0: 1}

    def prod(mask):
        if mask not in cache:
            bit = mask & -mask
            cache[mask] = sizes[bit.bit_length() - 1] * prod(mask ^ bit)
        return cache[mask]

    for factor in factors:
        mask = sum(1 << positions[v] for v in factor['scope'])
        for v in factor['scope']:
            index = positions[v]
            graph[index] |= mask ^ (1 << index)

    def remove(g, remaining, v):
        neighbors = g[v] & remaining
        bit = 1 << v
        out = list(g)
        out[v] = 0
        todo = neighbors
        while todo:
            low = todo & -todo
            u = low.bit_length() - 1
            out[u] = (out[u] | neighbors) & ~low & ~bit
            todo ^= low
        return tuple(out), remaining ^ bit, prod(neighbors | bit)

    def rollout(g, remaining, rule):
        cost = 0
        peak = 1
        suffix = []
        while remaining:
            choices = []
            todo = remaining
            while todo:
                bit = todo & -todo
                v = bit.bit_length() - 1
                bag = prod((g[v] & remaining) | bit)
                message = bag // sizes[v]
                rank = (message, bag, v) if rule == 'message' else (bag, message, v)
                choices.append((rank, v))
                todo ^= bit
            _, v = min(choices)
            g, remaining, bag = remove(g, remaining, v)
            cost += bag
            peak = max(peak, bag)
            suffix.append(v)
        return cost, peak, suffix

    initial = sum(f['m'] * product(domains, f['scope']) for f in factors)
    initial_peak = max((product(domains, f['scope']) for f in factors), default=1)
    best = None
    for rule in ['message', 'state']:
        cost, peak, suffix = rollout(tuple(graph), full, rule)
        candidate = (cost, peak, suffix)
        if best is None or candidate[:2] < best[:2]:
            best = candidate
    frontier = [(0, 1, (), tuple(graph), full)]
    explored = 0
    for depth in range(n):
        candidates = {}
        for cost, peak, order, g, remaining in frontier:
            todo = remaining
            while todo:
                bit = todo & -todo
                v = bit.bit_length() - 1
                ng, nr, bag = remove(g, remaining, v)
                nc = cost + bag
                np = max(peak, bag)
                explored += 1
                lower = nc + sum(sizes[i] for i in range(n) if nr & (1 << i))
                if lower > best[0]:
                    todo ^= bit
                    continue
                state = (nc, np, order + (v,), ng, nr)
                old = candidates.get(nr)
                if old is None or state[:2] < old[:2]:
                    candidates[nr] = state
                todo ^= bit
        ranked = []
        for state in candidates.values():
            cost, peak, order, g, remaining = state
            rollout_options = [rollout(g, remaining, r) for r in ['message', 'state']]
            tail, tail_peak, suffix = min(rollout_options, key=lambda z: z[:2])
            upper = (cost + tail, max(peak, tail_peak))
            if upper < best[:2]:
                best = (*upper, list(order) + suffix)
            ranked.append((upper, state))
        ranked.sort(key=lambda z: (z[0], z[1][:2]))
        frontier = [state for _, state in ranked[:width]]
        if not frontier:
            break
    return dict(rule='beam-' + str(width), initial_m_work=initial,
                elimination_assignments=best[0], total=initial + best[0],
                peak=max(initial_peak, best[1]), order=[variables[i] for i in best[2]],
                explored=explored)


def solve(model, factors, domains, orders, order):
    tables = []
    evaluations = 0
    for factor in factors:
        scope = factor['scope']
        values = {}
        for assignment in itertools.product(*(range(domains[v]) for v in scope)):
            values[assignment] = value(model, orders, factor, dict(zip(scope, assignment)))
            evaluations += 1
        tables.append((scope, values))
    history = []
    enumerated = 0
    for v in order:
        selected = [factor for factor in tables if v in factor[0]]
        tables = [factor for factor in tables if v not in factor[0]]
        scope = tuple(sorted({u for variables, _ in selected for u in variables}))
        remaining = tuple(u for u in scope if u != v)
        positions = {u: i for i, u in enumerate(scope)}
        projections = [tuple(positions[u] for u in variables) for variables, _ in selected]
        keep = tuple(positions[u] for u in remaining)
        values = {}
        choices = {}
        for assignment in itertools.product(*(range(domains[u]) for u in scope)):
            cost = sum(table[tuple(assignment[i] for i in projection)]
                       for (_, table), projection in zip(selected, projections))
            key = tuple(assignment[i] for i in keep)
            if key not in values or cost < values[key]:
                values[key] = cost
                choices[key] = assignment[positions[v]]
            enumerated += 1
        tables.append((remaining, values))
        history.append((v, remaining, choices))
    assignment = {}
    for v, remaining, choices in reversed(history):
        assignment[v] = choices[tuple(assignment[u] for u in remaining)]
    optimum = sum(table[()] for scope, table in tables)
    return dict(optimum=optimum, assignment=assignment, factor_evaluations=evaluations,
                elimination_assignments=enumerated)


def main():
    plan = json.loads((CONTRACTION.PLANS / 'cvc5.json').read_text())
    model = CONTRACTION.MODEL(plan)
    original = [e for e in CONTRACTION.describe_edges(model) if e['stratum'] == 2]
    fused, fusions = fusion(model, original)
    result = dict(fusions=fusions, rows=[])
    solutions = []
    for orientations in [False, True]:
        for name, edges, separate in [('original', original, False),
                                      ('fused', fused, False),
                                      ('fused-split', fused, True)]:
            domains, orders, factors = components(model, edges, orientations, separate)
            candidates = [greedy(factors, domains, rule=r) for r in ['state', 'fill', 'message']]
            if name == 'fused-split':
                candidates.append(beam(factors, domains, width=32))
            best = min(candidates, key=lambda row: (row['total'], row['peak']))
            solution = solve(model, factors, domains, orders, best['order'])
            assert solution['elimination_assignments'] == best['elimination_assignments']
            state = list(model.initial)
            for v, choice in solution['assignment'].items():
                state[v] = orders[v][choice]
            for item in fusions if name != 'original' else []:
                target = model.nodes[item['target']]
                target_order = state[target['variables'][1]]
                state[item['variable']] = tuple(item['head_refs'][c] for c in target_order)
            actual = sum(I(model.base_order(e['feed'].base, state),
                           tuple(e['feed'].references[c]
                                 for c in model.node_order(e['target'], state)
                                 if e['feed'].references[c] is not None)) for e in original)
            assert actual == solution['optimum'], (orientations, name, actual, solution['optimum'])
            row = dict(orientations=orientations, graph=name, factors=len(factors),
                       candidates=candidates, best=best, solution=solution,
                       original_cost=actual)
            result['rows'].append(row)
            solutions.append((orientations, solution['optimum']))
            print(orientations, name, 'work', best['total'], 'peak', best['peak'],
                  'I', solution['optimum'], flush=True)
            (ROOT / 'decoupling-layout-search.json').write_text(json.dumps(result, indent=2) + '\n')
        assert len({cost for orientation, cost in solutions if orientation == orientations}) == 1
    print('all reduced formulations agree on exact optimum', flush=True)


if __name__ == '__main__':
    main()
