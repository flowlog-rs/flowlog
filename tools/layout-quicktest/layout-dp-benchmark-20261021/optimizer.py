"""Select raw-I layouts with exact cutset-conditioned DP, later strata first."""

import argparse
from array import array
from collections import defaultdict
import hashlib
import importlib.util
import itertools
import json
from pathlib import Path
import sys
import time

from contraction import contract_orders


ROOT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location(
    'cutset_analysis', ROOT.parent / 'layout-dp-feasibility-20261021/search_all_cutsets.py')
SEARCH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SEARCH)
ANALYSIS = SEARCH.DEC


def strides(scope, domains):
    result = {}
    step = 1
    for variable in reversed(scope):
        result[variable] = step
        step *= domains[variable]
    return result


class Table:
    def __init__(self, scope, values, domains):
        self.scope = tuple(scope)
        self.values = values
        self.strides = strides(self.scope, domains)

    def conditioned(self, assignment, domains):
        scope = tuple(v for v in self.scope if v not in assignment)
        if scope == self.scope:
            return self
        offset = sum(self.strides[v] * assignment[v]
                     for v in self.scope if v in assignment)
        values = array('q')
        for choices in itertools.product(*(range(domains[v]) for v in scope)):
            index = offset + sum(self.strides[v] * c for v, c in zip(scope, choices))
            values.append(self.values[index])
        return Table(scope, values, domains)


def eliminate(tables, order, domains):
    tables = list(tables)
    history = []
    visits = 0
    for variable in order:
        bucket = [table for table in tables if variable in table.scope]
        tables = [table for table in tables if variable not in table.scope]
        scope = tuple(sorted({v for table in bucket for v in table.scope} - {variable}))
        values = array('q')
        choices = array('I')
        for assignment in itertools.product(*(range(domains[v]) for v in scope)):
            state = dict(zip(scope, assignment))
            bases = [sum(table.strides[v] * state[v] for v in table.scope
                         if v != variable) for table in bucket]
            best = None
            selected = 0
            for value in range(domains[variable]):
                cost = sum(table.values[base + table.strides[variable] * value]
                           for table, base in zip(bucket, bases))
                if best is None or cost < best:
                    best, selected = cost, value
                visits += 1
            values.append(best)
            choices.append(selected)
        result = Table(scope, values, domains)
        tables.append(result)
        history.append((variable, result.strides, choices))
    if any(table.scope for table in tables):
        raise ValueError('DP elimination left an unfixed layout variable')
    optimum = sum(table.values[0] for table in tables)
    assignment = {}
    for variable, positions, choices in reversed(history):
        index = sum(positions[v] * assignment[v] for v in positions)
        assignment[variable] = choices[index]
    return optimum, assignment, visits


def phase_fusion(model, original, frozen, orders):
    proposed, candidates = ANALYSIS.fusion(model, original)
    accepted = []
    for item in candidates:
        variable = item['variable']
        if variable in frozen or variable not in model.active:
            continue
        node = model.nodes[item['target']]
        target = node['variables'][1]
        available = set(orders[variable])
        if all(tuple(item['head_refs'][c] for c in order) in available
               for order in orders[target]):
            accepted.append(item)
    removed = {index for item in accepted for index in (item['first'], item['second'])}
    replacements = {str(item['first']) + '+' + str(item['second']) for item in accepted}
    edges = [edge for edge in original if edge['index'] not in removed]
    edges.extend(edge for edge in proposed if edge['index'] in replacements)
    return edges, accepted


def join_side(model, edge):
    base = edge['feed'].base
    if not base.startswith('j:'):
        return None
    join = model.joins[base[2:]]
    return None if join['anti'] else join['side']


def edge_value(model, edge, state, orders, assignment, changing=None):
    if edge['opaque']:
        return 0

    def selected(variable):
        return orders[variable][assignment[variable]] if variable in assignment else state[variable]

    blocks = []
    for block, (variable, refs) in enumerate(edge['sources']):
        order = selected(variable) if changing is None or changing == block else state[variable]
        blocks.append(tuple(refs[field] for field in order))
    side = join_side(model, edge)
    if side is not None and selected(side) == (1,):
        blocks[1], blocks[2] = blocks[2], blocks[1]
    source = sum(blocks, ())
    node = model.nodes[edge['target']]
    key, value = node['variables']
    columns = selected(key) + tuple(node['arity'][0] + c for c in selected(value))
    retained = tuple(edge['feed'].references[c] for c in columns
                     if edge['feed'].references[c] is not None)
    return ANALYSIS.I(source, retained)


def optimize_phase(model, original, state, frozen, contraction=True):
    started = time.perf_counter()
    orders, partitions, contraction_stats = contract_orders(
        model, original, state=state, fixed=frozen, enabled=contraction)
    for edge in original:
        side = join_side(model, edge)
        if side is not None:
            orders[side] = ([(0,), (1,)] if side in model.active and side not in frozen
                            else [state[side]])
    for variable, options in orders.items():
        if state[variable] in options:
            options.insert(0, options.pop(options.index(state[variable])))
    anchor = list(state)
    for variable, options in orders.items():
        anchor[variable] = options[0]
    edges, fusions = phase_fusion(model, original, frozen, orders)
    active = {v for v, options in orders.items() if len(options) > 1}
    domains = {v: len(orders[v]) for v in active}
    factors = []
    for edge in edges:
        if edge['opaque']:
            continue
        targets = {v for v, _ in edge['targets']} & active
        side = join_side(model, edge)
        if side in active:
            targets.add(side)
        retained = set(edge['feed'].references) - {None}
        changing = [i for i, (v, refs) in enumerate(edge['sources'])
                    if v in active and retained.intersection(refs)]
        for n, block in enumerate(changing or [None]):
            variable = edge['sources'][block][0] if block is not None else None
            factors.append(dict(
                scope=tuple(sorted(targets | ({variable} if variable is not None else set()))),
                edge=edge, block=block, mode='anchor' if block is None else
                ('first' if n == 0 else 'difference'), m=edge['m']))
    used = {v for factor in factors for v in factor['scope']}
    domains = {v: domains[v] for v in used}
    scale = len(domains) + 1
    for variable in sorted(domains):
        factors.append(dict(scope=(variable,), mode='tie', variable=variable, m=0))
    components = SEARCH.connected_components(factors)
    setup_seconds = time.perf_counter() - started
    search_start = time.perf_counter()
    for component in components:
        graph = SEARCH.Graph(component['variable_ids'], domains,
                             [factors[i] for i in component['factor_ids']])
        component.update(graph.search(10000, time.monotonic() + 1.0))
    search_seconds = time.perf_counter() - search_start
    table_start = time.perf_counter()
    tables = []
    evaluations = 0
    for factor in factors:
        values = array('q')
        scope = factor['scope']
        for choices in itertools.product(*(range(domains[v]) for v in scope)):
            assignment = dict(zip(scope, choices))
            if factor['mode'] == 'tie':
                variable = factor['variable']
                value = int(orders[variable][assignment[variable]] != model.initial[variable])
            else:
                edge = factor['edge']
                value = edge_value(model, edge, anchor, orders, assignment,
                                   factor['block'] if factor['block'] is not None else -1)
                if factor['mode'] == 'difference':
                    value -= edge_value(model, edge, anchor, orders, assignment, -1)
                value *= scale
            values.append(value)
            evaluations += 1
        tables.append(Table(scope, values, domains))
    table_seconds = time.perf_counter() - table_start
    dp_start = time.perf_counter()
    selected_assignment = {}
    packed_optimum = 0
    dp_visits = 0
    component_rows = []
    for component in components:
        local_factors = [factors[i] for i in component['factor_ids']]
        local_tables = [tables[i] for i in component['factor_ids']]
        cut = component['cut']
        candidates = [ANALYSIS.greedy(local_factors, domains, set(cut), rule)
                      for rule in ('state', 'fill', 'message')]
        chosen = min(candidates, key=lambda x: (x['elimination_assignments'], x['peak']))
        best = None
        best_assignment = None
        conditional_cache = {}
        for values in itertools.product(*(range(domains[v]) for v in cut)):
            fixed = dict(zip(cut, values))
            conditioned = []
            for index, table in enumerate(local_tables):
                key = index, tuple((v, fixed[v]) for v in table.scope if v in fixed)
                if key not in conditional_cache:
                    conditional_cache[key] = table.conditioned(fixed, domains)
                conditioned.append(conditional_cache[key])
            optimum, assignment, visits = eliminate(conditioned, chosen['order'], domains)
            dp_visits += visits
            if best is None or optimum < best:
                best, best_assignment = optimum, assignment | fixed
        packed_optimum += best
        selected_assignment.update(best_assignment)
        component_rows.append(dict(cut=cut, outer_product=component['outer_product'],
                                   proven=component['optimality_proven'],
                                   search_nodes=component['search_nodes'],
                                   order=chosen['order'], peak=chosen['peak']))
    result_state = list(anchor)
    for variable, choice in selected_assignment.items():
        result_state[variable] = orders[variable][choice]
    for item in reversed(fusions):
        node = model.nodes[item['target']]
        result_state[item['variable']] = tuple(item['head_refs'][c]
                                              for c in result_state[node['variables'][1]])
    model.validate(result_state)
    actual = sum(model.edge_cost(edge['index'], result_state) for edge in original)
    if actual != packed_optimum // scale:
        raise ValueError(f'DP reconstruction cost mismatch: {actual} versus {packed_optimum // scale}')
    if any(result_state[v] != state[v] for v in frozen):
        raise ValueError('DP changed a frozen downstream interface')
    before = sum(model.edge_cost(edge['index'], state) for edge in original)
    if actual > before:
        raise ValueError(f'Exact phase increased raw I: {before} to {actual}')
    return result_state, dict(
        baseline_I=before, optimum_I=actual, setup_seconds=setup_seconds,
        search_seconds=search_seconds, table_seconds=table_seconds,
        dp_seconds=time.perf_counter() - dp_start,
        total_seconds=time.perf_counter() - started,
        factor_evaluations=evaluations, dp_visits=dp_visits,
        variables=len(domains), factors=len(factors), fusions=fusions,
        contraction=contraction_stats, components=component_rows)


def optimize(plan, contraction=True):
    started = time.perf_counter()
    if any(s['aggregates'] for s in plan['strata']):
        raise ValueError('Explicit aggregate plans require the aggregate cost adapter')
    model = ANALYSIS.CONTRACTION.MODEL(plan)
    original = ANALYSIS.CONTRACTION.describe_edges(model)
    state = list(model.initial)
    frozen = set()
    phases = []
    for stratum, recursive, indices in model.phases:
        edges = [edge for edge in original if edge['index'] in indices]
        state, details = optimize_phase(model, edges, state, frozen, contraction)
        details['stratum'] = stratum
        details['recursive'] = recursive
        for edge in edges:
            frozen.update(model.base_variables(edge['feed'].base))
            frozen.update(model.node_variables(edge['target']))
        details['frozen_variables'] = sorted(frozen)
        phases.append(details)
        print('phase', stratum, 'recursive', recursive, 'I', details['baseline_I'], '->',
              details['optimum_I'], 'search', round(details['search_seconds'], 4),
              'table', round(details['table_seconds'], 3), 'DP',
              round(details['dp_seconds'], 3), flush=True)
    model.validate(state)
    result = dict(
        metric='raw Boolean inversions; all edges weight 1',
        policy='reverse strata; recursive edges first; exact conditional min-sum DP; freeze original interfaces',
        join_directions='optimized jointly with field layouts', contraction=contraction,
        tie_break='fewest changed free non-fused layout variables',
        total_seconds=time.perf_counter() - started,
        search_seconds=sum(p['search_seconds'] for p in phases),
        table_seconds=sum(p['table_seconds'] for p in phases),
        dp_seconds=sum(p['dp_seconds'] for p in phases),
        selected=json.loads(json.dumps(model.describe(state))), state=state, phases=phases,
        baseline_I=sum(model.costs(model.initial)), selected_I=sum(model.costs(state)),
        baseline_priority=model.priority(model.initial), selected_priority=model.priority(state),
        all_cutsets_proven=all(c['proven'] for p in phases for c in p['components']))
    replay = ANALYSIS.CONTRACTION.WIDTH.previous.verifier.state_from_config(model, result['selected'])
    if tuple(replay) != tuple(state):
        raise ValueError('Exported physical layouts do not reconstruct the DP state')
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plans', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--programs', nargs='*')
    parser.add_argument('--no-contraction', action='store_true')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    for path in sorted(args.plans.glob('*.json')):
        if args.programs and path.stem not in args.programs:
            continue
        output = args.output / path.name
        print('OPTIMIZE', path.stem, flush=True)
        result = optimize(json.loads(path.read_text()), not args.no_contraction)
        result['plan_sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
        output.write_text(json.dumps(result, indent=2) + '\n')
        print('DONE', path.stem, result['total_seconds'], flush=True)


if __name__ == '__main__':
    main()
