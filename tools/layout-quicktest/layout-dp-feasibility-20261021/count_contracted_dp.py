"""Count factorial DP budgets after conservative structural contraction.

The global mode checks every consumer and producer. The stratum mode treats
cross-stratum interfaces as freely selectable for the local objective.
"""

from collections import defaultdict
import csv
import importlib.util
import json
import math
from pathlib import Path


ROOT = Path(__file__).resolve().parent
PLANS = ROOT.parent / 'plans'
SPEC = importlib.util.spec_from_file_location(
    'contraction_width', PLANS.parent / 'optimizer-source/normalized-width/optimizer.py')
WIDTH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(WIDTH)
MODEL = WIDTH.previous.reverse.ReverseModel


def source_blocks(model, base):
    if not base.startswith('j:'):
        node = model.nodes[base]
        k, v = node['arity']
        return [(node['variables'][0], tuple(range(k))),
                (node['variables'][1], tuple(range(k, k + v)))]
    join = model.joins[base[2:]]
    left, right = [model.nodes[name] for name in join['inputs']]
    result = [(left['variables'][0],
               tuple(('k', i) for i in range(left['arity'][0])))]
    if not join['anti']:
        result.append((left['variables'][1],
                       tuple((0, i) for i in range(left['arity'][1]))))
    result.append((right['variables'][1],
                   tuple((1, i) for i in range(right['arity'][1]))))
    return result


def describe_edges(model):
    result = []
    for index, (feed, target, kind) in enumerate(model.edges):
        node = model.nodes[target]
        k, v = node['arity']
        targets = [(node['variables'][0], tuple(range(k))),
                   (node['variables'][1], tuple(range(k, k + v)))]
        sources = source_blocks(model, feed.base)
        origin = {ref: (role, variable, i)
                  for role, (variable, refs) in enumerate(sources)
                  for i, ref in enumerate(refs)}
        destination = {col: (variable, i)
                       for variable, cols in targets for i, col in enumerate(cols)}
        images = defaultdict(list)
        for col, ref in enumerate(feed.references):
            if ref is not None:
                images[ref].append(destination[col])
        result.append(dict(index=index, stratum=model.edge_strata[index],
                           kind=kind, feed=feed, target=target, sources=sources,
                           targets=targets, origin=origin, destination=destination,
                           images=images, m=len(model.base_order(feed.base, model.initial)),
                           opaque=index in model.opaque_edges))
    return result


def contract(model, edges):
    partitions = {v: [list(order)] if order else []
                  for v, order in enumerate(model.initial)
                  if v not in model.side_variables}
    split_events = 0
    holonomy_rejections = 0

    def positions(variable):
        return {field: group for group, fields in enumerate(partitions[variable])
                for field in fields}

    def split(variable, labels):
        nonlocal split_events
        groups = []
        for fields in partitions[variable]:
            parts = defaultdict(list)
            for field in fields:
                parts[labels[field]].append(field)
            groups.extend(parts.values())
        groups.sort(key=min)
        if groups == partitions[variable]:
            return False
        partitions[variable] = groups
        split_events += 1
        return True

    def singleton(variable):
        return split(variable, {i: i for i in model.initial[variable]})

    for edge in edges:
        if edge['opaque']:
            for variable, _ in edge['sources'] + edge['targets']:
                singleton(variable)

    while True:
        changed = True
        while changed:
            changed = False
            for edge in edges:
                if edge['opaque']:
                    continue
                images = edge['images']
                for variable, refs in edge['sources']:
                    labels = {}
                    for i, ref in enumerate(refs):
                        mapped = images[ref]
                        if not mapped:
                            labels[i] = ('drop',)
                        elif len(mapped) != 1:
                            labels[i] = ('alias', i)
                        else:
                            target_var, target_field = mapped[0]
                            labels[i] = ('target', target_var,
                                         positions(target_var)[target_field])
                    changed |= split(variable, labels)
                for variable, cols in edge['targets']:
                    labels = {}
                    for i, col in enumerate(cols):
                        ref = edge['feed'].references[col]
                        if ref is None or len(images[ref]) != 1:
                            labels[i] = ('constant_or_alias', i)
                        else:
                            role, source_var, source_field = edge['origin'][ref]
                            labels[i] = ('source', role, source_var,
                                         positions(source_var)[source_field])
                    changed |= split(variable, labels)

        parent = {}

        def find(field):
            parent.setdefault(field, field)
            if parent[field] != field:
                parent[field] = find(parent[field])
            return parent[field]

        def union(left, right):
            a, b = find(left), find(right)
            if a != b:
                parent[b] = a

        for edge in edges:
            if edge['opaque']:
                continue
            for variable, refs in edge['sources']:
                for group in partitions[variable]:
                    if len(group) < 2 or not edge['images'][refs[group[0]]]:
                        continue
                    for field in group:
                        mapped = edge['images'][refs[field]]
                        assert len(mapped) == 1
                        union((variable, field), mapped[0])
        rejected = []
        for variable, groups in partitions.items():
            for group in groups:
                if len({find((variable, field)) for field in group}) != len(group):
                    rejected.append((variable, group))
        if not rejected:
            break
        for variable, group in rejected:
            holonomy_rejections += 1
            group_set = set(group)
            labels = {field: ('single', field) if field in group_set else ('other',)
                      for field in model.initial[variable]}
            split(variable, labels)

    return partitions, dict(split_events=split_events,
                            holonomy_rejections=holonomy_rejections)


def named_groups(model, partitions, edges):
    touched = {v for edge in edges for v, _ in edge['sources'] + edge['targets']}
    return [dict(variable=v, groups=groups,
                 labels=[dict(node=n, block=b, label=model.nodes[n]['label'])
                         for n, b in model.variable_blocks[v]])
            for v, groups in partitions.items()
            if v in touched and any(len(group) > 1 for group in groups)]


def budget(model, stratum, edges, partitions):
    domains = {v: math.factorial(len(groups)) for v, groups in partitions.items()}
    old_r = new_r = 1
    head_details = []
    if stratum['recursive']:
        for rid in stratum['heads']:
            node = model.nodes['r:' + rid]
            variable = node['variables'][1]
            old = math.factorial(node['arity'][1])
            new = domains[variable]
            old_r *= old
            new_r *= new
            head_details.append(dict(name=model.relations[rid]['name'],
                                     attributes=model.relations[rid]['attributes'],
                                     groups=partitions[variable], old=old, new=new))
    old_s = new_s = 0
    details = []
    for edge in edges:
        variables = [v for v, _ in edge['sources'] + edge['targets']]
        before = edge['m'] * math.prod(math.factorial(len(model.initial[v]))
                                      for v in variables)
        after = edge['m'] * math.prod(domains[v] for v in variables)
        old_s += before
        new_s += after
        details.append(dict(index=edge['index'], kind=edge['kind'],
                            source=edge['feed'].base, target=edge['target'],
                            m=edge['m'], before=before, after=after,
                            source_groups=[partitions[v] for v, _ in edge['sources']],
                            target_groups=[partitions[v] for v, _ in edge['targets']]))
    return dict(stratum=stratum['index'], recursive=stratum['recursive'],
                transitions=len(edges), R_old=old_r, R_new=new_r,
                S_old=old_s, S_new=new_s, W_old=old_r * old_s,
                W_new=new_r * new_s, heads=head_details, edge_details=details)


def analyze(path):
    plan = json.loads(path.read_text())
    model = MODEL(plan)
    edges = describe_edges(model)
    global_groups, checks = contract(model, edges)
    global_rows = []
    local_rows = []
    for stratum in plan['strata']:
        selected = [edge for edge in edges if edge['stratum'] == stratum['index']]
        global_rows.append(budget(model, stratum, selected, global_groups))
        local_groups, local_checks = contract(model, selected)
        row = budget(model, stratum, selected, local_groups)
        row.update(checks=local_checks,
                   groups=named_groups(model, local_groups, selected))
        local_rows.append(row)
    return dict(global_mode=dict(strata=global_rows, checks=checks,
                                 groups=named_groups(model, global_groups, edges)),
                stratum_free_interfaces=dict(strata=local_rows))


def main():
    result = {path.stem: analyze(path) for path in sorted(PLANS.glob('*.json'))}
    baseline = {(row['query_id'], int(row['stratum'])): row
                for row in csv.DictReader((ROOT / 'factorial-kv-times-m-strata.csv').open())}
    independent = json.loads((ROOT / 'contraction-independent-global.json').read_text())
    independent_local = json.loads((ROOT / 'contraction-independent-stratum.json').read_text())
    rows = []
    for query, modes in result.items():
        for global_row, local in zip(modes['global_mode']['strata'],
                                     modes['stratum_free_interfaces']['strata']):
            sid = local['stratum']
            old = baseline[query, sid]
            other = next(row for row in independent[query]['strata']
                         if row['stratum'] == sid)
            assert global_row['W_old'] == int(old['total_m_W'])
            assert global_row['W_new'] == other['W_new'], (query, sid)
            other_local = next(row for row in independent_local[query]['strata']
                               if row['stratum'] == sid)
            assert local['W_new'] == other_local['W_new'], (query, sid)
            assert local['R_new'] == other_local['R_new'], (query, sid)
            assert local['W_new'] <= global_row['W_new'] <= global_row['W_old']
            rows.append(dict(query=old['query'], query_id=query, stratum=sid,
                             recursive=local['recursive'], transitions=local['transitions'],
                             R_old=local['R_old'], R_global=global_row['R_new'],
                             R_local=local['R_new'], W_old=local['W_old'],
                             W_global=global_row['W_new'], W_local=local['W_new'],
                             S_global=global_row['S_new'], S_local=local['S_new']))
    rows.sort(key=lambda row: (list(dict.fromkeys(k[0] for k in baseline)).index(row['query_id']),
                               row['stratum']))
    (ROOT / 'contracted-dp-details.json').write_text(json.dumps(result, indent=2) + '\n')
    with (ROOT / 'contracted-dp-strata.csv').open('w') as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    (ROOT / 'contracted-dp-strata.json').write_text(json.dumps(rows, indent=2) + '\n')
    print('strata', len(rows), 'transitions', sum(row['transitions'] for row in rows))
    print('grand totals', {key: sum(row[key] for row in rows)
                           for key in ['W_old', 'W_global', 'W_local']})
    for query in dict.fromkeys(row['query'] for row in rows):
        selected = [row for row in rows if row['query'] == query]
        print(query, {key: sum(row[key] for row in selected)
                      for key in ['W_old', 'W_global', 'W_local']})


if __name__ == '__main__':
    main()
