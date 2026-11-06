"""Restrict equivalent field groups to compatible consecutive layouts.

The restrictions preserve a minimum of the raw Boolean-record inversion
objective on the supplied edges. Fixed interfaces remain unchanged. Edges
with unsupported cost semantics disable contraction of their interfaces.
"""

from collections import defaultdict
import itertools


def contract_orders(model, edges, state=None, fixed=(), enabled=True):
    """Return full field permutations, compatible groups, and audit counters.

    Edges use the ``describe_edges`` representation. Every free variable is
    a permutation of its field indices. Fixed and pinned variables have one
    order, taken from ``state``. Join orientations are excluded.
    """
    state = tuple(tuple(order) for order in (state or model.initial))
    side_variables = set(model.side_variables)
    variables = set(range(len(model.initial))) - side_variables
    touched = {v for edge in edges
               for v, _ in edge['sources'] + edge['targets']} - side_variables
    frozen = (set(fixed) | set(model.pinned) | (variables - model.active))
    reductions = set(getattr(model, 'reductions', {}))
    unsupported = {edge['index'] for edge in edges
                   if edge.get('opaque', False)
                   or edge['target'] in reductions
                   or edge['feed'].base in reductions}
    partitions = {v: [list(range(len(model.initial[v])))]
                  if model.initial[v] else [] for v in variables}
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
        return split(variable, {field: field for field in model.initial[variable]})

    for variable in variables:
        if variable in frozen or not enabled:
            singleton(variable)
    for edge in edges:
        if edge['index'] in unsupported:
            for variable, _ in edge['sources'] + edge['targets']:
                singleton(variable)

    while True:
        changed = True
        while changed:
            changed = False
            for edge in edges:
                if edge['index'] in unsupported:
                    continue
                images = edge['images']
                for variable, refs in edge['sources']:
                    labels = {}
                    for field, ref in enumerate(refs):
                        mapped = images.get(ref, ())
                        if not mapped:
                            labels[field] = ('drop',)
                        elif len(mapped) != 1:
                            labels[field] = ('alias', field)
                        else:
                            target_var, target_field = mapped[0]
                            labels[field] = ('target', target_var,
                                             positions(target_var)[target_field])
                    changed |= split(variable, labels)
                for variable, cols in edge['targets']:
                    labels = {}
                    for field, col in enumerate(cols):
                        ref = edge['feed'].references[col]
                        if ref is None or len(images.get(ref, ())) != 1:
                            labels[field] = ('constant_or_alias', field)
                        else:
                            role, source_var, source_field = edge['origin'][ref]
                            labels[field] = ('source', role, source_var,
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
                parent[max(a, b)] = min(a, b)

        for edge in edges:
            if edge['index'] in unsupported:
                continue
            for variable, refs in edge['sources']:
                for group in partitions[variable]:
                    if len(group) < 2 or not edge['images'].get(refs[group[0]]):
                        continue
                    for field in group:
                        mapped = edge['images'][refs[field]]
                        if len(mapped) != 1:
                            raise ValueError('Contracted field has multiple images')
                        union((variable, field), mapped[0])

        rejected = []
        for variable, groups in partitions.items():
            for group in groups:
                if len({find((variable, field)) for field in group}) != len(group):
                    rejected.append((variable, group))
        if rejected:
            for variable, group in rejected:
                holonomy_rejections += 1
                bad = set(group)
                split(variable, {field: ('single', field) if field in bad
                                 else ('other',)
                                 for field in model.initial[variable]})
            continue
        break

    for variable, groups in partitions.items():
        for group in groups:
            # A reversed field map needs reversed local indices to preserve
            # one common internal field order at every materialization.
            group.sort(key=lambda field: find((variable, field)))

    orders = {}
    for variable in sorted(touched):
        if variable in frozen:
            orders[variable] = [state[variable]]
        else:
            orders[variable] = [tuple(field for group in sequence for field in group)
                                for sequence in itertools.permutations(partitions[variable])]
        if any(sorted(order) != sorted(model.initial[variable])
               for order in orders[variable]):
            raise ValueError('Contraction produced an invalid field permutation')

    stats = dict(enabled=enabled, split_events=split_events,
                 holonomy_rejections=holonomy_rejections,
                 unsupported_edges=sorted(unsupported, key=str),
                 fixed_variables=sorted(touched & frozen),
                 contracted_variables=sorted(v for v in touched
                                             if any(len(g) > 1 for g in partitions[v])),
                 groups={v: partitions[v] for v in sorted(touched)})
    return orders, partitions, stats
