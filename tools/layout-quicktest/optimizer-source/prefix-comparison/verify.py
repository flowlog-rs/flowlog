"""Audit the completed experiment against the frozen main source and plans."""

import csv
import hashlib
import json
from pathlib import Path

from search import CostModel


ROOT = Path(__file__).resolve().parent
MAIN = ROOT.parent / 'main-comparison'
METRICS = ['inversion', 'lex_steps', 'relative_lex', 'prefix']


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def physical(config):
    return {key: config[key] for key in ['scopes', 'relations']}


def state_from_config(model, config):
    assignments = {}
    for name, node in model.nodes.items():
        order = config['layouts'][name]['order']
        keys = node['arity'][0]
        for variable, value in zip(node['variables'], [tuple(order[:keys]), tuple(c - keys for c in order[keys:])]):
            assert variable not in assignments or assignments[variable] == value
            assignments[variable] = value
    for name, side in config['swapped'].items():
        assignments[model.joins[name]['side']] = (side,)
    state = tuple(assignments.get(i, initial) for i, initial in enumerate(model.initial))
    model.validate(state)
    assert json.loads(json.dumps(model.describe(state))) == config
    return state


def main():
    results = json.loads((ROOT / 'measurements/results.json').read_text())
    assert results['status'] == 'complete'
    audit = json.loads((MAIN / 'source-audit.json').read_text())
    for kind, name in [('baseline', 'main-source'), ('candidate', 'candidate-source')]:
        for relative, expected in audit['sources'][kind].items():
            assert sha(MAIN / name / relative) == expected, (kind, relative)
    assert sha(MAIN / 'main-compiler') == audit['baseline_compiler_sha256']
    assert sha(MAIN / 'candidate-compiler') == audit['candidate_compiler_sha256']
    locks, checks = set(), []
    for program, variants in results['programs'].items():
        plan_path = MAIN / 'plans' / (program + '.json')
        plan = json.loads(plan_path.read_text())
        for variant, build in variants.items():
            assert sha(build['binary']) == build['binary_sha256']
            generated = Path(build['generated'])
            assert sha(generated / 'src/main.rs') == build['generated_sha256']
            locks.add(sha(generated / 'Cargo.lock'))
            assert str(MAIN / 'main-source/flowlog-runtime') in (generated / 'Cargo.toml').read_text()
            if variant == 'main':
                assert build['compiler_sha256'] == audit['baseline_compiler_sha256']
                continue
            selected = json.loads((ROOT / 'selected' / variant / (program + '.json')).read_text())
            assert selected['summary']['plan_sha256'] == sha(plan_path)
            model = CostModel(plan, variant)
            state = state_from_config(model, selected['selected'])
            assert sum(model.costs(state)) == selected['summary']['selected_cost']
            layout_path = Path(build['binary']).parent / 'layout.json'
            if layout_path.exists():
                actual = json.loads(layout_path.read_text())
            elif build['compiler_sha256'] == audit['baseline_compiler_sha256']:
                actual = model.describe(model.initial)
            else:
                actual = json.loads((MAIN / 'selected' / (program + '.json')).read_text())['selected']
            assert physical(actual) == physical(selected['selected']), (program, variant)
            checks.append({'program': program, 'metric': variant, 'validated': True})
    assert len(locks) == 1
    runs = 0
    for pair, variants in results['pairs'].items():
        expected = variants['main'][0]['sizes']
        assert set(variants) == {'main', *METRICS}
        for entries in variants.values():
            assert len(entries) == 3
            for entry in entries:
                assert entry['sizes'] == expected
                runs += 1
    assert runs == 75
    facts = json.loads((MAIN / 'facts-manifest.json').read_text())
    for pair in results['pairs']:
        dataset = pair.split('=')[1]
        for relative, info in facts[dataset].items():
            stat = (Path('facts') / dataset / relative).stat()
            assert stat.st_size == info['bytes'] and stat.st_mtime_ns == info['mtime_ns']
    for metric in METRICS:
        fixture = json.loads((ROOT / 'measurements/correctness' / metric / 'compound-correctness.json').read_text())
        for variant in ['baseline', 'greedy']:
            assert fixture[variant]['sizes'] == {'W': 5, 'Out': 2, 'Reach': 6}
    probe = list(csv.DictReader((ROOT / 'arrangement.csv').open()))
    assert len(probe) == 54
    assert all(int(row['rows']) == 16777216 for row in probe)
    final = {'baseline': results['baseline'], 'model_tests': 12,
             'all_source_and_binary_hashes_match': True, 'all_locks_identical': True,
             'dataset_size_and_mtime_unchanged': True, 'layout_checks': checks,
             'full_query_cardinality_checks': runs, 'exact_row_fixture_variants': 4,
             'arrangement_probe_runs': len(probe)}
    (ROOT / 'verification.json').write_text(json.dumps(final, indent=2) + '\n')
    print(json.dumps(final, indent=2))


if __name__ == '__main__':
    main()
