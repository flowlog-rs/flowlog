"""Compare acyclic-edge greedies and legal join-layout local searches."""

from collections import defaultdict, deque
import csv
from dataclasses import dataclass
from functools import cache
from itertools import combinations, permutations, product
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def acyclic(edges):
    successors = defaultdict(list)
    indegree = defaultdict(int)
    for a, b in set(edges):
        successors[a].append(b)
        indegree[a] += 0
        indegree[b] += 1
    ready = deque(a for a, degree in indegree.items() if degree == 0)
    visited = 0
    while ready:
        a = ready.popleft()
        visited += 1
        for b in successors[a]:
            indegree[b] -= 1
            if indegree[b] == 0:
                ready.append(b)
    return visited == len(indegree)


def topological_order(nodes, edges):
    result = []
    remaining = set(nodes)
    while remaining:
        ready = sorted(a for a in remaining
                       if not any(v == a and u in remaining for u, v in edges))
        assert ready
        result.append(ready[0])
        remaining.remove(ready[0])
    return tuple(result)


def preserves_hard(order, hard):
    positions = {a: i for i, a in enumerate(order)}
    return all(positions[a] < positions[b] for a, b in hard)


def forward_reward(order, soft):
    positions = {a: i for i, a in enumerate(order)}
    return sum(weight for a, b, weight in soft if positions[a] < positions[b])


def edge_greedy(nodes, hard, soft, opportunity):
    edges = set(hard)
    remaining = set(range(len(soft)))
    while remaining:
        feasible = {i for i in remaining if acyclic(edges | {soft[i][:2]})}
        if not feasible:
            break
        priorities = []
        for i in feasible:
            weight = soft[i][2]
            blocked = sum(soft[j][2] for j in feasible - {i}
                          if not acyclic(edges | {soft[i][:2], soft[j][:2]})) if opportunity else 0
            priorities.append((weight / (1 + blocked), weight, -i, i))
        chosen = max(priorities)[3]
        edges.add(soft[chosen][:2])
        remaining.remove(chosen)
    order = topological_order(nodes, edges)
    assert preserves_hard(order, hard)
    return order


def insertions(order):
    result = set()
    for source in range(len(order)):
        shorter = order[:source] + order[source + 1:]
        for destination in range(len(order)):
            changed = shorter[:destination] + order[source:source + 1] + shorter[destination:]
            if changed != order:
                result.add(changed)
    return result


def improve_order(order, hard, soft):
    history = [forward_reward(order, soft)]
    while True:
        feasible = [candidate for candidate in insertions(order) if preserves_hard(candidate, hard)]
        if not feasible:
            return order, history
        candidate = min(feasible, key=lambda candidate: (-forward_reward(candidate, soft), candidate))
        reward = forward_reward(candidate, soft)
        if reward <= history[-1]:
            return order, history
        order = candidate
        history.append(reward)


def graph_cases():
    summaries = []
    for count in [3, 8, 32]:
        nodes = tuple(range(count + 2))
        hard = {(1, x) for x in nodes[2:]}
        soft = [(0, 1, 1)] + [(x, 0, 1) for x in nodes[2:]]
        naive = edge_greedy(nodes, hard, soft, False)
        aware = edge_greedy(nodes, hard, soft, True)
        improved, history = improve_order(naive, hard, soft)
        assert forward_reward(naive, soft) == 1
        assert forward_reward(aware, soft) == count
        assert forward_reward(improved, soft) == count
        summaries.append({"family": "one_bad_edge", "good_edges": count,
                          "optimum_reward": count, "edge_greedy": 1,
                          "opportunity_greedy": count, "edge_plus_insert": count,
                          "insertion_history": history})
    names = ["edge", "opportunity", "edge_insert", "opportunity_insert"]
    stats = {name: {"optimal": 0, "reward": 0, "worst_loss": 0} for name in names}
    nodes = tuple(range(5))
    hard = {(0, 1)}
    pairs = list(combinations(nodes, 2))
    legal = [order for order in permutations(nodes) if preserves_hard(order, hard)]
    optimum_sum = 0
    for bits in product([0, 1], repeat=len(pairs)):
        soft = [(a, b, 1) if bit == 0 else (b, a, 1) for (a, b), bit in zip(pairs, bits)]
        optimum = max(forward_reward(order, soft) for order in legal)
        optimum_sum += optimum
        naive = edge_greedy(nodes, hard, soft, False)
        aware = edge_greedy(nodes, hard, soft, True)
        outputs = [naive, aware, improve_order(naive, hard, soft)[0],
                   improve_order(aware, hard, soft)[0]]
        for name, order in zip(names, outputs):
            reward = forward_reward(order, soft)
            assert reward <= optimum
            stats[name]["optimal"] += reward == optimum
            stats[name]["reward"] += reward
            stats[name]["worst_loss"] = max(stats[name]["worst_loss"], optimum - reward)
    return {"trap_family": summaries, "all_five_vertex_tournaments": {
        "instances": 1024, "unit_soft_edges": 10, "hard_edges": [[0, 1]],
        "legal_orders_per_instance": 60, "sum_optimum_reward": optimum_sum,
        "algorithms": stats,
    }}


def distance(source, target):
    positions = {a: i for i, a in enumerate(target)}
    return sum(positions[a] > positions[b] for i, a in enumerate(source)
               for b in source[i + 1:])


@dataclass(frozen=True)
class TreeModel:
    first: str
    second: str
    last: str
    free_inputs: bool

    @property
    def schemas(self):
        return {"R": "kab", "S": "kcd", "T": "bdz"}

    @property
    def fields(self):
        schemas = self.schemas
        key = "".join(a for a in schemas[self.first] if a in schemas[self.second])
        left = "".join(a for a in schemas[self.first] if a not in key)
        right = "".join(a for a in schemas[self.second] if a not in key)
        needed = set("acz" + schemas[self.last])
        retained = "".join(a for a in key + left + right if a in needed)
        root_key = "".join(a for a in retained if a in schemas[self.last])
        root_left = "".join(a for a in retained if a not in root_key)
        root_right = "".join(a for a in schemas[self.last] if a not in root_key)
        return key, left, right, root_key, root_left, root_right, "acz"

    def orders(self, state):
        key, left, right, root_key, root_left, root_right, head, side1, side2 = state
        generated1 = key + (left + right if side1 == "0" else right + left)
        generated2 = root_key + (root_left + root_right if side2 == "0" else root_right + root_left)
        retained = root_key + root_left
        completed1 = retained + "".join(a for a in generated1 if a not in retained)
        completed2 = head + "".join(a for a in generated2 if a not in head)
        indexes = {self.first: key + left, self.second: key + right,
                   self.last: root_key + root_right}
        return indexes, generated1, completed1, generated2, completed2

    @cache
    def cost(self, state):
        indexes, generated1, completed1, generated2, completed2 = self.orders(state)
        inputs = 0 if self.free_inputs else sum(distance(self.schemas[name], order)
                                                for name, order in indexes.items())
        return inputs + distance(generated1, completed1) + distance(generated2, completed2)

    def valid(self, state):
        if any(sorted(order) != sorted(fields) for order, fields in zip(state[:7], self.fields)):
            return False
        if state[7] not in {"0", "1"} or state[8] not in {"0", "1"}:
            return False
        indexes, g1, c1, g2, c2 = self.orders(state)
        key, _, _, root_key, root_left, root_right, head, _, _ = state
        first_drop = "".join(a for a in g1 if a not in root_key + root_left)
        root_drop = "".join(a for a in g2 if a not in head)
        return (indexes[self.first].startswith(key) and indexes[self.second].startswith(key)
                and indexes[self.last] == root_key + root_right
                and c1 == root_key + root_left + first_drop and c2 == head + root_drop)

    def follow_output(self, state, join):
        changed = list(state)
        _, g1, _, g2, _ = self.orders(state)
        source = g1 if join == 0 else g2
        for index in ([3, 4] if join == 0 else [6]):
            changed[index] = "".join(a for a in source if a in self.fields[index])
        return tuple(changed)

    def follow_inputs(self, state, join):
        changed = list(state)
        _, _, c1, _, c2 = self.orders(state)
        target = c1 if join == 0 else c2
        for index in ([0, 1, 2] if join == 0 else [3, 4, 5]):
            changed[index] = "".join(a for a in target if a in self.fields[index])
        return tuple(changed)

    @cache
    def neighbors(self, state, joint):
        result = set()
        for index in range(7):
            for order in insertions(state[index]):
                changed = list(state)
                changed[index] = order
                result.add(tuple(changed))
        for join in [0, 1]:
            changed = list(state)
            changed[7 + join] = "1" if state[7 + join] == "0" else "0"
            flipped = tuple(changed)
            result.add(flipped)
            if joint:
                for base in [state, flipped]:
                    result.add(self.follow_output(base, join))
                    result.add(self.follow_inputs(base, join))
                    result.add(self.follow_output(self.follow_inputs(base, join), join))
                affected = [0, 1, 2, 3, 4] if join == 0 else [3, 4, 5, 6]
                for index in affected:
                    for order in insertions(flipped[index]):
                        changed = list(flipped)
                        changed[index] = order
                        result.add(tuple(changed))
        result.discard(state)
        assert all(self.valid(candidate) for candidate in result)
        return tuple(sorted(result))

    @cache
    def descend(self, state, joint):
        assert self.valid(state)
        history = [self.cost(state)]
        while True:
            candidates = self.neighbors(state, joint)
            candidate = min(candidates, key=lambda x: (self.cost(x), x))
            if self.cost(candidate) >= history[-1]:
                return state, tuple(history)
            state = candidate
            history.append(self.cost(state))

    def seeds(self):
        result = [tuple(self.fields) + ("0", "0")]
        for seed in range(1, 8):
            orders = []
            for index, fields in enumerate(self.fields):
                rotation = (seed // 2 + index) % len(fields) if fields else 0
                order = fields[rotation:] + fields[:rotation]
                orders.append(order[::-1] if seed & 1 else order)
            result.append(tuple(orders) + (str(seed & 1), str((seed >> 1) & 1)))
        return result


def layout_cases():
    rows = []
    trajectories = []
    for first, second, last in [("R", "S", "T"), ("R", "T", "S"), ("S", "T", "R")]:
        for free_inputs in [False, True]:
            model = TreeModel(first, second, last, free_inputs)
            domains = [list(map("".join, permutations(fields))) for fields in model.fields]
            states = list(product(*domains, ["0", "1"], ["0", "1"]))
            assert len(states) == 384
            optimum = min(model.cost(state) for state in states)
            for name, joint in [("single_move", False), ("join_joint_move", True)]:
                endings = [model.descend(state, joint) for state in states]
                costs = [model.cost(end) for end, _ in endings]
                default_end, default_history = model.descend(model.seeds()[0], joint)
                seed_ends = [model.descend(seed, joint)[0] for seed in model.seeds()]
                best_seed = min(seed_ends, key=lambda state: (model.cost(state), state))
                assert all(cost >= optimum for cost in costs)
                assert all(all(a > b for a, b in zip(history, history[1:]))
                           for _, history in endings)
                rows.append({
                    "tree": f"({first},{second}),{last}", "free_inputs": free_inputs,
                    "algorithm": name, "optimum_cost": optimum,
                    "default_start_cost": model.cost(model.seeds()[0]),
                    "default_final_cost": model.cost(default_end),
                    "eight_start_final_cost": model.cost(best_seed),
                    "all_starts": len(states), "optimal_endings": sum(cost == optimum for cost in costs),
                    "mean_excess_cost": sum(cost - optimum for cost in costs) / len(costs),
                    "worst_excess_cost": max(costs) - optimum,
                })
                trajectories.append({"tree": f"({first},{second}),{last}",
                                     "free_inputs": free_inputs, "algorithm": name,
                                     "default_history": default_history,
                                     "best_eight_start_state": best_seed})
    return rows, trajectories


def join_plateau_case():
    model = TreeModel("R", "S", "T", False)
    initial = ("k", "ab", "cd", "db", "ac", "z", "acz", "1", "0")
    changed = ("k", "ab", "cd", "bd", "ac", "z", "acz", "0", "0")
    single, single_history = model.descend(initial, False)
    joint, joint_history = model.descend(initial, True)
    assert single == initial
    assert single_history == (15,)
    assert model.cost(changed) == 13
    assert model.cost(joint) == 13
    return {
        "initial_J1_left": "S", "initial_J2_key_on_both_inputs": "db",
        "head_order": "acz", "initial_cost": 15,
        "single_move_history": single_history, "joint_move_history": joint_history,
        "changed_J1_left": "R", "changed_J2_key_on_both_inputs": "bd",
        "changed_cost": 13,
    }


def main():
    graphs = graph_cases()
    print("Unit-edge traps and all 1,024 constrained five-vertex tournaments checked.", flush=True)
    rows, trajectories = layout_cases()
    result = {"graphs": graphs, "layout_runs": rows, "trajectories": trajectories,
              "join_plateau": join_plateau_case(),
              "scope": "Exact small cost-model checks; no runtime data sampling or performance measurements"}
    (ROOT / "greedy-results.json").write_text(json.dumps(result, indent=2) + "\n")
    with (ROOT / "greedy-layouts.csv").open("w") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(json.dumps(graphs["all_five_vertex_tournaments"], indent=2), flush=True)
    for row in rows:
        print(row, flush=True)


if __name__ == "__main__":
    main()
