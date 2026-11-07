# Delta joins for CTADL's analyses

[CTADL] is Sandia's taint analyzer. It imports Android apps through JADX, or binaries through
Ghidra's P-code, and analyzes them with Soufflé programs, which [ctadl-flowlog] runs on FlowLog.
This note asks whether the study's policies for choosing a plan (REPORT [§10][s10] and [§11][s11])
pick well on those programs, and proposes one more, H8. As in the study, every plan but the binary
one is FlowLog's generated code, rewritten by a patch.

The workloads are four of ctadl-flowlog's [realistic programs][ctadl-realistic], two per front end:
the Android apps Binary Eye and Wikipedia, and the Ubuntu 22.04 binaries wget and sqlite3. Their
FlowLog programs are those ctadl-flowlog generates at b11379f. Every run used 32 workers.

## In brief

- **One rule family is worth switching.** The intermediates of eight near-identical VirtualAssign
  rules hold 6–20% of the records each program stores at its peak; no other holds more than about
  1%.
- **Switching all eight** lowers peak RSS by 13%, 10% and 3% on Binary Eye, Wikipedia and sqlite3,
  with exec time within a point. On wget it saves 1% of RSS and costs 4% exec.
- **The study's policies switch everything or almost nothing.** Without a margin, H1e and H3–H6
  switch wget too. H7 tests its margin rule by rule, and each rule holds only part of the family's
  saving, so it switches two rules on Binary Eye and nothing elsewhere.
- **H8, set after these runs, tests the margin per family.** It picks the best configuration in
  hindsight on all four workloads: −1% exec and −7% peak RSS (geometric means), none worse. On the
  study's 29 cases, where each family is one rule, it picks what H7 picks.

## Where delta joins apply

`scripts/candidates.py` sorts a generated program's joins by what their binary plan holds (REPORT
§9) and, given a probe run, sums what they hold at the program's peak
([`results/ctadl_candidates.txt`](results/ctadl_candidates.txt)). In the index phase:

| Share of the records stored at the program's peak | Binary Eye | Wikipedia | wget | sqlite3 |
|---|---|---|---|---|
| The eight VirtualAssign rules' intermediates | 19.5% | 17.9% | 6.1% | 8.1% |
| The largest other intermediate | 0.7% | 0.9% | 0.6% | 1.0% |
| All other intermediates | 8.9% | 10.2% | 1.8% | 3.4% |

The query phase's intermediates hold at most 1.1% together at its peak. Summed at their own peaks,
they hold up to 21% (sqlite3), and those of the P-code index phase's non-recursive rules 7.7%
(wget), but those peaks come at other times, so freeing them would barely lower peak memory.

**The family.** Here is r1, simplified (choice bookkeeping dropped, equalities substituted):

```
VirtualAssign_choice(v1, p13, v2, p23, ctx, _) :-
    Vertex(v2, p23), VirtualAssign_choice(v1, "", v2, p2, ctx, _), p2 != "",
    ComputedAccessPathNonEmptySplit(p23, p2, p13), isComputedAccessPath(p13),
    !CVar_isGlobal(v1), !CVar_isGlobal(v2), v1 != v2.
```

- r3 splits v1's access path instead of v2's, and r5 and r7 mirror r1 and r3, with the two
  variables swapped. Each has a twin, r2, r4, r6 or r8, that tests `p13 != p23` instead of
  `v1 != v2`.
- All three atoms change in one loop: A = Vertex, B = VirtualAssign_choice and
  C = ComputedAccessPathNonEmptySplit. The binary plan joins A and B on v2 into I and holds I,
  arranged by C's key. A delta join holds B by C's key instead, which is smaller in every rule.

**The plans.** [`patches/ctadl_va_delta.py`](patches/ctadl_va_delta.py) adds a delta join beside
each rule's binary plan, and the binary picks one when it builds its dataflow. `DJ_VA` names the
rules to switch (`all`, or a list such as `r1,r5`), and `DJ_VA_ORDER=cost` picks the cost order over
the text order. A path reads the atoms before its own as New and those after as Old (REPORT §1.4):

| Path | Text order (1 new index) | Cost order (3 new indexes, 2 of them small copies of C) |
|---|---|---|
| dA | B (Old), then C (Old) | C by A's columns (Old), then B (Old) |
| dB | A (New), then C (Old) | C by B's columns (Old), then A (New) |
| dC | B by C's key (New), then A (New) | the same |

## Results

All eight rules switched, in the cost order, against the binary plan. Records come from one probe
run each. Exec, the time to finish the dataflow, and peak RSS are medians of 5 interleaved runs, 3
on Wikipedia ([`results/ctadl_heuristics_out.md`](results/ctadl_heuristics_out.md)):

| Workload | Binary plan: exec, peak RSS | Records saved: predicted, probed | Exec | Peak RSS |
|---|---|---|---|---|
| Binary Eye | 8.79 s, 4,523 MB | 17.9%, 25.3% | −1% | −13% |
| Wikipedia | 34.59 s, 11,886 MB | 16.0%, 16.0% | 0% | −10% |
| wget | 10.68 s, 5,293 MB | 2.5%, 2.5% | +4% | −1% |
| sqlite3 | 29.51 s, 13,912 MB | 6.4%, 6.4% | −1% | −3% |

- **The outputs are unchanged.** All 22 checked runs, 12 of them with delta joins, match CTADL's
  own FlowLog run, table by table: 107 tables on JADX, 150 on P-code and 13 in the query phase, up
  to 52.6M rows. FlowLog against Soufflé is the subject of [ctadl-flowlog#7][ctadl-pr].
- **The size model is exact on traces.** It predicts the held I minus the new indexes. On Binary
  Eye, freeing the I's also empties their batchers, 7.5% more.
- **Peak RSS falls about half as much as stored records,** and the gain tracks how far I outgrows
  its inputs: r5's I/(A+B) is 4.5 on Binary Eye, 2.3 on Wikipedia, 1.1 on sqlite3 and 0.74 on wget.
- **wget's +4% is real:** its runs took 10.48–10.94 s with the binary plan and 11.02–11.28 s with
  all eight switched. Likely the lookups save little work there, while the new indexes cost upkeep.
- **The order matters little at run time.** The text order's exec and peak RSS are within a point
  of the cost order's, though its lookups hold far more rows on P-code (2.08M against 137K on
  sqlite3) and fewer on Wikipedia (744K against 5.7M).
- **Wall time is 40–65% SQLite writes,** the same rows for every plan, with up to ±10% disk noise.
  So the policies below are ranked by exec × RSS.

## Choosing a plan

[`scripts/ctadl_heuristics.py`](scripts/ctadl_heuristics.py) runs the study's policies
(`scripts/heuristics.py`) on the eight rules, with planner inputs from probe runs, and scores each
by the configuration it picks for each program. Geometric means over the four workloads:

| Policy | Picks | Exec | Peak RSS | Exec × RSS |
|---|---|---|---|---|
| H0 (FlowLog today), H1 Materialize, H2 structural | the binary plan everywhere | 0% | 0% | 0% |
| H1e Materialize eager, H3 size | all eight rules, text order | 0% | −6% | −6% |
| H4 size + CPU guards, H5 + router | all eight, cost order | 0% | −7% | −7% |
| H6 + lookup-row charge | r1, r2 and r5–r8, cost order | 0% | −7% | −6% |
| H7 + 5% margin | r5 and r6 on Binary Eye only | 0% | −3% | −3% |
| **H8: H5, with H7's tests per family** | **all eight, cost order, except on wget** | **−1%** | **−7%** | **−7%** |
| The best in hindsight | the same as H8 | −1% | −7% | −7% |

No policy makes a workload more than 5% worse; the worst pick, switching wget, costs it 4–5% exec.

- **H1 and H2 never fire:** every plan here needs a new index.
- **H1e and H3–H6 have no margin, so they switch wget.** The CPU guards cannot catch it: these
  plans match each update once (ρ = 1), and path work W is not measured.
- **H7's margin is per rule, so it misses savings split across a family.** On Wikipedia, r1, r2, r5
  and r6 save 3.7% each, 14.8% together; on sqlite3, r5–r8 save 1.5% each.

**H8** keeps H5's picks for a family of rules only if, together, they pass H7's tests: they save at
least 5% of the records the program stores, and more than 2 records for each row their lookups
hold, counted once. A family is the rules that join the same atoms into the same relation, here the
eight VirtualAssign rules. In the study each case is a family of its own, and H8 picks what H7
picks on all 29.

H8 decides per family, not per program, so that a large saving cannot carry a small one: in Doop
(§11), AssignCast's delta join saves 29% of the records, and InstanceFieldPointsTo's saves 0.5%
but costs 10% more time; H8, like H7, switches only the first. Any margin between wget's saving
(2.5%) and sqlite3's (6.4%) gives H8 the same picks here, and the study's cases allow 1–11%
([§11.4][s114]), so 5% fits both.

## Lessons for FlowLog's planner

1. **Score candidates at the program's peak.** An operator's own peak overstates what freeing it
   saves: the query phase's intermediates hold up to 21% at theirs, and at most 1.1% at the
   program's.
2. **Decide per rule family.** Generated programs repeat a rule in near-identical copies, each
   holding only a share of the saving.
3. **Let the input decide.** wget and sqlite3 run the same program, and only sqlite3 gains, so no
   choice made from the program alone suits both. One binary holds both plans, so it could choose
   at run time, from its input sizes or an earlier probe run.

## Caveats

- H8 was set after these runs, so, like H7, it needs a fresh test.
- Four workloads on the study's shared machine, with 3–5 timed runs per configuration.
- Only the VirtualAssign family was switched. No other intermediate holds more than about 1%, but
  together they hold 1.8–10.2%.
- Lookup rows per rule are bounds: a probe reports one peak for the whole program.

## How to rerun

Set CTADL up for FlowLog as [ctadl-flowlog's README][ctadl-setup] describes, with ctadl-flowlog at
b11379f and FlowLog at `main` 49d9999. FACTS below is a `ctadl import` directory, as ctadl-flowlog's
`bench.py` makes for each realistic program. From `research/delta-joins`, with `scripts/env.sh`
sourced:

```sh
scripts/ctadl_stage.py binaryeye FACTS
scripts/build.py ctadl_ji_dj ctadl_jq_p ctadl_pi_dj ctadl_pq_p
# Planner inputs: a probe run of the binary plan, each rule's sizes, and the joins by kind.
scripts/ctadl_run.py --label probe-ji-binaryeye --bin "$BIN/ctadl_ji_p" --phase binaryeye/index --probe
scripts/ctadl_heuristics.py --sizes --results "$RESULTS"
scripts/candidates.py "$WORK/crates/ctadl_ji_p/src/main.rs" --probe "$RESULTS/ctadl/probe-ji-binaryeye-0.log"
# Probe runs with outputs checked: p-binaryeye-base without --env, then each configuration.
scripts/ctadl_run.py --label p-binaryeye-cost-all --bin "$BIN/ctadl_ji_dj" --phase binaryeye/index \
  --probe --check --env DJ_VA=all --env DJ_VA_ORDER=cost
# Timed runs, then the scorecard.
REPS=5 scripts/ctadl_bench.sh binaryeye=base,cost-all,text-all
scripts/ctadl_heuristics.py --results "$RESULTS"
```

Run each step on all four workloads before the next, since the `ctadl_heuristics.py` steps read
all four: wikipedia as binaryeye, and wget and sqlite3 with the P-code builds (`ctadl_pi_*`, labels
`probe-pi-*`), staged with `--query CTADL/src/ctadl/models/pcode/default-query.json`. The query
phase's joins come from probe runs of `ctadl_jq_p` and `ctadl_pq_p` on `WORKLOAD/query`.
`ctadl_heuristics.py --needed` lists the picks still to time; with no arguments, it rescores the
runs in `results/`.

[CTADL]: https://github.com/sandialabs/ctadl
[ctadl-flowlog]: https://github.com/flowlog-rs/ctadl-flowlog
[ctadl-pr]: https://github.com/flowlog-rs/ctadl-flowlog/pull/7
[ctadl-setup]: https://github.com/flowlog-rs/ctadl-flowlog/blob/b11379f89779fc295eeeb4ef133dc285cd2678b5/flowlog-backend/README.md#setup
[ctadl-realistic]: https://github.com/flowlog-rs/ctadl-flowlog/blob/b11379f89779fc295eeeb4ef133dc285cd2678b5/flowlog-backend/README.md#realistic-programs
[s10]: REPORT.md#10-choosing-a-plan-seven-policies-on-more-workloads
[s11]: REPORT.md#11-held-out-test-the-policies-on-nine-new-cases
[s114]: REPORT.md#114-lessons
