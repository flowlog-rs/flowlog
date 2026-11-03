# Held-out picks, recorded before the held-out runs

Recorded 2026-11-03 23:10 UTC, before the timed runs of `scripts/heldout_bench.sh`.
The inputs come from the count and probe builds (`results/probe_doop_*_batik.txt`,
`results/probe_polonius_*_borrow.txt`); single smoke runs of the Family A and
Polonius variants had only checked that they compute the right relations. The
thresholds are the tuning set's, unchanged: WORK_MAX = 4, RHO_MAX = 1.5,
LOOKUP_ROW_WEIGHT = 2.

Path work W follows the convention of the tuning set's numbers: the partial
tuples after every hop but the last, plus the input rows that drive paths.
Rows of loop variables that drive paths are left out, because the binary plan
joins each of them too. Counting them as well would give W = 1.5 for Polonius
sb, 5.6 and 4.6 for Doop AC (written and cost order) and 75 for Doop IFPT, and
H4 to H6 would then keep the binary plan on both Doop rules.

Output of `scripts/heuristics.py --picks`, held-out part:

## Held-out set: plan picked per case ('-' = binary plan)

| Case | H0 | H1 | H1e | H2 | H3 | H4 | H5 | H6 |
|---|---|---|---|---|---|---|---|---|
| reach livejournal | - | - | - | lookup | lookup | lookup | lookup | lookup |
| cc livejournal | - | - | - | lookup | lookup | lookup | lookup | lookup |
| sssp livejournal | - | - | - | lookup | lookup | lookup | lookup | lookup |
| bipartite mind | - | - | - | lookups | lookups | lookups | lookups | lookups |
| csda httpd | - | - | - | lookup | lookup | lookup | lookup | lookup |
| polonius borrow (sa) | - | - | - | lookup | lookup | lookup | lookup | lookup |
| polonius borrow (sb) | - | lookup chain | lookup chain | lookup chain | - | - | - | - |
| doop batik (AC) | - | - | delta, written order | - | delta, written order | delta, cost order | delta, cost order | delta, cost order |
| doop batik (IFPT) | - | - | delta | - | delta | delta | delta | delta |
