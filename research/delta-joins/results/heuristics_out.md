# Tuning set (REPORT.md section 10)

## Cases (each plan's wall time and peak RSS relative to the binary plan; incremental: commit too)

| Case | Mode | Held I | Plan | New index | Freed | Lookup rows | Saved / stored | Work / I | rho | Time | RSS | Commit | Runs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| TC G5K | batch | - | lookup | 0.00M | - | n/m | - | n/m | 1.00 | -10% | +1% |  | §4 |
| polonius borrow (me, pm) | batch | - | lookups in both | 0.00M | - | n/m | - | n/m | 1.00 | -3% | -1% |  | 3/3 |
| polonius borrow (me, pm) | batch | - | lookup in me only | 0.00M | - | n/m | - | n/m | n/m | -2% | -1% |  | 3/3 |
| polonius borrow (me, pm) | batch | - | lookup in pm only | 0.00M | - | n/m | - | n/m | n/m | -2% | -0% |  | 3/3 |
| tri G40K | batch | 63.9M | lookup chain | 0.00M | - | 2.6M | +93.0% | 1.0 | 1.00 | -50% | -77% |  | 3/3 |
| tri roadNet | batch | 17.5M | lookup chain | 0.00M | - | 8.3M | +51.4% | 1.3 | 1.00 | -38% | -1% |  | 3/3 |
| tri BerkStan | batch | 222.5M | lookup chain | 0.00M | - | 69.7M | +90.7% | 1.0 | 1.00 | -27% | -22% |  | 3/3 |
| SG G5K | batch | 0.0M | lookup chain | 0.00M | - | 76.0M | +0.0% | 1.0 | 2.34 | +74% | +41% |  | 3/3 |
| dyck kernel (R3+R4) | batch | 0.0M | lookup chain | 0.00M | - | 0.0M | +0.0% | 1.0 | 1.00 | +5% | +20% |  | 3/3 |
| andersen load | batch | 1.3M | delta | 2.65M | - | n/m | -1.8% | n/m | 1.00 | +5% | -5% |  | 3/3 |
| andersen store | batch | 0.9M | delta | 10.50M | - | n/m | -13.1% | n/m | 1.00 | +16% | +22% |  | 3/3 |
| CSPA R6 httpd | batch | 124.1M | delta, MA symmetric | 0.00M | - | 19.0M | +20.1% | 2.0 | 1.03 | -4% | -15% |  | §5.1 |
| CSPA R6 httpd | batch | 124.1M | delta, new MA index | 88.90M | - | 19.4M | +5.7% | 2.0 | 1.03 | +3% | -7% |  | §5.1 |
| CSPA R6 postgresql | batch | 110.5M | delta, MA symmetric | 0.00M | - | 13.0M | +19.4% | 2.0 | 1.03 | -2% | -3% |  | 3/3 |
| CSPA R6 postgresql | batch | 110.5M | delta, new MA index | 89.48M | - | 12.5M | +3.7% | 2.0 | 1.03 | +5% | -3% |  | 3/3 |
| CSPA R6 linux | batch | 14.9M | delta, MA symmetric | 0.00M | - | 12.1M | +11.9% | 2.0 | 1.03 | +2% | +3% |  | 3/3 |
| CSPA R6 linux | batch | 14.9M | delta, new MA index | 13.78M | - | 12.1M | +0.9% | 2.0 | 1.03 | +5% | +18% |  | 3/3 |
| GALEN R3 | batch | 5.9M | delta | 0.10M | - | n/m | +3.3% | 64.6 | n/m | +49% | -2% |  | §6.1 |
| GALEN R4 | batch | 28.4M | delta, C first | 0.23M | - | n/m | +15.8% | 104.1 | n/m | +617% | -28% |  | §6.1 |
| GALEN R4 | batch | 28.4M | delta, P first | 0.23M | - | n/m | +15.8% | 14.9 | n/m | +29% | -24% |  | §7 |
| GALEN R4 | batch | 28.4M | delta + router | 0.69M | - | n/m | +15.6% | 2.5 | n/m | +1% | -26% |  | §7 |
| GALEN R6 | batch | 19.8M | delta | 0.00M | - | n/m | +11.1% | 2.3 | n/m | +9% | -9% |  | §5.2 |
| tri G40K-split | inc | 63.9M | delta + filter | 0.00M | - | 4.2M | +47.6% | 1.0 | 1.00 | -35% | -66% | -48% | 3/9 |
| tri G40K-split | inc | 63.9M | delta, no filter | 0.00M | - | 7.5M | +47.6% | 2.1 | 1.00 | +17% | -64% | -50% | 9/9 |
| tri G40K-split | inc | 63.9M | delta, Old first, no filter | 0.00M | - | 5.7M | +47.6% | 1.1 | 1.00 | -33% | -66% | -48% | 6/9 |
| tri roadNet-split | inc | 17.5M | delta + filter | 0.00M | - | 8.3M | +30.3% | 1.3 | 1.00 | -19% | +17% | -34% | 3/9 |
| tri roadNet-split | inc | 17.5M | delta, no filter | 0.00M | - | 14.9M | +30.3% | 2.9 | 1.00 | +26% | +47% | -35% | 9/9 |
| tri roadNet-split | inc | 17.5M | delta, Old first, no filter | 0.00M | - | 8.3M | +30.3% | 1.9 | 1.00 | -8% | +32% | -36% | 6/9 |
| SG G5K-split | inc | 24.7M | delta | 24.73M | - | 68.7M | +0.0% | 1.0 | 2.34 | +118% | +68% | +95% | 3/3 |
| dyck kernel-split | inc | 0.6M | delta + loop filter | 0.00M | - | 0.0M | +0.8% | 1.0 | 1.00 | +11% | +16% | +6% | 3/9 |
| dyck kernel-split | inc | 0.6M | delta + snapshot filter | 0.00M | - | 0.0M | +0.8% | 16.6 | 1.00 | +15% | +14% | +5% | 6/9 |
| dyck kernel-split | inc | 0.6M | delta, no filter | 0.00M | - | 0.0M | +0.8% | 32.3 | 1.00 | +20% | +13% | +5% | 9/9 |
| CSPA httpd-split (R4, R6) | inc | 261.6M | delta in both, symmetric | 0.00M | - | n/m | +26.5% | 2.0 | 1.03 | -6% | -12% | -12% | §8 |
| CSPA httpd-split (R4, R6) | inc | 261.6M | delta in R4 only | 0.00M | - | n/m | +26.5% | n/m | n/m | -2% | -6% | -10% | §8 |
| CSPA httpd-split (R4, R6) | inc | 261.6M | delta in R6 only | 0.00M | - | n/m | +26.5% | n/m | n/m | -3% | -8% | -4% | §8 |

## Decisions (plan picked per case; '-' = binary plan)

| Case | H0 | H1 | H1e | H2 | H3 | H4 | H5 | H6 | H7 | Best measured |
|---|---|---|---|---|---|---|---|---|---|---|
| TC G5K | - | - | - | lookup | lookup | lookup | lookup | lookup | lookup | lookup |
| polonius borrow (me, pm) | - | - | - | lookups in both | lookups in both | lookups in both | lookups in both | lookups in both | lookups in both | lookups in both |
| tri G40K | - | lookup chain | lookup chain | lookup chain | lookup chain | lookup chain | lookup chain | lookup chain | lookup chain | lookup chain |
| tri roadNet | - | lookup chain | lookup chain | lookup chain | lookup chain | lookup chain | lookup chain | lookup chain | lookup chain | lookup chain |
| tri BerkStan | - | lookup chain | lookup chain | lookup chain | lookup chain | lookup chain | lookup chain | lookup chain | lookup chain | lookup chain |
| SG G5K | - | lookup chain | lookup chain | lookup chain | - | - | - | - | - | - |
| dyck kernel (R3+R4) | - | lookup chain | lookup chain | lookup chain | - | - | - | - | - | - |
| andersen load | - | - | delta | - | - | - | - | - | - | delta |
| andersen store | - | - | delta | - | - | - | - | - | - | - |
| CSPA R6 httpd | - | delta, MA symmetric | delta, MA symmetric | delta, MA symmetric | delta, MA symmetric | delta, MA symmetric | delta, MA symmetric | delta, MA symmetric | delta, MA symmetric | delta, MA symmetric |
| CSPA R6 postgresql | - | delta, MA symmetric | delta, MA symmetric | delta, MA symmetric | delta, MA symmetric | delta, MA symmetric | delta, MA symmetric | delta, MA symmetric | delta, MA symmetric | delta, MA symmetric |
| CSPA R6 linux | - | delta, MA symmetric | delta, MA symmetric | delta, MA symmetric | delta, MA symmetric | delta, MA symmetric | delta, MA symmetric | - | - | - |
| GALEN R3 | - | - | delta | - | delta | - | - | - | - | - |
| GALEN R4 | - | - | delta, P first | - | delta, C first | - | delta + router | delta + router | delta + router | delta + router |
| GALEN R6 | - | - | delta | - | delta | delta | delta | delta | delta | delta |
| tri G40K-split | - | delta + filter | delta + filter | delta + filter | delta + filter | delta + filter | delta + filter | delta + filter | delta + filter | delta + filter |
| tri roadNet-split | - | delta + filter | delta + filter | delta + filter | delta + filter | delta + filter | delta + filter | delta + filter | delta + filter | delta + filter |
| SG G5K-split | - | - | delta | - | - | - | - | - | - | - |
| dyck kernel-split | - | delta + loop filter | delta + loop filter | delta + loop filter | delta + loop filter | delta + loop filter | delta + loop filter | delta + loop filter | - | - |
| CSPA httpd-split (R4, R6) | - | delta in both, symmetric | delta in both, symmetric | delta in both, symmetric | delta in both, symmetric | delta in both, symmetric | delta in both, symmetric | delta in both, symmetric | delta in both, symmetric | delta in both, symmetric |

## Scorecard (geometric means over cases with a measured choice)

| Heuristic | Plans changed | Time | RSS | Time x RSS | Regressions > 5% | Worst time |
|---|---|---|---|---|---|---|
| H0 binary (FlowLog today) | 0 | +0% | +0% | +0% | 0  | 1.00x |
| H1 Materialize default | 12 | -7% | -11% | -17% | 4 (SG G5K, dyck kernel (R3+R4), tri roadNet-split, dyck kernel-split) | 1.74x |
| H1e Materialize eager | 18 | +1% | -9% | -9% | 9 (SG G5K, dyck kernel (R3+R4), andersen store, GALEN R3, GALEN R4, GALEN R6, tri roadNet-split, SG G5K-split, dyck kernel-split) | 2.18x |
| H2 structural | 14 | -8% | -11% | -18% | 4 (SG G5K, dyck kernel (R3+R4), tri roadNet-split, dyck kernel-split) | 1.74x |
| H3 size | 15 | +1% | -15% | -14% | 5 (GALEN R3, GALEN R4, GALEN R6, tri roadNet-split, dyck kernel-split) | 7.17x |
| H4 size + CPU guards | 13 | -10% | -13% | -22% | 3 (GALEN R6, tri roadNet-split, dyck kernel-split) | 1.11x |
| H5 = H4 + router | 14 | -10% | -15% | -24% | 3 (GALEN R6, tri roadNet-split, dyck kernel-split) | 1.11x |
| H6 = H5 + lookup-row charge | 13 | -10% | -15% | -24% | 3 (GALEN R6, tri roadNet-split, dyck kernel-split) | 1.11x |
| H7 = H6 + margin (post hoc) | 12 | -11% | -15% | -25% | 2 (GALEN R6, tri roadNet-split) | 1.09x |
| Best measured per case | - | -11% | -16% | -25% | - | 1.09x |

# Held-out set (REPORT.md section 11)

## Cases (each plan's wall time and peak RSS relative to the binary plan; incremental: commit too)

| Case | Mode | Held I | Plan | New index | Freed | Lookup rows | Saved / stored | Work / I | rho | Time | RSS | Commit | Runs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| reach livejournal | batch | - | lookup | 0.00M | - | n/m | - | n/m | 1.00 | -3% | +4% |  | 5/5 |
| cc livejournal | batch | - | lookup | 0.00M | - | n/m | - | n/m | 1.00 | -16% | +1% |  | 5/5 |
| sssp livejournal | batch | - | lookup | 0.00M | - | n/m | - | n/m | 1.00 | -9% | -1% |  | 5/5 |
| bipartite mind | batch | - | lookups | 0.00M | - | n/m | - | n/m | 1.00 | -2% | +0% |  | 5/5 |
| csda httpd | batch | - | lookup | 0.00M | - | n/m | - | n/m | 1.00 | -10% | -1% |  | 5/5 |
| polonius borrow (sa) | batch | - | lookup | 0.00M | - | n/m | - | n/m | 1.00 | +1% | -0% |  | 3/3 |
| polonius borrow (sb) | batch | 0.0M | lookup chain | 0.00M | - | 0.0M | +0.0% | 1.0 | 1.00 | -1% | +0% |  | 3/3 |
| doop batik (AC) | batch | 11.7M | delta, written order | 0.05M | - | 0.0M | +6.9% | 2.4 | 0.99 | +3% | -11% |  | 3/3 |
| doop batik (AC) | batch | 11.7M | delta, cost order | 0.05M | 37.4M | 0.0M | +29.0% | 1.4 | 0.99 | -9% | -30% |  | 3/3 |
| doop batik (IFPT) | batch | 1.0M | delta | 0.11M | - | 0.0M | +0.5% | 2.2 | 1.01 | +10% | -0% |  | 3/3 |

## Decisions (plan picked per case; '-' = binary plan)

| Case | H0 | H1 | H1e | H2 | H3 | H4 | H5 | H6 | H7 | Best measured |
|---|---|---|---|---|---|---|---|---|---|---|
| reach livejournal | - | - | - | lookup | lookup | lookup | lookup | lookup | lookup | - |
| cc livejournal | - | - | - | lookup | lookup | lookup | lookup | lookup | lookup | lookup |
| sssp livejournal | - | - | - | lookup | lookup | lookup | lookup | lookup | lookup | lookup |
| bipartite mind | - | - | - | lookups | lookups | lookups | lookups | lookups | lookups | lookups |
| csda httpd | - | - | - | lookup | lookup | lookup | lookup | lookup | lookup | lookup |
| polonius borrow (sa) | - | - | - | lookup | lookup | lookup | lookup | lookup | lookup | - |
| polonius borrow (sb) | - | lookup chain | lookup chain | lookup chain | - | - | - | - | - | lookup chain |
| doop batik (AC) | - | - | delta, written order | - | delta, written order | delta, cost order | delta, cost order | delta, cost order | delta, cost order | delta, cost order |
| doop batik (IFPT) | - | - | delta | - | delta | delta | delta | delta | - | - |

## Scorecard (geometric means over cases with a measured choice)

| Heuristic | Plans changed | Time | RSS | Time x RSS | Regressions > 5% | Worst time |
|---|---|---|---|---|---|---|
| H0 binary (FlowLog today) | 0 | +0% | +0% | +0% | 0  | 1.00x |
| H1 Materialize default | 1 | -0% | +0% | -0% | 0  | 1.00x |
| H1e Materialize eager | 3 | +1% | -1% | -0% | 1 (doop batik (IFPT)) | 1.10x |
| H2 structural | 7 | -5% | +0% | -4% | 0  | 1.01x |
| H3 size | 8 | -3% | -1% | -4% | 1 (doop batik (IFPT)) | 1.10x |
| H4 size + CPU guards | 8 | -5% | -3% | -8% | 1 (doop batik (IFPT)) | 1.10x |
| H5 = H4 + router | 8 | -5% | -3% | -8% | 1 (doop batik (IFPT)) | 1.10x |
| H6 = H5 + lookup-row charge | 8 | -5% | -3% | -8% | 1 (doop batik (IFPT)) | 1.10x |
| H7 = H6 + margin (post hoc) | 7 | -6% | -3% | -9% | 0  | 1.01x |
| Best measured per case | - | -6% | -4% | -9% | - | 1.00x |

# Both sets

## Scorecard, both sets (geometric means over cases with a measured choice)

| Heuristic | Plans changed | Time | RSS | Time x RSS | Regressions > 5% | Worst time |
|---|---|---|---|---|---|---|
| H0 binary (FlowLog today) | 0 | +0% | +0% | +0% | 0  | 1.00x |
| H1 Materialize default | 13 | -5% | -8% | -12% | 4 (SG G5K, dyck kernel (R3+R4), tri roadNet-split, dyck kernel-split) | 1.74x |
| H1e Materialize eager | 21 | +1% | -7% | -6% | 10 (SG G5K, dyck kernel (R3+R4), andersen store, GALEN R3, GALEN R4, GALEN R6, tri roadNet-split, SG G5K-split, dyck kernel-split, doop batik (IFPT)) | 2.18x |
| H2 structural | 21 | -7% | -7% | -14% | 4 (SG G5K, dyck kernel (R3+R4), tri roadNet-split, dyck kernel-split) | 1.74x |
| H3 size | 23 | -0% | -11% | -11% | 6 (GALEN R3, GALEN R4, GALEN R6, tri roadNet-split, dyck kernel-split, doop batik (IFPT)) | 7.17x |
| H4 size + CPU guards | 21 | -9% | -10% | -18% | 4 (GALEN R6, tri roadNet-split, dyck kernel-split, doop batik (IFPT)) | 1.11x |
| H5 = H4 + router | 22 | -9% | -11% | -19% | 4 (GALEN R6, tri roadNet-split, dyck kernel-split, doop batik (IFPT)) | 1.11x |
| H6 = H5 + lookup-row charge | 21 | -9% | -11% | -19% | 4 (GALEN R6, tri roadNet-split, dyck kernel-split, doop batik (IFPT)) | 1.11x |
| H7 = H6 + margin (post hoc) | 19 | -9% | -12% | -20% | 2 (GALEN R6, tri roadNet-split) | 1.09x |
| Best measured per case | - | -9% | -12% | -20% | - | 1.09x |
