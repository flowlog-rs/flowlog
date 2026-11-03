#!/usr/bin/env python3
"""GALEN: fixed hop orders against proposing from the smaller index per row.

A fixed delta path pays, for each delta tuple, the fan-out of its first
index. Proposing per row from whichever candidate index has fewer matches
(dogs3's count, propose and validate) pays the smaller of the two counts,
plus a count lookup in each. For the costly path of rules 3, 4 and 6 this
prints both fixed orders' totals and the per-row minimum (REPORT.md
section 6.1).

usage: scripts/galen_wco.py
"""

import argparse

from galen_data import count_by, millions, relation


def main():
    argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter).parse_args()
    P, Q, C, U, R = (relation(name) for name in "PQCUR")
    P0, C0, U1, R1, Q2 = count_by(P, 0), count_by(C, 0), count_by(U, 1), count_by(R, 1), count_by(Q, 2)

    # Rule 4, dP(x,y): propose (w,z) from C(y,_,_), or w from P(x,_).
    by_c = sum(C0[y] for x, y in P)
    by_p = sum(P0[x] for x, y in P)
    best = sum(min(C0[y], P0[x]) for x, y in P)
    print(f"rule 4 dP(x,y): C by y {millions(by_c)} | P by x {millions(by_p)} | "
          f"per-row min {millions(best)}   (binary I2 = 28.4M)")

    # Rule 3, dQ(x,r,y): propose w from P(y,_), or (w,z) from U(_,r,_).
    by_p = sum(P0[y] for x, r, y in Q)
    by_u = sum(U1[r] for x, r, y in Q)
    best = sum(min(P0[y], U1[r]) for x, r, y in Q)
    print(f"rule 3 dQ(x,r,y): P by y {millions(by_p)} | U by r {millions(by_u)} | "
          f"per-row min {millions(best)}   (binary I1 = 5.9M)")

    # Rule 6, dQ(z,u,o): propose (y,e) from R(_,u,_), or (x,y) from Q(_,_,z).
    by_r = sum(R1[u] for z, u, o in Q)
    by_q = sum(Q2[z] for z, u, o in Q)
    best = sum(min(R1[u], Q2[z]) for z, u, o in Q)
    print(f"rule 6 dQ(z,u,o): R by u {millions(by_r)} | Q by z {millions(by_q)} | "
          f"per-row min {millions(best)}   (binary I3 = 19.8M)")


if __name__ == "__main__":
    main()
