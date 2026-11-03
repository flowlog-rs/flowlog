#!/usr/bin/env python3
"""GALEN's binary intermediates against the first hops of its delta paths.

A join of A and B on a key has, for each key, A's count times B's count rows.
Semi-naive evaluation makes each final P and Q tuple a delta exactly once, so
a path's first-hop total over the final relations bounds the work the path
does (REPORT.md sections 5.2 and 6.1). The arranged sizes quoted alongside
come from the probe builds.

usage: scripts/galen_fanout.py
"""

import argparse

from galen_data import count_by, join_size, millions, relation


def main():
    argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter).parse_args()
    P, Q, C, U, R = (relation(name) for name in "PQCUR")
    print(f"|P|={len(P):,} |Q|={len(Q):,} |C|={len(C):,} |U|={len(U):,} |R|={len(R):,}")

    print("\nRule 3  P(x,z) :- P(y,w), U(w,r,z), Q(x,r,y)")
    print("  binary I1 raw  P|x|U on w            ", millions(join_size(count_by(P, 1), count_by(U, 0))),
          "(arranged distinct: 5.9M)")
    print("  dP path hop1   dP(y,w) -> U by w      ", millions(join_size(count_by(P, 1), count_by(U, 0))))
    print("  dQ path (a)    dQ(x,r,y) -> P by y    ", millions(join_size(count_by(Q, 2), count_by(P, 0))),
          " then U by (w,r)")
    print("  dQ path (b)    dQ(x,r,y) -> U by r    ", millions(join_size(count_by(Q, 1), count_by(U, 1))),
          " then P exists (y,w)")

    print("\nRule 4  P(x,z) :- C(y,w,z), P(x,w), P(x,y)")
    print("  binary I2 raw  C|x|P on w            ", millions(join_size(count_by(C, 1), count_by(P, 1))),
          "(arranged: 28.4M)")
    print("  dP(x,w) path   -> C by w             ", millions(join_size(count_by(P, 1), count_by(C, 1))),
          " then P exists (x,y)")
    print("  dP(x,y) path   -> C by y             ", millions(join_size(count_by(P, 1), count_by(C, 0))),
          " then P exists (x,w)")

    print("\nRule 6  Q(x,e,o) :- Q(x,y,z), R(y,u,e), Q(z,u,o)")
    print("  binary I3 raw  Q|x|R on y            ", millions(join_size(count_by(Q, 1), count_by(R, 0))),
          "(arranged distinct: 19.8M)")
    print("  dQ1 path       dQ(x,y,z) -> R by y   ", millions(join_size(count_by(Q, 1), count_by(R, 0))),
          " then Q by (z,u)")
    print("  dQ3 path (A)   dQ(z,u,o) -> R by u   ", millions(join_size(count_by(Q, 1), count_by(R, 1))),
          " then Q by (z,y) [exists idx (c2,c1)]")
    print("  dQ3 path (B)   dQ(z,u,o) -> Q by c2=z", millions(join_size(count_by(Q, 0), count_by(Q, 2))),
          " then R by (y,u)")
    print("  final-out fanout check: distinct R.c0", len(count_by(R, 0)), "distinct R.c1", len(count_by(R, 1)))


if __name__ == "__main__":
    main()
