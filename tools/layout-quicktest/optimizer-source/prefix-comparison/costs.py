"""Data-independent distances between physical attribute permutations."""

from functools import lru_cache
from math import factorial


def lex_rank(order):
    return sum(sum(other < value for other in order[i + 1:]) * factorial(len(order) - i - 1)
               for i, value in enumerate(order))


def adjacent_rank(order):
    if len(order) < 2:
        return 0
    largest = len(order) - 1
    position = order.index(largest)
    parent = adjacent_rank(tuple(value for value in order if value != largest))
    offset = largest - position if parent % 2 == 0 else position
    return parent * len(order) + offset


def circular_rank(source, target, rank):
    delta = abs(rank(source) - rank(target))
    return min(delta, factorial(len(source)) - delta)


@lru_cache(maxsize=32768)
def distance(source, target, metric):
    """Use immutable column IDs for permutation ranks; edges are directed.

    Prefix weighting charges an inversion involving source position i by
    2**(-i). Every earlier position has twice the weight of the next one.
    """
    positions = {column: i for i, column in enumerate(target)}
    n = len(source)
    if metric == 'lex_steps':
        return abs(lex_rank(source) - lex_rank(target))
    if metric == 'relative_lex':
        source_positions = {column: i for i, column in enumerate(source)}
        return lex_rank(tuple(source_positions[column] for column in target))
    if metric == 'lex_ring':
        return circular_rank(source, target, lex_rank)
    if metric == 'adjacent_ring':
        return circular_rank(source, target, adjacent_rank)
    if metric == 'position_ring':
        return sum(min(abs(i - positions[column]), n - abs(i - positions[column]))
                   for i, column in enumerate(source))
    inversions = [(i, j) for i, a in enumerate(source)
                  for j in range(i + 1, n) if positions[a] > positions[source[j]]]
    if metric == 'inversion':
        return len(inversions)
    if metric == 'prefix':
        return sum(2.0 ** -i for i, _ in inversions)
    raise ValueError('Unknown metric: ' + metric)
