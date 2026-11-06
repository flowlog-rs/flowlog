"""Exact inversion counts for projected lexicographic Boolean cubes."""

from functools import lru_cache


@lru_cache(maxsize=65536)
def inversions(source, retained):
    """Count strict record inversions without enumerating the Boolean cube.

    Source fields must be distinct. Retained fields must belong to source;
    repeated retained references have the same order as their first occurrence.
    Dropped fields remain as duplicate witnesses, not pre-deduplicated rows.
    """
    if len(set(source)) != len(source):
        raise ValueError('Source order contains repeated fields')
    retained = tuple(dict.fromkeys(retained))
    if not set(retained) <= set(source):
        raise ValueError('Projection refers to fields absent from source')
    completed = retained + tuple(field for field in source if field not in retained)
    positions = {field: i for i, field in enumerate(completed)}
    target_prefixes = {}
    prefix = 0
    for field in completed:
        target_prefixes[field] = prefix
        prefix |= 1 << source.index(field)
    total = 0
    for i, first in enumerate(source):
        source_prefix = (1 << i) - 1
        for second in source[i + 1:]:
            if positions[first] > positions[second]:
                equal_fields = (source_prefix | target_prefixes[second]).bit_count()
                total += 1 << (2 * len(source) - equal_fields - 4)
    return total


def insertion_comparisons(source, retained):
    """Count comparisons of stable adjacent-shift insertion sort on the cube.

    The first projected row is minimal, so every later insertion makes one
    stopping comparison in addition to one comparison per strict inversion.
    This reference count is not the comparison count of Rust's general sort.
    """
    return (1 << len(source)) - 1 + inversions(source, retained)
