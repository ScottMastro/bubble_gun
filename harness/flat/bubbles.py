"""Lightweight bubble / chain records for the flat pipeline.

``find_bubbles`` / ``connect_bubbles`` / ``find_parents`` operate
directly on integer seg indices and emit these namedtuples. No
``BubbleGun.Node`` object is allocated.
"""
from collections import namedtuple


FlatBubble = namedtuple("FlatBubble", [
    "key",          # tuple(sorted((source_idx, sink_idx))) — stable id
    "source",       # int seg idx
    "sink",         # int seg idx
    "inside",       # tuple[int] — sorted inside seg idxs
    "btype",        # "simple" | "insertion" | "super"
    "parent_key",   # tuple or None
    "chain_id",     # int; 0 until connect_bubbles assigns
])


FlatChain = namedtuple("FlatChain", [
    "id",           # int
    "bubble_keys",  # tuple[tuple] in chain order
])


FindResult = namedtuple("FindResult", ["bubbles", "chains"])
# bubbles: dict[tuple, FlatBubble]
# chains:  list[FlatChain]


def _all_neighbors(flat, idx):
    """All neighbor seg idxs from both sides (order-agnostic — the caller
    sorts). Mirrors ``BubbleGun.Node.neighbors``."""
    out = []
    for ni, _, _ in flat.start_neighbors(idx):
        out.append(ni)
    for ni, _, _ in flat.end_neighbors(idx):
        out.append(ni)
    return out


def classify(flat, source, sink, inside):
    """Return one of 'simple', 'insertion', 'super'. Ported 1:1 from
    the patched ``BubbleGun.Bubble._classify``.
    """
    if len(inside) == 2:
        a, b = inside
        if (flat.start_degree(a) == 1 and flat.end_degree(a) == 1
                and flat.start_degree(b) == 1 and flat.end_degree(b) == 1):
            nbrs_a = sorted(_all_neighbors(flat, a))
            nbrs_b = sorted(_all_neighbors(flat, b))
            if nbrs_a == nbrs_b:
                # source and sink must not be directly connected
                if sink not in _all_neighbors(flat, source):
                    return "simple"
    if len(inside) == 1:
        a = inside[0]
        if flat.start_degree(a) == 1 and flat.end_degree(a) == 1:
            nbrs = sorted(_all_neighbors(flat, a))
            if nbrs == sorted([source, sink]):
                return "insertion"
    return "super"
