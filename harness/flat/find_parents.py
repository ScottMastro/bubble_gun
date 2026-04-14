"""Flat port of the pangyplot-patched ``find_parents``.

Assigns ``parent_key`` to every bubble by finding the smallest
super-bubble that strictly contains it. Pure int-set arithmetic — no
Node objects.

Input:
    raw_bubbles: dict[bkey, (source_idx, sink_idx, inside_tuple)]
    btype_by_key: dict[bkey, str]  ("simple" | "insertion" | "super")

Output:
    parent_by_key: dict[bkey, bkey | None]
"""


def find_parents(raw_bubbles, btype_by_key):
    # Collect super-bubbles and their node sets.
    sb_node_sets = {}
    for bkey, (src, snk, inside) in raw_bubbles.items():
        if btype_by_key[bkey] == "super":
            s = set(inside)
            s.add(src)
            s.add(snk)
            sb_node_sets[bkey] = frozenset(s)

    # Invert: seg idx -> list of super-bubble keys it appears in.
    idx_to_sbs = {}
    for sbkey, nodes in sb_node_sets.items():
        for idx in nodes:
            idx_to_sbs.setdefault(idx, []).append(sbkey)

    parent_by_key = {}
    for bkey, (src, snk, inside) in raw_bubbles.items():
        b_nodes = set(inside)
        b_nodes.add(src)
        b_nodes.add(snk)

        candidates = idx_to_sbs.get(src, ())
        best_key = None
        best_size = None
        for sbkey in candidates:
            if sbkey == bkey:
                continue
            sb_nodes = sb_node_sets[sbkey]
            if b_nodes < sb_nodes:  # strict subset
                sz = len(sb_nodes)
                if best_size is None or sz < best_size:
                    best_key = sbkey
                    best_size = sz

        parent_by_key[bkey] = best_key

    return parent_by_key
