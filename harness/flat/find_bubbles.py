"""Flat port of BubbleGun's ``find_bubbles`` (with the pangyplot perf
patch). Operates directly on the ``FlatGraph`` CSR — no ``Node``
objects.

Output: ``dict[(a_idx, b_idx), (source_idx, sink_idx, inside_tuple)]``
where the key is ``tuple(sorted((source, sink)))``. Inside tuple holds
the seg idxs found inside the bubble, in BFS visitation order.

Classification and packaging into ``FlatBubble`` happens in the driver
(commit 5) so this module is purely the BFS.
"""


def precompute_parent_sets(flat):
    """Per-seg (start_parents, end_parents) as frozensets of neighbor
    idxs. Mirrors ``_precompute_parent_ids`` in the pangyplot-patched
    ``BubbleGun/find_bubbles.py``."""
    n = len(flat)
    out = [None] * n
    for idx in range(n):
        start_ids = frozenset(ni for ni, _, _ in flat.start_neighbors(idx))
        end_ids = frozenset(ni for ni, _, _ in flat.end_neighbors(idx))
        out[idx] = (start_ids, end_ids)
    return out


def _find_sb_from(flat, parent_sets, s_idx, direction):
    """Port of patched ``find_sb_alg``. Returns
    ``(source_idx, sink_idx, tuple(inside_idxs))`` or ``None``."""
    seen = set()
    visited = set()
    nodes_inside = []
    seen.add((s_idx, direction))
    S = {(s_idx, direction)}

    while S:
        v_idx, v_dir = S.pop()
        visited.add(v_idx)
        if v_idx != s_idx:
            nodes_inside.append(v_idx)
        seen.discard((v_idx, v_dir))

        if v_dir == 0:
            children = list(flat.start_neighbors(v_idx))
        else:
            children = list(flat.end_neighbors(v_idx))

        if not children:
            break

        aborted = False
        for u_idx, u_side, _ovl in children:
            if u_side == 0:
                u_child_direction = 1
                u_parent_ids = parent_sets[u_idx][0]
            else:
                u_child_direction = 0
                u_parent_ids = parent_sets[u_idx][1]

            if u_idx == s_idx:
                S.clear()
                aborted = True
                break

            seen.add((u_idx, 1 - u_side))

            if u_idx in visited:
                continue

            if u_parent_ids <= visited:
                S.add((u_idx, u_child_direction))

        if aborted:
            break

        if len(S) == 1 and len(seen) == 1:
            t_idx, _t_dir = next(iter(S))
            S.clear()
            if not nodes_inside:
                break
            return (s_idx, t_idx, tuple(nodes_inside))

    return None


def find_bubbles(flat, parent_sets=None):
    """Run the BFS from every seg in both directions. Dedupes via
    ``key = tuple(sorted((source, sink)))`` — the same bubble found
    from two different seeds maps to the same key.
    """
    if parent_sets is None:
        parent_sets = precompute_parent_sets(flat)
    bubbles = {}
    n = len(flat)
    for idx in range(n):
        for d in (0, 1):
            res = _find_sb_from(flat, parent_sets, idx, d)
            if res is None:
                continue
            source, sink, _inside = res
            key = (source, sink) if source < sink else (sink, source)
            # Last-write-wins, mirroring the legacy find_bubbles behavior
            # (`graph.bubbles[bubble.key] = bubble`). With matching seed
            # iteration order, the final (source, sink) orientation per
            # key matches legacy exactly.
            bubbles[key] = res
    return bubbles
