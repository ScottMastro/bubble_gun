"""One-shot unitig contraction on a FlatGraph.

A "compactable join" connects side s_a of segment a to side s_b of
segment b when BOTH joined sides have degree 1 and the join is not a
self-loop. Each maximal chain of compactable joins is a unitig; every
unitig is collapsed into a single segment in the output FlatGraph.

TODO(phase3): reconstruct merged sequences with overlap trimming. For
Phase 1 the harness drops ``seq`` post-compaction, so we only carry
``seq_len`` (naive sum — overlaps ignored). That is enough for the
find_bubbles topology check.
"""
import numpy as np

from harness.flat.graph import FlatGraph


def _unique_nbr_on_side(flat, idx, side):
    """If seg ``idx`` has exactly one neighbor on ``side`` and it's not a
    self-loop, return ``(nbr_idx, nbr_side, overlap)``. Else ``None``."""
    if side == 0:
        if flat.start_degree(idx) != 1:
            return None
        nbr = next(flat.start_neighbors(idx))
    else:
        if flat.end_degree(idx) != 1:
            return None
        nbr = next(flat.end_neighbors(idx))
    if nbr[0] == idx:
        return None
    return nbr


def _compactable_map(flat):
    """Return a list ``c[idx][side] = (nbr_idx, nbr_side, overlap)`` or
    ``None`` for every (seg, side) that participates in a compactable
    (reciprocally degree-1) join."""
    n = len(flat)
    c = [[None, None] for _ in range(n)]
    for idx in range(n):
        for side in (0, 1):
            out = _unique_nbr_on_side(flat, idx, side)
            if out is None:
                continue
            nbr_idx, nbr_side, _ovl = out
            back = _unique_nbr_on_side(flat, nbr_idx, nbr_side)
            if back is None:
                continue
            b_idx, b_side, _b_ovl = back
            if b_idx != idx or b_side != side:
                continue
            c[idx][side] = out
    return c


class _UF:
    __slots__ = ("p",)

    def __init__(self, n):
        self.p = list(range(n))

    def find(self, x):
        p = self.p
        while p[x] != x:
            p[x] = p[p[x]]
            x = p[x]
        return x

    def union(self, x, y):
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return
        if rx < ry:
            self.p[ry] = rx
        else:
            self.p[rx] = ry


def _walk_unitig(members_set, start_member, compactable):
    """Given a member set that shares one union-find root and a starting
    endpoint, walk the compactable chain in order. Return
    ``(ordered_members, inner_side_at_head, inner_side_at_tail)``."""
    c0 = compactable[start_member][0]
    c1 = compactable[start_member][1]
    if (c0 is None) == (c1 is None):
        # start_member is not an endpoint — caller's mistake
        raise RuntimeError("walk_unitig called from non-endpoint")
    cur_inner = 0 if c0 is not None else 1

    order = [start_member]
    inner_sides = [cur_inner]

    cur = start_member
    while True:
        nbr_idx, nbr_arrival_side, _ovl = compactable[cur][cur_inner]
        order.append(nbr_idx)
        inner_sides.append(nbr_arrival_side)
        next_side = 1 - nbr_arrival_side
        if compactable[nbr_idx][next_side] is None:
            break  # reached the other endpoint
        cur = nbr_idx
        cur_inner = next_side

    return order, inner_sides[0], inner_sides[-1]


def compact(flat):
    n = len(flat)
    compactable = _compactable_map(flat)

    uf = _UF(n)
    for idx in range(n):
        for side in (0, 1):
            c = compactable[idx][side]
            if c is not None:
                uf.union(idx, c[0])

    groups = {}
    for i in range(n):
        groups.setdefault(uf.find(i), []).append(i)

    # Build per-unitig record: members in walk order + outer-side info
    unitigs = []
    for root, members in groups.items():
        if len(members) == 1:
            unitigs.append({
                "members": members,
                "inner_head": None,
                "inner_tail": None,
            })
            continue

        # Find an endpoint (member with exactly one compactable side).
        endpoint = None
        for m in members:
            c0 = compactable[m][0]
            c1 = compactable[m][1]
            if (c0 is None) != (c1 is None):
                endpoint = m
                break

        if endpoint is None:
            # Circular unitig (every side compactable) — pathological.
            # Emit each member as a singleton so downstream sees a
            # consistent graph instead of crashing.
            for m in members:
                unitigs.append({"members": [m], "inner_head": None, "inner_tail": None})
            continue

        order, inner_head, inner_tail = _walk_unitig(set(members), endpoint, compactable)
        unitigs.append({
            "members": order,
            "inner_head": inner_head,
            "inner_tail": inner_tail,
        })

    # Stable unitig idx ordering: ascending min-member-idx.
    # Representative GFA id for each unitig = lowest-idx member's id
    # (matches legacy's insertion-order walk rule).
    unitigs.sort(key=lambda u: min(u["members"]))

    new_n = len(unitigs)
    new_seg_ids = []
    new_seq_lens = []
    # old_to_new[(old_idx, old_side)] = (new_idx, new_side)
    # new_side: 0 if this (old, old_side) lands on unitig's start-outer,
    #           1 if on unitig's end-outer. Interior (old, old_side) pairs
    #           are not mapped (they never appear in the new adjacency).
    old_to_new = {}

    for new_idx, u in enumerate(unitigs):
        members = u["members"]
        rep = min(members)
        new_seg_ids.append(flat.seg_ids[rep])
        new_seq_lens.append(int(sum(int(flat.seq_len[m]) for m in members)))

        if len(members) == 1:
            m = members[0]
            old_to_new[(m, 0)] = (new_idx, 0)
            old_to_new[(m, 1)] = (new_idx, 1)
        else:
            head = members[0]
            tail = members[-1]
            old_to_new[(head, 1 - u["inner_head"])] = (new_idx, 0)
            old_to_new[(tail, 1 - u["inner_tail"])] = (new_idx, 1)

    # Rewrite adjacency: for every mapped (old, old_side), scan its
    # half-edges and translate each non-compactable target.
    start_buckets = [set() for _ in range(new_n)]
    end_buckets = [set() for _ in range(new_n)]

    for (old_idx, old_side), (new_idx, new_side) in old_to_new.items():
        nbrs = (flat.start_neighbors(old_idx) if old_side == 0
                else flat.end_neighbors(old_idx))
        for nbr_old_idx, nbr_old_side, ovl in nbrs:
            # Skip compactable joins — they're consumed into the unitig.
            c = compactable[old_idx][old_side]
            if c is not None and c[0] == nbr_old_idx and c[1] == nbr_old_side:
                continue
            target = old_to_new.get((nbr_old_idx, nbr_old_side))
            if target is None:
                # Would mean neighbor's side is interior to some unitig,
                # which is impossible when the source side isn't compactable.
                raise RuntimeError(
                    f"non-compactable edge points into unitig interior: "
                    f"{(old_idx, old_side)} -> {(nbr_old_idx, nbr_old_side)}")
            nbr_new_idx, nbr_new_side = target
            entry = (nbr_new_idx, int(nbr_new_side), int(ovl))
            if new_side == 0:
                start_buckets[new_idx].add(entry)
            else:
                end_buckets[new_idx].add(entry)

    out = FlatGraph()
    out.seg_ids = new_seg_ids
    out.id_to_idx = {sid: i for i, sid in enumerate(new_seg_ids)}
    out.seq_len = np.asarray(new_seq_lens, dtype=np.int32)
    out.start_indptr, out.start_nbr_idx, out.start_nbr_side, out.start_overlap = \
        _bucket_to_csr(new_n, start_buckets)
    out.end_indptr, out.end_nbr_idx, out.end_nbr_side, out.end_overlap = \
        _bucket_to_csr(new_n, end_buckets)
    return out


def _bucket_to_csr(n, buckets):
    indptr = np.zeros(n + 1, dtype=np.int32)
    for i in range(n):
        indptr[i + 1] = indptr[i] + len(buckets[i])
    m = int(indptr[-1])
    nbr_idx = np.empty(m, dtype=np.int32)
    nbr_side = np.empty(m, dtype=np.int8)
    overlap = np.empty(m, dtype=np.int32)
    for i in range(n):
        off = indptr[i]
        for j, (ni, ns, ov) in enumerate(sorted(buckets[i])):
            nbr_idx[off + j] = ni
            nbr_side[off + j] = ns
            overlap[off + j] = ov
    return indptr, nbr_idx, nbr_side, overlap
