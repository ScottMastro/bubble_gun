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


def _compactable_map(flat):
    """Return a list ``c[idx][side] = (nbr_idx, nbr_side, overlap)`` or
    ``None`` for every (seg, side) that participates in a compactable
    (reciprocally degree-1) join."""
    n = len(flat)
    si = flat.start_indptr.tolist()
    sx = flat.start_nbr_idx.tolist()
    ss = flat.start_nbr_side.tolist()
    so = flat.start_overlap.tolist()
    ei = flat.end_indptr.tolist()
    ex = flat.end_nbr_idx.tolist()
    es = flat.end_nbr_side.tolist()
    eo = flat.end_overlap.tolist()

    def unique(idx, side):
        if side == 0:
            lo = si[idx]
            if si[idx + 1] - lo != 1:
                return None
            ni = sx[lo]
            if ni == idx:
                return None
            return (ni, ss[lo], so[lo])
        else:
            lo = ei[idx]
            if ei[idx + 1] - lo != 1:
                return None
            ni = ex[lo]
            if ni == idx:
                return None
            return (ni, es[lo], eo[lo])

    c = [[None, None] for _ in range(n)]
    for idx in range(n):
        for side in (0, 1):
            out = unique(idx, side)
            if out is None:
                continue
            nbr_idx, nbr_side, _ovl = out
            back = unique(nbr_idx, nbr_side)
            if back is None:
                continue
            if back[0] != idx or back[1] != side:
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

    si = flat.start_indptr.tolist()
    sx = flat.start_nbr_idx.tolist()
    ss = flat.start_nbr_side.tolist()
    so = flat.start_overlap.tolist()
    ei = flat.end_indptr.tolist()
    ex = flat.end_nbr_idx.tolist()
    es = flat.end_nbr_side.tolist()
    eo = flat.end_overlap.tolist()

    for (old_idx, old_side), (new_idx, new_side) in old_to_new.items():
        if old_side == 0:
            lo, hi = si[old_idx], si[old_idx + 1]
            nbr_idx_arr, nbr_side_arr, ovl_arr = sx, ss, so
        else:
            lo, hi = ei[old_idx], ei[old_idx + 1]
            nbr_idx_arr, nbr_side_arr, ovl_arr = ex, es, eo

        c = compactable[old_idx][old_side]
        c_nbr_idx = c[0] if c is not None else -1
        c_nbr_side = c[1] if c is not None else -1

        target_bucket = start_buckets[new_idx] if new_side == 0 else end_buckets[new_idx]

        for i in range(lo, hi):
            nbr_old_idx = nbr_idx_arr[i]
            nbr_old_side = nbr_side_arr[i]
            if nbr_old_idx == c_nbr_idx and nbr_old_side == c_nbr_side:
                continue  # consumed by unitig
            target = old_to_new.get((nbr_old_idx, nbr_old_side))
            if target is None:
                raise RuntimeError(
                    f"non-compactable edge points into unitig interior: "
                    f"{(old_idx, old_side)} -> {(nbr_old_idx, nbr_old_side)}")
            target_bucket.add((target[0], target[1], ovl_arr[i]))

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
    flat_idx = []
    flat_side = []
    flat_ovl = []
    sizes = [0] * n
    fi_ext = flat_idx.extend
    fs_ext = flat_side.extend
    fo_ext = flat_ovl.extend
    for i in range(n):
        bucket = buckets[i]
        if not bucket:
            continue
        entries = sorted(bucket)
        sizes[i] = len(entries)
        fi_ext(e[0] for e in entries)
        fs_ext(e[1] for e in entries)
        fo_ext(e[2] for e in entries)
    indptr = np.empty(n + 1, dtype=np.int32)
    indptr[0] = 0
    np.cumsum(sizes, out=indptr[1:], dtype=np.int32)
    nbr_idx = np.asarray(flat_idx, dtype=np.int32)
    nbr_side = np.asarray(flat_side, dtype=np.int8)
    overlap = np.asarray(flat_ovl, dtype=np.int32)
    return indptr, nbr_idx, nbr_side, overlap
