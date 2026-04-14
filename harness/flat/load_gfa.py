"""Stream a GFA file into a ``FlatGraph``.

Mirrors BubbleGun ``graph_io.read_gfa`` edge handling — the four
from_strand / to_strand cases each add a pair of half-edges (one per
endpoint). Duplicate four-tuples are dropped, same as the reference.

No ``Node`` objects are allocated at any point.
"""
from collections import defaultdict

import numpy as np

from harness.flat.graph import FlatGraph


_SIDE_START = 0
_SIDE_END = 1


def _parse_overlap(tok):
    if tok == "*":
        return 0
    # overlap tokens are like "0M" — drop the trailing char
    return int(tok[:-1])


def load(gfa_path):
    g = FlatGraph()

    # Pass 1: collect S lines and buffered L lines. Segment idx is
    # assigned in file-encounter order so a stable round-trip with
    # read_gfa is possible downstream.
    raw_edges = []  # (from_id, from_strand, to_id, to_strand, overlap)
    seq_lens = []

    with open(gfa_path) as fh:
        for line in fh:
            if not line:
                continue
            kind = line[0]
            if kind == "S":
                parts = line.rstrip("\n").split("\t")
                sid = parts[1]
                seq = parts[2]
                if sid in g.id_to_idx:
                    continue  # duplicate S line; keep the first
                g.id_to_idx[sid] = len(g.seg_ids)
                g.seg_ids.append(sid)
                seq_lens.append(len(seq) if seq != "*" else 0)
            elif kind == "L":
                parts = line.rstrip("\n").split("\t")
                raw_edges.append((parts[1], parts[2], parts[3], parts[4],
                                  _parse_overlap(parts[5])))

    n = len(g.seg_ids)
    g.seq_len = np.asarray(seq_lens, dtype=np.int32)

    # Pass 2: for each L line emit the pair of half-edges. Collect per
    # side as plain Python lists first, dedup via a set, then build CSR.
    # start_buckets[idx] = list of (nbr_idx, nbr_side, overlap)
    start_buckets = defaultdict(list)
    end_buckets = defaultdict(list)
    seen_start = defaultdict(set)
    seen_end = defaultdict(set)

    def add_start(a_idx, nbr_idx, nbr_side, ovl):
        key = (nbr_idx, nbr_side, ovl)
        if key in seen_start[a_idx]:
            return
        seen_start[a_idx].add(key)
        start_buckets[a_idx].append(key)

    def add_end(a_idx, nbr_idx, nbr_side, ovl):
        key = (nbr_idx, nbr_side, ovl)
        if key in seen_end[a_idx]:
            return
        seen_end[a_idx].add(key)
        end_buckets[a_idx].append(key)

    for from_id, from_strand, to_id, to_strand, ovl in raw_edges:
        a = g.id_to_idx.get(from_id)
        b = g.id_to_idx.get(to_id)
        if a is None or b is None:
            continue  # dangling link; legacy just warns and skips

        from_start = (from_strand == "-")
        to_end = (to_strand == "-")

        if from_start and to_end:        # L x - y -
            add_start(a, b, _SIDE_END, ovl)
            add_end(b, a, _SIDE_START, ovl)
        elif from_start and not to_end:  # L x - y +
            add_start(a, b, _SIDE_START, ovl)
            add_start(b, a, _SIDE_START, ovl)
        elif (not from_start) and (not to_end):  # L x + y +
            add_end(a, b, _SIDE_START, ovl)
            add_start(b, a, _SIDE_END, ovl)
        else:                            # L x + y -
            add_end(a, b, _SIDE_END, ovl)
            add_end(b, a, _SIDE_END, ovl)

    g.start_indptr, g.start_nbr_idx, g.start_nbr_side, g.start_overlap = \
        _build_csr(n, start_buckets)
    g.end_indptr, g.end_nbr_idx, g.end_nbr_side, g.end_overlap = \
        _build_csr(n, end_buckets)

    return g


def _build_csr(n, buckets):
    # Flatten in idx order, then bulk-convert once. Avoids the
    # per-element numpy assignment that dominates large-graph load.
    flat_idx = []
    flat_side = []
    flat_ovl = []
    sizes = [0] * n
    fi_ext = flat_idx.extend
    fs_ext = flat_side.extend
    fo_ext = flat_ovl.extend
    for i in range(n):
        entries = buckets.get(i)
        if not entries:
            continue
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
