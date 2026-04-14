"""Flat numpy/CSR representation of a GFA graph.

Replaces the ``{id: BubbleGun.Node}`` dict-of-objects model with contiguous
integer-indexed arrays. Each segment has a small internal idx (0..n-1);
string ids from the GFA are kept on the side for snapshot output.

Adjacency is stored as two CSR-style structures (start side / end side),
each composed of three parallel 1D arrays:

    start_indptr:    int32[n+1]
    start_nbr_idx:   int32[m_start]
    start_nbr_side:  int8[m_start]     (0 = nbr's start, 1 = nbr's end)
    start_overlap:   int32[m_start]

...and the same four for ``end_*``. Self-edges and parallel edges are
preserved; duplicates (same four-tuple) are deduped during load to
match BubbleGun's ``graph_io.read_gfa`` behavior.
"""
from dataclasses import dataclass, field
from typing import List

import numpy as np


@dataclass
class FlatGraph:
    seg_ids: List[str] = field(default_factory=list)       # idx -> GFA id
    id_to_idx: dict = field(default_factory=dict)          # GFA id -> idx

    seq_len: np.ndarray = None                             # int32[n]

    start_indptr: np.ndarray = None                        # int32[n+1]
    start_nbr_idx: np.ndarray = None                       # int32[m]
    start_nbr_side: np.ndarray = None                      # int8[m]
    start_overlap: np.ndarray = None                       # int32[m]

    end_indptr: np.ndarray = None
    end_nbr_idx: np.ndarray = None
    end_nbr_side: np.ndarray = None
    end_overlap: np.ndarray = None

    def __len__(self):
        return len(self.seg_ids)

    def start_degree(self, idx):
        return int(self.start_indptr[idx + 1] - self.start_indptr[idx])

    def end_degree(self, idx):
        return int(self.end_indptr[idx + 1] - self.end_indptr[idx])

    def start_neighbors(self, idx):
        """Yield ``(nbr_idx, nbr_side, overlap)`` for neighbors attached
        to this segment's start side."""
        lo = self.start_indptr[idx]
        hi = self.start_indptr[idx + 1]
        for i in range(lo, hi):
            yield (int(self.start_nbr_idx[i]),
                   int(self.start_nbr_side[i]),
                   int(self.start_overlap[i]))

    def end_neighbors(self, idx):
        lo = self.end_indptr[idx]
        hi = self.end_indptr[idx + 1]
        for i in range(lo, hi):
            yield (int(self.end_nbr_idx[i]),
                   int(self.end_nbr_side[i]),
                   int(self.end_overlap[i]))

    def total_edges(self):
        """Count of half-edges on start side + end side (= 2 × #L lines,
        modulo dedup). Mirrors what the legacy Node model reports as
        ``sum(len(n.start) + len(n.end) for n in graph.nodes.values())``."""
        return int(len(self.start_nbr_idx) + len(self.end_nbr_idx))
