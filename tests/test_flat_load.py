"""Parity checks for FlatGraph loader vs BubbleGun.graph_io.read_gfa.

Phase 1 commit 1: only loading — no compaction, no adapter, no
find_bubbles. If load degree distributions match, we can layer
compaction on top with confidence.
"""
import os

from BubbleGun.Graph import Graph

from harness.flat.load_gfa import load

FIXTURE = os.path.join(os.path.dirname(__file__), "..",
                       "harness", "fixtures", "DRB1-3123.gfa")


def test_drb1_segment_count():
    legacy = Graph(graph_file=FIXTURE)
    flat = load(FIXTURE)
    assert len(flat) == len(legacy.nodes)


def test_drb1_ids_match():
    legacy = Graph(graph_file=FIXTURE)
    flat = load(FIXTURE)
    assert set(flat.seg_ids) == set(legacy.nodes.keys())


def test_drb1_seq_len_matches():
    legacy = Graph(graph_file=FIXTURE)
    flat = load(FIXTURE)
    for sid, idx in flat.id_to_idx.items():
        assert int(flat.seq_len[idx]) == legacy.nodes[sid].seq_len


def test_drb1_degrees_match():
    legacy = Graph(graph_file=FIXTURE)
    flat = load(FIXTURE)
    mismatches = []
    for sid, idx in flat.id_to_idx.items():
        ls = len(legacy.nodes[sid].start)
        le = len(legacy.nodes[sid].end)
        fs = flat.start_degree(idx)
        fe = flat.end_degree(idx)
        if (ls, le) != (fs, fe):
            mismatches.append((sid, (ls, le), (fs, fe)))
    assert not mismatches, f"{len(mismatches)} degree mismatches; first: {mismatches[:5]}"


def test_drb1_total_half_edges_match():
    legacy = Graph(graph_file=FIXTURE)
    flat = load(FIXTURE)
    legacy_total = sum(len(n.start) + len(n.end) for n in legacy.nodes.values())
    assert flat.total_edges() == legacy_total


def test_drb1_adjacency_sets_match():
    """Compare (nbr_id, nbr_side, overlap) sets per node — the full
    adjacency fingerprint, order-independent."""
    legacy = Graph(graph_file=FIXTURE)
    flat = load(FIXTURE)

    diffs = []
    for sid, idx in flat.id_to_idx.items():
        lnode = legacy.nodes[sid]
        legacy_start = set((x[0], x[1], x[2]) for x in lnode.start)
        legacy_end = set((x[0], x[1], x[2]) for x in lnode.end)

        flat_start = {(flat.seg_ids[ni], ns, ov)
                      for ni, ns, ov in flat.start_neighbors(idx)}
        flat_end = {(flat.seg_ids[ni], ns, ov)
                    for ni, ns, ov in flat.end_neighbors(idx)}

        if legacy_start != flat_start or legacy_end != flat_end:
            diffs.append(sid)
            if len(diffs) > 3:
                break

    assert not diffs, f"adjacency mismatch on {diffs}"
