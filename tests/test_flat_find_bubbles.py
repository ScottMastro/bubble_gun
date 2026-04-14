"""Isolated parity for the flat BFS vs legacy find_bubbles.

Runs both on the compacted DRB1 graph and asserts the set of
``(source, sink)`` keys and each bubble's inside-set match exactly.
Full-snapshot parity is still covered by ``test_flat_parity.py``
once the rest of Phase 2 lands.
"""
import os

from BubbleGun.Graph import Graph
from BubbleGun.compact_graph import compact_graph
import BubbleGun.find_bubbles as fb_legacy

from harness.flat.load_gfa import load as flat_load
from harness.flat.compact import compact as flat_compact
from harness.flat.find_bubbles import find_bubbles as flat_find

FIXTURE = os.path.join(os.path.dirname(__file__), "..",
                       "harness", "fixtures", "DRB1-3123.gfa")


def _legacy_bubbles():
    g = Graph(graph_file=FIXTURE)
    compact_graph(g)
    for n in g.nodes.values():
        n.seq = ""
    fb_legacy.find_bubbles(g)
    out = {}
    for b in g.bubbles.values():
        key = tuple(sorted([str(b.source.id), str(b.sink.id)]))
        inside = frozenset(str(n.id) for n in b.inside)
        out[key] = inside
    return out


def _flat_bubbles():
    f = flat_load(FIXTURE)
    f = flat_compact(f)
    raw = flat_find(f)
    out = {}
    for (a, b), (src, snk, inside) in raw.items():
        key = tuple(sorted([f.seg_ids[src], f.seg_ids[snk]]))
        inside_ids = frozenset(f.seg_ids[i] for i in inside)
        out[key] = inside_ids
    return out


def test_bubble_keys_match():
    legacy = _legacy_bubbles()
    flat = _flat_bubbles()
    assert set(legacy.keys()) == set(flat.keys()), (
        f"legacy-only: {sorted(set(legacy) - set(flat))[:5]} ... "
        f"flat-only: {sorted(set(flat) - set(legacy))[:5]}")


def test_bubble_inside_sets_match():
    legacy = _legacy_bubbles()
    flat = _flat_bubbles()
    mismatches = [k for k in legacy if legacy[k] != flat.get(k)]
    assert not mismatches, (
        f"{len(mismatches)} inside mismatches; first: {mismatches[0]} "
        f"legacy={sorted(legacy[mismatches[0]])[:6]} "
        f"flat={sorted(flat[mismatches[0]])[:6]}")
