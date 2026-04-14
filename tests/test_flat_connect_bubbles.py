"""Parity for flat connect_bubbles vs legacy on DRB1.

Asserts: same chain count, same canonical chain signatures (sorted
tuple of sorted-bubble-key tuples), and the chain_id_by_key map
covers every bubble exactly once.
"""
import os

from BubbleGun.Graph import Graph
from BubbleGun.compact_graph import compact_graph
import BubbleGun.find_bubbles as fb_legacy
import BubbleGun.connect_bubbles as cb_legacy

from harness.flat.load_gfa import load as flat_load
from harness.flat.compact import compact as flat_compact
from harness.flat.find_bubbles import find_bubbles as flat_find
from harness.flat.connect_bubbles import connect_bubbles as flat_connect

FIXTURE = os.path.join(os.path.dirname(__file__), "..",
                       "harness", "fixtures", "DRB1-3123.gfa")


def _legacy_chain_sigs():
    g = Graph(graph_file=FIXTURE)
    compact_graph(g)
    for n in g.nodes.values():
        n.seq = ""
    fb_legacy.find_bubbles(g)
    cb_legacy.connect_bubbles(g)
    sigs = set()
    for c in g.b_chains:
        sig = tuple(sorted(tuple(sorted([str(b.source.id), str(b.sink.id)]))
                           for b in c.bubbles))
        sigs.add(sig)
    return sigs


def _flat_result():
    f = flat_load(FIXTURE)
    f = flat_compact(f)
    raw = flat_find(f)
    chains, cid = flat_connect(f, raw)
    return f, raw, chains, cid


def test_chain_signatures_match_legacy():
    f, raw, chains, _ = _flat_result()
    flat_sigs = set()
    for c in chains:
        sig = tuple(sorted(tuple(sorted([f.seg_ids[k[0]], f.seg_ids[k[1]]]))
                           for k in c.bubble_keys))
        flat_sigs.add(sig)
    assert flat_sigs == _legacy_chain_sigs()


def test_every_bubble_assigned_a_chain():
    _, raw, _, cid_by_key = _flat_result()
    missing = [k for k in raw if k not in cid_by_key]
    assert not missing, f"{len(missing)} bubbles not assigned to any chain"


def test_chain_ids_are_dense_and_unique():
    _, _, chains, _ = _flat_result()
    ids = sorted(c.id for c in chains)
    assert ids == list(range(1, len(chains) + 1)), \
        "chain ids are not a contiguous 1..N range"
