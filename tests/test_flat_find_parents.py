"""Parity: flat find_parents vs pangyplot-patched legacy on DRB1."""
import os

from BubbleGun.Graph import Graph
from BubbleGun.compact_graph import compact_graph
import BubbleGun.find_bubbles as fb_legacy
import BubbleGun.connect_bubbles as cb_legacy
import BubbleGun.find_parents as fp_legacy

from harness.flat.load_gfa import load as flat_load
from harness.flat.compact import compact as flat_compact
from harness.flat.find_bubbles import find_bubbles as flat_find
from harness.flat.connect_bubbles import connect_bubbles as flat_connect
from harness.flat.find_parents import find_parents as flat_fp
from harness.flat.bubbles import classify

FIXTURE = os.path.join(os.path.dirname(__file__), "..",
                       "harness", "fixtures", "DRB1-3123.gfa")


def _legacy_parents():
    g = Graph(graph_file=FIXTURE)
    compact_graph(g)
    for n in g.nodes.values():
        n.seq = ""
    fb_legacy.find_bubbles(g)
    cb_legacy.connect_bubbles(g)
    fp_legacy.find_parents(g)

    id_to_key = {b.id: tuple(sorted([str(b.source.id), str(b.sink.id)]))
                 for b in g.bubbles.values()}
    parents = {}
    for b in g.bubbles.values():
        bkey = tuple(sorted([str(b.source.id), str(b.sink.id)]))
        parents[bkey] = id_to_key.get(b.parent_sb) if b.parent_sb else None
    return parents


def _flat_parents():
    f = flat_load(FIXTURE)
    f = flat_compact(f)
    raw = flat_find(f)
    flat_connect(f, raw)
    btypes = {k: classify(f, src, snk, list(inside))
              for k, (src, snk, inside) in raw.items()}
    parents = flat_fp(raw, btypes)

    out = {}
    for bkey, pkey in parents.items():
        skey = tuple(sorted([f.seg_ids[bkey[0]], f.seg_ids[bkey[1]]]))
        out[skey] = (None if pkey is None
                     else tuple(sorted([f.seg_ids[pkey[0]], f.seg_ids[pkey[1]]])))
    return out


def test_parent_map_matches_legacy():
    legacy = _legacy_parents()
    flat = _flat_parents()
    mismatches = [k for k in legacy if legacy[k] != flat.get(k)]
    assert not mismatches, (
        f"{len(mismatches)} parent mismatches; first: {mismatches[0]} "
        f"legacy={legacy[mismatches[0]]} flat={flat.get(mismatches[0])}")
