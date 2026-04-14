"""FlatGraph → BubbleGun Graph adapter.

Produces a ``{id: BubbleGun.Node}`` dict wrapped in a ``BubbleGun.Graph``
so the unchanged ``find_bubbles`` / ``connect_bubbles`` / ``find_parents``
pipeline can consume it. Nodes carry only what the BFS reads:

- ``id``
- ``seq_len``
- ``start`` / ``end`` — Python ``list`` of ``(nbr_id, nbr_side, overlap)``
  tuples (legacy compact_graph indexes ``end[0]`` so lists, not sets)
- ``seq = ""`` and ``optional_info = ""`` (defaults; harness drops seq)

``start_parent_ids`` / ``end_parent_ids`` are left as ``None``. The
patched ``find_bubbles._precompute_parent_ids`` populates them itself.
"""
from BubbleGun.Graph import Graph
from BubbleGun.Node import Node


def to_graph(flat):
    g = Graph()
    nodes = {}
    for idx, sid in enumerate(flat.seg_ids):
        node = Node(sid)
        node.seq = ""
        node.seq_len = int(flat.seq_len[idx])
        node.start = [(flat.seg_ids[ni], int(ns), int(ov))
                      for ni, ns, ov in flat.start_neighbors(idx)]
        node.end = [(flat.seg_ids[ni], int(ns), int(ov))
                    for ni, ns, ov in flat.end_neighbors(idx)]
        nodes[sid] = node
    g.nodes = nodes
    return g
