"""Canonical, diff-friendly JSON snapshot of a BubbleGun Graph's bubble set.

The snapshot is intentionally upstream of any pangyplot-specific transform
(e.g. ``construct_bubble_index``). It captures the raw bubble/chain output
of ``find_bubbles → connect_bubbles → find_parents`` so we can diff between
implementations of the BubbleGun algorithm itself.

Numeric chain/bubble ids assigned inside BubbleGun are iteration-order-
dependent (graph.b_chains is a set, graph.bubbles is a dict of tuple keys),
so the snapshot identifies every entity by a stable composite key based on
source/sink segment ids.
"""
import json


def _bkey(bubble):
    """Stable bubble key: sorted [source_id, sink_id]."""
    return sorted([str(bubble.source.id), str(bubble.sink.id)])


def _bkey_tuple(bubble):
    return tuple(_bkey(bubble))


def _bubble_type(bubble):
    if bubble.is_simple():
        return "simple"
    if bubble.is_insertion():
        return "insertion"
    return "super"


def build(graph):
    # Map numeric bubble id → stable key so parent references can be rewritten.
    id_to_key = {}
    for b in graph.bubbles.values():
        if b.id:
            id_to_key[b.id] = _bkey(b)

    # Map numeric chain id → canonical chain signature (sorted tuple of bubble keys).
    chain_signatures = {}
    for chain in graph.b_chains:
        sig = tuple(sorted(_bkey_tuple(b) for b in chain.bubbles))
        chain_signatures[chain.id] = sig

    bubbles = []
    for b in graph.bubbles.values():
        parent_key = id_to_key.get(b.parent_sb) if b.parent_sb else None
        chain_sig = chain_signatures.get(b.chain_id)
        bubbles.append({
            "key": _bkey(b),
            "source": str(b.source.id),
            "sink": str(b.sink.id),
            "inside": sorted(str(n.id) for n in b.inside),
            "type": _bubble_type(b),
            "parent_key": parent_key,
            "chain_signature_index": None,  # filled in below
            "chain_sig": chain_sig,          # temp, stripped before serialization
        })
    bubbles.sort(key=lambda r: r["key"])

    # Assign deterministic chain signature indices based on sorted order.
    unique_sigs = sorted({r["chain_sig"] for r in bubbles if r["chain_sig"] is not None})
    sig_to_index = {sig: i for i, sig in enumerate(unique_sigs)}
    for r in bubbles:
        r["chain_signature_index"] = sig_to_index.get(r["chain_sig"])
        del r["chain_sig"]

    chains = []
    for i, sig in enumerate(unique_sigs):
        chains.append({
            "index": i,
            "bubble_keys": [list(k) for k in sig],
        })

    counts = {"simple": 0, "insertion": 0, "super": 0}
    for b in bubbles:
        counts[b["type"]] += 1

    return {
        "bubble_counts": counts,
        "chain_count": len(chains),
        "bubbles": bubbles,
        "chains": chains,
    }


def dump(graph, path):
    data = build(graph)
    with open(path, "w") as f:
        json.dump(data, f, sort_keys=True, indent=2)
        f.write("\n")
    return data


def load(path):
    with open(path) as f:
        return json.load(f)
