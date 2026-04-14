"""Flat port of BubbleGun's ``connect_bubbles``.

Takes the raw bubble dict emitted by ``flat.find_bubbles.find_bubbles``
(``dict[key, (source_idx, sink_idx, inside_tuple)]``) and groups
bubbles into chains by following source/sink adjacency.

Output:
- ``chains: list[FlatChain]`` — ordered, with canonical-signature-
  derived chain ids.
- ``chain_id_by_key: dict[bkey, int]`` — lookup for packaging the
  FlatBubble records later.
"""
from harness.flat.bubbles import FlatChain


def _chain_walk(start_idx, idx_to_keys, raw_bubbles):
    """Pop bubbles along the chain starting at seg idx ``start_idx``.
    Returns a list of bubble keys in walk order. Mutates
    ``idx_to_keys`` (consumes bubbles as they're visited)."""
    walk = []
    cur = start_idx
    while True:
        bucket = idx_to_keys.get(cur)
        if not bucket:
            break
        bkey = next(iter(bucket))
        bucket.discard(bkey)
        walk.append(bkey)
        src, snk, _ = raw_bubbles[bkey]
        nxt = snk if cur == src else src
        # Remove this bubble from the other endpoint's bucket too so
        # we don't loop back on it.
        nbucket = idx_to_keys.get(nxt)
        if nbucket is not None:
            nbucket.discard(bkey)
        cur = nxt
    return walk


def connect_bubbles(flat, raw_bubbles):
    # idx -> set of bubble keys that use this seg as source OR sink
    idx_to_keys = {}
    for bkey, (src, snk, _inside) in raw_bubbles.items():
        idx_to_keys.setdefault(src, set()).add(bkey)
        idx_to_keys.setdefault(snk, set()).add(bkey)

    # Endpoint seeds: idxs appearing in exactly one bubble.
    endpoint_idxs = [i for i, s in idx_to_keys.items() if len(s) == 1]

    chain_walks = []  # list[list[bkey]]
    for seed in endpoint_idxs:
        if not idx_to_keys.get(seed):
            continue
        walk = _chain_walk(seed, idx_to_keys, raw_bubbles)
        if walk:
            chain_walks.append(walk)

    # Remaining bubbles (e.g. circular chains with no degree-1 ends).
    for seed, bucket in list(idx_to_keys.items()):
        if not bucket:
            continue
        walk = _chain_walk(seed, idx_to_keys, raw_bubbles)
        if walk:
            chain_walks.append(walk)

    # Assign chain ids via canonical signature sort (sorted tuple of
    # sorted bubble keys). Deterministic across runs — doesn't depend
    # on the set iteration order that gave legacy unstable chain ids.
    signatures = [tuple(sorted(w)) for w in chain_walks]
    order = sorted(range(len(chain_walks)), key=lambda i: signatures[i])

    chains = []
    chain_id_by_key = {}
    for new_id_minus_1, src_idx in enumerate(order):
        cid = new_id_minus_1 + 1  # legacy uses 1-based
        walk = chain_walks[src_idx]
        chains.append(FlatChain(id=cid, bubble_keys=tuple(walk)))
        for bkey in walk:
            chain_id_by_key[bkey] = cid

    return chains, chain_id_by_key
