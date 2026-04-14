"""Drive the BubbleGun pipeline on a fixture GFA and produce
runtime stats and/or a canonical bubble snapshot.

Example:
    python -m harness.run --gfa harness/fixtures/DRB1-3123.gfa \\
        --snapshot harness/goldens/DRB1-3123.bubbles.json \\
        --record-stats
"""
import argparse
import os

from BubbleGun.Graph import Graph
import BubbleGun.find_bubbles as find_bubbles_mod
import BubbleGun.connect_bubbles as connect_bubbles_mod
import BubbleGun.find_parents as find_parents_mod

from harness import snapshot as snapshot_mod
from harness import stats as stats_mod


def run(gfa_path, fixture_name=None):
    fixture = fixture_name or os.path.splitext(os.path.basename(gfa_path))[0]
    rec = stats_mod.Recorder(fixture)

    with rec.phase("load"):
        graph = Graph(graph_file=gfa_path)

    with rec.phase("compact"):
        # Graph imports compact_graph but doesn't call it in __init__.
        from BubbleGun.compact_graph import compact_graph
        compact_graph(graph)

    # Drop sequences — mirrors pangyplot's bubble_gun.shoot() to keep memory
    # behavior comparable to production.
    for node in graph.nodes.values():
        node.seq = ""

    with rec.phase("find_bubbles"):
        find_bubbles_mod.find_bubbles(graph)

    with rec.phase("connect_bubbles"):
        connect_bubbles_mod.connect_bubbles(graph)

    with rec.phase("find_parents"):
        find_parents_mod.find_parents(graph)

    return graph, rec


def _main():
    p = argparse.ArgumentParser()
    p.add_argument("--gfa", required=True)
    p.add_argument("--snapshot", help="write canonical bubble JSON to this path")
    p.add_argument("--record-stats", action="store_true",
                   help="append timing/RSS entry to stats.jsonl")
    p.add_argument("--fixture-name", help="override label used in stats")
    args = p.parse_args()

    graph, rec = run(args.gfa, args.fixture_name)

    data = snapshot_mod.build(graph)
    bc = data["bubble_counts"]
    cc = data["chain_count"]

    if args.snapshot:
        snapshot_mod.dump(graph, args.snapshot)
        print(f"wrote snapshot → {args.snapshot}")

    if args.record_stats:
        entry = rec.record(bc, cc)
        print(f"recorded: total={entry['total_s']}s rss={entry['peak_rss_mb']}MB "
              f"simple={bc['simple']} super={bc['super']} insertion={bc['insertion']} "
              f"chains={cc}")
    else:
        print(f"total={round(sum(rec.phase_times.values()), 3)}s "
              f"simple={bc['simple']} super={bc['super']} insertion={bc['insertion']} "
              f"chains={cc}")
        for k in rec._order:
            print(f"  {k:18s} {rec.phase_times[k]:.3f}s")


if __name__ == "__main__":
    _main()
