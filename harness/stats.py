"""Append-only runtime/memory stats for harness runs.

One JSON object per line in ``stats.jsonl`` at the repo root. Call
``--report`` to eyeball the tail as a table.
"""
import argparse
import datetime
import json
import os
import resource
import subprocess
import sys
import time
from contextlib import contextmanager

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
STATS_PATH = os.path.join(REPO_ROOT, "stats.jsonl")

_PAGE_SIZE = os.sysconf("SC_PAGE_SIZE") if hasattr(os, "sysconf") else 4096


def _peak_rss_mb():
    # ru_maxrss is KB on Linux, bytes on macOS.
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return rss / (1024 * 1024)
    return rss / 1024


def _resident_rss_mb():
    """Current RSS from /proc/self/statm (Linux). Falls back to ru_maxrss."""
    try:
        with open("/proc/self/statm") as f:
            pages = int(f.read().split()[1])
        return pages * _PAGE_SIZE / (1024 * 1024)
    except (OSError, IndexError, ValueError):
        return _peak_rss_mb()


def _git_sha():
    try:
        return subprocess.check_output(
            ["git", "-C", REPO_ROOT, "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        return "unknown"


class Recorder:
    def __init__(self, fixture):
        self.fixture = fixture
        self.phase_times = {}
        self.phase_peak_delta = {}  # peak RSS grew by this much during phase
        self.phase_rss_after = {}   # resident RSS when phase exited
        self._order = []

    @contextmanager
    def phase(self, name):
        t0 = time.perf_counter()
        peak_before = _peak_rss_mb()
        try:
            yield
        finally:
            dt = time.perf_counter() - t0
            peak_after = _peak_rss_mb()
            self.phase_times[name] = round(dt, 4)
            self.phase_peak_delta[name] = round(peak_after - peak_before, 1)
            self.phase_rss_after[name] = round(_resident_rss_mb(), 1)
            self._order.append(name)

    def record(self, bubble_counts, chain_count, path=STATS_PATH, extras=None):
        entry = {
            "ts": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
            "git_sha": _git_sha(),
            "fixture": self.fixture,
            "phase_order": self._order,
            "phase_times_s": self.phase_times,
            "phase_peak_delta_mb": self.phase_peak_delta,
            "phase_rss_after_mb": self.phase_rss_after,
            "total_s": round(sum(self.phase_times.values()), 4),
            "peak_rss_mb": round(_peak_rss_mb(), 1),
            "bubble_counts": bubble_counts,
            "chain_count": chain_count,
        }
        if extras:
            entry.update(extras)
        with open(path, "a") as f:
            f.write(json.dumps(entry) + "\n")
        return entry


def measure_graph_bytes(graph):
    """Walk a BubbleGun Graph and sum sys.getsizeof on nodes + containers.

    Rough but consistent: counts each node's __slots__ attrs and the
    adjacency set/list plus their tuple members. Doesn't follow into
    interned ids or shared strings (so underestimates by a constant).
    """
    total = sys.getsizeof(graph.nodes)
    for node in graph.nodes.values():
        total += sys.getsizeof(node)
        total += sys.getsizeof(node.id)
        total += sys.getsizeof(node.start)
        for t in node.start:
            total += sys.getsizeof(t)
        total += sys.getsizeof(node.end)
        for t in node.end:
            total += sys.getsizeof(t)
        if node.optional_info:
            total += sys.getsizeof(node.optional_info)
    return total


def report(n=10, path=STATS_PATH):
    if not os.path.exists(path):
        print(f"no stats file at {path}")
        return
    with open(path) as f:
        lines = [json.loads(x) for x in f if x.strip()]
    tail = lines[-n:]
    header = (f"{'ts':20s} {'sha':10s} {'fixture':14s} "
              f"{'total_s':>8s} {'rss_mb':>8s} "
              f"{'load_rss':>9s} {'cmpct_rss':>9s} {'find_rss':>9s} "
              f"{'bubbles':>8s} {'chains':>7s}")
    print(header)
    print("-" * len(header))
    for e in tail:
        bc = e.get("bubble_counts", {})
        after = e.get("phase_rss_after_mb", {})
        total_bubbles = bc.get("simple", 0) + bc.get("super", 0) + bc.get("insertion", 0)
        print(f"{e['ts']:20s} {e['git_sha']:10s} {e['fixture']:14s} "
              f"{e['total_s']:>8.3f} {e['peak_rss_mb']:>8.1f} "
              f"{after.get('load', 0):>9.1f} "
              f"{after.get('compact', 0):>9.1f} "
              f"{after.get('find_bubbles', 0):>9.1f} "
              f"{total_bubbles:>8d} {e.get('chain_count', 0):>7d}")


def _main():
    p = argparse.ArgumentParser()
    p.add_argument("--report", action="store_true")
    p.add_argument("-n", type=int, default=10)
    args = p.parse_args()
    if args.report:
        report(args.n)


if __name__ == "__main__":
    _main()
