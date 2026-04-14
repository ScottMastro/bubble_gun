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


def _peak_rss_mb():
    # ru_maxrss is KB on Linux, bytes on macOS.
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return rss / (1024 * 1024)
    return rss / 1024


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
        self._order = []

    @contextmanager
    def phase(self, name):
        t0 = time.perf_counter()
        try:
            yield
        finally:
            dt = time.perf_counter() - t0
            self.phase_times[name] = round(dt, 4)
            self._order.append(name)

    def record(self, bubble_counts, chain_count, path=STATS_PATH):
        entry = {
            "ts": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
            "git_sha": _git_sha(),
            "fixture": self.fixture,
            "phase_order": self._order,
            "phase_times_s": self.phase_times,
            "total_s": round(sum(self.phase_times.values()), 4),
            "peak_rss_mb": round(_peak_rss_mb(), 1),
            "bubble_counts": bubble_counts,
            "chain_count": chain_count,
        }
        with open(path, "a") as f:
            f.write(json.dumps(entry) + "\n")
        return entry


def report(n=10, path=STATS_PATH):
    if not os.path.exists(path):
        print(f"no stats file at {path}")
        return
    with open(path) as f:
        lines = [json.loads(x) for x in f if x.strip()]
    tail = lines[-n:]
    header = f"{'ts':20s} {'sha':10s} {'fixture':14s} {'total_s':>8s} {'rss_mb':>8s} {'simple':>7s} {'super':>7s} {'ins':>5s} {'chains':>7s}"
    print(header)
    print("-" * len(header))
    for e in tail:
        bc = e.get("bubble_counts", {})
        print(f"{e['ts']:20s} {e['git_sha']:10s} {e['fixture']:14s} "
              f"{e['total_s']:>8.3f} {e['peak_rss_mb']:>8.1f} "
              f"{bc.get('simple', 0):>7d} {bc.get('super', 0):>7d} "
              f"{bc.get('insertion', 0):>5d} {e.get('chain_count', 0):>7d}")


def _main():
    p = argparse.ArgumentParser()
    p.add_argument("--report", action="store_true")
    p.add_argument("-n", type=int, default=10)
    args = p.parse_args()
    if args.report:
        report(args.n)


if __name__ == "__main__":
    _main()
