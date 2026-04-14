"""End-to-end parity: flat representation + adapter + legacy
find_bubbles/connect_bubbles/find_parents must produce a bubble
snapshot byte-identical to the committed legacy golden.

This is Phase 1's gating correctness check.
"""
import difflib
import json
import os

import pytest

from harness.run import run
from harness.snapshot import build

FIXTURE = os.path.join(os.path.dirname(__file__), "..",
                       "harness", "fixtures", "DRB1-3123.gfa")
GOLDEN = os.path.join(os.path.dirname(__file__), "..",
                      "harness", "goldens", "DRB1-3123.bubbles.json")


def _canonical_text(data):
    return json.dumps(data, sort_keys=True, indent=2) + "\n"


@pytest.mark.skipif(not os.path.exists(GOLDEN), reason="golden missing")
def test_drb1_flat_matches_golden():
    graph, _, _ = run(FIXTURE, fixture_name="DRB1-3123", representation="flat")
    current = build(graph)

    with open(GOLDEN) as f:
        golden = json.load(f)

    if current == golden:
        return

    diff = difflib.unified_diff(
        _canonical_text(golden).splitlines(keepends=True),
        _canonical_text(current).splitlines(keepends=True),
        fromfile="golden(legacy)",
        tofile="flat",
        n=3,
    )
    pytest.fail("flat DRB1 snapshot diverges from legacy golden:\n"
                + "".join(diff)[:4000])
