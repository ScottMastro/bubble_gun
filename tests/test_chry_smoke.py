"""chrY perf canary — only runs if the fixture is locally available.

chrY is a scale test, not a golden test. We record runtime/RSS to
stats.jsonl but don't assert on specific bubble counts (those can
legitimately shift as we swap the data model — the DRB1 golden
guards correctness).
"""
import os

import pytest

from harness.run import run
from harness.snapshot import build

FIXTURE = os.path.join(os.path.dirname(__file__), "..", "harness", "fixtures", "chrY.gfa.gz")

pytestmark = [
    pytest.mark.slow,
    pytest.mark.skipif(not os.path.exists(FIXTURE),
                       reason="chrY fixture not present; see harness/fixtures/chrY.README.md"),
]


def test_chry_completes():
    graph, rec = run(FIXTURE, fixture_name="chrY")
    data = build(graph)
    bc = data["bubble_counts"]
    entry = rec.record(bc, data["chain_count"])

    # No strict assertions on counts — record them so we can eyeball stats.jsonl.
    assert bc["simple"] + bc["super"] + bc["insertion"] > 1000, \
        f"chrY produced suspiciously few bubbles: {bc}"
    assert entry["total_s"] > 0
