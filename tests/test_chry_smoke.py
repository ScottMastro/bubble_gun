"""chrY perf canary — only runs if the fixture is locally available.

chrY is a scale test. We record runtime/RSS to stats.jsonl under both
representations and assert that the bubble snapshots match between
them — so any topology drift on a larger fixture fails loudly, even
without a committed chrY golden.
"""
import os

import pytest

from harness.run import run

FIXTURE = os.path.join(os.path.dirname(__file__), "..", "harness", "fixtures", "chrY.gfa.gz")

pytestmark = [
    pytest.mark.slow,
    pytest.mark.skipif(not os.path.exists(FIXTURE),
                       reason="chrY fixture not present; see harness/fixtures/chrY.README.md"),
]


@pytest.fixture(scope="module")
def chry_snapshots():
    out = {}
    for rep in ("legacy", "flat"):
        data, rec, extras = run(FIXTURE, fixture_name="chrY", representation=rep)
        rec.record(data["bubble_counts"], data["chain_count"], extras=extras)
        out[rep] = data
    return out


@pytest.mark.parametrize("rep", ["legacy", "flat"])
def test_chry_nontrivial(chry_snapshots, rep):
    bc = chry_snapshots[rep]["bubble_counts"]
    assert bc["simple"] + bc["super"] + bc["insertion"] > 1000, \
        f"chrY ({rep}) produced suspiciously few bubbles: {bc}"


def test_chry_flat_matches_legacy(chry_snapshots):
    assert chry_snapshots["legacy"] == chry_snapshots["flat"], \
        "chrY bubble snapshots diverge between legacy and flat"
