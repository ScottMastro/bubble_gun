import difflib
import json
import os

import pytest

from harness.run import run

FIXTURE = os.path.join(os.path.dirname(__file__), "..", "harness", "fixtures", "DRB1-3123.gfa")
GOLDEN = os.path.join(os.path.dirname(__file__), "..", "harness", "goldens", "DRB1-3123.bubbles.json")


def _canonical_text(data):
    return json.dumps(data, sort_keys=True, indent=2) + "\n"


@pytest.mark.skipif(not os.path.exists(GOLDEN),
                    reason="baseline snapshot missing; regenerate with "
                           "`python -m harness.run --gfa <fixture> --snapshot <path>`")
def test_drb1_matches_golden():
    current, _, _ = run(FIXTURE, fixture_name="DRB1-3123")

    with open(GOLDEN) as f:
        golden = json.load(f)

    if current == golden:
        return

    diff = difflib.unified_diff(
        _canonical_text(golden).splitlines(keepends=True),
        _canonical_text(current).splitlines(keepends=True),
        fromfile="golden",
        tofile="current",
        n=3,
    )
    pytest.fail("DRB1 bubble snapshot diverges from golden:\n" + "".join(diff)[:4000])
