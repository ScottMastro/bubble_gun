import os

import pytest

from harness.run import run

FIXTURE = os.path.join(os.path.dirname(__file__), "..", "harness", "fixtures", "DRB1-3123.gfa")


@pytest.fixture(scope="module")
def drb1_snapshot():
    data, _, _ = run(FIXTURE, fixture_name="DRB1-3123")
    return data


def test_bubbles_found(drb1_snapshot):
    assert len(drb1_snapshot["bubbles"]) > 0


def test_chains_found(drb1_snapshot):
    assert drb1_snapshot["chain_count"] > 0


def test_bubble_counts_order_of_magnitude(drb1_snapshot):
    bc = drb1_snapshot["bubble_counts"]
    assert bc["simple"] > 100
    assert bc["super"] > 10
    assert bc["insertion"] >= 0


def test_every_bubble_has_source_sink_inside(drb1_snapshot):
    for bubble in drb1_snapshot["bubbles"]:
        assert bubble["source"]
        assert bubble["sink"]
        assert len(bubble["inside"]) > 0
