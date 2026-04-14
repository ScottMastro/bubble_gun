import os

import pytest

from harness.run import run
from harness.snapshot import build

FIXTURE = os.path.join(os.path.dirname(__file__), "..", "harness", "fixtures", "DRB1-3123.gfa")


@pytest.fixture(scope="module")
def drb1_graph():
    graph, _, _ = run(FIXTURE, fixture_name="DRB1-3123")
    return graph


def test_bubbles_found(drb1_graph):
    assert len(drb1_graph.bubbles) > 0


def test_chains_found(drb1_graph):
    assert len(drb1_graph.b_chains) > 0


def test_bubble_counts_order_of_magnitude(drb1_graph):
    data = build(drb1_graph)
    bc = data["bubble_counts"]
    assert bc["simple"] > 100
    assert bc["super"] > 10
    assert bc["insertion"] >= 0


def test_every_bubble_has_source_sink_inside(drb1_graph):
    for bubble in drb1_graph.bubbles.values():
        assert bubble.source is not None
        assert bubble.sink is not None
        assert len(bubble.inside) > 0
