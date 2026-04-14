"""Pin ``harness.flat.bubbles.classify`` against hand-built GFAs.

These fixtures are written to ``tmp_path`` at test time rather than
committed — the shapes are small enough that inlining is more
readable than carrying GFA files around.
"""
import os

from harness.flat.bubbles import classify
from harness.flat.load_gfa import load


def _write(tmp_path, name, text):
    p = tmp_path / name
    p.write_text(text)
    return str(p)


# GFA overlap field: "0M" = 0-base match.


def test_insertion_three_node(tmp_path):
    """A - B, A - C, B - C (single node inside)."""
    gfa = (
        "S\tA\tAAA\tLN:i:3\n"
        "S\tB\tB\tLN:i:1\n"
        "S\tC\tCCC\tLN:i:3\n"
        "L\tA\t+\tB\t+\t0M\n"
        "L\tB\t+\tC\t+\t0M\n"
        "L\tA\t+\tC\t+\t0M\n"
    )
    flat = load(_write(tmp_path, "insertion.gfa", gfa))
    a = flat.id_to_idx["A"]
    b = flat.id_to_idx["B"]
    c = flat.id_to_idx["C"]
    assert classify(flat, a, c, [b]) == "insertion"


def test_simple_bubble_two_inside(tmp_path):
    """A splits into B and C, both merge at D."""
    gfa = (
        "S\tA\tAAA\tLN:i:3\n"
        "S\tB\tBBB\tLN:i:3\n"
        "S\tC\tCCC\tLN:i:3\n"
        "S\tD\tDDD\tLN:i:3\n"
        "L\tA\t+\tB\t+\t0M\n"
        "L\tA\t+\tC\t+\t0M\n"
        "L\tB\t+\tD\t+\t0M\n"
        "L\tC\t+\tD\t+\t0M\n"
    )
    flat = load(_write(tmp_path, "simple.gfa", gfa))
    a = flat.id_to_idx["A"]
    b = flat.id_to_idx["B"]
    c = flat.id_to_idx["C"]
    d = flat.id_to_idx["D"]
    assert classify(flat, a, d, [b, c]) == "simple"


def test_simple_degenerates_to_super_when_source_sink_linked(tmp_path):
    """If A also directly connects to D, the bubble is not simple."""
    gfa = (
        "S\tA\tAAA\tLN:i:3\n"
        "S\tB\tBBB\tLN:i:3\n"
        "S\tC\tCCC\tLN:i:3\n"
        "S\tD\tDDD\tLN:i:3\n"
        "L\tA\t+\tB\t+\t0M\n"
        "L\tA\t+\tC\t+\t0M\n"
        "L\tB\t+\tD\t+\t0M\n"
        "L\tC\t+\tD\t+\t0M\n"
        "L\tA\t+\tD\t+\t0M\n"
    )
    flat = load(_write(tmp_path, "shortcut.gfa", gfa))
    a = flat.id_to_idx["A"]
    b = flat.id_to_idx["B"]
    c = flat.id_to_idx["C"]
    d = flat.id_to_idx["D"]
    assert classify(flat, a, d, [b, c]) == "super"


def test_super_three_inside(tmp_path):
    """Three inside nodes → always super."""
    gfa = (
        "S\tA\tAAA\tLN:i:3\n"
        "S\tB\tBBB\tLN:i:3\n"
        "S\tC\tCCC\tLN:i:3\n"
        "S\tE\tEEE\tLN:i:3\n"
        "S\tD\tDDD\tLN:i:3\n"
        "L\tA\t+\tB\t+\t0M\n"
        "L\tA\t+\tC\t+\t0M\n"
        "L\tA\t+\tE\t+\t0M\n"
        "L\tB\t+\tD\t+\t0M\n"
        "L\tC\t+\tD\t+\t0M\n"
        "L\tE\t+\tD\t+\t0M\n"
    )
    flat = load(_write(tmp_path, "super.gfa", gfa))
    a = flat.id_to_idx["A"]
    b = flat.id_to_idx["B"]
    c = flat.id_to_idx["C"]
    e = flat.id_to_idx["E"]
    d = flat.id_to_idx["D"]
    assert classify(flat, a, d, [b, c, e]) == "super"
