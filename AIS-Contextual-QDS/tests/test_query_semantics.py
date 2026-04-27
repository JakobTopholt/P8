"""Query-semantics edge-case tests."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.query_semantics import (
    corridor_membership_optimized,
    corridor_membership_segment_exact,
    normalize_query_mode,
    zone_entry_optimized,
    zone_entry_segment_exact,
)


def test_normalize_query_mode_supports_only_current_modes() -> None:
    assert normalize_query_mode(None, default="optimized") == "optimized"
    assert normalize_query_mode("segment_exact", default="optimized") == "segment_exact"


def test_zone_entry_rejects_start_inside_for_both_modes() -> None:
    assert zone_entry_optimized(
        starts_inside=True,
        has_point_inside=True,
        line_crosses_boundary=True,
    ) is False
    assert zone_entry_segment_exact(
        starts_inside=True,
        segment_enters_zone=True,
        line_crosses_boundary=True,
    ) is False


def test_zone_boundary_touch_only_is_negative() -> None:
    assert zone_entry_optimized(
        starts_inside=False,
        has_point_inside=False,
        line_crosses_boundary=False,
    ) is False
    assert zone_entry_segment_exact(
        starts_inside=False,
        segment_enters_zone=False,
        line_crosses_boundary=False,
    ) is False


def test_zone_entry_accepts_crossing_events_for_both_modes() -> None:
    assert zone_entry_optimized(
        starts_inside=False,
        has_point_inside=True,
        line_crosses_boundary=False,
    ) is True
    assert zone_entry_segment_exact(
        starts_inside=False,
        segment_enters_zone=True,
        line_crosses_boundary=False,
    ) is True
    assert zone_entry_segment_exact(
        starts_inside=False,
        segment_enters_zone=False,
        line_crosses_boundary=True,
    ) is True


def test_corridor_optimized_accepts_point_inside_even_without_overlap() -> None:
    assert corridor_membership_optimized(
        has_point_inside=True,
        overlap_meters=0.0,
        min_overlap_meters=1.0,
    ) is True


def test_corridor_segment_exact_accepts_boundary_covered_point() -> None:
    assert corridor_membership_segment_exact(
        point_covered=True,
        segment_overlap_meters=0.0,
        min_overlap_meters=1.0,
    ) is True


def test_corridor_modes_require_overlap_threshold_without_point_hit() -> None:
    assert corridor_membership_optimized(
        has_point_inside=False,
        overlap_meters=0.5,
        min_overlap_meters=1.0,
    ) is False
    assert corridor_membership_segment_exact(
        point_covered=False,
        segment_overlap_meters=0.5,
        min_overlap_meters=1.0,
    ) is False


def test_optimized_corridor_can_be_more_permissive_than_segment_exact() -> None:
    assert corridor_membership_optimized(
        has_point_inside=False,
        overlap_meters=1.2,
        min_overlap_meters=1.0,
    ) is True
    assert corridor_membership_segment_exact(
        point_covered=False,
        segment_overlap_meters=0.6,
        min_overlap_meters=1.0,
    ) is False
