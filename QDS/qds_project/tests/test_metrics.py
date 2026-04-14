"""Tests for AIS evaluation metrics."""
import pytest
import torch
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.evaluation.metrics import query_error, compression_ratio, query_latency

def _make_points_and_queries():
    torch.manual_seed(0)
    pts = torch.rand(50, 5)
    pts[:, 1] = pts[:, 1] * 10 + 50  # lat in [50, 60]
    pts[:, 2] = pts[:, 2] * 10 - 5   # lon in [-5, 5]
    pts[:, 0] = torch.arange(50).float()  # time
    queries = torch.tensor([
        [50.0, 60.0, -5.0, 5.0, 0.0, 49.0],
        [52.0, 58.0, -2.0, 2.0, 0.0, 25.0],
    ])
    return pts, queries

class TestQueryError:
    def test_identical_datasets(self):
        pts, queries = _make_points_and_queries()
        err = query_error(pts, pts, queries)
        assert err == pytest.approx(0.0, abs=1e-6)

    def test_non_negative(self):
        pts, queries = _make_points_and_queries()
        reduced = pts[:25]
        err = query_error(pts, reduced, queries)
        assert err >= 0.0

    def test_empty_simplified(self):
        pts, queries = _make_points_and_queries()
        empty = torch.zeros(0, 5)
        err = query_error(pts, empty, queries)
        assert err >= 0.0

class TestCompressionRatio:
    def test_no_compression(self):
        pts, _ = _make_points_and_queries()
        ratio = compression_ratio(pts, pts)
        assert ratio == pytest.approx(1.0)

    def test_half_compression(self):
        pts, _ = _make_points_and_queries()
        ratio = compression_ratio(pts, pts[:25])
        assert ratio == pytest.approx(0.5)

    def test_range(self):
        pts, _ = _make_points_and_queries()
        for k in [1, 10, 25, 50]:
            ratio = compression_ratio(pts, pts[:k])
            assert 0.0 < ratio <= 1.0

class TestQueryLatency:
    def test_returns_positive_float(self):
        pts, queries = _make_points_and_queries()
        latency = query_latency(pts, queries)
        assert isinstance(latency, float)
        assert latency >= 0.0
