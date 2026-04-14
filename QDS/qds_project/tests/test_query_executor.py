"""Tests for AIS spatiotemporal query execution."""
import pytest
import torch
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.queries.query_executor import run_query, run_queries

def _make_points() -> torch.Tensor:
    """5 points with known positions: [time, lat, lon, speed, heading]."""
    return torch.tensor([
        [0.0,  50.0, 0.0, 5.0, 0.0],
        [1.0,  51.0, 1.0, 10.0, 90.0],
        [2.0,  52.0, 2.0, 15.0, 180.0],
        [3.0,  53.0, 3.0, 20.0, 270.0],
        [4.0,  54.0, 4.0, 25.0, 360.0],
    ])

class TestRunQuery:
    def test_all_points_selected(self):
        pts = _make_points()
        query = torch.tensor([49.0, 55.0, -1.0, 5.0, -1.0, 5.0])
        result = run_query(pts, query)
        assert abs(result.item() - 75.0) < 1e-5

    def test_no_points_selected(self):
        pts = _make_points()
        query = torch.tensor([60.0, 65.0, 10.0, 20.0, 10.0, 20.0])
        result = run_query(pts, query)
        assert result.item() == 0.0

    def test_single_point_selected(self):
        pts = _make_points()
        query = torch.tensor([50.5, 51.5, 0.5, 1.5, 0.5, 1.5])
        result = run_query(pts, query)
        assert abs(result.item() - 10.0) < 1e-5

    def test_boundary_inclusive(self):
        pts = _make_points()
        query = torch.tensor([50.0, 50.0, 0.0, 0.0, 0.0, 0.0])
        result = run_query(pts, query)
        assert abs(result.item() - 5.0) < 1e-5

class TestRunQueries:
    def test_output_shape(self):
        pts = _make_points()
        queries = torch.tensor([
            [49.0, 55.0, -1.0, 5.0, -1.0, 5.0],
            [60.0, 65.0, 10.0, 20.0, 10.0, 20.0],
        ])
        results = run_queries(pts, queries)
        assert results.shape == (2,)

    def test_results_non_negative(self):
        pts = _make_points()
        queries = torch.stack([
            torch.tensor([49.0, 55.0, -1.0, 5.0, -1.0, 5.0]),
            torch.tensor([50.5, 51.5, 0.5, 1.5, 0.5, 1.5]),
        ])
        results = run_queries(pts, queries)
        assert (results >= 0).all()

    def test_chunked_execution_matches_default(self):
        pts = torch.tensor([
            [0.0, 50.0, 0.0, 5.0, 0.0],
            [1.0, 51.0, 1.0, 10.0, 90.0],
            [2.0, 52.0, 2.0, 15.0, 180.0],
            [3.0, 53.0, 3.0, 20.0, 270.0],
            [4.0, 54.0, 4.0, 25.0, 360.0],
        ])
        queries = torch.tensor([
            [49.0, 55.0, -1.0, 5.0, -1.0, 5.0],
            [50.5, 51.5, 0.5, 1.5, 0.5, 1.5],
            [60.0, 65.0, 10.0, 20.0, 10.0, 20.0],
        ])

        default_results = run_queries(pts, queries)
        chunked_results = run_queries(
            pts,
            queries,
            point_chunk_size=2,
            query_chunk_size=1,
        )

        assert torch.allclose(chunked_results, default_results)
