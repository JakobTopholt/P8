"""Tests for AIS baseline simplification methods."""
import pytest
import torch
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.evaluation.baselines import random_sampling, uniform_temporal_sampling, douglas_peucker

def _make_points(n=100) -> torch.Tensor:
    torch.manual_seed(42)
    pts = torch.zeros(n, 5)
    pts[:, 0] = torch.arange(n).float()  # time
    pts[:, 1] = 50.0 + torch.rand(n)    # lat
    pts[:, 2] = 0.0 + torch.rand(n)     # lon
    pts[:, 3] = torch.rand(n) * 20.0    # speed
    pts[:, 4] = torch.rand(n) * 360.0   # heading
    return pts

class TestRandomSampling:
    def test_output_size(self):
        pts = _make_points(100)
        sampled = random_sampling(pts, ratio=0.3)
        assert sampled.shape[0] == 30

    def test_output_columns(self):
        pts = _make_points(100)
        sampled = random_sampling(pts, ratio=0.3)
        assert sampled.shape[1] == 5

    def test_non_empty(self):
        pts = _make_points(50)
        sampled = random_sampling(pts, ratio=0.2)
        assert sampled.shape[0] > 0

    def test_output_is_subset(self):
        pts = _make_points(50)
        sampled = random_sampling(pts, ratio=0.5)
        assert sampled.shape[0] <= pts.shape[0]

class TestUniformTemporalSampling:
    def test_output_size(self):
        pts = _make_points(100)
        sampled = uniform_temporal_sampling(pts, ratio=0.2)
        assert sampled.shape[0] == 20

    def test_output_columns(self):
        pts = _make_points(100)
        sampled = uniform_temporal_sampling(pts, ratio=0.2)
        assert sampled.shape[1] == 5

    def test_non_empty(self):
        pts = _make_points(50)
        sampled = uniform_temporal_sampling(pts, ratio=0.2)
        assert sampled.shape[0] > 0

class TestDouglasPeucker:
    def test_output_columns(self):
        pts = _make_points(50)
        sampled = douglas_peucker(pts, epsilon=0.01)
        assert sampled.shape[1] == 5

    def test_non_empty(self):
        pts = _make_points(50)
        sampled = douglas_peucker(pts, epsilon=0.01)
        assert sampled.shape[0] > 0
