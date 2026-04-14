"""Tests for the TrajectoryQDSModel and TurnAwareQDSModel."""
import pytest
import torch
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.models.trajectory_qds_model import (
    TrajectoryQDSModel,
    normalize_points_and_queries,
    NUM_QUERY_TYPES,
    QUERY_TYPE_ID_RANGE,
    QUERY_TYPE_ID_INTERSECTION,
    QUERY_TYPE_ID_AGGREGATION,
    QUERY_TYPE_ID_NEAREST,
)
from src.models.turn_aware_qds_model import TurnAwareQDSModel

class TestTrajectoryQDSModel:
    def test_output_shape(self):
        model = TrajectoryQDSModel()
        points = torch.randn(20, 7)
        queries = torch.randn(10, 6)
        scores = model(points, queries)
        assert scores.shape == (20,), f"Expected (20,), got {scores.shape}"

    def test_output_range(self):
        model = TrajectoryQDSModel()
        points = torch.randn(30, 7)
        queries = torch.randn(15, 6)
        scores = model(points, queries)
        assert (scores >= 0).all() and (scores <= 1).all()

    def test_single_point(self):
        model = TrajectoryQDSModel()
        points = torch.randn(1, 7)
        queries = torch.randn(5, 6)
        scores = model(points, queries)
        assert scores.shape == (1,)

    def test_single_query(self):
        model = TrajectoryQDSModel()
        points = torch.randn(10, 7)
        queries = torch.randn(1, 6)
        scores = model(points, queries)
        assert scores.shape == (10,)

    def test_forward_no_grad(self):
        model = TrajectoryQDSModel()
        points = torch.randn(10, 7)
        queries = torch.randn(5, 6)
        with torch.no_grad():
            scores = model(points, queries)
        assert scores.shape == (10,)

    def test_gradients_flow(self):
        model = TrajectoryQDSModel()
        model.query_chunk_size = 1
        points = torch.randn(10, 7)
        queries = torch.randn(5, 6)
        scores = model(points, queries)
        loss = scores.mean()
        loss.backward()
        for param in model.parameters():
            assert param.grad is not None

    def test_chunked_query_attention_matches_unchunked(self):
        torch.manual_seed(0)
        chunked = TrajectoryQDSModel()
        chunked.query_chunk_size = 1
        reference = TrajectoryQDSModel()
        reference.load_state_dict(chunked.state_dict())
        reference.query_chunk_size = 512
        chunked.eval()
        reference.eval()

        points = torch.randn(12, 7)
        queries = torch.randn(9, 6)
        type_ids = torch.randint(0, NUM_QUERY_TYPES, (9,))

        with torch.no_grad():
            chunked_scores = chunked(points, queries, query_type_ids=type_ids)
            reference_scores = reference(points, queries, query_type_ids=type_ids)

        assert torch.allclose(chunked_scores, reference_scores, atol=1e-6, rtol=1e-5)

    def test_query_type_ids_explicit(self):
        """Passing explicit query_type_ids must produce the same output shape."""
        model = TrajectoryQDSModel()
        points = torch.randn(20, 7)
        queries = torch.randn(10, 6)
        type_ids = torch.randint(0, NUM_QUERY_TYPES, (10,))
        scores = model(points, queries, query_type_ids=type_ids)
        assert scores.shape == (20,)

    def test_query_type_ids_default_equals_zero_ids(self):
        """Omitting query_type_ids must give the same result as all-zero IDs."""
        model = TrajectoryQDSModel()
        model.eval()
        points = torch.randn(10, 7)
        queries = torch.randn(5, 6)
        zero_ids = torch.zeros(5, dtype=torch.long)
        with torch.no_grad():
            scores_default = model(points, queries)
            scores_zero    = model(points, queries, query_type_ids=zero_ids)
        assert torch.allclose(scores_default, scores_zero)

    def test_query_type_ids_affect_output(self):
        """Different query type IDs must produce different scores (type embedding is active)."""
        model = TrajectoryQDSModel()
        model.eval()
        torch.manual_seed(0)
        points = torch.randn(10, 7)
        queries = torch.randn(5, 6)
        range_ids     = torch.full((5,), QUERY_TYPE_ID_RANGE,       dtype=torch.long)
        nearest_ids   = torch.full((5,), QUERY_TYPE_ID_NEAREST,     dtype=torch.long)
        with torch.no_grad():
            scores_range  = model(points, queries, query_type_ids=range_ids)
            scores_nearest = model(points, queries, query_type_ids=nearest_ids)
        assert not torch.allclose(scores_range, scores_nearest), (
            "Scores must differ when query type IDs differ"
        )

    def test_all_query_types_valid(self):
        """Each of the four integer query type constants must run without error."""
        model = TrajectoryQDSModel()
        points = torch.randn(8, 7)
        queries = torch.randn(4, 6)
        for type_id in (
            QUERY_TYPE_ID_RANGE,
            QUERY_TYPE_ID_INTERSECTION,
            QUERY_TYPE_ID_AGGREGATION,
            QUERY_TYPE_ID_NEAREST,
        ):
            ids = torch.full((4,), type_id, dtype=torch.long)
            scores = model(points, queries, query_type_ids=ids)
            assert scores.shape == (8,)

    def test_single_query_with_type_id(self):
        """Single-query path must handle a scalar type_id tensor correctly."""
        model = TrajectoryQDSModel()
        points = torch.randn(10, 7)
        queries = torch.randn(6)  # 1-D single query
        type_id = torch.tensor(QUERY_TYPE_ID_AGGREGATION)
        scores = model(points, queries, query_type_ids=type_id)
        assert scores.shape == (10,)

    def test_query_type_constants(self):
        """Integer query type constants must cover the four expected types."""
        assert NUM_QUERY_TYPES == 4
        assert len({
            QUERY_TYPE_ID_RANGE,
            QUERY_TYPE_ID_INTERSECTION,
            QUERY_TYPE_ID_AGGREGATION,
            QUERY_TYPE_ID_NEAREST,
        }) == 4

    def test_has_point_self_attention(self):
        """TrajectoryQDSModel must expose a point_self_attn attribute."""
        model = TrajectoryQDSModel()
        assert hasattr(model, "point_self_attn")

    def test_has_layer_norms(self):
        """TrajectoryQDSModel must expose point_norm and cross_norm LayerNorm layers."""
        model = TrajectoryQDSModel()
        assert hasattr(model, "point_norm")
        assert hasattr(model, "cross_norm")

class TestNormalizePointsAndQueries:
    def test_output_shapes(self):
        points = torch.randn(20, 7)
        queries = torch.randn(10, 6)
        norm_pts, norm_q = normalize_points_and_queries(points, queries)
        assert norm_pts.shape == points.shape
        assert norm_q.shape == queries.shape

    def test_output_shapes_8_features(self):
        """normalize_points_and_queries must preserve 8-feature shape."""
        points = torch.randn(20, 8)
        queries = torch.randn(10, 6)
        norm_pts, norm_q = normalize_points_and_queries(points, queries)
        assert norm_pts.shape == points.shape
        assert norm_q.shape == queries.shape

    def test_binary_flags_preserved(self):
        """is_start/is_end columns (5, 6) must remain binary after normalisation."""
        points = torch.zeros(10, 7)
        points[0, 5] = 1.0   # is_start for first point
        points[-1, 6] = 1.0  # is_end for last point
        queries = torch.randn(5, 6)
        norm_pts, _ = normalize_points_and_queries(points, queries)
        assert norm_pts[0, 5].item() == pytest.approx(1.0, abs=1e-5)
        assert norm_pts[-1, 6].item() == pytest.approx(1.0, abs=1e-5)
        assert norm_pts[1:-1, 5].sum().item() == pytest.approx(0.0, abs=1e-5)
        assert norm_pts[1:-1, 6].sum().item() == pytest.approx(0.0, abs=1e-5)

    def test_turn_score_preserved(self):
        """turn_score column (7) must remain unchanged after normalisation."""
        points = torch.zeros(10, 8)
        points[:, 7] = torch.linspace(0.0, 1.0, 10)
        queries = torch.randn(5, 6)
        norm_pts, _ = normalize_points_and_queries(points, queries)
        assert torch.allclose(norm_pts[:, 7], points[:, 7], atol=1e-5)


class TestTurnAwareQDSModel:
    def test_output_shape(self):
        model = TurnAwareQDSModel()
        points = torch.randn(20, 8)
        queries = torch.randn(10, 6)
        scores = model(points, queries)
        assert scores.shape == (20,), f"Expected (20,), got {scores.shape}"

    def test_output_range(self):
        model = TurnAwareQDSModel()
        points = torch.randn(30, 8)
        queries = torch.randn(15, 6)
        scores = model(points, queries)
        assert (scores >= 0).all() and (scores <= 1).all()

    def test_single_point(self):
        model = TurnAwareQDSModel()
        points = torch.randn(1, 8)
        queries = torch.randn(5, 6)
        scores = model(points, queries)
        assert scores.shape == (1,)

    def test_single_query(self):
        model = TurnAwareQDSModel()
        points = torch.randn(10, 8)
        queries = torch.randn(1, 6)
        scores = model(points, queries)
        assert scores.shape == (10,)

    def test_forward_no_grad(self):
        model = TurnAwareQDSModel()
        points = torch.randn(10, 8)
        queries = torch.randn(5, 6)
        with torch.no_grad():
            scores = model(points, queries)
        assert scores.shape == (10,)

    def test_gradients_flow(self):
        model = TurnAwareQDSModel()
        model.query_chunk_size = 1
        points = torch.randn(10, 8)
        queries = torch.randn(5, 6)
        scores = model(points, queries)
        loss = scores.mean()
        loss.backward()
        for param in model.parameters():
            assert param.grad is not None

    def test_point_features_constant(self):
        """TurnAwareQDSModel must expect exactly 8 input point features."""
        assert TurnAwareQDSModel.POINT_FEATURES == 8
