"""Trajectory simplification algorithms used by baseline pipelines."""

from .douglas_peucker import simplify_douglas_peucker_indices
from .query_driven import B3PointEvidence, b3_score_components, score_b3_points, simplify_b3_indices
from .uniform import simplify_uniform_indices

__all__ = [
    "B3PointEvidence",
    "b3_score_components",
    "score_b3_points",
    "simplify_b3_indices",
    "simplify_uniform_indices",
    "simplify_douglas_peucker_indices",
]
