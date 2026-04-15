"""Trajectory simplification algorithms used by baseline pipelines."""

from .douglas_peucker import simplify_douglas_peucker_indices
from .uniform import simplify_uniform_indices

__all__ = [
    "simplify_uniform_indices",
    "simplify_douglas_peucker_indices",
]
