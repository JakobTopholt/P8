"""Trajectory simplification algorithms used by baseline pipelines."""

from .douglas_peucker import simplify_douglas_peucker_indices
from .methods import (
    CANONICAL_METHODS,
    METHOD_DOUGLAS_PEUCKER,
    METHOD_QUERY_WITNESS,
    METHOD_UNIFORM,
    normalize_method_name,
    normalize_method_names,
)
from .query_witness import (
    QueryWitnessPointEvidence,
    QueryWitnessScoreComponents,
    query_witness_score_components,
    score_query_witness_points,
    simplify_query_witness_indices,
)
from .uniform import simplify_uniform_indices

__all__ = [
    "CANONICAL_METHODS",
    "METHOD_DOUGLAS_PEUCKER",
    "METHOD_QUERY_WITNESS",
    "METHOD_UNIFORM",
    "normalize_method_name",
    "normalize_method_names",
    "QueryWitnessPointEvidence",
    "QueryWitnessScoreComponents",
    "query_witness_score_components",
    "score_query_witness_points",
    "simplify_query_witness_indices",
    "simplify_uniform_indices",
    "simplify_douglas_peucker_indices",
]
