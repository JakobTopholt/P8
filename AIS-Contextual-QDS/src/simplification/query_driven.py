"""Compatibility wrapper for the renamed query-witness simplifier."""

from .query_witness import (
    B3PointEvidence,
    B3ScoreComponents,
    QueryWitnessPointEvidence,
    QueryWitnessScoreComponents,
    b3_score_components,
    query_witness_score_components,
    score_b3_points,
    score_query_witness_points,
    simplify_b3_indices,
    simplify_query_witness_indices,
)

__all__ = [
    "QueryWitnessPointEvidence",
    "QueryWitnessScoreComponents",
    "query_witness_score_components",
    "score_query_witness_points",
    "simplify_query_witness_indices",
    "B3PointEvidence",
    "B3ScoreComponents",
    "b3_score_components",
    "score_b3_points",
    "simplify_b3_indices",
]
