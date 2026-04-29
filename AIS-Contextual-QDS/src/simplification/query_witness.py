"""Query-witness, context-free simplification method."""

from __future__ import annotations

import math
from dataclasses import dataclass

from .uniform import simplify_uniform_indices


@dataclass(frozen=True)
class QueryWitnessPointEvidence:
    """Trajectory-local query and shape evidence available to the query-witness scorer."""

    zone_point_hits: int = 0
    zone_entry_segment_witnesses: int = 0
    zone_transition_witnesses: int = 0
    zone_entry_event_witnesses: int = 0
    corridor_point_hit: bool = False
    corridor_segment_witnesses: int = 0
    corridor_transition_witness: bool = False
    corridor_entry_event_witness: bool = False
    local_turn_degrees: float = 0.0
    local_deviation_m: float = 0.0


@dataclass(frozen=True)
class QueryWitnessScoreComponents:
    """Named score components used for deterministic query-witness ranking."""

    primary_zone_entry: float
    primary_corridor_membership: float
    strict_event_count: float
    point_membership: float
    local_shape: float

    @property
    def total(self) -> float:
        """Weighted query-witness score. Endpoints are handled as forced anchors."""
        return (
            1000.0 * self.primary_zone_entry
            + 1000.0 * self.primary_corridor_membership
            + 100.0 * self.strict_event_count
            + 10.0 * self.point_membership
            + self.local_shape
        )


def _bounded_non_negative(value: float | None) -> float:
    if value is None or value <= 0.0:
        return 0.0
    return float(value)


def query_witness_score_components(
    evidence: QueryWitnessPointEvidence,
    *,
    max_local_deviation_m: float,
) -> QueryWitnessScoreComponents:
    """Compute query-witness score components for one non-anchor point."""
    turn_norm = min(_bounded_non_negative(evidence.local_turn_degrees) / 180.0, 1.0)
    deviation = _bounded_non_negative(evidence.local_deviation_m)
    deviation_norm = deviation / max_local_deviation_m if max_local_deviation_m > 0.0 else 0.0
    deviation_norm = min(max(deviation_norm, 0.0), 1.0)
    local_shape = 0.65 * deviation_norm + 0.35 * turn_norm

    primary_zone_entry = 3.0 * max(0, evidence.zone_entry_segment_witnesses)
    primary_corridor_membership = (
        3.0 * max(0, evidence.corridor_segment_witnesses)
        + (2.0 if evidence.corridor_point_hit else 0.0)
    )
    strict_event_count = (
        2.0 * max(0, evidence.zone_entry_event_witnesses)
        + max(0, evidence.zone_transition_witnesses)
        + (2.0 if evidence.corridor_entry_event_witness else 0.0)
        + (1.0 if evidence.corridor_transition_witness else 0.0)
    )
    point_membership = max(0, evidence.zone_point_hits) + (1.0 if evidence.corridor_point_hit else 0.0)

    return QueryWitnessScoreComponents(
        primary_zone_entry=primary_zone_entry,
        primary_corridor_membership=primary_corridor_membership,
        strict_event_count=strict_event_count,
        point_membership=point_membership,
        local_shape=local_shape,
    )


def score_query_witness_points(evidence: list[QueryWitnessPointEvidence]) -> list[float]:
    """Return scalar query-witness scores for all points, with endpoints marked as anchors."""
    if not evidence:
        return []

    max_deviation = max(_bounded_non_negative(item.local_deviation_m) for item in evidence)
    scores: list[float] = []
    for idx, item in enumerate(evidence):
        if idx == 0 or idx == len(evidence) - 1:
            scores.append(float("inf"))
            continue
        scores.append(query_witness_score_components(item, max_local_deviation_m=max_deviation).total)
    return scores


def simplify_query_witness_indices(evidence: list[QueryWitnessPointEvidence], target_points: int) -> list[int]:
    """Keep highest-scoring query-witness points while preserving first and last point."""
    n_points = len(evidence)
    if n_points <= 0:
        return []

    if n_points <= 2 or target_points >= n_points:
        return list(range(n_points))

    target_points = max(2, target_points)
    interior_needed = target_points - 2
    if interior_needed <= 0:
        return [0, n_points - 1]

    coverage_target = min(target_points, max(2, math.ceil(target_points * 0.5)))
    selected: set[int] = set(simplify_uniform_indices(n_points, coverage_target))
    if len(selected) >= target_points:
        return sorted(selected)[:target_points]

    max_deviation = max(_bounded_non_negative(item.local_deviation_m) for item in evidence)
    ranked: list[tuple[float, float, float, float, float, int]] = []
    for idx, item in enumerate(evidence[1:-1], start=1):
        components = query_witness_score_components(item, max_local_deviation_m=max_deviation)
        query_total = components.primary_zone_entry + components.primary_corridor_membership
        ranked.append(
            (
                components.total,
                query_total,
                components.strict_event_count,
                components.point_membership,
                components.local_shape,
                -idx,
            )
        )

    ranked_indices = sorted(range(1, n_points - 1), key=lambda idx: ranked[idx - 1], reverse=True)
    for idx in ranked_indices:
        if len(selected) >= target_points:
            break
        selected.add(idx)
    return sorted(selected)


B3PointEvidence = QueryWitnessPointEvidence
B3ScoreComponents = QueryWitnessScoreComponents
b3_score_components = query_witness_score_components
score_b3_points = score_query_witness_points
simplify_b3_indices = simplify_query_witness_indices
