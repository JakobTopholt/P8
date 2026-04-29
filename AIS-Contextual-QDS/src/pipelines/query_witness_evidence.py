"""Database-backed query-witness evidence extraction."""

from __future__ import annotations

from typing import Any, Mapping, Protocol, Sequence

from psycopg import Connection, sql

from ..config import AppConfig
from ..simplification import QueryWitnessPointEvidence


class SourcePointLike(Protocol):
    """Minimum trajectory-point interface needed for evidence alignment."""

    source_point_seq: int


def fetch_query_witness_point_evidence(
    conn: Connection[Any],
    config: AppConfig,
    trajectories: Mapping[int, Sequence[SourcePointLike]],
) -> dict[int, list[QueryWitnessPointEvidence]]:
    """Fetch query-witness inputs and local-shape evidence for selected trajectories."""
    if not trajectories:
        return {}

    schema = config.database.schema
    trajectory_ids = sorted(trajectories)
    feature_sql = f"""
WITH selected_points AS (
    SELECT
        p.trajectory_id,
        p.point_seq,
        p.geom,
        LAG(p.geom) OVER (PARTITION BY p.trajectory_id ORDER BY p.point_seq) AS prev_geom,
        LEAD(p.geom) OVER (PARTITION BY p.trajectory_id ORDER BY p.point_seq) AS next_geom
    FROM {schema}.trajectory_points_raw p
    WHERE p.trajectory_id = ANY(%(trajectory_ids)s::bigint[])
),
zones AS (
    SELECT zone_name, geom
    FROM {schema}.context_zones
    WHERE zone_name = ANY(%(zone_names)s)
),
corridor AS (
    SELECT corridor_name, geom
    FROM {schema}.context_corridors
    WHERE corridor_name = %(corridor_name)s
),
point_zone_hits AS (
    SELECT
        p.trajectory_id,
        p.point_seq,
        COALESCE(SUM(CASE WHEN ST_Covers(z.geom, p.geom) THEN 1 ELSE 0 END), 0)::integer AS zone_point_hits
    FROM selected_points p
    CROSS JOIN zones z
    GROUP BY p.trajectory_id, p.point_seq
),
point_corridor_hits AS (
    SELECT
        p.trajectory_id,
        p.point_seq,
        BOOL_OR(ST_Covers(c.geom, p.geom)) AS corridor_point_hit
    FROM selected_points p
    CROSS JOIN corridor c
    GROUP BY p.trajectory_id, p.point_seq
),
segments AS (
    SELECT
        p1.trajectory_id,
        p1.point_seq AS from_point_seq,
        p2.point_seq AS to_point_seq,
        ST_MakeLine(p1.geom, p2.geom) AS segment_geom,
        p1.geom AS from_geom,
        p2.geom AS to_geom
    FROM selected_points p1
    JOIN selected_points p2
      ON p2.trajectory_id = p1.trajectory_id
     AND p2.point_seq = p1.point_seq + 1
),
zone_segment_witnesses AS (
    SELECT
        s.trajectory_id,
        s.from_point_seq,
        s.to_point_seq,
        COALESCE(
            SUM(
                CASE
                    WHEN (NOT ST_Contains(z.geom, s.from_geom) AND ST_Contains(z.geom, s.to_geom))
                         OR ST_Crosses(s.segment_geom, z.geom)
                    THEN 1
                    ELSE 0
                END
            ),
            0
        )::integer AS zone_entry_segment_witnesses,
        COALESCE(
            SUM(
                CASE
                    WHEN ST_Covers(z.geom, s.from_geom) IS DISTINCT FROM ST_Covers(z.geom, s.to_geom) THEN 1
                    ELSE 0
                END
            ),
            0
        )::integer AS zone_transition_witnesses,
        COALESCE(
            SUM(
                CASE
                    WHEN NOT ST_Covers(z.geom, s.from_geom) AND ST_Covers(z.geom, s.to_geom) THEN 1
                    ELSE 0
                END
            ),
            0
        )::integer AS zone_entry_event_witnesses
    FROM segments s
    CROSS JOIN zones z
    GROUP BY s.trajectory_id, s.from_point_seq, s.to_point_seq
),
corridor_segment_witnesses AS (
    SELECT
        s.trajectory_id,
        s.from_point_seq,
        s.to_point_seq,
        CASE
            WHEN BOOL_OR(
                CASE
                    WHEN ST_Intersects(s.segment_geom, c.geom)
                    THEN ST_Length(ST_Intersection(s.segment_geom, c.geom)::geography) >= %(min_overlap_m)s
                    ELSE FALSE
                END
            ) THEN 1
            ELSE 0
        END AS corridor_segment_witnesses,
        BOOL_OR(ST_Covers(c.geom, s.from_geom) IS DISTINCT FROM ST_Covers(c.geom, s.to_geom))
            AS corridor_transition_witness,
        BOOL_OR(NOT ST_Covers(c.geom, s.from_geom) AND ST_Covers(c.geom, s.to_geom))
            AS corridor_entry_event_witness
    FROM segments s
    CROSS JOIN corridor c
    GROUP BY s.trajectory_id, s.from_point_seq, s.to_point_seq
),
segment_endpoint_witnesses AS (
    SELECT
        trajectory_id,
        point_seq,
        COALESCE(SUM(zone_entry_segment_witnesses), 0)::integer AS zone_entry_segment_witnesses,
        COALESCE(SUM(zone_transition_witnesses), 0)::integer AS zone_transition_witnesses,
        COALESCE(SUM(zone_entry_event_witnesses), 0)::integer AS zone_entry_event_witnesses,
        COALESCE(SUM(corridor_segment_witnesses), 0)::integer AS corridor_segment_witnesses,
        BOOL_OR(corridor_transition_witness) AS corridor_transition_witness,
        BOOL_OR(corridor_entry_event_witness) AS corridor_entry_event_witness
    FROM (
        SELECT
            z.trajectory_id,
            z.from_point_seq AS point_seq,
            z.zone_entry_segment_witnesses,
            z.zone_transition_witnesses,
            z.zone_entry_event_witnesses,
            COALESCE(c.corridor_segment_witnesses, 0) AS corridor_segment_witnesses,
            COALESCE(c.corridor_transition_witness, FALSE) AS corridor_transition_witness,
            COALESCE(c.corridor_entry_event_witness, FALSE) AS corridor_entry_event_witness
        FROM zone_segment_witnesses z
        LEFT JOIN corridor_segment_witnesses c
          ON c.trajectory_id = z.trajectory_id
         AND c.from_point_seq = z.from_point_seq
         AND c.to_point_seq = z.to_point_seq
        UNION ALL
        SELECT
            z.trajectory_id,
            z.to_point_seq AS point_seq,
            z.zone_entry_segment_witnesses,
            z.zone_transition_witnesses,
            z.zone_entry_event_witnesses,
            COALESCE(c.corridor_segment_witnesses, 0) AS corridor_segment_witnesses,
            COALESCE(c.corridor_transition_witness, FALSE) AS corridor_transition_witness,
            COALESCE(c.corridor_entry_event_witness, FALSE) AS corridor_entry_event_witness
        FROM zone_segment_witnesses z
        LEFT JOIN corridor_segment_witnesses c
          ON c.trajectory_id = z.trajectory_id
         AND c.from_point_seq = z.from_point_seq
         AND c.to_point_seq = z.to_point_seq
    ) endpoint_rows
    GROUP BY trajectory_id, point_seq
),
local_shape AS (
    SELECT
        p.trajectory_id,
        p.point_seq,
        CASE
            WHEN p.prev_geom IS NULL OR p.next_geom IS NULL THEN 0.0::double precision
            ELSE COALESCE(
                DEGREES(
                    ABS(
                        ATAN2(
                            SIN(ST_Azimuth(p.geom, p.next_geom) - ST_Azimuth(p.prev_geom, p.geom)),
                            COS(ST_Azimuth(p.geom, p.next_geom) - ST_Azimuth(p.prev_geom, p.geom))
                        )
                    )
                ),
                0.0
            )
        END AS local_turn_degrees,
        CASE
            WHEN p.prev_geom IS NULL OR p.next_geom IS NULL OR ST_Equals(p.prev_geom, p.next_geom)
                THEN 0.0::double precision
            ELSE COALESCE(
                ST_Distance(p.geom::geography, ST_MakeLine(p.prev_geom, p.next_geom)::geography),
                0.0
            )
        END AS local_deviation_m
    FROM selected_points p
)
SELECT
    p.trajectory_id,
    p.point_seq,
    COALESCE(pzh.zone_point_hits, 0) AS zone_point_hits,
    COALESCE(sew.zone_entry_segment_witnesses, 0) AS zone_entry_segment_witnesses,
    COALESCE(sew.zone_transition_witnesses, 0) AS zone_transition_witnesses,
    COALESCE(sew.zone_entry_event_witnesses, 0) AS zone_entry_event_witnesses,
    COALESCE(pch.corridor_point_hit, FALSE) AS corridor_point_hit,
    COALESCE(sew.corridor_segment_witnesses, 0) AS corridor_segment_witnesses,
    COALESCE(sew.corridor_transition_witness, FALSE) AS corridor_transition_witness,
    COALESCE(sew.corridor_entry_event_witness, FALSE) AS corridor_entry_event_witness,
    COALESCE(ls.local_turn_degrees, 0.0) AS local_turn_degrees,
    COALESCE(ls.local_deviation_m, 0.0) AS local_deviation_m
FROM selected_points p
LEFT JOIN point_zone_hits pzh
  ON pzh.trajectory_id = p.trajectory_id
 AND pzh.point_seq = p.point_seq
LEFT JOIN point_corridor_hits pch
  ON pch.trajectory_id = p.trajectory_id
 AND pch.point_seq = p.point_seq
LEFT JOIN segment_endpoint_witnesses sew
  ON sew.trajectory_id = p.trajectory_id
 AND sew.point_seq = p.point_seq
LEFT JOIN local_shape ls
  ON ls.trajectory_id = p.trajectory_id
 AND ls.point_seq = p.point_seq
ORDER BY p.trajectory_id, p.point_seq;
"""

    evidence_by_seq: dict[int, dict[int, QueryWitnessPointEvidence]] = {}
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SET LOCAL work_mem = {};").format(sql.Literal("128MB")))
        cur.execute(
            feature_sql,
            {
                "trajectory_ids": trajectory_ids,
                "zone_names": config.context.zone_names,
                "corridor_name": config.context.corridor_name,
                "min_overlap_m": config.queries.min_corridor_overlap_meters,
            },
        )
        for (
            trajectory_id,
            point_seq,
            zone_point_hits,
            zone_entry_segment_witnesses,
            zone_transition_witnesses,
            zone_entry_event_witnesses,
            corridor_point_hit,
            corridor_segment_witnesses,
            corridor_transition_witness,
            corridor_entry_event_witness,
            local_turn_degrees,
            local_deviation_m,
        ) in cur:
            evidence_by_seq.setdefault(int(trajectory_id), {})[int(point_seq)] = QueryWitnessPointEvidence(
                zone_point_hits=int(zone_point_hits),
                zone_entry_segment_witnesses=int(zone_entry_segment_witnesses),
                zone_transition_witnesses=int(zone_transition_witnesses),
                zone_entry_event_witnesses=int(zone_entry_event_witnesses),
                corridor_point_hit=bool(corridor_point_hit),
                corridor_segment_witnesses=int(corridor_segment_witnesses),
                corridor_transition_witness=bool(corridor_transition_witness),
                corridor_entry_event_witness=bool(corridor_entry_event_witness),
                local_turn_degrees=float(local_turn_degrees or 0.0),
                local_deviation_m=float(local_deviation_m or 0.0),
            )

    features: dict[int, list[QueryWitnessPointEvidence]] = {}
    for trajectory_id, points in trajectories.items():
        sequence_features = evidence_by_seq.get(trajectory_id, {})
        features[trajectory_id] = [
            sequence_features.get(point.source_point_seq, QueryWitnessPointEvidence())
            for point in points
        ]

    return features


fetch_b3_point_evidence = fetch_query_witness_point_evidence
