"""Strict point-hit and event-count metrics for simplification benchmarks."""

from __future__ import annotations

from typing import Any

from psycopg import Connection, sql

from ..config import AppConfig
from .metrics import classification_metrics


def _safe_metric_key(raw_value: str) -> str:
    return "".join(char if char.isalnum() else "_" for char in raw_value.lower()).strip("_")


def _empty_confusion_counts() -> dict[str, int]:
    return {
        "tp": 0,
        "fp": 0,
        "fn": 0,
        "tn": 0,
        "support": 0,
        "true_positive": 0,
        "predicted_positive": 0,
    }


def _strict_query_ctes(schema: str) -> str:
    return f"""
run_trajectories AS (
    SELECT DISTINCT trajectory_id
    FROM {schema}.trajectories_simplified_points
    WHERE run_id = %(run_id)s
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
raw_points AS (
    SELECT p.trajectory_id, p.point_seq, p.geom
    FROM {schema}.trajectory_points_raw p
    JOIN run_trajectories rt ON rt.trajectory_id = p.trajectory_id
),
run_points AS (
    SELECT trajectory_id, point_seq, geom
    FROM {schema}.trajectories_simplified_points
    WHERE run_id = %(run_id)s
),
raw_zone_flags AS (
    SELECT
        p.trajectory_id,
        p.point_seq,
        z.zone_name,
        ST_Covers(z.geom, p.geom) AS inside_zone
    FROM raw_points p
    CROSS JOIN zones z
),
run_zone_flags AS (
    SELECT
        p.trajectory_id,
        p.point_seq,
        z.zone_name,
        ST_Covers(z.geom, p.geom) AS inside_zone
    FROM run_points p
    CROSS JOIN zones z
),
raw_zone_events AS (
    SELECT
        trajectory_id,
        point_seq,
        zone_name,
        inside_zone,
        LAG(inside_zone) OVER (PARTITION BY trajectory_id, zone_name ORDER BY point_seq) AS prev_inside_zone
    FROM raw_zone_flags
),
run_zone_events AS (
    SELECT
        trajectory_id,
        point_seq,
        zone_name,
        inside_zone,
        LAG(inside_zone) OVER (PARTITION BY trajectory_id, zone_name ORDER BY point_seq) AS prev_inside_zone
    FROM run_zone_flags
),
raw_zone_summary AS (
    SELECT
        trajectory_id,
        zone_name,
        BOOL_OR(inside_zone) AS point_membership,
        COALESCE(
            SUM(
                CASE
                    WHEN point_seq > 1 AND inside_zone AND NOT COALESCE(prev_inside_zone, FALSE) THEN 1
                    ELSE 0
                END
            ),
            0
        )::integer AS entry_count
    FROM raw_zone_events
    GROUP BY trajectory_id, zone_name
),
run_zone_summary AS (
    SELECT
        trajectory_id,
        zone_name,
        BOOL_OR(inside_zone) AS point_membership,
        COALESCE(
            SUM(
                CASE
                    WHEN point_seq > 1 AND inside_zone AND NOT COALESCE(prev_inside_zone, FALSE) THEN 1
                    ELSE 0
                END
            ),
            0
        )::integer AS entry_count
    FROM run_zone_events
    GROUP BY trajectory_id, zone_name
),
zone_pairs AS (
    SELECT
        rz.trajectory_id,
        rz.zone_name,
        rz.point_membership AS raw_point_membership,
        sz.point_membership AS run_point_membership,
        rz.entry_count AS raw_entry_count,
        sz.entry_count AS run_entry_count
    FROM raw_zone_summary rz
    JOIN run_zone_summary sz
      ON sz.trajectory_id = rz.trajectory_id
     AND sz.zone_name = rz.zone_name
),
raw_corridor_flags AS (
    SELECT
        p.trajectory_id,
        p.point_seq,
        ST_Covers(c.geom, p.geom) AS inside_corridor
    FROM raw_points p
    CROSS JOIN corridor c
),
run_corridor_flags AS (
    SELECT
        p.trajectory_id,
        p.point_seq,
        ST_Covers(c.geom, p.geom) AS inside_corridor
    FROM run_points p
    CROSS JOIN corridor c
),
raw_corridor_events AS (
    SELECT
        trajectory_id,
        point_seq,
        inside_corridor,
        LAG(inside_corridor) OVER (PARTITION BY trajectory_id ORDER BY point_seq) AS prev_inside_corridor
    FROM raw_corridor_flags
),
run_corridor_events AS (
    SELECT
        trajectory_id,
        point_seq,
        inside_corridor,
        LAG(inside_corridor) OVER (PARTITION BY trajectory_id ORDER BY point_seq) AS prev_inside_corridor
    FROM run_corridor_flags
),
raw_corridor_summary AS (
    SELECT
        trajectory_id,
        BOOL_OR(inside_corridor) AS point_membership,
        COALESCE(
            SUM(
                CASE
                    WHEN point_seq > 1 AND inside_corridor AND NOT COALESCE(prev_inside_corridor, FALSE) THEN 1
                    ELSE 0
                END
            ),
            0
        )::integer AS entry_count
    FROM raw_corridor_events
    GROUP BY trajectory_id
),
run_corridor_summary AS (
    SELECT
        trajectory_id,
        BOOL_OR(inside_corridor) AS point_membership,
        COALESCE(
            SUM(
                CASE
                    WHEN point_seq > 1 AND inside_corridor AND NOT COALESCE(prev_inside_corridor, FALSE) THEN 1
                    ELSE 0
                END
            ),
            0
        )::integer AS entry_count
    FROM run_corridor_events
    GROUP BY trajectory_id
),
corridor_pairs AS (
    SELECT
        rc.trajectory_id,
        rc.point_membership AS raw_point_membership,
        sc.point_membership AS run_point_membership,
        rc.entry_count AS raw_entry_count,
        sc.entry_count AS run_entry_count
    FROM raw_corridor_summary rc
    JOIN run_corridor_summary sc ON sc.trajectory_id = rc.trajectory_id
)
"""


def _classification_count_payload(prefix: str, counts: dict[str, int]) -> dict[str, float]:
    metrics = classification_metrics(counts["tp"], counts["fp"], counts["fn"])
    return {
        f"{prefix}_precision": metrics["precision"],
        f"{prefix}_recall": metrics["recall"],
        f"{prefix}_f1": metrics["f1"],
        f"{prefix}_tp": float(counts["tp"]),
        f"{prefix}_fp": float(counts["fp"]),
        f"{prefix}_fn": float(counts["fn"]),
        f"{prefix}_tn": float(counts["tn"]),
        f"{prefix}_support": float(counts["support"]),
        f"{prefix}_true_positive": float(counts["true_positive"]),
        f"{prefix}_predicted_positive": float(counts["predicted_positive"]),
    }


def compute_strict_point_event_metrics(
    conn: Connection[Any],
    config: AppConfig,
    run_id: int,
) -> dict[str, float]:
    """Compute strict point-hit and entry-count metrics for a simplification run."""
    schema = config.database.schema
    common_ctes = _strict_query_ctes(schema)
    params = {
        "run_id": run_id,
        "zone_names": config.context.zone_names,
        "corridor_name": config.context.corridor_name,
    }

    zone_sql = f"""
WITH {common_ctes}
SELECT
    zone_name,
    COALESCE(SUM(CASE WHEN raw_point_membership AND run_point_membership THEN 1 ELSE 0 END), 0) AS tp,
    COALESCE(SUM(CASE WHEN NOT raw_point_membership AND run_point_membership THEN 1 ELSE 0 END), 0) AS fp,
    COALESCE(SUM(CASE WHEN raw_point_membership AND NOT run_point_membership THEN 1 ELSE 0 END), 0) AS fn,
    COALESCE(SUM(CASE WHEN NOT raw_point_membership AND NOT run_point_membership THEN 1 ELSE 0 END), 0) AS tn,
    COALESCE(COUNT(*), 0) AS support,
    COALESCE(SUM(CASE WHEN raw_point_membership THEN 1 ELSE 0 END), 0) AS true_positive,
    COALESCE(SUM(CASE WHEN run_point_membership THEN 1 ELSE 0 END), 0) AS predicted_positive,
    COALESCE(SUM(ABS(raw_entry_count - run_entry_count)), 0) AS entry_abs_error,
    COALESCE(SUM(CASE WHEN raw_entry_count = run_entry_count THEN 1 ELSE 0 END), 0) AS entry_exact,
    COALESCE(SUM(raw_entry_count), 0) AS raw_entry_total,
    COALESCE(SUM(run_entry_count), 0) AS run_entry_total
FROM zone_pairs
GROUP BY zone_name
ORDER BY zone_name;
"""
    corridor_sql = f"""
WITH {common_ctes}
SELECT
    COALESCE(SUM(CASE WHEN raw_point_membership AND run_point_membership THEN 1 ELSE 0 END), 0) AS tp,
    COALESCE(SUM(CASE WHEN NOT raw_point_membership AND run_point_membership THEN 1 ELSE 0 END), 0) AS fp,
    COALESCE(SUM(CASE WHEN raw_point_membership AND NOT run_point_membership THEN 1 ELSE 0 END), 0) AS fn,
    COALESCE(SUM(CASE WHEN NOT raw_point_membership AND NOT run_point_membership THEN 1 ELSE 0 END), 0) AS tn,
    COALESCE(COUNT(*), 0) AS support,
    COALESCE(SUM(CASE WHEN raw_point_membership THEN 1 ELSE 0 END), 0) AS true_positive,
    COALESCE(SUM(CASE WHEN run_point_membership THEN 1 ELSE 0 END), 0) AS predicted_positive,
    COALESCE(SUM(ABS(raw_entry_count - run_entry_count)), 0) AS entry_abs_error,
    COALESCE(SUM(CASE WHEN raw_entry_count = run_entry_count THEN 1 ELSE 0 END), 0) AS entry_exact,
    COALESCE(SUM(raw_entry_count), 0) AS raw_entry_total,
    COALESCE(SUM(run_entry_count), 0) AS run_entry_total
FROM corridor_pairs;
"""

    with conn.cursor() as cur:
        cur.execute(sql.SQL("SET LOCAL work_mem = {};").format(sql.Literal("128MB")))
        cur.execute(zone_sql, params)
        zone_rows = cur.fetchall()
        cur.execute(corridor_sql, params)
        corridor_row = cur.fetchone()

    payload: dict[str, float] = {}
    aggregate_zone_counts = _empty_confusion_counts()
    zone_metric_values: list[dict[str, float]] = []
    zone_event_exact_rates: list[float] = []
    zone_event_maes: list[float] = []
    total_zone_entry_abs_error = 0.0
    total_zone_entry_exact = 0.0
    total_zone_support = 0.0
    total_raw_zone_entries = 0.0
    total_run_zone_entries = 0.0

    for (
        zone_name,
        tp,
        fp,
        fn,
        tn,
        support,
        true_positive,
        predicted_positive,
        entry_abs_error,
        entry_exact,
        raw_entry_total,
        run_entry_total,
    ) in zone_rows:
        counts = {
            "tp": int(tp),
            "fp": int(fp),
            "fn": int(fn),
            "tn": int(tn),
            "support": int(support),
            "true_positive": int(true_positive),
            "predicted_positive": int(predicted_positive),
        }
        for key in aggregate_zone_counts:
            aggregate_zone_counts[key] += counts[key]

        zone_key = _safe_metric_key(str(zone_name))
        payload.update(_classification_count_payload(f"zone_point_membership_{zone_key}", counts))
        metrics = classification_metrics(counts["tp"], counts["fp"], counts["fn"])
        zone_metric_values.append(metrics)

        support_value = float(support)
        exact_rate = float(entry_exact) / support_value if support_value > 0 else 0.0
        mae = float(entry_abs_error) / support_value if support_value > 0 else 0.0
        payload.update(
            {
                f"zone_entry_event_count_{zone_key}_exact_rate": exact_rate,
                f"zone_entry_event_count_{zone_key}_mae": mae,
                f"zone_entry_event_count_{zone_key}_total_abs_error": float(entry_abs_error),
                f"zone_entry_event_count_{zone_key}_support": support_value,
                f"zone_entry_event_count_{zone_key}_raw_total": float(raw_entry_total),
                f"zone_entry_event_count_{zone_key}_run_total": float(run_entry_total),
            }
        )
        zone_event_exact_rates.append(exact_rate)
        zone_event_maes.append(mae)
        total_zone_entry_abs_error += float(entry_abs_error)
        total_zone_entry_exact += float(entry_exact)
        total_zone_support += support_value
        total_raw_zone_entries += float(raw_entry_total)
        total_run_zone_entries += float(run_entry_total)

    payload.update(_classification_count_payload("zone_point_membership", aggregate_zone_counts))
    if zone_metric_values:
        payload["zone_point_membership_macro_precision"] = sum(
            item["precision"] for item in zone_metric_values
        ) / len(zone_metric_values)
        payload["zone_point_membership_macro_recall"] = sum(item["recall"] for item in zone_metric_values) / len(
            zone_metric_values
        )
        payload["zone_point_membership_macro_f1"] = sum(item["f1"] for item in zone_metric_values) / len(
            zone_metric_values
        )
    if zone_event_exact_rates:
        payload["zone_entry_event_count_macro_exact_rate"] = sum(zone_event_exact_rates) / len(zone_event_exact_rates)
        payload["zone_entry_event_count_macro_mae"] = sum(zone_event_maes) / len(zone_event_maes)
    payload["zone_entry_event_count_exact_rate"] = total_zone_entry_exact / total_zone_support if total_zone_support > 0 else 0.0
    payload["zone_entry_event_count_mae"] = total_zone_entry_abs_error / total_zone_support if total_zone_support > 0 else 0.0
    payload["zone_entry_event_count_total_abs_error"] = total_zone_entry_abs_error
    payload["zone_entry_event_count_support"] = total_zone_support
    payload["zone_entry_event_count_raw_total"] = total_raw_zone_entries
    payload["zone_entry_event_count_run_total"] = total_run_zone_entries

    (
        tp,
        fp,
        fn,
        tn,
        support,
        true_positive,
        predicted_positive,
        entry_abs_error,
        entry_exact,
        raw_entry_total,
        run_entry_total,
    ) = corridor_row
    corridor_counts = {
        "tp": int(tp),
        "fp": int(fp),
        "fn": int(fn),
        "tn": int(tn),
        "support": int(support),
        "true_positive": int(true_positive),
        "predicted_positive": int(predicted_positive),
    }
    payload.update(_classification_count_payload("corridor_point_membership", corridor_counts))
    support_value = float(support)
    payload.update(
        {
            "corridor_entry_event_count_exact_rate": float(entry_exact) / support_value if support_value > 0 else 0.0,
            "corridor_entry_event_count_mae": float(entry_abs_error) / support_value if support_value > 0 else 0.0,
            "corridor_entry_event_count_total_abs_error": float(entry_abs_error),
            "corridor_entry_event_count_support": support_value,
            "corridor_entry_event_count_raw_total": float(raw_entry_total),
            "corridor_entry_event_count_run_total": float(run_entry_total),
        }
    )

    return payload
