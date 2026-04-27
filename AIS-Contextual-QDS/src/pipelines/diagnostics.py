"""Dataset and semantics diagnostics for AIS-QDS experiments."""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from typing import Any, Callable

from psycopg import Connection

from ..config import AppConfig
from ..db import execute_sql, fetch_one
from ..query_semantics import normalize_query_mode

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class _TrajectoryLabelRecord:
    trajectory_id: int
    n_points: int
    positive_zones: tuple[str, ...]
    corridor_membership: bool

    @property
    def query_positive(self) -> bool:
        return bool(self.positive_zones) or self.corridor_membership


def _rate(numerator: int, denominator: int) -> float:
    return float(numerator) / float(denominator) if denominator > 0 else 0.0


def _stable_key(trajectory_id: int, seed: int, salt: str) -> str:
    raw = f"{trajectory_id}:{seed}:{salt}".encode("utf-8")
    return hashlib.md5(raw).hexdigest()


def _label_row_count(conn: Connection[Any], schema: str, *, label_mode: str, corridor_name: str) -> int:
    return int(
        fetch_one(
            conn,
            (
                f"SELECT COUNT(*) FROM {schema}.trajectory_query_labels "
                "WHERE label_mode = %(label_mode)s AND corridor_name = %(corridor_name)s;"
            ),
            {"label_mode": label_mode, "corridor_name": corridor_name},
        )
        or 0
    )


def _fetch_split_trajectory_counts(
    conn: Connection[Any],
    schema: str,
    *,
    subset_name: str,
) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
SELECT split, COUNT(*)
FROM {schema}.trajectory_dev_eval_subset
WHERE subset_name = %(subset_name)s
GROUP BY split
ORDER BY split;
""",
            {"subset_name": subset_name},
        )
        return {str(split): int(count_value) for split, count_value in cur.fetchall()}


def label_balance(
    conn: Connection[Any],
    config: AppConfig,
    *,
    mode: str | None = None,
    subset_name: str | None = None,
    min_zone_positives: int = 20,
    min_corridor_positives: int = 20,
) -> dict[str, object]:
    """Summarize query-label balance overall and for a configured subset."""
    schema = config.database.schema
    resolved_mode = normalize_query_mode(mode, default=config.performance.label_mode)
    selected_subset = subset_name or config.subsets.subset_name

    trajectory_count = int(fetch_one(conn, f"SELECT COUNT(*) FROM {schema}.trajectories_raw;") or 0)
    expected_label_rows = trajectory_count * len(config.context.zone_names)
    label_rows = _label_row_count(
        conn,
        schema,
        label_mode=resolved_mode,
        corridor_name=config.context.corridor_name,
    )

    zone_rows: list[dict[str, object]] = []
    with conn.cursor() as cur:
        cur.execute(
            f"""
SELECT
    q.zone_name,
    COUNT(*) AS label_rows,
    COALESCE(SUM(CASE WHEN q.zone_entry THEN 1 ELSE 0 END), 0) AS positive_rows,
    COUNT(DISTINCT CASE WHEN q.zone_entry THEN q.trajectory_id END) AS positive_trajectories
FROM {schema}.trajectory_query_labels q
WHERE q.label_mode = %(label_mode)s
  AND q.corridor_name = %(corridor_name)s
  AND q.zone_name = ANY(%(zone_names)s)
GROUP BY q.zone_name
ORDER BY q.zone_name;
""",
            {
                "label_mode": resolved_mode,
                "corridor_name": config.context.corridor_name,
                "zone_names": config.context.zone_names,
            },
        )
        raw_zone_rows = {str(row[0]): row for row in cur.fetchall()}

    for zone_name in config.context.zone_names:
        row = raw_zone_rows.get(zone_name)
        rows = int(row[1]) if row else 0
        positives = int(row[2]) if row else 0
        positive_trajectories = int(row[3]) if row else 0
        zone_rows.append(
            {
                "zone_name": zone_name,
                "label_rows": rows,
                "positive_rows": positives,
                "positive_trajectories": positive_trajectories,
                "positive_rate": _rate(positives, rows),
            }
        )

    with conn.cursor() as cur:
        cur.execute(
            f"""
WITH per_trajectory AS (
    SELECT
        q.trajectory_id,
        BOOL_OR(q.corridor_membership) AS corridor_membership
    FROM {schema}.trajectory_query_labels q
    WHERE q.label_mode = %(label_mode)s
      AND q.corridor_name = %(corridor_name)s
      AND q.zone_name = ANY(%(zone_names)s)
    GROUP BY q.trajectory_id
)
SELECT
    COUNT(*) AS trajectories,
    COALESCE(SUM(CASE WHEN corridor_membership THEN 1 ELSE 0 END), 0) AS positive_trajectories
FROM per_trajectory;
""",
            {
                "label_mode": resolved_mode,
                "corridor_name": config.context.corridor_name,
                "zone_names": config.context.zone_names,
            },
        )
        corridor_count, corridor_positive = cur.fetchone()

    split_counts = _fetch_split_trajectory_counts(conn, schema, subset_name=selected_subset)
    subset_payload: dict[str, object] = {
        "subset_name": selected_subset,
        "splits": {},
    }

    if split_counts:
        with conn.cursor() as cur:
            cur.execute(
                f"""
SELECT
    s.split,
    q.zone_name,
    COUNT(*) AS label_rows,
    COALESCE(SUM(CASE WHEN q.zone_entry THEN 1 ELSE 0 END), 0) AS positive_rows,
    COUNT(DISTINCT CASE WHEN q.zone_entry THEN q.trajectory_id END) AS positive_trajectories
FROM {schema}.trajectory_dev_eval_subset s
JOIN {schema}.trajectory_query_labels q
  ON q.trajectory_id = s.trajectory_id
WHERE s.subset_name = %(subset_name)s
  AND q.label_mode = %(label_mode)s
  AND q.corridor_name = %(corridor_name)s
  AND q.zone_name = ANY(%(zone_names)s)
GROUP BY s.split, q.zone_name
ORDER BY s.split, q.zone_name;
""",
                {
                    "subset_name": selected_subset,
                    "label_mode": resolved_mode,
                    "corridor_name": config.context.corridor_name,
                    "zone_names": config.context.zone_names,
                },
            )
            split_zone_rows = {
                (str(split), str(zone_name)): (int(label_count), int(positive_rows), int(positive_trajectories))
                for split, zone_name, label_count, positive_rows, positive_trajectories in cur.fetchall()
            }

            cur.execute(
                f"""
WITH per_trajectory AS (
    SELECT
        s.split,
        q.trajectory_id,
        BOOL_OR(q.corridor_membership) AS corridor_membership
    FROM {schema}.trajectory_dev_eval_subset s
    JOIN {schema}.trajectory_query_labels q
      ON q.trajectory_id = s.trajectory_id
    WHERE s.subset_name = %(subset_name)s
      AND q.label_mode = %(label_mode)s
      AND q.corridor_name = %(corridor_name)s
      AND q.zone_name = ANY(%(zone_names)s)
    GROUP BY s.split, q.trajectory_id
)
SELECT
    split,
    COUNT(*) AS trajectories,
    COALESCE(SUM(CASE WHEN corridor_membership THEN 1 ELSE 0 END), 0) AS positive_trajectories
FROM per_trajectory
GROUP BY split
ORDER BY split;
""",
                {
                    "subset_name": selected_subset,
                    "label_mode": resolved_mode,
                    "corridor_name": config.context.corridor_name,
                    "zone_names": config.context.zone_names,
                },
            )
            split_corridor_rows = {
                str(split): (int(trajectories), int(positive_trajectories))
                for split, trajectories, positive_trajectories in cur.fetchall()
            }

        split_payload: dict[str, object] = {}
        for split_name in sorted(split_counts):
            zone_payload = []
            for zone_name in config.context.zone_names:
                rows, positives, positive_trajectories = split_zone_rows.get((split_name, zone_name), (0, 0, 0))
                zone_payload.append(
                    {
                        "zone_name": zone_name,
                        "label_rows": rows,
                        "positive_rows": positives,
                        "positive_trajectories": positive_trajectories,
                        "positive_rate": _rate(positives, rows),
                    }
                )
            corridor_rows, corridor_split_positive = split_corridor_rows.get(split_name, (0, 0))
            split_payload[split_name] = {
                "trajectories": split_counts[split_name],
                "zones": zone_payload,
                "corridor": {
                    "trajectories": corridor_rows,
                    "positive_trajectories": corridor_split_positive,
                    "positive_rate": _rate(corridor_split_positive, corridor_rows),
                },
            }
        subset_payload["splits"] = split_payload

    warnings: list[str] = []
    if label_rows != expected_label_rows:
        warnings.append(
            f"Labels for mode={resolved_mode!r} are incomplete. Expected {expected_label_rows}, found {label_rows}."
        )

    for zone_row in zone_rows:
        if int(zone_row["positive_trajectories"]) < min_zone_positives:
            warnings.append(
                "Overall positives are low for "
                f"{zone_row['zone_name']}: {zone_row['positive_trajectories']} trajectories."
            )

    for split_name, split_payload in dict(subset_payload.get("splits", {})).items():
        for zone_row in split_payload["zones"]:
            if int(zone_row["positive_trajectories"]) < min_zone_positives:
                warnings.append(
                    f"Subset {selected_subset}/{split_name} has only "
                    f"{zone_row['positive_trajectories']} positives for {zone_row['zone_name']}."
                )
        corridor_data = split_payload["corridor"]
        if int(corridor_data["positive_trajectories"]) < min_corridor_positives:
            warnings.append(
                f"Subset {selected_subset}/{split_name} has only "
                f"{corridor_data['positive_trajectories']} corridor-positive trajectories."
            )

    return {
        "label_mode": resolved_mode,
        "expected_label_rows": expected_label_rows,
        "label_rows": label_rows,
        "labels_complete": label_rows == expected_label_rows,
        "overall": {
            "trajectories": trajectory_count,
            "zones": zone_rows,
            "corridor": {
                "trajectories": int(corridor_count),
                "positive_trajectories": int(corridor_positive),
                "positive_rate": _rate(int(corridor_positive), int(corridor_count)),
            },
        },
        "subset": subset_payload,
        "warnings": warnings,
    }


def _fetch_label_records(
    conn: Connection[Any],
    config: AppConfig,
    *,
    label_mode: str,
) -> list[_TrajectoryLabelRecord]:
    schema = config.database.schema
    with conn.cursor() as cur:
        cur.execute(
            f"""
SELECT
    t.trajectory_id,
    t.n_points,
    COALESCE(
        ARRAY_AGG(q.zone_name ORDER BY q.zone_name) FILTER (WHERE q.zone_entry),
        ARRAY[]::text[]
    ) AS positive_zones,
    BOOL_OR(q.corridor_membership) AS corridor_membership
FROM {schema}.trajectories_raw t
JOIN {schema}.trajectory_query_labels q
  ON q.trajectory_id = t.trajectory_id
WHERE q.label_mode = %(label_mode)s
  AND q.corridor_name = %(corridor_name)s
  AND q.zone_name = ANY(%(zone_names)s)
GROUP BY t.trajectory_id, t.n_points
ORDER BY t.trajectory_id;
""",
            {
                "label_mode": label_mode,
                "corridor_name": config.context.corridor_name,
                "zone_names": config.context.zone_names,
            },
        )
        rows = cur.fetchall()

    return [
        _TrajectoryLabelRecord(
            trajectory_id=int(trajectory_id),
            n_points=int(n_points),
            positive_zones=tuple(str(zone) for zone in positive_zones),
            corridor_membership=bool(corridor_membership),
        )
        for trajectory_id, n_points, positive_zones, corridor_membership in rows
    ]


def create_hardcase_subset(
    conn: Connection[Any],
    config: AppConfig,
    *,
    subset_name: str | None = None,
    label_mode: str | None = None,
    dev_size: int | None = None,
    eval_size: int | None = None,
    min_zone_positives_per_split: int = 20,
    min_corridor_positives_per_split: int = 60,
    positive_fraction: float = 0.50,
    truncate: bool = True,
) -> dict[str, object]:
    """Create a deterministic label-balanced hard-case subset."""
    schema = config.database.schema
    resolved_mode = normalize_query_mode(label_mode, default=config.performance.label_mode)
    selected_subset_name = subset_name or f"{config.subsets.subset_name}_hardcase"
    split_targets = {
        "dev": dev_size or config.subsets.dev_size,
        "eval": eval_size or config.subsets.eval_size,
    }
    if not 0.0 <= positive_fraction <= 1.0:
        raise ValueError("positive_fraction must be in [0, 1].")
    if any(size <= 0 for size in split_targets.values()):
        raise ValueError("Hard-case split sizes must be positive.")

    records = _fetch_label_records(conn, config, label_mode=resolved_mode)
    if not records:
        raise RuntimeError(
            f"No labels available for label_mode={resolved_mode!r}. "
            f"Run `python -m src.cli compute-labels --mode {resolved_mode}` first."
        )

    seed = config.subsets.random_seed
    selected: dict[str, list[int]] = {"dev": [], "eval": []}
    selected_ids: set[int] = set()
    counts = {
        split: {
            "zones": {zone_name: 0 for zone_name in config.context.zone_names},
            "corridor": 0,
            "query_positive": 0,
        }
        for split in selected
    }

    def can_add(split: str, record: _TrajectoryLabelRecord) -> bool:
        return record.trajectory_id not in selected_ids and len(selected[split]) < split_targets[split]

    def add(split: str, record: _TrajectoryLabelRecord) -> bool:
        if not can_add(split, record):
            return False
        selected[split].append(record.trajectory_id)
        selected_ids.add(record.trajectory_id)
        if record.query_positive:
            counts[split]["query_positive"] += 1
        if record.corridor_membership:
            counts[split]["corridor"] += 1
        for zone_name in record.positive_zones:
            if zone_name in counts[split]["zones"]:
                counts[split]["zones"][zone_name] += 1
        return True

    def ranked(candidates: list[_TrajectoryLabelRecord], salt: str) -> list[_TrajectoryLabelRecord]:
        return sorted(candidates, key=lambda record: _stable_key(record.trajectory_id, seed, salt))

    def add_balanced(
        candidates: list[_TrajectoryLabelRecord],
        *,
        salt: str,
        split_count: Callable[[str], int],
        target_per_split: int,
    ) -> None:
        for index, record in enumerate(ranked(candidates, salt)):
            if all(split_count(split) >= target_per_split for split in selected):
                break
            preferred = "dev" if index % 2 == 0 else "eval"
            fallback = "eval" if preferred == "dev" else "dev"
            for split in (preferred, fallback):
                if split_count(split) < target_per_split and add(split, record):
                    break

    for zone_name in config.context.zone_names:
        add_balanced(
            [record for record in records if zone_name in record.positive_zones],
            salt=f"zone:{zone_name}",
            split_count=lambda split, zone_name=zone_name: counts[split]["zones"][zone_name],
            target_per_split=min_zone_positives_per_split,
        )

    add_balanced(
        [record for record in records if record.corridor_membership],
        salt="corridor",
        split_count=lambda split: counts[split]["corridor"],
        target_per_split=min_corridor_positives_per_split,
    )

    def hard_score(record: _TrajectoryLabelRecord) -> tuple[int, int, str]:
        query_score = (len(record.positive_zones) * 100) + (25 if record.corridor_membership else 0)
        return (-query_score, -record.n_points, _stable_key(record.trajectory_id, seed, "hard-fill"))

    positive_records = sorted(
        [record for record in records if record.query_positive],
        key=hard_score,
    )
    negative_records = sorted(
        [record for record in records if not record.query_positive],
        key=lambda record: (-record.n_points, _stable_key(record.trajectory_id, seed, "negative-fill")),
    )
    all_records = sorted(
        records,
        key=lambda record: (-record.n_points, _stable_key(record.trajectory_id, seed, "fallback-fill")),
    )

    for split, target_size in split_targets.items():
        positive_target = int(round(target_size * positive_fraction))
        for record in positive_records:
            if counts[split]["query_positive"] >= positive_target or len(selected[split]) >= target_size:
                break
            add(split, record)
        for record in negative_records:
            if len(selected[split]) >= target_size:
                break
            add(split, record)
        for record in all_records:
            if len(selected[split]) >= target_size:
                break
            add(split, record)

    incomplete = {
        split: {
            "target": target,
            "selected": len(selected[split]),
        }
        for split, target in split_targets.items()
        if len(selected[split]) != target
    }
    if incomplete:
        raise RuntimeError(f"Could not fill requested hard-case subset sizes: {incomplete}")

    if truncate:
        execute_sql(
            conn,
            f"DELETE FROM {schema}.trajectory_dev_eval_subset WHERE subset_name = %(subset_name)s;",
            {"subset_name": selected_subset_name},
        )

    rows = [
        (selected_subset_name, trajectory_id, split)
        for split in ("dev", "eval")
        for trajectory_id in selected[split]
    ]
    with conn.cursor() as cur:
        cur.executemany(
            (
                f"INSERT INTO {schema}.trajectory_dev_eval_subset (subset_name, trajectory_id, split) "
                "VALUES (%s, %s, %s) "
                "ON CONFLICT (subset_name, trajectory_id) DO UPDATE SET split = EXCLUDED.split;"
            ),
            rows,
        )

    LOGGER.info(
        "Created hard-case subset %s with %s dev and %s eval trajectories.",
        selected_subset_name,
        len(selected["dev"]),
        len(selected["eval"]),
    )
    return {
        "subset_name": selected_subset_name,
        "label_mode": resolved_mode,
        "split_targets": split_targets,
        "positive_fraction": positive_fraction,
        "min_zone_positives_per_split": min_zone_positives_per_split,
        "min_corridor_positives_per_split": min_corridor_positives_per_split,
        "selected": {
            split: {
                "trajectories": len(selected[split]),
                "query_positive": counts[split]["query_positive"],
                "corridor_positive": counts[split]["corridor"],
                "zone_positive": counts[split]["zones"],
            }
            for split in ("dev", "eval")
        },
        "balance": label_balance(
            conn,
            config,
            mode=resolved_mode,
            subset_name=selected_subset_name,
            min_zone_positives=min_zone_positives_per_split,
            min_corridor_positives=min_corridor_positives_per_split,
        ),
    }


def compare_label_modes(
    conn: Connection[Any],
    config: AppConfig,
    *,
    base_mode: str = "optimized",
    candidate_mode: str = "segment_exact",
    subset_name: str | None = None,
    split: str | None = None,
) -> dict[str, object]:
    """Compare stored query labels between two semantics modes."""
    schema = config.database.schema
    resolved_base = normalize_query_mode(base_mode, default="optimized")
    resolved_candidate = normalize_query_mode(candidate_mode, default="segment_exact")
    selected_subset = subset_name or config.subsets.subset_name

    trajectory_count = int(fetch_one(conn, f"SELECT COUNT(*) FROM {schema}.trajectories_raw;") or 0)
    expected_rows = trajectory_count * len(config.context.zone_names)
    base_rows = _label_row_count(
        conn,
        schema,
        label_mode=resolved_base,
        corridor_name=config.context.corridor_name,
    )
    candidate_rows = _label_row_count(
        conn,
        schema,
        label_mode=resolved_candidate,
        corridor_name=config.context.corridor_name,
    )

    subset_join = ""
    subset_where = ""
    params: dict[str, object] = {
        "base_mode": resolved_base,
        "candidate_mode": resolved_candidate,
        "corridor_name": config.context.corridor_name,
        "zone_names": config.context.zone_names,
        "subset_name": selected_subset,
    }
    if subset_name is not None or split is not None:
        subset_join = f"JOIN {schema}.trajectory_dev_eval_subset s ON s.trajectory_id = b.trajectory_id"
        subset_where = "AND s.subset_name = %(subset_name)s"
        if split is not None:
            if split not in {"dev", "eval"}:
                raise ValueError("split must be one of dev/eval when comparing label modes.")
            subset_where += " AND s.split = %(split)s"
            params["split"] = split

    with conn.cursor() as cur:
        cur.execute(
            f"""
WITH pairs AS (
    SELECT
        b.trajectory_id,
        b.zone_name,
        b.zone_entry AS base_zone_entry,
        c.zone_entry AS candidate_zone_entry,
        b.corridor_membership AS base_corridor_membership,
        c.corridor_membership AS candidate_corridor_membership
    FROM {schema}.trajectory_query_labels b
    JOIN {schema}.trajectory_query_labels c
      ON c.trajectory_id = b.trajectory_id
     AND c.zone_name = b.zone_name
     AND c.corridor_name = b.corridor_name
    {subset_join}
    WHERE b.label_mode = %(base_mode)s
      AND c.label_mode = %(candidate_mode)s
      AND b.corridor_name = %(corridor_name)s
      AND b.zone_name = ANY(%(zone_names)s)
      {subset_where}
)
SELECT
    zone_name,
    COUNT(*) AS comparable_pairs,
    COALESCE(SUM(CASE WHEN base_zone_entry THEN 1 ELSE 0 END), 0) AS base_positive,
    COALESCE(SUM(CASE WHEN candidate_zone_entry THEN 1 ELSE 0 END), 0) AS candidate_positive,
    COALESCE(SUM(CASE WHEN base_zone_entry <> candidate_zone_entry THEN 1 ELSE 0 END), 0) AS disagreements
FROM pairs
GROUP BY zone_name
ORDER BY zone_name;
""",
            params,
        )
        zone_rows = [
            {
                "zone_name": str(zone_name),
                "comparable_pairs": int(comparable_pairs),
                "base_positive": int(base_positive),
                "candidate_positive": int(candidate_positive),
                "disagreements": int(disagreements),
                "disagreement_rate": _rate(int(disagreements), int(comparable_pairs)),
            }
            for zone_name, comparable_pairs, base_positive, candidate_positive, disagreements in cur.fetchall()
        ]

        cur.execute(
            f"""
WITH pairs AS (
    SELECT
        b.trajectory_id,
        BOOL_OR(b.corridor_membership) AS base_corridor_membership,
        BOOL_OR(c.corridor_membership) AS candidate_corridor_membership
    FROM {schema}.trajectory_query_labels b
    JOIN {schema}.trajectory_query_labels c
      ON c.trajectory_id = b.trajectory_id
     AND c.zone_name = b.zone_name
     AND c.corridor_name = b.corridor_name
    {subset_join}
    WHERE b.label_mode = %(base_mode)s
      AND c.label_mode = %(candidate_mode)s
      AND b.corridor_name = %(corridor_name)s
      AND b.zone_name = ANY(%(zone_names)s)
      {subset_where}
    GROUP BY b.trajectory_id
)
SELECT
    COUNT(*) AS comparable_trajectories,
    COALESCE(SUM(CASE WHEN base_corridor_membership THEN 1 ELSE 0 END), 0) AS base_positive,
    COALESCE(SUM(CASE WHEN candidate_corridor_membership THEN 1 ELSE 0 END), 0) AS candidate_positive,
    COALESCE(SUM(CASE WHEN base_corridor_membership <> candidate_corridor_membership THEN 1 ELSE 0 END), 0) AS disagreements
FROM pairs;
""",
            params,
        )
        corridor_row = cur.fetchone()

    comparable_pairs = sum(int(row["comparable_pairs"]) for row in zone_rows)
    warnings: list[str] = []
    if base_rows != expected_rows:
        warnings.append(f"Base labels are incomplete: expected {expected_rows}, found {base_rows}.")
    if candidate_rows != expected_rows:
        warnings.append(
            f"Candidate labels are incomplete: expected {expected_rows}, found {candidate_rows}. "
            f"Run `python -m src.cli compute-labels --mode {resolved_candidate}` for a full audit."
        )
    if comparable_pairs == 0:
        warnings.append("No comparable label pairs found for the selected modes/subset.")

    comparable_trajectories = int(corridor_row[0] or 0)
    corridor_disagreements = int(corridor_row[3] or 0)
    return {
        "base_mode": resolved_base,
        "candidate_mode": resolved_candidate,
        "scope": {
            "subset_name": selected_subset if subset_name is not None or split is not None else None,
            "split": split,
        },
        "label_rows": {
            "expected_full_rows": expected_rows,
            resolved_base: base_rows,
            resolved_candidate: candidate_rows,
        },
        "ready_for_full_audit": base_rows == expected_rows and candidate_rows == expected_rows,
        "zone_entry": {
            "comparable_pairs": comparable_pairs,
            "zones": zone_rows,
            "total_disagreements": sum(int(row["disagreements"]) for row in zone_rows),
        },
        "corridor_membership": {
            "comparable_trajectories": comparable_trajectories,
            "base_positive": int(corridor_row[1] or 0),
            "candidate_positive": int(corridor_row[2] or 0),
            "disagreements": corridor_disagreements,
            "disagreement_rate": _rate(corridor_disagreements, comparable_trajectories),
        },
        "warnings": warnings,
    }
