"""Ground-truth query label computation for raw trajectories."""

from __future__ import annotations

import logging
from typing import Any

from psycopg import Connection, sql

from ..config import AppConfig
from ..db import execute_sql, fetch_one
from ..query_semantics import build_raw_label_insert_sql, normalize_query_mode

LOGGER = logging.getLogger(__name__)


def run(
    conn: Connection[Any],
    config: AppConfig,
    *,
    truncate: bool = True,
    mode: str | None = None,
) -> dict[str, int | str]:
    """Compute zone-entry and corridor-membership labels."""
    schema = config.database.schema
    resolved_mode = normalize_query_mode(mode, default=config.performance.label_mode)

    if truncate:
        execute_sql(
            conn,
            f"DELETE FROM {schema}.trajectory_query_labels WHERE label_mode = %(label_mode)s;",
            {"label_mode": resolved_mode},
        )

    zone_count = int(
        fetch_one(
            conn,
            f"SELECT COUNT(*) FROM {schema}.context_zones WHERE zone_name = ANY(%(zone_names)s);",
            {"zone_names": config.context.zone_names},
        )
        or 0
    )
    if zone_count != len(config.context.zone_names):
        raise RuntimeError(
            "Context zones are missing. "
            f"Expected {len(config.context.zone_names)} configured zones, found {zone_count}."
        )

    corridor_count = int(
        fetch_one(
            conn,
            (
                f"SELECT COUNT(*) FROM {schema}.context_corridors "
                "WHERE corridor_name = %(corridor_name)s;"
            ),
            {"corridor_name": config.context.corridor_name},
        )
        or 0
    )
    if corridor_count != 1:
        raise RuntimeError(
            "Context corridor is missing. "
            f"Expected corridor '{config.context.corridor_name}' in {schema}.context_corridors."
        )

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SET LOCAL work_mem = {};").format(
                sql.Literal("128MB" if resolved_mode == "segment_exact" else "64MB")
            )
        )

    label_sql = build_raw_label_insert_sql(schema, mode=resolved_mode)

    LOGGER.info("Computing trajectory-level query labels using mode=%s.", resolved_mode)
    execute_sql(
        conn,
        label_sql,
        {
            "zone_names": config.context.zone_names,
            "corridor_name": config.context.corridor_name,
            "label_mode": resolved_mode,
            "min_overlap_m": config.queries.min_corridor_overlap_meters,
        },
    )

    label_count = int(
        fetch_one(
            conn,
            f"SELECT COUNT(*) FROM {schema}.trajectory_query_labels WHERE label_mode = %(label_mode)s;",
            {"label_mode": resolved_mode},
        )
        or 0
    )
    zone_positive = int(
        fetch_one(
            conn,
            (
                f"SELECT COUNT(*) FROM {schema}.trajectory_query_labels "
                "WHERE label_mode = %(label_mode)s AND zone_entry = TRUE;"
            ),
            {"label_mode": resolved_mode},
        )
        or 0
    )
    corridor_positive = int(
        fetch_one(
            conn,
            (
                f"SELECT COUNT(*) FROM {schema}.trajectory_query_labels "
                "WHERE label_mode = %(label_mode)s AND corridor_membership = TRUE;"
            ),
            {"label_mode": resolved_mode},
        )
        or 0
    )

    LOGGER.info(
        "Computed %s labels (%s zone-positive, %s corridor-positive).",
        label_count,
        zone_positive,
        corridor_positive,
    )
    return {
        "mode": resolved_mode,
        "labels": label_count,
        "zone_positive": zone_positive,
        "corridor_positive": corridor_positive,
    }
