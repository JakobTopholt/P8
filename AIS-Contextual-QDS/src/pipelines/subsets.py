"""Subset creation for development/evaluation splits."""

from __future__ import annotations

import logging
from typing import Any

from psycopg import Connection

from ..config import AppConfig
from ..db import execute_sql, fetch_one

LOGGER = logging.getLogger(__name__)


def run(conn: Connection[Any], config: AppConfig, *, truncate: bool = True) -> dict[str, int]:
    """Create a deterministic dev/eval subset for fast experiments."""
    schema = config.database.schema
    subset_name = config.subsets.subset_name

    if truncate:
        execute_sql(
            conn,
            f"DELETE FROM {schema}.trajectory_dev_eval_subset WHERE subset_name = %(subset_name)s;",
            {"subset_name": subset_name},
        )

    split_sql = f"""
WITH ranked AS (
    SELECT
        trajectory_id,
        ROW_NUMBER() OVER (
            ORDER BY md5(trajectory_id::text || %(seed)s::text)
        ) AS rn
    FROM {schema}.trajectories_raw
)
INSERT INTO {schema}.trajectory_dev_eval_subset (subset_name, trajectory_id, split)
SELECT
    %(subset_name)s,
    trajectory_id,
    CASE WHEN rn <= %(dev_size)s THEN 'dev' ELSE 'eval' END AS split
FROM ranked
WHERE rn <= %(dev_size)s + %(eval_size)s;
"""

    execute_sql(
        conn,
        split_sql,
        {
            "subset_name": subset_name,
            "seed": config.subsets.random_seed,
            "dev_size": config.subsets.dev_size,
            "eval_size": config.subsets.eval_size,
        },
    )

    dev_count = int(
        fetch_one(
            conn,
            (
                f"SELECT COUNT(*) FROM {schema}.trajectory_dev_eval_subset "
                "WHERE subset_name = %(subset_name)s AND split = 'dev';"
            ),
            {"subset_name": subset_name},
        )
        or 0
    )

    eval_count = int(
        fetch_one(
            conn,
            (
                f"SELECT COUNT(*) FROM {schema}.trajectory_dev_eval_subset "
                "WHERE subset_name = %(subset_name)s AND split = 'eval';"
            ),
            {"subset_name": subset_name},
        )
        or 0
    )

    LOGGER.info("Subset '%s' assigned %s dev and %s eval trajectories.", subset_name, dev_count, eval_count)
    return {"dev": dev_count, "eval": eval_count}
