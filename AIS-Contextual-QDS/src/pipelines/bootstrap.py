"""Database bootstrap step."""

from __future__ import annotations

import logging

from psycopg import Connection

from ..config import AppConfig
from ..db import execute_sql, fetch_one
from ..paths import resolve_project_path

LOGGER = logging.getLogger(__name__)


def ensure_schema_compatibility(conn: Connection, schema: str) -> None:
    """Apply lightweight compatibility migrations for existing project schemas."""
    schema_exists = bool(fetch_one(conn, "SELECT to_regnamespace(%(schema)s) IS NOT NULL;", {"schema": schema}))
    if not schema_exists:
        return

    labels_table = f"{schema}.trajectory_query_labels"
    runs_table = f"{schema}.simplification_runs"
    points_table = f"{schema}.trajectory_points_raw"
    point_features_table = f"{schema}.trajectory_point_context_features"

    labels_exists = bool(fetch_one(conn, "SELECT to_regclass(%(table)s) IS NOT NULL;", {"table": labels_table}))
    runs_exists = bool(fetch_one(conn, "SELECT to_regclass(%(table)s) IS NOT NULL;", {"table": runs_table}))
    points_exists = bool(fetch_one(conn, "SELECT to_regclass(%(table)s) IS NOT NULL;", {"table": points_table}))

    if labels_exists:
        execute_sql(
            conn,
            f"""
ALTER TABLE {labels_table}
    ADD COLUMN IF NOT EXISTS label_mode TEXT;
ALTER TABLE {labels_table}
    ALTER COLUMN label_mode SET DEFAULT 'optimized';
UPDATE {labels_table}
SET label_mode = 'optimized'
WHERE label_mode IS NULL;
ALTER TABLE {labels_table}
    ALTER COLUMN label_mode SET NOT NULL;
ALTER TABLE {labels_table}
    DROP CONSTRAINT IF EXISTS trajectory_query_labels_label_mode_check;
ALTER TABLE {labels_table}
    ADD CONSTRAINT trajectory_query_labels_label_mode_check
    CHECK (label_mode IN ('optimized', 'segment_exact'));

DO $$
DECLARE
    pk_name text;
    pk_def text;
BEGIN
    SELECT conname, pg_get_constraintdef(oid)
    INTO pk_name, pk_def
    FROM pg_constraint
    WHERE conrelid = format('%I.%I', '{schema}', 'trajectory_query_labels')::regclass
      AND contype = 'p';

    IF pk_name IS NULL THEN
        EXECUTE format(
            'ALTER TABLE %I.trajectory_query_labels ADD CONSTRAINT trajectory_query_labels_pkey PRIMARY KEY (trajectory_id, zone_name, corridor_name, label_mode)',
            '{schema}'
        );
    ELSIF pk_def NOT ILIKE '%label_mode%' THEN
        EXECUTE format('ALTER TABLE %I.trajectory_query_labels DROP CONSTRAINT %I', '{schema}', pk_name);
        EXECUTE format(
            'ALTER TABLE %I.trajectory_query_labels ADD CONSTRAINT trajectory_query_labels_pkey PRIMARY KEY (trajectory_id, zone_name, corridor_name, label_mode)',
            '{schema}'
        );
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_{schema}_trajectory_query_labels_mode_zone
    ON {labels_table} (label_mode, zone_name, zone_entry);
CREATE INDEX IF NOT EXISTS idx_{schema}_trajectory_query_labels_mode_corridor
    ON {labels_table} (label_mode, corridor_name, corridor_membership);
            """,
        )

    if points_exists:
        execute_sql(
            conn,
            f"""
CREATE TABLE IF NOT EXISTS {point_features_table} (
    trajectory_id BIGINT NOT NULL,
    point_seq INTEGER NOT NULL,
    inside_zone_name TEXT,
    nearest_zone_name TEXT,
    inside_corridor BOOLEAN NOT NULL DEFAULT FALSE,
    distance_to_nearest_zone_boundary_m DOUBLE PRECISION,
    distance_to_corridor_boundary_m DOUBLE PRECISION,
    zone_transition BOOLEAN NOT NULL DEFAULT FALSE,
    corridor_transition BOOLEAN NOT NULL DEFAULT FALSE,
    local_turn_degrees DOUBLE PRECISION,
    local_deviation_m DOUBLE PRECISION,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trajectory_id, point_seq),
    FOREIGN KEY (trajectory_id, point_seq) REFERENCES {schema}.trajectory_points_raw (trajectory_id, point_seq) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_{schema}_trajectory_point_context_features_zone
    ON {point_features_table} (inside_zone_name, zone_transition);

CREATE INDEX IF NOT EXISTS idx_{schema}_trajectory_point_context_features_corridor
    ON {point_features_table} (inside_corridor, corridor_transition);
""",
        )

    if runs_exists:
        execute_sql(
            conn,
            f"""
ALTER TABLE {runs_table}
    ADD COLUMN IF NOT EXISTS evaluation_mode TEXT;
ALTER TABLE {runs_table}
    ADD COLUMN IF NOT EXISTS truth_label_mode TEXT;
ALTER TABLE {runs_table}
    ADD COLUMN IF NOT EXISTS trajectory_split TEXT;
ALTER TABLE {runs_table}
    ADD COLUMN IF NOT EXISTS subset_name TEXT;
ALTER TABLE {runs_table}
    ADD COLUMN IF NOT EXISTS run_metadata JSONB;

ALTER TABLE {runs_table}
    ALTER COLUMN evaluation_mode SET DEFAULT 'optimized';
ALTER TABLE {runs_table}
    ALTER COLUMN truth_label_mode SET DEFAULT 'optimized';
ALTER TABLE {runs_table}
    ALTER COLUMN trajectory_split SET DEFAULT 'dev';
ALTER TABLE {runs_table}
    ALTER COLUMN subset_name SET DEFAULT '';
ALTER TABLE {runs_table}
    ALTER COLUMN run_metadata SET DEFAULT '{{}}'::jsonb;

UPDATE {runs_table}
SET evaluation_mode = 'optimized'
WHERE evaluation_mode IS NULL;
UPDATE {runs_table}
SET truth_label_mode = 'optimized'
WHERE truth_label_mode IS NULL;
UPDATE {runs_table}
SET trajectory_split = 'dev'
WHERE trajectory_split IS NULL;
UPDATE {runs_table}
SET subset_name = ''
WHERE subset_name IS NULL;
UPDATE {runs_table}
SET run_metadata = '{{}}'::jsonb
WHERE run_metadata IS NULL;

ALTER TABLE {runs_table}
    ALTER COLUMN evaluation_mode SET NOT NULL;
ALTER TABLE {runs_table}
    ALTER COLUMN truth_label_mode SET NOT NULL;
ALTER TABLE {runs_table}
    ALTER COLUMN trajectory_split SET NOT NULL;
ALTER TABLE {runs_table}
    ALTER COLUMN subset_name SET NOT NULL;
ALTER TABLE {runs_table}
    ALTER COLUMN run_metadata SET NOT NULL;

ALTER TABLE {runs_table}
    DROP CONSTRAINT IF EXISTS simplification_runs_evaluation_mode_check;
ALTER TABLE {runs_table}
    DROP CONSTRAINT IF EXISTS simplification_runs_truth_label_mode_check;
ALTER TABLE {runs_table}
    DROP CONSTRAINT IF EXISTS simplification_runs_trajectory_split_check;

ALTER TABLE {runs_table}
    ADD CONSTRAINT simplification_runs_evaluation_mode_check
    CHECK (evaluation_mode IN ('optimized', 'segment_exact'));
ALTER TABLE {runs_table}
    ADD CONSTRAINT simplification_runs_truth_label_mode_check
    CHECK (truth_label_mode IN ('optimized', 'segment_exact'));
ALTER TABLE {runs_table}
    ADD CONSTRAINT simplification_runs_trajectory_split_check
    CHECK (trajectory_split IN ('all', 'dev', 'eval'));

DO $$
DECLARE
    unique_name text;
    unique_def text;
BEGIN
    SELECT conname, pg_get_constraintdef(oid)
    INTO unique_name, unique_def
    FROM pg_constraint
    WHERE conrelid = format('%I.%I', '{schema}', 'simplification_runs')::regclass
      AND contype = 'u'
      AND pg_get_constraintdef(oid) ILIKE '%run_tag%'
      AND pg_get_constraintdef(oid) ILIKE '%method_name%'
      AND pg_get_constraintdef(oid) ILIKE '%budget_ratio%'
    ORDER BY oid
    LIMIT 1;

    IF unique_name IS NULL THEN
        EXECUTE format(
            'ALTER TABLE %I.simplification_runs ADD CONSTRAINT simplification_runs_run_identity_key UNIQUE (run_tag, method_name, budget_ratio, evaluation_mode, truth_label_mode, trajectory_split, subset_name)',
            '{schema}'
        );
    ELSIF unique_def NOT ILIKE '%evaluation_mode%' OR unique_def NOT ILIKE '%truth_label_mode%' OR unique_def NOT ILIKE '%trajectory_split%' OR unique_def NOT ILIKE '%subset_name%' THEN
        EXECUTE format('ALTER TABLE %I.simplification_runs DROP CONSTRAINT %I', '{schema}', unique_name);
        EXECUTE format(
            'ALTER TABLE %I.simplification_runs ADD CONSTRAINT simplification_runs_run_identity_key UNIQUE (run_tag, method_name, budget_ratio, evaluation_mode, truth_label_mode, trajectory_split, subset_name)',
            '{schema}'
        );
    END IF;
END $$;
""",
        )


def run(conn: Connection, config: AppConfig) -> None:
    """Create schema and core tables for the project."""
    schema_sql_path = resolve_project_path(config.paths.schema_sql_file)
    sql_text = schema_sql_path.read_text(encoding="utf-8")
    sql_text = sql_text.replace("__SCHEMA__", config.database.schema)

    LOGGER.info("Bootstrapping schema '%s' using %s", config.database.schema, schema_sql_path)
    execute_sql(conn, sql_text)
    ensure_schema_compatibility(conn, config.database.schema)
    LOGGER.info("Bootstrap completed.")
