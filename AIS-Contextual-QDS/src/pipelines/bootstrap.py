"""Database bootstrap step."""

from __future__ import annotations

import logging

from psycopg import Connection

from ..config import AppConfig
from ..db import execute_sql
from ..paths import resolve_project_path

LOGGER = logging.getLogger(__name__)


def run(conn: Connection, config: AppConfig) -> None:
    """Create schema and core tables for the project."""
    schema_sql_path = resolve_project_path(config.paths.schema_sql_file)
    sql_text = schema_sql_path.read_text(encoding="utf-8")
    sql_text = sql_text.replace("__SCHEMA__", config.database.schema)

    LOGGER.info("Bootstrapping schema '%s' using %s", config.database.schema, schema_sql_path)
    execute_sql(conn, sql_text)
    LOGGER.info("Bootstrap completed.")
