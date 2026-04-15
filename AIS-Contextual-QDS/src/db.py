"""Database helper utilities for PostGIS workflows."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

import psycopg
from psycopg import Connection


@contextmanager
def get_connection(database_url: str) -> Iterator[Connection[Any]]:
    """Yield a transactional psycopg connection."""
    conn = psycopg.connect(database_url)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute_sql(conn: Connection[Any], sql_statement: str, params: dict[str, Any] | None = None) -> None:
    """Execute a SQL statement."""
    with conn.cursor() as cur:
        cur.execute(sql_statement, params)


def fetch_one(conn: Connection[Any], sql_statement: str, params: dict[str, Any] | None = None) -> Any:
    """Execute a query and return the first column of the first row."""
    with conn.cursor() as cur:
        cur.execute(sql_statement, params)
        row = cur.fetchone()
    if row is None:
        return None
    return row[0]
