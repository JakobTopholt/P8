"""Session and system tuning helpers for local PostgreSQL workloads."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from psycopg import Connection, sql

SESSION_PROFILES: dict[str, dict[str, str]] = {
    "default": {},
    "laptop_safe": {
        "jit": "off",
        "work_mem": "16MB",
        "maintenance_work_mem": "256MB",
        "temp_buffers": "16MB",
        "effective_cache_size": "2GB",
        "random_page_cost": "1.25",
        "effective_io_concurrency": "32",
        "max_parallel_workers_per_gather": "1",
    },
}

SYSTEM_PROFILES: dict[str, dict[str, str]] = {
    "laptop_safe": {
        "shared_buffers": "512MB",
        "work_mem": "16MB",
        "maintenance_work_mem": "256MB",
        "temp_buffers": "16MB",
        "effective_cache_size": "2GB",
        "random_page_cost": "1.25",
        "effective_io_concurrency": "32",
        "jit": "off",
        "max_parallel_workers_per_gather": "1",
        "max_parallel_workers": "2",
    },
}


@dataclass(frozen=True)
class AppliedSetting:
    """One PostgreSQL setting after attempting to apply a tuning profile."""

    name: str
    previous_value: str
    requested_value: str
    context: str
    pending_restart: bool


def normalize_session_profile(profile_name: str | None) -> str:
    """Normalize configured session profile."""
    raw_value = "default" if profile_name is None else profile_name
    normalized = raw_value.strip().lower()
    if normalized not in SESSION_PROFILES:
        raise ValueError(f"session profile must be one of {sorted(SESSION_PROFILES)}, got {raw_value!r}")
    return normalized


def normalize_system_profile(profile_name: str) -> str:
    """Normalize tuning profile used for ALTER SYSTEM updates."""
    normalized = profile_name.strip().lower()
    if normalized not in SYSTEM_PROFILES:
        raise ValueError(f"system profile must be one of {sorted(SYSTEM_PROFILES)}, got {profile_name!r}")
    return normalized


def apply_session_profile(conn: Connection[Any], profile_name: str | None) -> str:
    """Apply a session-scoped tuning profile to the current connection."""
    normalized = normalize_session_profile(profile_name)
    settings = SESSION_PROFILES[normalized]
    if not settings:
        return normalized

    with conn.cursor() as cur:
        for setting_name, setting_value in settings.items():
            cur.execute(
                sql.SQL("SET SESSION {} = {};").format(
                    sql.SQL(setting_name),
                    sql.Literal(setting_value),
                )
            )
    return normalized


def apply_system_profile(
    conn: Connection[Any],
    profile_name: str,
    *,
    dry_run: bool = False,
) -> dict[str, object]:
    """Apply a system-wide ALTER SYSTEM tuning profile and reload config."""
    normalized = normalize_system_profile(profile_name)
    requested = SYSTEM_PROFILES[normalized]

    applied: list[AppliedSetting] = []
    previous_autocommit = conn.autocommit
    reload_ok = False
    pending_restart_names: list[str] = []
    try:
        if not previous_autocommit:
            conn.commit()
        conn.autocommit = True

        with conn.cursor() as cur:
            for setting_name, setting_value in requested.items():
                cur.execute(
                    "SELECT setting, context, pending_restart FROM pg_settings WHERE name = %s;",
                    (setting_name,),
                )
                row = cur.fetchone()
                if row is None:
                    raise RuntimeError(f"PostgreSQL setting not found: {setting_name}")

                previous_value, context, pending_restart = row
                if not dry_run:
                    cur.execute(
                        sql.SQL("ALTER SYSTEM SET {} = {};").format(
                            sql.SQL(setting_name),
                            sql.Literal(setting_value),
                        )
                    )
                applied.append(
                    AppliedSetting(
                        name=setting_name,
                        previous_value=str(previous_value),
                        requested_value=setting_value,
                        context=str(context),
                        pending_restart=bool(pending_restart),
                    )
                )

            if not dry_run:
                cur.execute("SELECT pg_reload_conf();")
                reload_ok = bool(cur.fetchone()[0])

            if not dry_run:
                cur.execute(
                    "SELECT name FROM pg_settings WHERE pending_restart = TRUE AND name = ANY(%s) ORDER BY name;",
                    (list(requested.keys()),),
                )
                pending_restart_names = [str(row[0]) for row in cur.fetchall()]

            cur.execute("SHOW config_file;")
            config_file = str(cur.fetchone()[0])
    finally:
        conn.autocommit = previous_autocommit

    return {
        "profile": normalized,
        "dry_run": dry_run,
        "reload_ok": reload_ok,
        "config_file": config_file,
        "settings": [
            {
                "name": item.name,
                "previous_value": item.previous_value,
                "requested_value": item.requested_value,
                "context": item.context,
                "pending_restart": item.name in pending_restart_names if not dry_run else item.context == "postmaster",
            }
            for item in applied
        ],
        "pending_restart": pending_restart_names if not dry_run else sorted(
            item.name for item in applied if item.context == "postmaster"
        ),
    }
