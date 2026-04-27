"""PostgreSQL tuning profile tests."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.postgres_tuning import (
    SESSION_PROFILES,
    SYSTEM_PROFILES,
    normalize_session_profile,
    normalize_system_profile,
)


def test_normalize_session_profile_accepts_known_values() -> None:
    assert normalize_session_profile(None) == "default"
    assert normalize_session_profile("LAPTOP_SAFE") == "laptop_safe"


def test_normalize_system_profile_accepts_known_values() -> None:
    assert normalize_system_profile("laptop_safe") == "laptop_safe"


def test_invalid_profiles_raise() -> None:
    with pytest.raises(ValueError, match="session profile"):
        normalize_session_profile("workstation")

    with pytest.raises(ValueError, match="system profile"):
        normalize_system_profile("workstation")


def test_laptop_safe_profiles_include_expected_settings() -> None:
    assert SESSION_PROFILES["laptop_safe"]["jit"] == "off"
    assert SESSION_PROFILES["laptop_safe"]["work_mem"] == "16MB"
    assert SYSTEM_PROFILES["laptop_safe"]["shared_buffers"] == "512MB"
    assert SYSTEM_PROFILES["laptop_safe"]["max_parallel_workers_per_gather"] == "1"
    assert SYSTEM_PROFILES["laptop_safe"]["max_parallel_workers"] == "2"
