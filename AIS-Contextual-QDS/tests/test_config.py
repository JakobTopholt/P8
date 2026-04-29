"""Configuration loading tests."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_config


def test_load_default_config() -> None:
    config = load_config(PROJECT_ROOT / "configs" / "mvp.example.yaml")

    assert config.database.schema == "ais_qds"
    assert config.context.zone_names == [
        "zone_port_approach",
        "zone_anchor_or_waiting_area",
        "zone_narrow_passage_control",
    ]
    assert config.queries.retained_point_budgets == [0.10, 0.20, 0.30, 0.40, 0.50]
    assert config.subsets.dev_size == 300
    assert config.baselines.methods == ["uniform", "douglas_peucker", "query_witness"]
    assert config.baselines.default_split == "dev"
    assert config.performance.label_mode == "optimized"
    assert config.performance.evaluation_mode == "optimized"
    assert config.performance.session_profile == "laptop_safe"


def test_load_iteration1_10day_config() -> None:
    config = load_config(PROJECT_ROOT / "configs" / "iteration1_10days.example.yaml")

    assert config.project.name == "geofence-aware-query-driven-simplification-iteration1-10days"
    assert config.scope.window_start == "2026-01-01T00:00:00+00:00"
    assert config.scope.window_end == "2026-01-11T00:00:00+00:00"
    assert config.subsets.subset_name == "great_belt_iter1_10days"
    assert config.queries.retained_point_budgets == [0.10, 0.20, 0.30, 0.40, 0.50]
    assert config.performance.label_mode == "optimized"
    assert config.performance.evaluation_mode == "optimized"
    assert config.performance.session_profile == "laptop_safe"


def test_invalid_budget_raises(tmp_path: Path) -> None:
    config_path = PROJECT_ROOT / "configs" / "mvp.example.yaml"
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    raw["queries"]["retained_point_budgets"] = [0.1, 1.2]

    broken_path = tmp_path / "broken.yaml"
    broken_path.write_text(yaml.safe_dump(raw), encoding="utf-8")

    with pytest.raises(ValueError, match="budget"):
        load_config(broken_path)


def test_invalid_label_mode_raises(tmp_path: Path) -> None:
    config_path = PROJECT_ROOT / "configs" / "mvp.example.yaml"
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    raw["performance"]["label_mode"] = "approx"

    broken_path = tmp_path / "broken_label_mode.yaml"
    broken_path.write_text(yaml.safe_dump(raw), encoding="utf-8")

    with pytest.raises(ValueError, match="query mode"):
        load_config(broken_path)


def test_legacy_method_aliases_are_normalized(tmp_path: Path) -> None:
    config_path = PROJECT_ROOT / "configs" / "mvp.example.yaml"
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    raw["baselines"]["methods"] = ["uniform", "dp", "b3", "query_witness"]

    legacy_path = tmp_path / "legacy_methods.yaml"
    legacy_path.write_text(yaml.safe_dump(raw), encoding="utf-8")

    config = load_config(legacy_path)

    assert config.baselines.methods == ["uniform", "douglas_peucker", "query_witness"]


@pytest.mark.parametrize("legacy_mode", ["exact", "fast"])
def test_legacy_modes_are_rejected(tmp_path: Path, legacy_mode: str) -> None:
    config_path = PROJECT_ROOT / "configs" / "mvp.example.yaml"
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    raw["performance"]["evaluation_mode"] = legacy_mode

    broken_path = tmp_path / f"broken_legacy_mode_{legacy_mode}.yaml"
    broken_path.write_text(yaml.safe_dump(raw), encoding="utf-8")

    with pytest.raises(ValueError, match="query mode"):
        load_config(broken_path)


def test_invalid_session_profile_raises(tmp_path: Path) -> None:
    config_path = PROJECT_ROOT / "configs" / "mvp.example.yaml"
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    raw["performance"]["session_profile"] = "desktop_ultra"

    broken_path = tmp_path / "broken_session_profile.yaml"
    broken_path.write_text(yaml.safe_dump(raw), encoding="utf-8")

    with pytest.raises(ValueError, match="session profile"):
        load_config(broken_path)
