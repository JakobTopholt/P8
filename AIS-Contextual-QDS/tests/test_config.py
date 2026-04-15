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
    assert config.baselines.methods == ["uniform", "dp"]
    assert config.baselines.default_split == "dev"


def test_invalid_budget_raises(tmp_path: Path) -> None:
    config_path = PROJECT_ROOT / "configs" / "mvp.example.yaml"
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    raw["queries"]["retained_point_budgets"] = [0.1, 1.2]

    broken_path = tmp_path / "broken.yaml"
    broken_path.write_text(yaml.safe_dump(raw), encoding="utf-8")

    with pytest.raises(ValueError, match="budget"):
        load_config(broken_path)
