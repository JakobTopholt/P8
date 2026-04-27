"""Context loader parsing and matching tests."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.pipelines.context_loader import FeatureRecord, _find_named_feature, read_features


def test_read_geojson_feature_collection(tmp_path: Path) -> None:
    geojson_path = tmp_path / "zones.geojson"
    payload = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"name": "zone_port_approach"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[11.0, 55.0], [11.1, 55.0], [11.1, 55.1], [11.0, 55.1], [11.0, 55.0]]],
                },
            },
            {
                "type": "Feature",
                "properties": {"name": "zone_anchor_or_waiting_area"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[11.2, 55.0], [11.3, 55.0], [11.3, 55.1], [11.2, 55.1], [11.2, 55.0]]],
                },
            },
        ],
    }
    geojson_path.write_text(json.dumps(payload), encoding="utf-8")

    records = read_features(geojson_path)

    assert len(records) == 2
    assert records[0].geometry_type == "Polygon"
    assert records[0].properties["name"] == "zone_port_approach"


def test_read_geojson_geometry_object(tmp_path: Path) -> None:
    geometry_path = tmp_path / "region.geojson"
    geometry_path.write_text(
        json.dumps(
            {
                "type": "Polygon",
                "coordinates": [[[11.0, 55.0], [11.5, 55.0], [11.5, 55.5], [11.0, 55.5], [11.0, 55.0]]],
            }
        ),
        encoding="utf-8",
    )

    records = read_features(geometry_path)

    assert len(records) == 1
    assert records[0].geometry_type == "Polygon"


def test_find_named_feature_from_multiple() -> None:
    features = [
        FeatureRecord(geometry={"type": "Polygon", "coordinates": []}, properties={"name": "A"}),
        FeatureRecord(geometry={"type": "Polygon", "coordinates": []}, properties={"name": "B"}),
    ]

    feature = _find_named_feature(features, expected_name="B", name_property="name")
    assert feature.properties["name"] == "B"


def test_find_named_feature_rejects_single_wrong_named_feature() -> None:
    features = [
        FeatureRecord(geometry={"type": "Polygon", "coordinates": []}, properties={"name": "A"}),
    ]

    with pytest.raises(ValueError, match="Single feature"):
        _find_named_feature(features, expected_name="B", name_property="name")


def test_find_named_feature_accepts_single_unnamed_feature() -> None:
    features = [
        FeatureRecord(geometry={"type": "Polygon", "coordinates": []}, properties={}),
    ]

    feature = _find_named_feature(features, expected_name="B", name_property="name")
    assert feature is features[0]


def test_find_named_feature_errors_without_unique_match() -> None:
    features = [
        FeatureRecord(geometry={"type": "Polygon", "coordinates": []}, properties={"name": "A"}),
        FeatureRecord(geometry={"type": "Polygon", "coordinates": []}, properties={"name": "C"}),
    ]

    with pytest.raises(ValueError, match="Could not uniquely match"):
        _find_named_feature(features, expected_name="B", name_property="name")
