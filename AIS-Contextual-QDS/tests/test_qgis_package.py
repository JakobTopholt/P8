"""QGIS package writer tests."""

from __future__ import annotations

import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.visualization.qgis_package import make_feature, sanitize_layer_name, write_feature_collection, write_qgis_project


def test_qgis_package_writes_geojson_and_project(tmp_path: Path) -> None:
    feature = make_feature(
        {"type": "LineString", "coordinates": [[10.0, 55.0], [11.0, 56.0]]},
        {"trajectory_id": 1, "query_mismatch_count": 0},
    )

    geojson_path = write_feature_collection(
        tmp_path / "raw_trajectories.geojson",
        name="raw_trajectories",
        features=[feature],
    )
    project_path = write_qgis_project(
        tmp_path / "ais_qds_inspection.qgs",
        project_name="AIS-QDS Inspection",
        layers=[
            {
                "name": "raw_trajectories",
                "path": "raw_trajectories.geojson",
                "geometry_type": "LineString",
            }
        ],
    )

    payload = json.loads(geojson_path.read_text(encoding="utf-8"))
    assert payload["type"] == "FeatureCollection"
    assert payload["features"][0]["properties"]["trajectory_id"] == 1

    tree = ET.parse(project_path)
    root = tree.getroot()
    assert root.tag == "qgis"
    assert root.find(".//layername").text == "raw_trajectories"
    assert root.find(".//provider").text == "ogr"
    assert root.find(".//datasource").text == "./raw_trajectories.geojson"


def test_sanitize_layer_name() -> None:
    assert sanitize_layer_name("uniform 0.10 / raw") == "uniform_0_10_raw"
