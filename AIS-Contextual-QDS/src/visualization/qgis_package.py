"""QGIS-ready GeoJSON package helpers."""

from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from ..paths import resolve_project_path


def sanitize_layer_name(value: str) -> str:
    """Return a stable filesystem/layer-safe name."""
    cleaned = re.sub(r"[^A-Za-z0-9_]+", "_", value.strip())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "layer"


def make_feature(geometry: dict[str, Any], properties: dict[str, Any]) -> dict[str, Any]:
    """Build a GeoJSON feature."""
    return {
        "type": "Feature",
        "properties": properties,
        "geometry": geometry,
    }


def write_feature_collection(
    output_path: Path,
    *,
    name: str,
    features: list[dict[str, Any]],
) -> Path:
    """Write a GeoJSON FeatureCollection."""
    resolved = resolve_project_path(output_path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "type": "FeatureCollection",
        "name": name,
        "features": features,
    }
    resolved.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return resolved.resolve()


def _qgis_geometry_type(geojson_geometry_type: str) -> str:
    if "Point" in geojson_geometry_type:
        return "Point"
    if "LineString" in geojson_geometry_type:
        return "Line"
    if "Polygon" in geojson_geometry_type:
        return "Polygon"
    return "Unknown"


def write_qgis_project(
    output_path: Path,
    *,
    project_name: str,
    layers: list[dict[str, str]],
) -> Path:
    """Write a lightweight QGIS project that references exported GeoJSON layers."""
    resolved = resolve_project_path(output_path)
    resolved.parent.mkdir(parents=True, exist_ok=True)

    root = ET.Element(
        "qgis",
        {
            "projectname": project_name,
            "version": "3.34",
        },
    )
    ET.SubElement(root, "title").text = project_name
    project_layers = ET.SubElement(root, "projectlayers")
    layer_tree_group = ET.SubElement(root, "layer-tree-group", {"name": project_name, "checked": "Qt::Checked"})

    for idx, layer in enumerate(layers, start=1):
        layer_name = layer["name"]
        layer_id = f"{sanitize_layer_name(layer_name)}_{idx}"
        ET.SubElement(layer_tree_group, "layer-tree-layer", {"id": layer_id, "name": layer_name, "checked": "Qt::Checked"})

        map_layer = ET.SubElement(
            project_layers,
            "maplayer",
            {
                "type": "vector",
                "geometry": _qgis_geometry_type(layer["geometry_type"]),
                "styleCategories": "AllStyleCategories",
            },
        )
        ET.SubElement(map_layer, "id").text = layer_id
        ET.SubElement(map_layer, "datasource").text = f"./{layer['path']}"
        ET.SubElement(map_layer, "layername").text = layer_name
        ET.SubElement(map_layer, "provider", {"encoding": "UTF-8"}).text = "ogr"

    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    tree.write(resolved, encoding="utf-8", xml_declaration=True)
    return resolved.resolve()
