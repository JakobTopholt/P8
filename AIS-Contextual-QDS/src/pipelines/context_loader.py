"""Load study region, zones, and corridor geometries into PostGIS."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from psycopg import Connection

from ..config import AppConfig

LOGGER = logging.getLogger(__name__)

_POLYGON_TYPES = {"Polygon", "MultiPolygon"}
_LINE_TYPES = {"LineString", "MultiLineString"}


@dataclass(frozen=True)
class FeatureRecord:
    """One spatial feature with properties."""

    geometry: dict[str, Any]
    properties: dict[str, Any]

    @property
    def geometry_type(self) -> str:
        return str(self.geometry.get("type", ""))

    @property
    def geometry_json(self) -> str:
        return json.dumps(self.geometry, separators=(",", ":"))


def _read_geojson_features(path: Path) -> list[FeatureRecord]:
    with path.open("r", encoding="utf-8") as file_obj:
        payload = json.load(file_obj)

    if not isinstance(payload, dict):
        raise ValueError(f"GeoJSON payload must be an object: {path}")

    payload_type = str(payload.get("type", ""))
    raw_features: list[dict[str, Any]]
    if payload_type == "FeatureCollection":
        features = payload.get("features")
        if not isinstance(features, list):
            raise ValueError(f"GeoJSON FeatureCollection has invalid 'features': {path}")
        raw_features = [feature for feature in features if isinstance(feature, dict)]
    elif payload_type == "Feature":
        raw_features = [payload]
    else:
        raw_features = [{"type": "Feature", "geometry": payload, "properties": {}}]

    records: list[FeatureRecord] = []
    for feature in raw_features:
        geometry = feature.get("geometry")
        if not isinstance(geometry, dict):
            continue

        geometry_type = str(geometry.get("type", ""))
        if not geometry_type or geometry_type.lower() == "null":
            continue

        properties = feature.get("properties")
        if not isinstance(properties, dict):
            properties = {}

        records.append(FeatureRecord(geometry=geometry, properties=properties))

    return records


def _read_shapefile_features(path: Path) -> list[FeatureRecord]:
    try:
        import shapefile  # type: ignore
    except Exception as exc:  # pragma: no cover - exercised in runtime, not tests
        raise RuntimeError(
            "Shapefile support requires the 'pyshp' package. Install with: pip install pyshp"
        ) from exc

    reader = shapefile.Reader(str(path))
    field_names = [field[0] for field in reader.fields[1:]]

    records: list[FeatureRecord] = []
    for shape_record in reader.iterShapeRecords():
        geometry = shape_record.shape.__geo_interface__
        geometry_type = str(geometry.get("type", ""))
        if not geometry_type or geometry_type.lower() == "null":
            continue

        attributes = dict(zip(field_names, list(shape_record.record), strict=False))
        records.append(FeatureRecord(geometry=geometry, properties=attributes))

    return records


def read_features(path: Path) -> list[FeatureRecord]:
    """Read spatial features from GeoJSON or Shapefile."""
    suffix = path.suffix.lower()
    if suffix in {".geojson", ".json"}:
        records = _read_geojson_features(path)
    elif suffix == ".shp":
        records = _read_shapefile_features(path)
    else:
        raise ValueError(f"Unsupported context file type: {path}")

    if not records:
        raise ValueError(f"No valid features found in {path}")
    return records


def _find_named_feature(
    features: list[FeatureRecord],
    *,
    expected_name: str,
    name_property: str | None,
) -> FeatureRecord:
    if len(features) == 1:
        feature = features[0]
        if name_property:
            actual_name = str(feature.properties.get(name_property, "")).strip()
            if actual_name and actual_name != expected_name:
                raise ValueError(
                    f"Single feature has {name_property!r}={actual_name!r}, expected {expected_name!r}."
                )
        return feature

    if not name_property:
        raise ValueError(
            f"Found {len(features)} features but no name property configured for expected {expected_name!r}."
        )

    matches = [
        feature
        for feature in features
        if str(feature.properties.get(name_property, "")).strip() == expected_name
    ]
    if len(matches) == 1:
        return matches[0]

    available = sorted(
        {
            str(feature.properties.get(name_property, "")).strip()
            for feature in features
            if str(feature.properties.get(name_property, "")).strip()
        }
    )
    raise ValueError(
        f"Could not uniquely match feature for {expected_name!r} via property {name_property!r}. "
        f"Available names: {available}"
    )


def _largest_polygon_expression(geojson_param: str) -> str:
    return f"""
(
    WITH raw AS (
        SELECT ST_SetSRID(ST_GeomFromGeoJSON({geojson_param}), 4326) AS g
    ),
    polygons AS (
        SELECT (ST_Dump(ST_CollectionExtract(ST_MakeValid(g), 3))).geom AS poly
        FROM raw
    )
    SELECT poly
    FROM polygons
    ORDER BY ST_Area(poly::geography) DESC
    LIMIT 1
)
"""


def _largest_buffered_polygon_expression(geojson_param: str, buffer_param: str) -> str:
    return f"""
(
    WITH raw AS (
        SELECT ST_SetSRID(ST_GeomFromGeoJSON({geojson_param}), 4326) AS g
    ),
    buffered AS (
        SELECT ST_Buffer(g::geography, {buffer_param})::geometry AS g
        FROM raw
    ),
    polygons AS (
        SELECT (ST_Dump(ST_CollectionExtract(ST_MakeValid(g), 3))).geom AS poly
        FROM buffered
    )
    SELECT poly
    FROM polygons
    ORDER BY ST_Area(poly::geography) DESC
    LIMIT 1
)
"""


def _upsert_study_region(conn: Connection[Any], schema: str, region_name: str, feature: FeatureRecord) -> None:
    if feature.geometry_type not in _POLYGON_TYPES:
        raise ValueError(
            f"Study region geometry must be Polygon or MultiPolygon, got {feature.geometry_type!r}."
        )

    geom_expr = _largest_polygon_expression("%(geom_json)s")
    sql = f"""
INSERT INTO {schema}.study_region (region_name, geom, is_active)
VALUES (%(region_name)s, {geom_expr}, TRUE)
ON CONFLICT (region_name)
DO UPDATE SET geom = EXCLUDED.geom, is_active = TRUE;
"""
    with conn.cursor() as cur:
        cur.execute(sql, {"region_name": region_name, "geom_json": feature.geometry_json})
        cur.execute(
            f"UPDATE {schema}.study_region SET is_active = FALSE WHERE region_name <> %(region_name)s;",
            {"region_name": region_name},
        )


def _upsert_zone(conn: Connection[Any], schema: str, zone_name: str, feature: FeatureRecord) -> None:
    if feature.geometry_type not in _POLYGON_TYPES:
        raise ValueError(f"Zone {zone_name!r} must be Polygon or MultiPolygon, got {feature.geometry_type!r}.")

    geom_expr = _largest_polygon_expression("%(geom_json)s")
    sql = f"""
INSERT INTO {schema}.context_zones (zone_name, geom)
VALUES (%(zone_name)s, {geom_expr})
ON CONFLICT (zone_name)
DO UPDATE SET geom = EXCLUDED.geom;
"""
    with conn.cursor() as cur:
        cur.execute(sql, {"zone_name": zone_name, "geom_json": feature.geometry_json})


def _upsert_corridor(
    conn: Connection[Any],
    schema: str,
    corridor_name: str,
    feature: FeatureRecord,
    *,
    corridor_buffer_meters: float | None,
) -> None:
    geometry_type = feature.geometry_type

    if geometry_type in _POLYGON_TYPES:
        geom_expr = _largest_polygon_expression("%(geom_json)s")
        params: dict[str, Any] = {"corridor_name": corridor_name, "geom_json": feature.geometry_json}
    elif geometry_type in _LINE_TYPES:
        if corridor_buffer_meters is None or corridor_buffer_meters <= 0:
            raise ValueError(
                "Corridor geometry is line-based. Provide a positive --corridor-buffer-meters value."
            )
        geom_expr = _largest_buffered_polygon_expression("%(geom_json)s", "%(buffer_m)s")
        params = {
            "corridor_name": corridor_name,
            "geom_json": feature.geometry_json,
            "buffer_m": corridor_buffer_meters,
        }
    else:
        raise ValueError(
            f"Corridor geometry must be Polygon/MultiPolygon or LineString/MultiLineString, got {geometry_type!r}."
        )

    sql = f"""
INSERT INTO {schema}.context_corridors (corridor_name, geom)
VALUES (%(corridor_name)s, {geom_expr})
ON CONFLICT (corridor_name)
DO UPDATE SET geom = EXCLUDED.geom;
"""
    with conn.cursor() as cur:
        cur.execute(sql, params)


def run(
    conn: Connection[Any],
    config: AppConfig,
    *,
    study_region_path: Path,
    zones_path: Path,
    corridor_path: Path,
    zones_name_property: str,
    corridor_name_property: str,
    append: bool,
    corridor_buffer_meters: float | None,
) -> dict[str, Any]:
    """Load context layers from file inputs into PostGIS tables."""
    schema = config.database.schema

    study_features = read_features(study_region_path)
    zone_features = read_features(zones_path)
    corridor_features = read_features(corridor_path)

    study_feature = _find_named_feature(
        study_features,
        expected_name=config.scope.region_name,
        name_property="name",
    )

    zones_by_name: dict[str, FeatureRecord] = {}
    for zone_name in config.context.zone_names:
        zones_by_name[zone_name] = _find_named_feature(
            zone_features,
            expected_name=zone_name,
            name_property=zones_name_property,
        )

    corridor_feature = _find_named_feature(
        corridor_features,
        expected_name=config.context.corridor_name,
        name_property=corridor_name_property,
    )

    with conn.cursor() as cur:
        if not append:
            cur.execute(f"TRUNCATE TABLE {schema}.context_zones, {schema}.context_corridors, {schema}.study_region;")

    _upsert_study_region(conn, schema, config.scope.region_name, study_feature)
    for zone_name, feature in zones_by_name.items():
        _upsert_zone(conn, schema, zone_name, feature)
    _upsert_corridor(
        conn,
        schema,
        config.context.corridor_name,
        corridor_feature,
        corridor_buffer_meters=corridor_buffer_meters,
    )

    LOGGER.info(
        "Loaded context: region=%s zones=%s corridor=%s",
        config.scope.region_name,
        config.context.zone_names,
        config.context.corridor_name,
    )

    return {
        "study_region": config.scope.region_name,
        "zones": list(config.context.zone_names),
        "corridor": config.context.corridor_name,
        "study_region_file": str(study_region_path),
        "zones_file": str(zones_path),
        "corridor_file": str(corridor_path),
        "append": append,
        "corridor_buffer_meters": corridor_buffer_meters,
    }
