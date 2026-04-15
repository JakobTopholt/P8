-- Context seed template for MVP startup.
-- Replace placeholder WKT geometries before executing.

-- Recommended: run after `python -m src.cli bootstrap`.

BEGIN;

-- 1) Study region (exactly one active polygon)
INSERT INTO ais_qds.study_region (region_name, geom, is_active)
VALUES (
    'great_belt_study_area',
    ST_GeomFromText('POLYGON((11.0 54.6, 11.0 56.0, 12.4 56.0, 12.4 54.6, 11.0 54.6))', 4326),
    TRUE
)
ON CONFLICT (region_name)
DO UPDATE
SET geom = EXCLUDED.geom,
    is_active = EXCLUDED.is_active;

UPDATE ais_qds.study_region
SET is_active = (region_name = 'great_belt_study_area');

-- 2) Three geofence zones
INSERT INTO ais_qds.context_zones (zone_name, geom)
VALUES
(
    'zone_port_approach',
    ST_GeomFromText('POLYGON((11.6 55.0, 11.6 55.2, 11.9 55.2, 11.9 55.0, 11.6 55.0))', 4326)
),
(
    'zone_anchor_or_waiting_area',
    ST_GeomFromText('POLYGON((11.9 55.2, 11.9 55.4, 12.15 55.4, 12.15 55.2, 11.9 55.2))', 4326)
),
(
    'zone_narrow_passage_control',
    ST_GeomFromText('POLYGON((11.75 55.4, 11.75 55.55, 12.0 55.55, 12.0 55.4, 11.75 55.4))', 4326)
)
ON CONFLICT (zone_name)
DO UPDATE
SET geom = EXCLUDED.geom;

-- 3) One corridor polygon (replace with buffered fairway geometry)
INSERT INTO ais_qds.context_corridors (corridor_name, geom)
VALUES (
    'corridor_main_transit_lane',
    ST_GeomFromText('POLYGON((11.55 54.9, 11.55 55.7, 12.05 55.7, 12.05 54.9, 11.55 54.9))', 4326)
)
ON CONFLICT (corridor_name)
DO UPDATE
SET geom = EXCLUDED.geom;

COMMIT;
