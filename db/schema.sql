CREATE TABLE IF NOT EXISTS ais_points_cleaned (
    id BIGSERIAL PRIMARY KEY,
    mmsi BIGINT,
    ts TIMESTAMPTZ,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    sog DOUBLE PRECISION,
    cog DOUBLE PRECISION,
    mobile_type TEXT,
    ship_type TEXT,
    geom GEOMETRY(Point, 4326)
);

CREATE INDEX IF NOT EXISTS idx_ais_points_cleaned_ts
    ON ais_points_cleaned (ts);

CREATE INDEX IF NOT EXISTS idx_ais_points_cleaned_geom
    ON ais_points_cleaned
    USING GIST (geom);

CREATE INDEX IF NOT EXISTS idx_ais_points_cleaned_mmsi_ts
    ON ais_points_cleaned (mmsi, ts);

CREATE TABLE IF NOT EXISTS ais_import_progress (
    source_path TEXT PRIMARY KEY,
    source_size BIGINT NOT NULL,
    source_mtime_ns BIGINT NOT NULL,
    delimiter TEXT NOT NULL,
    rows_read BIGINT NOT NULL DEFAULT 0,
    inserted BIGINT NOT NULL DEFAULT 0,
    skipped BIGINT NOT NULL DEFAULT 0,
    skip_missing_ts BIGINT NOT NULL DEFAULT 0,
    skip_missing_core BIGINT NOT NULL DEFAULT 0,
    skip_bad_coords BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
