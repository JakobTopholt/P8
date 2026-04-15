CREATE SCHEMA IF NOT EXISTS __SCHEMA__;

CREATE TABLE IF NOT EXISTS __SCHEMA__.study_region (
    region_name TEXT PRIMARY KEY,
    geom geometry(Polygon, 4326) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx___SCHEMA___study_region_geom
    ON __SCHEMA__.study_region
    USING GIST (geom);

CREATE TABLE IF NOT EXISTS __SCHEMA__.context_zones (
    zone_name TEXT PRIMARY KEY,
    geom geometry(Polygon, 4326) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx___SCHEMA___context_zones_geom
    ON __SCHEMA__.context_zones
    USING GIST (geom);

CREATE TABLE IF NOT EXISTS __SCHEMA__.context_corridors (
    corridor_name TEXT PRIMARY KEY,
    geom geometry(Polygon, 4326) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx___SCHEMA___context_corridors_geom
    ON __SCHEMA__.context_corridors
    USING GIST (geom);

CREATE TABLE IF NOT EXISTS __SCHEMA__.trajectories_raw (
    trajectory_id BIGINT PRIMARY KEY,
    mmsi BIGINT NOT NULL,
    start_ts TIMESTAMPTZ NOT NULL,
    end_ts TIMESTAMPTZ NOT NULL,
    n_points INTEGER NOT NULL,
    geom geometry(LineString, 4326) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx___SCHEMA___trajectories_raw_mmsi
    ON __SCHEMA__.trajectories_raw (mmsi);

CREATE INDEX IF NOT EXISTS idx___SCHEMA___trajectories_raw_geom
    ON __SCHEMA__.trajectories_raw
    USING GIST (geom);

CREATE TABLE IF NOT EXISTS __SCHEMA__.trajectory_points_raw (
    point_id BIGSERIAL PRIMARY KEY,
    trajectory_id BIGINT NOT NULL,
    point_seq INTEGER NOT NULL,
    mmsi BIGINT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    sog DOUBLE PRECISION,
    cog DOUBLE PRECISION,
    nav_status TEXT,
    geom geometry(Point, 4326) NOT NULL,
    source_point_id BIGINT,
    UNIQUE (trajectory_id, point_seq)
);

CREATE INDEX IF NOT EXISTS idx___SCHEMA___trajectory_points_raw_traj_seq
    ON __SCHEMA__.trajectory_points_raw (trajectory_id, point_seq);

CREATE INDEX IF NOT EXISTS idx___SCHEMA___trajectory_points_raw_mmsi_ts
    ON __SCHEMA__.trajectory_points_raw (mmsi, ts);

CREATE INDEX IF NOT EXISTS idx___SCHEMA___trajectory_points_raw_geom
    ON __SCHEMA__.trajectory_points_raw
    USING GIST (geom);

CREATE TABLE IF NOT EXISTS __SCHEMA__.trajectory_query_labels (
    trajectory_id BIGINT NOT NULL,
    zone_name TEXT NOT NULL,
    corridor_name TEXT NOT NULL,
    zone_entry BOOLEAN NOT NULL,
    corridor_membership BOOLEAN NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trajectory_id, zone_name, corridor_name),
    FOREIGN KEY (trajectory_id) REFERENCES __SCHEMA__.trajectories_raw (trajectory_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx___SCHEMA___trajectory_query_labels_zone
    ON __SCHEMA__.trajectory_query_labels (zone_name, zone_entry);

CREATE INDEX IF NOT EXISTS idx___SCHEMA___trajectory_query_labels_corridor
    ON __SCHEMA__.trajectory_query_labels (corridor_name, corridor_membership);

CREATE TABLE IF NOT EXISTS __SCHEMA__.trajectory_dev_eval_subset (
    subset_name TEXT NOT NULL,
    trajectory_id BIGINT NOT NULL,
    split TEXT NOT NULL,
    assigned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (subset_name, trajectory_id),
    FOREIGN KEY (trajectory_id) REFERENCES __SCHEMA__.trajectories_raw (trajectory_id) ON DELETE CASCADE,
    CHECK (split IN ('dev', 'eval'))
);

CREATE INDEX IF NOT EXISTS idx___SCHEMA___trajectory_dev_eval_subset_split
    ON __SCHEMA__.trajectory_dev_eval_subset (subset_name, split);

CREATE TABLE IF NOT EXISTS __SCHEMA__.simplification_runs (
    run_id BIGSERIAL PRIMARY KEY,
    run_tag TEXT NOT NULL,
    method_name TEXT NOT NULL,
    budget_ratio DOUBLE PRECISION NOT NULL,
    config_path TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_tag, method_name, budget_ratio)
);

CREATE TABLE IF NOT EXISTS __SCHEMA__.trajectories_simplified_points (
    run_id BIGINT NOT NULL,
    trajectory_id BIGINT NOT NULL,
    point_seq INTEGER NOT NULL,
    source_point_seq INTEGER,
    mmsi BIGINT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    geom geometry(Point, 4326) NOT NULL,
    PRIMARY KEY (run_id, trajectory_id, point_seq),
    FOREIGN KEY (run_id) REFERENCES __SCHEMA__.simplification_runs (run_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx___SCHEMA___trajectories_simplified_points_geom
    ON __SCHEMA__.trajectories_simplified_points
    USING GIST (geom);

CREATE TABLE IF NOT EXISTS __SCHEMA__.benchmark_metrics (
    run_id BIGINT NOT NULL,
    metric_key TEXT NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, metric_key),
    FOREIGN KEY (run_id) REFERENCES __SCHEMA__.simplification_runs (run_id) ON DELETE CASCADE
);
