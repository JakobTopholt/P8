# Source Layout

The source tree is organized by responsibility rather than by sprint:

## Entry and Configuration

- `cli.py`
  User-facing command line workflow. This is where command aliases, defaults, and orchestration live.
- `config.py`
  Dataclasses and validation for YAML configs.
- `paths.py`
  Project-relative path helpers.
- `logging_utils.py`
  Logging setup shared by CLI commands.
- `db.py`
  Small psycopg helpers for transactional SQL execution.

## Query Semantics and Runtime Tuning

- `query_semantics.py`
  Shared query labeling and evaluation SQL builders for the `optimized` and `segment_exact` modes.
- `postgres_tuning.py`
  Session-level and system-level PostgreSQL tuning profiles for local runs.

## Pipeline Steps

- `pipelines/bootstrap.py`
  Create the project schema and tables.
- `pipelines/context_loader.py`
  Load study-region, zone, and corridor geometry into PostGIS.
- `pipelines/trajectories.py`
  Build raw trajectories and raw trajectory-point tables from cleaned AIS points.
- `pipelines/labels.py`
  Compute truth labels for zone entry and corridor membership.
- `pipelines/subsets.py`
  Create the deterministic dev/eval subset split.
- `pipelines/features.py`
  Compute reusable per-point context and local-shape features for contextual methods.
- `pipelines/benchmarks.py`
  Neutral benchmark entrypoint for baseline and future query-driven methods.
- `pipelines/baselines.py`
  Implement the current uniform and Douglas-Peucker benchmark methods.
- `pipelines/reports.py`
  Export benchmark summaries and figures.
- `pipelines/visual_inspection.py`
  Produce standalone HTML inspection artifacts.
- `pipelines/qgis_export.py`
  Produce GeoJSON and QGIS inspection packages.
- `pipelines/status.py`
  Report current table counts for the active schema.

## Baseline Methods

- `simplification/uniform.py`
  Uniform point-retention baseline.
- `simplification/douglas_peucker.py`
  Douglas-Peucker baseline with target-point search.

## Evaluation and Output

- `evaluation/metrics.py`
  Precision, recall, and F1 helpers.
- `evaluation/reporting.py`
  Summary-table and SVG figure writers.
- `visualization/inspection.py`
  HTML/SVG inspection rendering.
- `visualization/qgis_package.py`
  GeoJSON and `.qgs` packaging helpers.

## Practical Reading Order

If you are re-entering the project after some time away, this is the shortest path back in:

1. `config.py`
2. `cli.py`
3. `query_semantics.py`
4. `pipelines/trajectories.py`
5. `pipelines/labels.py`
6. `pipelines/features.py`
7. `pipelines/benchmarks.py`
8. `pipelines/baselines.py`

That gives you the current defaults, workflow shape, semantics modes, raw data construction, truth generation, feature layer, and evaluation path in order.
