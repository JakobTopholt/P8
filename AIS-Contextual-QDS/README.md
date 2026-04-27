# AIS-Contextual-QDS

Contextual query-driven simplification for Danish AIS trajectories, currently focused on the first working Great Belt iteration.

## Current Default

The repository now defaults to the 10-day iteration config:

- Config: `configs/iteration1_10days.example.yaml`
- Region: Great Belt / Storebaelt
- Vessel class: cargo
- Time window: `2026-01-01` through `2026-01-10`
- Query families: zone entry and corridor membership
- Default semantics mode: `optimized`

The broader 4-week MVP config still exists as `configs/mvp.example.yaml`, but it is now a reference config rather than the default day-to-day entrypoint.

## Workflow

The project is easiest to understand as four layers:

1. Context and scope
   Load the study region, zones, corridor, and configuration.
2. Dataset preparation
   Build trajectories, compute truth labels, and create the dev/eval subset.
3. Benchmarking
   Run simplification baselines and persist metrics.
4. Inspection
   Export HTML or QGIS artifacts for manual review.

The recommended command flow reflects that structure:

```bash
python -m src.cli bootstrap
python -m src.cli load-context \
  --study-region-file data/context/study_region.geojson \
  --zones-file data/context/zones.geojson \
  --corridor-file data/context/corridor.geojson \
  --corridor-buffer-meters 700
python -m src.cli prepare-data
python -m src.cli benchmark --overwrite
python -m src.cli summarize-baselines
python -m src.cli inspect-html --method uniform --budget 0.10
python -m src.cli inspect-qgis --method uniform --budget 0.10
```

`prepare-data` is the user-facing replacement for the older `sprint1` command. The old name still works as a compatibility alias.

Before heavy runs, you can now sanity-check the environment with:

```bash
python -m src.cli doctor
```

## Semantics Modes

There are two supported query-semantics modes:

- `optimized`
  Default mode. Uses line-level geometry plus point-hit aggregation. This is the mode intended for normal development, laptop runs, and most benchmarking.
- `segment_exact`
  Audit/truth mode. Uses adjacent-segment logic and is intentionally more expensive. This is the mode to run on stronger hardware when you want the closest match to the literal segment-based interpretation.

Examples:

```bash
python -m src.cli compute-labels --mode optimized
python -m src.cli compute-labels --mode segment_exact
python -m src.cli benchmark --evaluation-mode segment_exact --overwrite
python -m src.cli benchmark --evaluation-mode optimized --truth-label-mode optimized --overwrite
```

Truth labels are now stored by `label_mode`, so `optimized` and `segment_exact` labels can coexist in the same schema without overwriting each other.

## Quick Start

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Set `DATABASE_URL`, for example:

```bash
export DATABASE_URL=postgresql://ais:aisdev@localhost:5433/ais
```

3. Confirm PostGIS is running and `public.ais_points_cleaned` exists.

4. Move into the project directory:

```bash
cd AIS-Contextual-QDS
```

5. Optional but recommended on the laptop:

```bash
python -m src.cli tune-postgres --profile laptop_safe
```

That applies the repository’s safe local tuning profile. Some settings, such as `shared_buffers`, may still require a PostgreSQL restart before they take full effect.

## Commands

### Setup

- `bootstrap`
  Create the schema and base tables.
- `load-context`
  Load study-region, zone, and corridor geometry into PostGIS.
- `status`
  Show table counts for the current schema.
- `doctor`
  Run environment and data-readiness checks, including label availability by mode and pending PostgreSQL restart requirements.

### Data Preparation

- `build-trajectories`
  Build raw trajectories from cleaned AIS points.
- `compute-labels`
  Compute trajectory-level truth labels.
- `create-dev-subset`
  Create the deterministic dev/eval split.
- `prepare-data`
  Run bootstrap, trajectory build, labels, subset creation, and status in sequence.

### Evaluation

- `benchmark`
  Run the baseline simplifiers across one or more retained-point budgets.
- `summarize-baselines`
  Export CSV, JSON, Markdown, and SVG summaries from stored benchmark metrics.

### Inspection

- `inspect-html`
  Export a standalone HTML inspection report.
- `inspect-qgis`
  Export GeoJSON layers and a QGIS project.

### Infrastructure

- `tune-postgres`
  Apply the local PostgreSQL tuning profile.

You can always inspect the CLI directly:

```bash
python -m src.cli --help
python -m src.cli prepare-data --help
python -m src.cli benchmark --help
```

## Makefile

The Makefile mirrors the same workflow and now defaults to the 10-day iteration config.

See the available targets with:

```bash
make help
```

Common examples:

```bash
make prepare-data
make benchmark
make doctor
make compute-labels MODE=segment_exact
make benchmark EVALUATION_MODE=segment_exact TRUTH_LABEL_MODE=segment_exact RUN_TAG=truth_audit
make inspect-html RUN_TAG=truth_audit TRUTH_LABEL_MODE=segment_exact
make inspect-qgis RUN_TAG=truth_audit TRUTH_LABEL_MODE=segment_exact
```

If you want to switch to the wider reference config:

```bash
make prepare-data CONFIG=configs/mvp.example.yaml
```

## Project Layout

```text
AIS-Contextual-QDS/
├── Context-And-Planning/      # planning docs and research context
├── configs/                   # iteration/reference configs
├── sql/                       # schema bootstrap and SQL helpers
├── src/
│   ├── cli.py                 # workflow-oriented CLI entrypoint
│   ├── config.py              # config models and validation
│   ├── postgres_tuning.py     # local PostgreSQL tuning profiles
│   ├── query_semantics.py     # optimized vs segment_exact semantics
│   ├── pipelines/             # runnable pipeline steps
│   ├── simplification/        # uniform and Douglas-Peucker baselines
│   ├── evaluation/            # metrics and report export
│   └── visualization/         # HTML and QGIS packaging helpers
├── tests/                     # unit tests
├── results/                   # metrics, figures, logs, inspection exports
└── Makefile                   # developer shortcuts
```

## Current State

What is already in place:

- Config-driven PostGIS schema bootstrap
- Context loading from GeoJSON / Shapefile sources
- Raw trajectory construction from cleaned AIS points
- Truth labeling for zone entry and corridor membership
- Deterministic dev/eval subset creation
- Uniform and Douglas-Peucker baseline benchmarking
- Summary export to CSV, JSON, Markdown, and SVG
- HTML and QGIS inspection exports
- `optimized` and `segment_exact` semantics modes

What is not yet in place:

- The contextual scorer beyond the current baseline stage
- Boundary-distance and transition features from later planning milestones
- Final thesis-grade audit comparisons between `optimized` and `segment_exact`

## Notes

- Corridor overlap checks use metric distance through `geography` casts.
- Context files are expected in EPSG:4326.
- The current default path is intentionally narrow and reproducible so iteration stays fast.
- Full `segment_exact` label generation can be slow on the older laptop and is best treated as a stronger-hardware audit step.
