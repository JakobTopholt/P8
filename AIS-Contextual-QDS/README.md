# AIS-Contextual-QDS

Geofence-aware query-driven simplification for Danish AIS, scoped to the MVP you locked in planning docs.

## Locked MVP (from `Context-And-Planning`)

- Region: Great Belt / Storebaelt study area
- Vessel class: cargo only
- Time span: fixed 4-week window (config-driven)
- Query families: zone entry + corridor membership (trajectory-level yes/no)
- Context layers: land mask, 3 zones, 1 corridor polygon
- Budgets: retained-point ratios `[0.10, 0.20, 0.30, 0.40, 0.50]`

## Infrastructure now in place

- Config-driven pipeline (`configs/mvp.example.yaml`)
- PostGIS schema bootstrap (`sql/001_ais_qds_schema.sql`)
- Runnable CLI for Sprint 1 setup:
  - `bootstrap`
  - `load-context`
  - `build-trajectories`
  - `compute-labels`
  - `create-dev-subset`
  - `status`
  - `sprint1` (all above, sequential)
- Runnable Sprint 2 baseline benchmarking:
  - `run-baselines` (uniform + DP, budget sweep, metric persistence)
  - `summarize-baselines` (CSV/JSON/Markdown table + SVG plot export)
- Reproducible folder structure for data/results/tests/notebooks

## Project Structure

```text
AIS-Contextual-QDS/
├── Context-And-Planning/                # locked planning and scope docs
├── configs/
│   └── mvp.example.yaml
├── sql/
│   └── 001_ais_qds_schema.sql
├── src/
│   ├── cli.py
│   ├── config.py
│   ├── db.py
│   ├── logging_utils.py
│   ├── paths.py
│   ├── evaluation/
│   │   └── metrics.py
│   ├── simplification/
│   │   ├── uniform.py
│   │   └── douglas_peucker.py
│   └── pipelines/
│       ├── bootstrap.py
│       ├── trajectories.py
│       ├── labels.py
│       ├── subsets.py
│       ├── baselines.py
│       └── status.py
├── tests/
├── data/
├── results/
└── Makefile
```

## Quick Start

1. Ensure `DATABASE_URL` is set (for example: `postgresql://ais:aisdev@localhost:5433/ais`).
2. Install project dependencies:

```bash
pip install -r requirements.txt
```

3. Confirm PostGIS is running and `public.ais_points_cleaned` exists.
4. Move into this project folder:

```bash
cd AIS-Contextual-QDS
```

5. Bootstrap schema and tables:

```bash
python -m src.cli --config configs/mvp.example.yaml bootstrap
```

6. Load context geometry into the new schema (`ais_qds` by default):

- one active study region in `ais_qds.study_region`
- 3 zones in `ais_qds.context_zones` (matched by `name` property by default)
- 1 corridor polygon in `ais_qds.context_corridors`

```bash
python -m src.cli --config configs/mvp.example.yaml load-context \
  --study-region-file data/context/study_region.geojson \
  --zones-file data/context/zones.geojson \
  --corridor-file data/context/corridor.geojson
```

For line-based corridor inputs, provide a buffer distance in meters:

```bash
python -m src.cli --config configs/mvp.example.yaml load-context \
  --study-region-file data/context/study_region.geojson \
  --zones-file data/context/zones.geojson \
  --corridor-file data/context/corridor_centerline.geojson \
  --corridor-buffer-meters 300
```

`sql/010_seed_context_template.sql` remains available as a manual SQL fallback.

7. Run Sprint 1 data build pipeline:

```bash
python -m src.cli --config configs/mvp.example.yaml sprint1
```

8. Inspect table counts:

```bash
python -m src.cli --config configs/mvp.example.yaml status
```

9. Run Sprint 2 baselines (on dev split by default):

```bash
python -m src.cli --config configs/mvp.example.yaml run-baselines --overwrite
```

10. Export summary artifacts for latest run:

```bash
python -m src.cli --config configs/mvp.example.yaml summarize-baselines
```

You can run equivalent commands via `make`:

```bash
make bootstrap
make load-context STUDY_REGION_FILE=data/context/study_region.geojson ZONES_FILE=data/context/zones.geojson CORRIDOR_FILE=data/context/corridor.geojson
make build-trajectories
make compute-labels
make create-dev-subset
make run-baselines
make summarize-baselines
make status
```

## Command Map to Sprint Tasks

- T2/T3: `build-trajectories`
- T5: `compute-labels`
- T6: `create-dev-subset`
- T7/T8/T9/T10/T12 skeleton: `run-baselines`
- T1/T4 are represented in config + `load-context` + context tables (scope and layer setup)

## Notes

- Distance and overlap logic is PostGIS-based and expects metric computations via `geography` casts where needed.
- Context files are expected to be in EPSG:4326 coordinates.
- This scaffold intentionally avoids heavy ML components to stay aligned with MVP scope lock.
- The Sprint 2 baseline runner is now in place for uniform + DP at shared budget settings.
