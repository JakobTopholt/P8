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
- Visual inspection export:
  - `export-visual-inspection` (self-contained HTML maps for raw/context/simplified comparison)
  - `export-qgis-inspection` (GeoJSON layers + QGIS project for desktop GIS inspection)
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

For the first iteration, use the 10-day config:

```bash
CONFIG=configs/iteration1_10days.example.yaml
```

That config targets cargo trajectories from `2026-01-01T00:00:00+00:00`
through `2026-01-10T23:59:59...+00:00`; the configured `window_end` is the
exclusive timestamp `2026-01-11T00:00:00+00:00`.

5. Bootstrap schema and tables:

```bash
python -m src.cli --config "$CONFIG" bootstrap
```

6. Load context geometry into the new schema (`ais_qds` by default):

- one active study region in `ais_qds.study_region`
- 3 zones in `ais_qds.context_zones` (matched by `name` property by default)
- 1 corridor polygon in `ais_qds.context_corridors` after buffering the stored corridor centerline

```bash
python -m src.cli --config "$CONFIG" load-context \
  --study-region-file data/context/study_region.geojson \
  --zones-file data/context/zones.geojson \
  --corridor-file data/context/corridor.geojson \
  --corridor-buffer-meters 700
```

If you later replace `corridor.geojson` with an already-buffered polygon file,
omit `--corridor-buffer-meters`:

```bash
python -m src.cli --config "$CONFIG" load-context \
  --study-region-file data/context/study_region.geojson \
  --zones-file data/context/zones.geojson \
  --corridor-file data/context/corridor_polygon.geojson
```

`sql/010_seed_context_template.sql` remains available as a manual SQL fallback.

7. Run Sprint 1 data build pipeline:

```bash
python -m src.cli --config "$CONFIG" sprint1
```

8. Inspect table counts:

```bash
python -m src.cli --config "$CONFIG" status
```

9. Run Sprint 2 baselines (on dev split by default):

```bash
python -m src.cli --config "$CONFIG" run-baselines --overwrite
```

10. Export summary artifacts for latest run:

```bash
python -m src.cli --config "$CONFIG" summarize-baselines
```

11. Export a visual inspection HTML report:

Raw trajectories + context only:

```bash
python -m src.cli --config "$CONFIG" export-visual-inspection
```

Compare a simplification run against raw trajectories:

```bash
python -m src.cli --config "$CONFIG" export-visual-inspection \
  --method uniform \
  --budget 0.10
```

Inspect specific trajectories:

```bash
python -m src.cli --config "$CONFIG" export-visual-inspection \
  --trajectory-ids 101,205,309 \
  --limit 3
```

The command writes a standalone HTML file under `results/figures/` by default.

12. Export a QGIS-ready inspection package:

Raw trajectories + context only:

```bash
python -m src.cli --config "$CONFIG" export-qgis-inspection
```

Compare a simplification run against raw trajectories:

```bash
python -m src.cli --config "$CONFIG" export-qgis-inspection \
  --method uniform \
  --budget 0.10
```

The command writes a folder under `results/figures/` containing GeoJSON layers
and `ais_qds_inspection.qgs`. Open the `.qgs` file in QGIS, or add the GeoJSON
layers manually.

You can run equivalent commands via `make`:

```bash
make bootstrap
make load-context STUDY_REGION_FILE=data/context/study_region.geojson ZONES_FILE=data/context/zones.geojson CORRIDOR_FILE=data/context/corridor.geojson CORRIDOR_BUFFER_METERS=700
make build-trajectories
make compute-labels
make create-dev-subset
make run-baselines
make summarize-baselines
make export-visual-inspection
make export-qgis-inspection
make status
```

For `make`, pass the first-iteration config explicitly:

```bash
make sprint1 CONFIG=configs/iteration1_10days.example.yaml
make run-baselines CONFIG=configs/iteration1_10days.example.yaml
make export-qgis-inspection CONFIG=configs/iteration1_10days.example.yaml
```

## Command Map to Sprint Tasks

- T2/T3: `build-trajectories`
- T5: `compute-labels`
- T6: `create-dev-subset`
- T7/T8/T9/T10/T12 skeleton: `run-baselines`
- T11: `export-visual-inspection` and `export-qgis-inspection`
- T1/T4 are represented in config + `load-context` + context tables (scope and layer setup)

## Notes

- Distance and overlap logic is PostGIS-based and expects metric computations via `geography` casts where needed.
- Context files are expected to be in EPSG:4326 coordinates.
- This scaffold intentionally avoids heavy ML components to stay aligned with MVP scope lock.
- The Sprint 2 baseline runner is now in place for uniform + DP at shared budget settings.
