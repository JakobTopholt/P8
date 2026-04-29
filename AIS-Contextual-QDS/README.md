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
- Current corridor threshold: `min_corridor_overlap_meters = 1.0`

The broader 4-week MVP config still exists as `configs/mvp.example.yaml`, but it is now a reference config rather than the default day-to-day entrypoint.

## Current Evaluation Guardrails

- Use `dev` for stress-budget search, method design, and scoring-weight choices.
- Keep `eval` as a confirmation split after budgets and comparison rules are fixed.
- Do not set a fixed acceptance threshold before the stress curves are visible.
- Compare methods by metric-vs-budget curves and identify where diminishing returns begin.
- Primary query F1 remains the gate; strict point-membership and event-count metrics are used to rank methods when primary query F1 is saturated.
- B3 is query-witness and trajectory-local only; B4 is the first method allowed to use static context priors such as boundary/corridor distances.
- B5 is optional extension work after B4: adaptive, learned, or interaction-based query-context scoring. It is not part of the current MVP success criteria.

## Planning Docs

- `Context-And-Planning/ContextOutline.md`: methodology narrative, research questions, scope, method ladder, and evaluation strategy.
- `Context-And-Planning/DefinedChoices-AIS-QueryDrivenSimplification.md`: locked project choices, semantics, metrics, features, and deferred decisions.
- `Context-And-Planning/TaskBoard-and-SprintPlan.md`: active execution board, current baseline evidence, and next sprint tasks.

## Workflow

The project is easiest to understand as five layers:

1. Context and scope
   Load the study region, zones, corridor, and configuration.
2. Dataset preparation
   Build trajectories, compute truth labels, and create the dev/eval subset.
3. Feature preparation
   Compute reusable per-point context and local-shape features for contextual methods.
4. Benchmarking
   Run simplification methods and persist metrics.
5. Inspection
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
python -m src.cli label-balance
python -m src.cli create-hardcase-subset
python -m src.cli compute-features
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
  Audit/truth mode. Uses adjacent-segment logic and is intentionally more expensive than `optimized`. Benchmark runs materialize adjacent simplified segments so this mode is practical for the current hardcase subsets, but it remains the stricter mode to use for final checks and larger-machine runs.

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
- `create-hardcase-subset`
  Create a deterministic query-balanced subset, defaulting to `<subset_name>_hardcase`.
- `compute-features`
  Compute reusable point-level context features: zone/corridor state, nearest zone, boundary distances, transitions, local turn angle, and local deviation.
- `prepare-data`
  Run bootstrap, trajectory build, labels, subset creation, and status in sequence.

### Evaluation

- `label-balance`
  Report overall and split-level positives for each zone and the corridor.
- `benchmark`
  Run simplification benchmarks across one or more retained-point budgets.
- `summarize-baselines`
  Export CSV, JSON, Markdown, and SVG summaries from stored benchmark metrics.
- `compare-label-modes`
  Compare stored labels between semantics modes, primarily `optimized` and `segment_exact`.

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
make label-balance
make create-hardcase-subset
make benchmark
make doctor
make compute-labels MODE=segment_exact
make compute-features
make compare-label-modes
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
│   ├── simplification/        # uniform, Douglas-Peucker, and B3 simplifiers
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
- Reusable point-context features for the next contextual method stage
- Summary export to CSV, JSON, Markdown, and SVG
- HTML and QGIS inspection exports
- `optimized` and `segment_exact` semantics modes
- Label-balance diagnostics and hard-case subset creation
- Benchmark diagnostic counts, including TP/FP/FN/TN and per-zone zone-entry counts
- Strict point-membership and event-count metrics that catch errors hidden by trajectory-level labels
- AIS-audited Great Belt context geometries documented in `data/context/GEOMETRY_AUDIT.md`

What is not yet in place:

- The contextual scorer beyond the current baseline stage
- Final thesis-grade audit comparisons between `optimized` and `segment_exact`
- Official chart-grade context polygons; the current layers are defensible query-workload geometries, not nautical chart products

## Notes

- Corridor overlap checks use metric distance through `geography` casts.
- Context files are expected in EPSG:4326.
- The current default path is intentionally narrow and reproducible so iteration stays fast.
- `segment_exact` is stricter and more expensive than `optimized`. Cached adjacent simplified segments make it practical for current hardcase subsets, while larger windows may still benefit from stronger hardware.
- If `label-balance` reports very low zone positives, run `create-hardcase-subset` and benchmark with `SUBSET_NAME=<configured>_hardcase`.
- The headline benchmark F1 values are trajectory-level labels. Use the strict point and event metrics in summary exports when judging whether a simplifier is preserving boundary behavior.
- Early saturation of trajectory-level F1 is an expected result for coarse yes/no queries. It means the simplified trajectory preserved enough evidence for the final query answer, not that it preserved all query-relevant behavior.
- False query artifacts are already counted at the primary label level through `zone_entry_fp` and `corridor_membership_fp`. For example, if a simplified segment appears to enter a zone that the raw trajectory did not enter, that is a zone-entry false positive. Fine-grained false-crossing artifact counts remain a planned error-taxonomy/inspection metric rather than a separate implemented benchmark column.
- `segment_exact` keeps adjacent-segment semantics. Benchmark runs materialize simplified segments in `trajectories_simplified_segments` so exact predictions can reuse cached segment geometry; if the cache is absent, prediction SQL falls back to rebuilding adjacent segments from simplified points.
