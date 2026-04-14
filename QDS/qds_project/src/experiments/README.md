# Experiments Module

End-to-end AIS Query-Driven Simplification experiment pipeline.

---

## Components

### `run_ais_experiment.py`

Main orchestration script for the end-to-end QDS pipeline. Coordinates data
generation/loading, query generation, model training, simplification, baseline
evaluation, and visualisation.

The pipeline supports `baseline` and `turn_aware` model variants, and
workload modes for `uniform`, `density`, `mixed`,
`intersection`, `aggregation`, `nearest`, `multi`, and `all`.

### `experiment_cli.py`

CLI argument parsing and validation. Handles all command-line argument
definitions, type conversions, and range/constraint validation.

### `experiment_config.py`

Shared configuration and result dataclasses (e.g., `DataConfig`, `QueryConfig`,
`ModelConfig`, `MethodMetrics`). Centralizes configuration structures used
throughout the pipeline.

### `experiment_pipeline_helpers.py`

Helper functions for core pipeline operations: query generation (uniform,
density-biased, mixed), trajectory simplification, baseline computation
(random sampling, uniform temporal sampling, Douglas-Peucker), evaluation
metrics (query error, compression ratio, query latency), and visualisation.

---

## Pipeline

1. **Data**: Generate synthetic AIS data (or load from CSV if `--csv_path` is provided).
2. **Queries**: Generate a spatiotemporal query workload.
3. **Labels**: Compute ground-truth importance labels (leave-one-out).
4. **Training**: Train the `TrajectoryQDSModel` or `TurnAwareQDSModel`.
5. **Simplification**: Simplify trajectories with the trained model.
6. **Baselines**: Apply random sampling, uniform temporal sampling, and
   Douglas-Peucker at the same compression ratio.
7. **Evaluation**: Compute query error, compression ratio, and query latency
   for all methods and print a comparison table.
8. **Visualisation**: Save plots to the system temporary directory and
   `results/`.

---

## Query Workloads

| Workload       | Description                                                      |
|----------------|------------------------------------------------------------------|
| `uniform`      | Query centres drawn uniformly from the bounding box              |
| `density`      | Query centres anchored to real AIS points (high-traffic focus)   |
| `mixed`        | Blend of uniform and density-biased queries (`--density_ratio`)  |
| `intersection` | Count trajectories with at least one point in the box            |
| `aggregation`  | Count points inside the box                                      |
| `nearest`      | Minimum distance to an AIS point near the query time             |
| `multi`        | Shuffled mixture of the typed query families                     |
| `all`          | Run all workloads sequentially and print a combined table        |

---

## CLI Usage

```bash
# Synthetic data, density workload (default)
python -m src.experiments.run_ais_experiment \
    --n_ships 10 --n_points 100 --n_queries 50 --epochs 30 --threshold 0.5

# Auto-select threshold by target retention ratio
python -m src.experiments.run_ais_experiment \
    --n_ships 50 --n_points 150 --n_queries 150 --target_ratio 0.10

# Specific workload type
python -m src.experiments.run_ais_experiment --workload density --n_queries 100

# Real AIS CSV
python -m src.experiments.run_ais_experiment \
    --csv_path /path/to/ais_data.csv --n_queries 100 --epochs 50
```

When `--csv_path` and `--save_csv` are both set, retained points are exported
to `MLClean-<original_filename>.csv` next to the input file.

---

## Parameters

| Parameter                       | Default    | Description                                                                                                            |
|---------------------------------|------------|------------------------------------------------------------------------------------------------------------------------|
| `--n_ships`                     | 10         | Number of synthetic vessels (ignored when `--csv_path` is set)                                                         |
| `--n_points`                    | 100        | Points per vessel trajectory (ignored when `--csv_path` is set)                                                        |
| `--n_queries`                   | 100        | Number of spatiotemporal queries                                                                                       |
| `--epochs`                      | 50         | Training epochs                                                                                                        |
| `--threshold`                   | 0.5        | Importance threshold for global threshold mode (ignored when `compression_ratio` is set)                               |
| `--target_ratio`                | None       | Auto-select threshold to retain this fraction of points (global mode only)                                             |
| `--compression_ratio`           | 0.2        | Per-trajectory fraction to retain in (0, 1]; pass 0 to use global threshold mode                                       |
| `--min_points_per_trajectory`   | 5          | Minimum number of points to retain per trajectory                                                                      |
| `--max_train_points`            | None       | Cap training points via random sample (full dataset still used for evaluation)                                         |
| `--model_max_points`            | 300000     | Cap for full-set model inference; above this, query scores are used directly                                           |
| `--point_batch_size`            | 50000      | Mini-batch size over points during training                                                                            |
| `--importance_chunk_size`       | 200000     | Chunk size for importance label computation                                                                            |
| `--dp_max_points`               | 200000     | Maximum points for running the Douglas-Peucker baseline                                                                |
| `--skip_baselines`              | False      | Skip baseline generation and evaluation                                                                                |
| `--skip_visualizations`         | False      | Skip all visualisation plot generation                                                                                 |
| `--max_visualization_points`    | 200000     | Maximum points in scatter visualisation plots                                                                          |
| `--max_visualization_ships`     | 200        | Maximum trajectories in trajectory line plots                                                                          |
| `--max_points_per_ship_plot`    | 2000       | Maximum points per trajectory line in plots                                                                            |
| `--workload`                    | density    | `uniform`, `density`, `mixed`, `intersection`, `aggregation`, `nearest`, `multi`, or `all`                             |
| `--density_ratio`               | 0.7        | Fraction of density-biased queries in a `mixed` workload                                                               |
| `--query_spatial_fraction`      | 0.03       | Maximum spatial query width as a fraction of the effective lat/lon range                                               |
| `--query_temporal_fraction`     | 0.10       | Maximum temporal query width as a fraction of the time range                                                           |
| `--query_spatial_lower_quantile`| 0.01       | Lower quantile for robust spatial bounds (uniform query placement)                                                     |
| `--query_spatial_upper_quantile`| 0.99       | Upper quantile for robust spatial bounds (uniform query placement)                                                     |
| `--model_type`                  | baseline   | `baseline` (TrajectoryQDSModel), `turn_aware` (TurnAwareQDSModel), or `all`                                            |
| `--turn_bias_weight`            | 0.1        | Additive weight for turn-score bias during simplification (turn-aware model)                                           |
| `--turn_score_method`           | heading    | Turn score method: `heading` (COG deltas) or `geometry` (lat/lon vectors)                                              |
| `--csv_path`                    | None       | Path to real AIS CSV file                                                                                              |
| `--save_csv`                    | False      | Save cleaned CSV of retained points when loading from `--csv_path`                                                     |
