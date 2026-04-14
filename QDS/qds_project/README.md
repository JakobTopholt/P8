# AIS Trajectory Query-Driven Simplification (QDS)

A machine learning research project that learns to compress AIS (Automatic
Identification System) vessel trajectory datasets while preserving the accuracy
of a spatiotemporal query workload. The model identifies which trajectory
points are important based on how much they influence query results, and
discards the rest.

---

## Research Problem

AIS data streams report vessel positions (lat, lon, speed, heading) every few
minutes, producing large trajectory datasets. Many downstream analytics tasks
can be answered from much smaller subsets of the data.

**Query-Driven Simplification** answers:

> *Which trajectory points can be removed without significantly changing the
> answers to a set of spatiotemporal queries?*

For a trajectory dataset **D** and a query workload **Q**, each point receives
an importance score:

```
importance_i = mean_q | result(D, q) - result(D \ {p_i}, q) |
```

The model learns to approximate this score from point features and the query
workload, enabling fast inference-time compression without re-running the
expensive leave-one-out computation.

---

## Model Variants

### Baseline — `TrajectoryQDSModel`

- **Input**: 7-feature point vector — `[time, lat, lon, speed, heading, is_start, is_end]`
- **Query-type awareness**: learned 16-dim embedding per query type (range /
  intersection / aggregation / nearest) concatenated to the 6 numeric query
  features before encoding, enabling type-specific importance signals.
- **Point self-attention**: a lightweight self-attention layer lets each point
  attend to its trajectory neighbours before cross-attention with queries,
  capturing neighbourhood context (e.g. "this point precedes a sharp turn").
- **LayerNorm**: applied after every attention block for stable training on
  variable-length AIS trajectories.

### Turn-Aware — `TurnAwareQDSModel`

- **Input**: 8-feature point vector — `[time, lat, lon, speed, heading, is_start, is_end, turn_score]`
- Same architecture but the extra `turn_score` feature lets the model learn
  that trajectory bends carry structural importance independent of the query
  workload.

See [`src/models/README.md`](src/models/README.md) for the full architecture diagram and helper function documentation.

---

## Model Architecture

```
Points [N, F] ──► Point Encoder (F→64→64) ──► Point Self-Attn + Norm ─────────────────────────────────► (+) ──► Importance Predictor (64→32→1→σ) ──► scores [N]
                                                                                                           ▲
Queries [M, 6] ─► cat(<type_embed>) ─► Query Encoder (6+16→64→64) ──► Cross-Attention (Q attends K,V) + Norm ┘
Query Types [M] ─► Type Embedding (→16) ┘
```

| Component            | Architecture                                            |
|----------------------|---------------------------------------------------------|
| Point Encoder        | Linear(F→64) → ReLU → Linear(64→64)                     |
| Point Self-Attention | MultiheadAttention(embed=64, heads=4) + LayerNorm       |
| Query Type Embedding | Embedding(4, 16)                                        |
| Query Encoder        | Linear(6+16→64) → ReLU → Linear(64→64)                  |
| Cross-Attention      | MultiheadAttention(embed=64, heads=4) + LayerNorm       |
| Importance Predictor | Linear(64→32) → ReLU → Linear(32→1) → σ                 |

Query result: SUM of speed for all points inside the query rectangle.

---

## Repository Structure

```
qds_project/
├── requirements.txt
├── src/
│   ├── data/               # AIS data loading and synthetic generation
│   │   ├── README.md
│   │   ├── ais_loader.py
│   │   └── trajectory_dataset.py
│   ├── queries/            # Spatiotemporal query generation and execution
│   │   ├── README.md
│   │   ├── query_generator.py
│   │   ├── query_executor.py
│   │   └── query_types.py
│   ├── models/             # Neural network model definitions
│   │   ├── README.md
│   │   ├── trajectory_qds_model.py
│   │   ├── turn_aware_qds_model.py
│   │   └── __init__.py
│   ├── training/           # Importance labels and training loop
│   │   ├── README.md
│   │   ├── importance_labels.py
│   │   └── train_model.py
│   ├── simplification/     # Trajectory simplification logic
│   │   ├── README.md
│   │   └── simplify_trajectories.py
│   ├── evaluation/         # Metrics and baseline methods
│   │   ├── README.md
│   │   ├── metrics.py
│   │   └── baselines.py
│   ├── visualization/      # Plotting utilities
│   │   ├── README.md
│   │   ├── trajectory_visualizer.py
│   │   └── importance_visualizer.py
│   └── experiments/        # End-to-end experiment pipeline
│       ├── README.md
│       └── run_ais_experiment.py
└── tests/
    ├── test_data.py
    ├── test_query_executor.py
    ├── test_query_generator.py
    ├── test_query_types.py
    ├── test_model.py
    ├── test_metrics.py
    ├── test_baselines.py
    └── test_simplification.py
```

---

## Installation

```bash
pip install -r qds_project/requirements.txt
```

---

## Quick Start

### Run the full end-to-end experiment (synthetic AIS data)

```bash
cd qds_project
python -m src.experiments.run_ais_experiment \
    --n_ships 10 \
    --n_points 100 \
    --n_queries 50 \
    --epochs 30 \
    --threshold 0.5
```

Use automatic threshold selection by target retained ratio:

```bash
cd qds_project
python -m src.experiments.run_ais_experiment \
    --n_ships 50 \
    --n_points 150 \
    --n_queries 150 \
    --target_ratio 0.10
```

Choose query workload type (`uniform`, `density`, `mixed`, `intersection`, `aggregation`, `nearest`, `multi`, or `all`):

```bash
cd qds_project
python -m src.experiments.run_ais_experiment --workload density --n_queries 100

# Typed workloads
python -m src.experiments.run_ais_experiment --workload intersection --n_queries 100
python -m src.experiments.run_ais_experiment --workload aggregation  --n_queries 100
python -m src.experiments.run_ais_experiment --workload nearest      --n_queries 100
python -m src.experiments.run_ais_experiment --workload multi        --n_queries 100

# Run all workloads and compare
python -m src.experiments.run_ais_experiment --workload all --n_queries 100
```

### Use real AIS data (CSV)

Supported column aliases: `mmsi`, `lat`/`latitude`, `lon`/`longitude`,
`speed`/`sog`, `heading`/`cog`, `timestamp`/`time`/`datetime`.

```bash
cd qds_project
python -m src.experiments.run_ais_experiment \
    --csv_path /path/to/ais_data.csv \
    --n_queries 100 \
    --epochs 50
```

When `--save_csv` is also set, retained points are exported to
`MLClean-<original_filename>.csv` next to the input file.

### Train only

```bash
cd qds_project
python -m src.training.train_model \
    --n_ships 20 \
    --n_points 200 \
    --n_queries 100 \
    --epochs 50 \
    --save_path results/model.pt
```

---

## Running Tests

```bash
cd qds_project
python -m pytest tests/ -v
```

---

## Configuration

All scripts accept command-line arguments. Key parameters:

| Parameter            | Default  | Description                                                                                |
|----------------------|----------|--------------------------------------------------------------------------------------------|
| `--n_ships`          | 10       | Number of synthetic vessels                                                                |
| `--n_points`         | 100      | Points per vessel trajectory                                                               |
| `--n_queries`        | 100      | Number of spatiotemporal queries                                                           |
| `--epochs`           | 50       | Training epochs                                                                            |
| `--lr`               | 1e-3     | Learning rate                                                                              |
| `--threshold`        | 0.5      | Importance threshold for simplification                                                    |
| `--target_ratio`     | None     | Auto-select threshold to retain this fraction                                              |
| `--workload`         | density  | `uniform`, `density`, `mixed`, `intersection`, `aggregation`, `nearest`, `multi`, or `all` |
| `--density_ratio`    | 0.7      | Fraction of density-biased queries (mixed mode)                                            |
| `--model_type`       | baseline | `baseline`, `turn_aware`, or `all`                                                         |
| `--turn_score_method`| heading  | Turn score method: `heading` or `geometry`                                                 |
| `--csv_path`         | None     | Path to real AIS CSV file                                                                  |
| `--max_train_points` | None     | Cap training points (for large datasets)                                                   |

---

## Evaluation Metrics

| Metric            | Formula                                                |
|-------------------|--------------------------------------------------------|
| Query Error       | mean_q \|orig(q) - simp(q)\| / (\|orig(q)\| + 1e-8)    |
| Compression Ratio | \|simplified\| / \|original\|                          |
| Query Latency     | Average wall-clock time per query (seconds)            |

---

## Baseline Methods

| Baseline               | Description                                                |
|------------------------|------------------------------------------------------------|
| Random Sampling        | Uniformly random subset                                    |
| Uniform Temporal       | Every k-th point sorted by time                            |
| Douglas-Peucker        | Recursive line simplification on lat/lon coordinates       |
| ML QDS (baseline)      | Learned importance scores — `TrajectoryQDSModel`           |
| ML QDS (turn-aware)    | Learned importance scores — `TurnAwareQDSModel`            |

---

## Simplification Modes

**Per-trajectory compression** (default, `compression_ratio=0.2`): each
trajectory is independently compressed to retain
`max(min_points, int(compression_ratio * len))` points, with endpoints
guaranteed.

**Global threshold mode** (`compression_ratio=None`): points below `threshold`
are discarded; endpoint and minimum-point-floor constraints are applied per
trajectory.

See [`src/simplification/README.md`](src/simplification/README.md) for details.

---

## Output Visualizations

When running the experiment, visualizations are saved to the system temporary
directory and `results/`:

- `ais_trajectories.png` — vessel paths in lat/lon space
- `ais_queries.png` — trajectories + semi-transparent query rectangles
- `ais_importance.png` — scatter plot coloured by importance score
- `ais_combined.png` — combined: lines + importance colours + queries
- `results/simplification_visualization.png` — simplification and query overlay
- `results/simplification_time_slices.png` — 4 time-window panels

See [`src/visualization/README.md`](src/visualization/README.md) for details.

---

## Python API

```python
import sys
sys.path.insert(0, 'qds_project')

from src.data.ais_loader import generate_synthetic_ais_data, load_ais_csv
from src.data.trajectory_dataset import TrajectoryDataset
from src.queries.query_generator import generate_density_biased_queries
from src.queries.query_executor import run_queries
from src.models.trajectory_qds_model import TrajectoryQDSModel
from src.training.importance_labels import compute_importance
from src.training.train_model import train_model
from src.simplification.simplify_trajectories import simplify_trajectories
from src.evaluation.metrics import query_error, compression_ratio

# 1. Generate or load AIS trajectory data
trajectories = generate_synthetic_ais_data(n_ships=10, n_points_per_ship=100)

# 2. Get flat point cloud
ds = TrajectoryDataset(trajectories)
points = ds.get_all_points()          # [N, 8]

# 3. Generate spatiotemporal query workload
queries = generate_density_biased_queries(trajectories, n_queries=100)  # [M, 6]

# 4. Train QDS model
model = train_model(trajectories, queries, epochs=50)

# 5. Simplify trajectories (per-trajectory compression, 20% retained)
simplified, mask, scores = simplify_trajectories(
    points, model, queries, compression_ratio=0.2
)

# 6. Evaluate
print(f"Query error:       {query_error(points[:, :5], simplified[:, :5], queries):.4f}")
print(f"Compression ratio: {compression_ratio(points, simplified):.4f}")
```

