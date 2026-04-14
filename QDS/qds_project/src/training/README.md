# Training Module

Computes ground-truth importance labels and trains the QDS model.

---

## Components

### `importance_labels.py`

**`compute_importance(points, queries, sample_points, chunk_size)`**  
Computes normalised per-point importance scores using leave-one-out query
error analysis.

#### Algorithm

For each point `p_i`, importance is defined as the mean relative query error
when that point is removed:

```
importance_i = mean_q | result(D, q) - result(D \ {p_i}, q) |
```

Since the query result is the SUM of speed values, removing point `p_i` from
query `q` reduces the result by exactly `speed_i` when `p_i` is inside `q`.
This yields an efficient closed-form approximation:

```
importance_i = mean_q [ I(p_i ∈ q) * speed_i / |result(D, q)| ]
```

The computation is performed in two chunked passes over the point cloud to
avoid large `[N, M]` memory allocations:
1. **Pass 1**: Compute full query results `result(D, q)` for all queries.
2. **Pass 2**: For each point, compute the relative contribution per query.

Raw scores are normalised to `[0, 1]` at the end. If `sample_points` is
provided, only a random subset of points receive computed scores; the rest
are set to 0. This reduces cost for very large datasets.

---

### `train_model.py`

**`train_model(trajectories, queries, epochs, lr, save_path, importance, max_points, point_batch_size, model_type)`**  
Trains a `TrajectoryQDSModel` (baseline, 7-feature input),
`TurnAwareQDSModel` (turn-aware, 8-feature input) to predict the
ground-truth importance scores computed by `compute_importance`.

#### Training Details

- **Loss**: Weighted MSE — `loss = mean( (predicted - target)² * (1 + 9 * target) )`  
  The weighting emphasises high-importance points (weight up to 10×), since
  correctly ranking the most important points is critical for compression
  quality.
- **Optimizer**: Adam
- **Normalisation**: Inputs are min-max normalised via
  `normalize_points_and_queries` before training for stable dynamics.
- **Mini-batching**: When `point_batch_size` is set, the point cloud is split
  into mini-batches; the full query set is used in every batch.
- **Subsampling**: `max_points` caps the number of training points via random
  sampling before model-specific feature construction. The full dataset is
  still used for evaluation and simplification.

#### CLI Usage

```bash
python -m src.training.train_model \
    --n_ships 20 \
    --n_points 200 \
    --n_queries 100 \
    --epochs 50 \
    --save_path results/model.pt
```

| Argument          | Default | Description                              |
|-------------------|---------|------------------------------------------|
| `--n_ships`       | 10      | Number of synthetic vessels              |
| `--n_points`      | 100     | Points per vessel                        |
| `--n_queries`     | 100     | Number of spatiotemporal queries         |
| `--epochs`        | 50      | Training epochs                          |
| `--lr`            | 1e-3    | Adam learning rate                       |
| `--max_points`    | None    | Cap training points (random sample)      |
| `--point_batch_size` | 50000 | Mini-batch size over points             |
| `--save_path`     | auto    | Path to save model weights               |
