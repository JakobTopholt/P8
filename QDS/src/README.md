# src

Package root for AIS-QDS v2.

The code is organized around the full pipeline:
data -> queries -> training -> simplification -> evaluation -> experiments.

## Package Layout

| Path | Contents |
| --- | --- |
| `data/` | AIS CSV loading, synthetic data generation, and trajectory boundary helpers. |
| `queries/` | Typed query definitions, workload generation, and query executors. |
| `models/` | Query-conditioned trajectory encoders and attention utilities. |
| `training/` | F1-contribution labels, window batching, scaler persistence, and training/checkpoint helpers. |
| `simplification/` | Trajectory-local top-k simplification. |
| `evaluation/` | Baselines, F1 metrics, and tabular reporting. |
| `experiments/` | CLI parsing, config dataclasses, and the end-to-end pipeline. |
| `visualization/` | Placeholder package for future plotting utilities. |

## Module Notes

- Each subpackage has its own README with file-level details.
- `src.experiments.run_ais_experiment` is the main entry point.
- `src.models` expects normalized point and query tensors produced by `src.training.scaler.FeatureScaler`.
