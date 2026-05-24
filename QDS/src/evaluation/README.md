# Evaluation Module

This module compares query-aware ML simplification against stochastic and geometric baselines, then reports per-type and aggregate query F1 where higher is better.

## Files

| File | Purpose |
| --- | --- |
| `baselines.py` | Simplification methods: `MLQDSMethod`, `RandomMethod`, `NewUniformTemporalMethod`, `DouglasPeuckerMethod`, and `OracleMethod`. |
| `metrics.py` | F1 functions and the `MethodEvaluation` container. |
| `evaluate_methods.py` | Runs a method on flattened points and boundaries, then formats the comparison tables. |

## Methods

- `MLQDSMethod` uses the trained model, the persisted scaler, and the eval workload to produce workload-weighted per-point scores, then applies the standard per-trajectory top-k simplifier directly.
- `RandomMethod` retains random points per trajectory.
- `UniformTemporalMethod` is the legacy central-window temporal baseline and is kept only for compatibility tests.
- `NewUniformTemporalMethod` (`newUniformTemporal` in result tables) keeps truly evenly spaced points in each trajectory and is the default temporal baseline.
- `DouglasPeuckerMethod` approximates geometric importance from perpendicular distance to the trajectory endpoints.
- `OracleMethod` is a diagnostic upper bound that uses oracle labels directly.

## Metrics

- Range queries are scored over point hits inside the spatiotemporal box, so random point retention is measured by how much of the original query-hit mass it preserves rather than by one retained point recovering an entire trajectory.
- kNN, similarity, and clustering queries still require answer-set agreement, but are also weighted by retained point support so a method is not fully rewarded for preserving only one point from an answer trajectory.
- `f1_score(original, simplified)` - harmonic-mean agreement between original and simplified answer sets.
- `clustering_f1(original_labels, simplified_labels)` - F1 over same-cluster trajectory co-membership pairs, ignoring noise label `-1`.
- Retained point gap reports the average original-index spacing between consecutive retained points per trajectory. Lower values mean retained points are more evenly dense along the original trajectory, and the JSON output also includes normalized and max gap values.
- `MethodEvaluation` stores aggregate F1, per-type F1, compression ratio, retained point gap, and latency in milliseconds.

## Reporting

`evaluate_method` evaluates one simplification method against a typed query workload. `print_method_comparison_table` renders F1 values to six decimals so close methods are not hidden by rounding, includes `AvgPtGap` for retained-point spacing, and `print_shift_table` renders the train-vs-eval workload shift table written by the experiment pipeline. Tables should be read as higher-is-better F1 scores, while lower `AvgPtGap` means smaller average spacing between retained points.
