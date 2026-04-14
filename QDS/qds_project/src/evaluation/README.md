# Evaluation Module

Provides metrics and baseline methods for evaluating AIS trajectory
simplification quality.

---

## Components

### `metrics.py`

**`query_error(original_points, simplified_points, queries)`**  
Mean relative query error between the original and simplified point clouds.

```
error = mean_q [ |result(original, q) - result(simplified, q)| / max(|result(original, q)|, ε) ]
```

The implementation clamps the denominator with `ε = 1e-8` so zero-result
queries remain numerically stable. Lower values indicate better preservation of
the query workload.

**`compression_ratio(original_points, simplified_points)`**  
Fraction of points retained after simplification: `K / N`. A value of 1.0
means no compression; smaller values indicate more aggressive compression.

**`query_latency(points, queries)`**  
Average wall-clock time per query in seconds, measured by running all queries
against the point cloud once with `time.perf_counter`.

**`compute_compression_metrics(original_points, simplified_points, trajectory_boundaries, retained_mask)`**  
Returns a dictionary with the retained-point ratio, average points per
trajectory before and after simplification, the number of trajectories that
still have at least one retained point, and the total trajectory count.

**`compute_typed_query_error(original_points, simplified_points, typed_queries, ...)`**  
Supports mixed workloads by choosing the error metric per query type:
`range`, `aggregation`, and `intersection` use relative error, while
`nearest` uses absolute distance error.

---

### `baselines.py`

Reference simplification methods for comparison with the ML QDS model.

**`random_sampling(points, ratio)`**  
Retains a uniformly random subset of `round(ratio * N)` points. Provides a
lower bound on simplification quality.

**`uniform_temporal_sampling(points, ratio)`**  
Sorts points by time and retains every k-th point to achieve the target ratio.
Provides evenly spaced temporal coverage without any query awareness.

**`douglas_peucker(points, epsilon)`**  
Recursive 2D line simplification on lat/lon coordinates. Removes points that
deviate less than `epsilon` degrees from the straight-line path between their
neighbours. The algorithm is implemented iteratively (stack-based) to avoid
Python recursion limits on large trajectories.

---

## Summary Table

| Method                 | Description                                               |
|------------------------|-----------------------------------------------------------|
| Random Sampling        | Uniformly random subset                                   |
| Uniform Temporal       | Every k-th point sorted by time                           |
| Douglas-Peucker        | Recursive line simplification on lat/lon                  |
| ML QDS (baseline)      | Learned importance scores — `TrajectoryQDSModel`          |
| ML QDS (turn-aware)    | Learned importance scores — `TurnAwareQDSModel`           |
