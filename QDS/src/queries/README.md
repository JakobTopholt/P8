# Queries Module

This module defines the typed query format used by v2, the workload generator, and the concrete query executors that produce the labels and evaluation targets.

## Files

| File | Purpose |
| --- | --- |
| `query_types.py` | Query type IDs, workload-mix parsing, and padded query feature conversion. |
| `query_generator.py` | Deterministic typed workload generation from a trajectory set. |
| `query_executor.py` | Concrete range, kNN, similarity, and clustering query execution. |

## Query Types

| Name | ID | Input params | Return value |
| --- | --- | --- | --- |
| `range` | 0 | spatial-temporal box | set of matching trajectory IDs |
| `knn` | 1 | anchor point, time window, `k` | set of nearest distinct trajectory IDs |
| `similarity` | 2 | centroid, time box, radius, reference snippet | ranked trajectory IDs, consumed as a set for F1 |
| `clustering` | 3 | box, `eps`, `min_samples` | per-trajectory DBSCAN labels over trajectory centroids |

## Workload Format

- `normalize_workload_mix` lowercases the keys, drops non-positive weights, validates query names, and normalizes the mix to sum to 1.
- `parse_workload_mix` accepts CLI strings like `range=0.8,knn=0.2`.
- `pad_query_features` converts heterogeneous typed query dicts into a `[M, 12]` tensor plus a `[M]` tensor of type IDs.
- Feature layout:
    - range: `lat_min`, `lat_max`, `lon_min`, `lon_max`, `t_start`, `t_end`
    - knn: `lat`, `lon`, `t_center`, `t_half_window`, `k`
    - similarity: `lat_query_centroid`, `lon_query_centroid`, `t_start`, `t_end`, `radius`, plus the mean of the reference snippet in slots 5-7 when present
    - clustering: `lat_min`, `lat_max`, `lon_min`, `lon_max`, `t_start`, `t_end`, `eps`, `min_samples`

## Workload Generation

`generate_typed_query_workload` builds a mixed workload from the full trajectory set, allocates query counts from the requested mix, shuffles the result deterministically, and returns the `TypedQueryWorkload` container used by experiments and training.

Range and kNN anchors use a 70/30 density sampler. The generator builds a lat/lon density map for the whole dataset; 70% of range/kNN anchors are sampled with probability proportional to the density of the point's grid cell, and 30% are sampled uniformly from all points. Similarity and clustering keep the existing uniform anchor behavior.

Range query footprint is configurable. `range_spatial_fraction` controls the latitude/longitude half-width and `range_time_fraction` controls the time half-window as fractions of the dataset spans. Smaller values are useful when increasing `n_queries`, because the default range boxes can cover most of a dense AIS dataset after enough queries.

When `target_coverage` is provided, the generator still emits exactly `n_queries` queries. While the measured union coverage is below the target, anchors are biased toward points not yet covered; once the target is reached, generation returns to the regular sampler. Coverage is measured as point-level query signal coverage: range/clustering boxes, dense kNN neighbourhoods, and similarity spatiotemporal radius regions.

## Execution Semantics

- `execute_range_query` returns the set of trajectory IDs with points inside the box.
- `execute_knn_query` computes the nearest point per trajectory inside the time window and returns the `k` nearest distinct trajectory IDs.
- `execute_similarity_query` filters trajectories by centroid and radius, then ranks trajectory IDs with a lightweight DTW-like distance; evaluation drops ranking and uses set F1.
- `execute_clustering_query` clusters per-trajectory representatives inside the box and returns labels indexed by trajectory ID.
- `execute_typed_query` dispatches by the `type` field and returns the type-specific result object.
