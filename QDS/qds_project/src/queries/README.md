# Queries Module

Generates and executes spatiotemporal queries over AIS trajectory data.

---

## Components

### `query_generator.py`

Generates random spatiotemporal range queries derived from the actual
trajectory data bounds. Each range query is a 6-element vector:

```
[lat_min, lat_max, lon_min, lon_max, time_start, time_end]
```

Query extents are sampled as fractions of the total data range so that each
query covers a realistic portion of the dataset.

#### Range Query Generation Strategies

**`generate_uniform_queries(trajectories, n_queries, spatial_fraction, temporal_fraction)`**  
Query centres are drawn independently and uniformly from the full lat/lon/time
extent of the dataset. Models an unbiased, ad-hoc workload where any location
is equally likely to be queried.

**`generate_density_biased_queries(trajectories, n_queries, spatial_fraction, temporal_fraction)`**  
Query centres are anchored to real AIS data points, concentrating queries in
high-traffic regions.

**`generate_mixed_queries(trajectories, total_queries, density_ratio, ...)`**  
A shuffled blend of uniform and density-biased queries.

**`generate_spatiotemporal_queries(trajectories, n_queries, ...)`** *(deprecated)*  
Backward-compatible wrapper delegating to the above functions.

**`generate_multi_type_workload(trajectories, total_queries, ratios, ...)`**  
Generates a shuffled workload with typed queries. The default split is equal
across `range`, `intersection`, `aggregation`, and `nearest` query types.

#### Typed Query Generators

New typed generators return `list[dict]` with a `"type"` key and a `"params"` dict.

**`generate_intersection_queries(trajectories, n_queries, ...)`**  
Returns intersection-type queries anchored to high-density regions.
Each query will count the number of trajectories with at least one point
inside the spatiotemporal box.

**`generate_aggregation_queries(trajectories, n_queries, ...)`**  
Returns aggregation-type queries with uniformly sampled centres.
Each query will count the number of data-points inside the spatiotemporal box.

**`generate_nearest_neighbor_queries(trajectories, n_queries, ..., k=1)`**  
Returns kNN-type queries anchored to real AIS data points with small
spatial jitter.  Each query returns the **mean Euclidean distance** from the
query location to the *k* closest AIS points within the time window.  When
``k=1`` (the default) this is identical to the classical single-nearest-neighbor
distance, preserving backward compatibility.

The ``k`` parameter is stored in the query's ``"params"`` dict so the
executor knows how many neighbours to aggregate.

**`generate_multi_type_workload(trajectories, total_queries, ratios, ...)`**  
Generates a shuffled, mixed workload containing multiple query types.
`ratios` is a dict mapping type names to their fraction of the workload
(must sum to 1.0; default is equal split across all four types).

```python
from src.queries.query_generator import generate_multi_type_workload

queries = generate_multi_type_workload(
    trajectories,
    total_queries=100,
    ratios={"range": 0.4, "intersection": 0.2, "aggregation": 0.2, "nearest": 0.2},
)
# queries is a list[dict], each with "type" and "params" keys
```

#### Robust Spatial Bounds

Width sampling uses a clipped spread estimate based on the 5th–95th
percentiles (`_effective_spatial_ranges`) to prevent extremely wide queries
when the global bounding box is dominated by outliers.

---

### `query_executor.py`

Executes spatiotemporal range queries against a point cloud.

A query selects all points whose `(lat, lon, time)` fall inside the query
rectangle and returns the **SUM of the speed column** for those points.

**`run_query(points, query)`**  
Run a single query against a `[N, 5]` point cloud. Returns a scalar tensor.

**`run_queries(points, queries)`**  
Run all `M` queries in a vectorised, chunked manner. Returns a `[M]` result tensor.

Point tensor columns: `[time, lat, lon, speed, heading]`  
Query tensor columns: `[lat_min, lat_max, lon_min, lon_max, time_start, time_end]`

`run_queries` executes queries in point chunks so large datasets do not build a
full `[N, M]` mask at once.

### `query_types.py`

Unified dispatcher and typed executors for the new query types.

`execute_query(query, points, trajectories=None)` routes based on the query
`"type"` field:

| Type | Result | Notes |
|------|--------|-------|
| `range` | Sum of speed | Uses the existing range-query executor |
| `intersection` | Count of ships | Falls back to matching points when `trajectories` is not supplied |
| `aggregation` | Count of points | Counts points inside the box |
| `nearest` | Minimum distance | Searches within the time window, then widens to all points if needed |

`execute_typed_queries(queries, points, trajectories=None)` executes a list of
typed query dicts and returns a list of scalar results.

**Query types**

| Type           | Result              | Parameters                                                             |
|----------------|---------------------|------------------------------------------------------------------------|
| `range`        | Sum of speed        | lat/lon/time box                                                       |
| `intersection` | Count of ships      | lat/lon/time box (+ optional `trajectories`)                           |
| `aggregation`  | Count of points     | lat/lon/time box                                                       |
| `nearest`      | Mean k-NN distance  | `query_lat`, `query_lon`, `query_time`, `time_window`, `k` (default 1) |

**`execute_query(query, points, trajectories=None)`**  
Route a single typed query dict to the correct executor.

**`execute_typed_queries(queries, points, trajectories=None)`**  
Execute a list of typed queries and return a list of scalar results.

```python
from src.queries.query_types import execute_query, execute_typed_queries

# Single aggregation query
result = execute_query(
    {"type": "aggregation", "params": {"lat_min": 50, "lat_max": 55, "lon_min": -5, "lon_max": 5, "time_start": 0, "time_end": 100}},
    points,
)

# Nearest-neighbor query
dist = execute_query(
    {"type": "nearest", "params": {"query_lat": 52.0, "query_lon": 1.0, "query_time": 50.0, "time_window": 5.0, "k": 3}},
    points,
)
# k=3: returns mean distance to the 3 nearest AIS points
```

