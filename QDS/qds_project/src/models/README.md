# Models Module

Defines neural network architectures for predicting per-point importance scores
given AIS trajectory data and a spatiotemporal query workload.

---

## Architecture Overview

All models share the same four-stage architecture:

```
Points [N, F] ──► Point Encoder (F→64→64) ──► Point Self-Attn + Norm ──────────────────────────────────► (+) ──► Importance Predictor (64→32→1→σ) ──► scores [N]
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

**Point self-attention**: before interacting with queries, each point attends to
its trajectory neighbours (key and value are both the point sequence) so that
neighbourhood context — e.g. "this point precedes a sharp turn" — can be learned.
A residual connection and LayerNorm follow the self-attention block.

**Query-type embedding**: a learned 16-dimensional vector for each of the four
query types (range / intersection / aggregation / nearest) is concatenated to
the 6 numeric query features before encoding.  This lets the model learn
type-specific importance signals without changing the numeric feature schema.

**Cross-attention mechanism**: queries (Q) attend to point embeddings (K, V).
The resulting attention weights `[M, N]` are transposed to `[N, M]` and
matrix-multiplied with the attended query embeddings `[M, D]`, producing a
per-point query-conditioned context vector `[N, D]`. This is added to the
original point embeddings before predicting importance scores.  A residual
connection and LayerNorm follow the cross-attention block.

Implementation note: the model computes this aggregation in bounded query
chunks, and the baseline point self-attention block does not request attention
weights because they are not used. Both changes preserve the same outputs while
avoiding large transient attention allocations on large workloads.

---

## Query-Type Integer Constants

`trajectory_qds_model.py` exports integer constants that map to the query type
strings used in `query_types.py`:

| Constant                     | Value | Query type      |
|------------------------------|-------|-----------------|
| `QUERY_TYPE_ID_RANGE`        | 0     | `"range"`       |
| `QUERY_TYPE_ID_INTERSECTION` | 1     | `"intersection"`|
| `QUERY_TYPE_ID_AGGREGATION`  | 2     | `"aggregation"` |
| `QUERY_TYPE_ID_NEAREST`      | 3     | `"nearest"`     |
| `NUM_QUERY_TYPES`            | 4     | —               |

---

## Components

### `trajectory_qds_model.py`

**`TrajectoryQDSModel`** — Baseline QDS model with query-type awareness.

- Point encoder input: **7 features** — `[time, lat, lon, speed, heading, is_start, is_end]`
- Query encoder input: **6 features + 16-dim type embedding** — `[lat_min, lat_max, lon_min, lon_max, time_start, time_end, <type_embed>]`
- Additional `query_type_ids` forward argument: `[M]` long tensor — integer type
  label per query.  Defaults to `QUERY_TYPE_ID_RANGE` (0) for all queries when
  omitted, preserving backward compatibility.
- Output: `[N]` importance scores in `[0, 1]`

**`normalize_points_and_queries(points, queries, query_type_ids=None)`**
Min-max normalises points and queries using point-cloud feature ranges so that
training and inference inputs are on consistent scales. The same scaling is
applied to semantically matching fields (time, lat, lon). Features at columns
5 and beyond (is_start, is_end, turn_score, …) are already
in `[0, 1]` and are preserved unchanged. The `query_type_ids` parameter is
accepted for API symmetry but is not used numerically.

---

### `turn_aware_qds_model.py`

**`TurnAwareQDSModel`** — Turn-aware QDS model variant.

Identical architecture to the v1 `TrajectoryQDSModel` (no query-type embedding
or self-attention) but accepts an extended **8-feature** point vector that
includes `turn_score` as the final column.

- Point encoder input: **8 features** — `[time, lat, lon, speed, heading, is_start, is_end, turn_score]`
- Query encoder input: **6 features** (same as baseline)
- Output: `[N]` importance scores in `[0, 1]`

The `turn_score` feature allows the model to learn that points at trajectory
bends may carry structural importance independent of the query workload,
producing simplified trajectories that better preserve trajectory shapes.
