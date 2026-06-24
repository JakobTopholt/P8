# Models Module

This module contains the query-conditioned trajectory encoder used by the v2 rebuild.

## Files

| File | Purpose |
| --- | --- |
| `attention_utils.py` | Chunked cross-attention helper that lets points attend to typed query embeddings. |
| `trajectory_qds_model.py` | Base transformer model and min-max normalization helper. |
| `turn_aware_qds_model.py` | Same model architecture with the extra `turn_score` feature. |

## Model Flow

```text
points[L,Fp] -> point encoder -> + sinusoidal positional encoding -> local Transformer -> H[L,D]
queries[M,Fq] + query_type_ids[M] -> type embedding -> query encoder -> Q[M,D]
H and Q -> chunked cross-attention -> context C[L,D]
H + C -> four per-type heads -> logits[L,4]
```

## Key Behavior

- `TrajectoryQDSModel` expects `query_type_ids` during training and falls back to zero IDs only in eval mode.
- `chunked_cross_attention_context` accumulates query-chunk outputs and divides by the total query count, so context scale stays stable as workloads grow.
- The attention direction is point-to-query, which keeps the per-point representation query-conditioned without leaking across trajectories.
- `normalize_points_and_queries` is the shared min-max transform used by the persisted `FeatureScaler`.

## Shapes And Defaults

- Point features: 7 columns for the baseline model, 8 for the turn-aware model.
- Query features: 12 padded features from `src.queries.query_types.pad_query_features`.
- Output: one logit per query type for each point.
- Default query chunk size: 128, which keeps the attention helper exact for the common case where the whole workload fits in one chunk.
