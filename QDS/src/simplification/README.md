# Simplification Module

This module turns per-point scores into a compressed trajectory mask.

## Files

| File | Purpose |
| --- | --- |
| `simplify_trajectories.py` | Deterministic top-k retention with endpoint preservation. |

## Behavior

- `deterministic_topk_with_jitter` adds a repeatable pseudo-random tie-breaker so equal scores do not collapse to the same selection.
- `simplify_with_scores` applies top-k selection independently inside each trajectory boundary.
- Each trajectory keeps its endpoints even when the compression ratio is low.
- There is no global threshold across trajectories; the selection is always trajectory-local.

## Used By

The evaluation baselines, the model-based simplifier, and the oracle diagnostic all funnel through this module so their retained-point masks behave consistently.
