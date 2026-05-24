# Generalized-experiment coverage (query-blind / `--no_query_model`)

Structure:
- `same_queries/` — eval reused the **exact training queries** (`--queries_from`). Only valid for **Feb 10** (the day the training queries were generated on). Reference snippet preserved in GeoJSON so similarity round-trips correctly.
- `regenerated_queries/` — eval **regenerated** queries on the eval day with matched params (seed 42). The only option for cross-day (Feb 11 / Feb 12), since training queries are anchored to Feb 10 data.

Target grid: 3 days × 5 workloads × 4 cr (0.01 / 0.05 / 0.15 / 0.30).
Workloads: clustering, knn, range (no top_k); mixed_topk50, similarity_topk50 (top_k=50 line).

Legend: ✅ present · ⏳ pending (job in queue) · ❌ missing

## SAME-QUERIES — Feb 10 only
| workload | cr01 | cr05 | cr15 | cr30 |
|---|---|---|---|---|
| clustering | ❌ | ✅ | ✅ | ✅ |
| knn | ❌ | ✅ | ✅ | ✅ |
| range | ❌ | ✅ | ✅ | ✅ |
| mixed_topk50 | ⏳854854 | ⏳854855 | ⏳854856 | ⏳854857 |
| similarity_topk50 | ⏳854853 | ✅ | ✅ | ✅ |

Genuinely missing (need new same-queries runs): **clustering cr01, knn cr01, range cr01**.

## REGENERATED — Feb 11
| workload | cr01 | cr05 | cr15 | cr30 |
|---|---|---|---|---|
| clustering | ❌ | ✅ | ✅ | ❌ |
| knn | ❌ | ✅ | ✅ | ❌ |
| range | ❌ | ✅ | ✅ | ✅ |
| mixed_topk50 | ❌ | ❌ | ❌ | ❌ |
| similarity_topk50 | ❌ | ✅ | ✅ | ❌ |

(mixed top_k=5 exists for cr05/15/30; mixed_topk50 not yet run cross-day.)

## REGENERATED — Feb 12
| workload | cr01 | cr05 | cr15 | cr30 |
|---|---|---|---|---|
| clustering | ❌ | ✅ | ✅ | ✅ |
| knn | ❌ | ✅ | ✅ | ✅ |
| range | ❌ | ✅ | ✅ | ✅ |
| mixed_topk50 | ❌ | ❌ | ❌ | ❌ |
| similarity_topk50 | ❌ | ✅ | ✅ | ✅ |

## Note on cross-day "same queries"
"Same queries" is only meaningful on Feb 10. The Feb 10 training queries reference specific Feb-10 trajectory points/times; applying them to Feb 11/12 data would match almost nothing. Cross-day generalization therefore must regenerate queries per day (matched params, seed 42).
