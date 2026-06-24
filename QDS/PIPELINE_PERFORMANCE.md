# Pipeline Performance — where time, GPU, CPU, and memory go

Reference for the AIS-QDS v2 training pipeline. Numbers below are measured from
real `phase8_v6` (multi-day) and `phase8_v7` (1-day cr=0.05) jobs, captured in
`/ceph/home/student.aau.dk/hq11zw/P8/AI_lab_setup/Jobs/output/`. Use this when
reasoning about where to optimise next, or when picking `--mem` and `--gres`
for a new sbatch script.

## Time percentages by phase

Each row is one phase printed by `experiment_pipeline_helpers.py` (`[phase] starting / done`).
Columns are representative real runs.

| Phase | 1-day mixed B1 (5 010 s) | 1-day kNN (5 172 s) | 6-day kNN (28 700 s+) |
|---|---|---|---|
| `[load-data]` | 222 s — **4.4%** | 222 s — 4.3% | 322 s — 1.1% |
| `[generate-workloads]` | 62 s — 1.2% | 212 s — 4.1% | 2 345 s — **8.2%** |
| `[build-datasets]` | <1 s — 0% | <1 s — 0% | 1 s — 0% |
| `[train-model]` | ~3 990 s — **79.6%** | 4 163 s — **80.5%** | 22 282 s — **77.7%** |
| `[evaluate-matched]` | 650 s — **13.0%** | 662 s — 12.8% | 1 130 s+ — 4%+ |
| ↳ MLQDS eval | 385 s — 7.7% | 534 s — 10.3% | 566 s+ — 2%+ |
| ↳ uniform eval | 69 s — 1.4% | similar | similar |
| ↳ DouglasPeucker eval | 78 s — 1.5% | similar | similar |
| ↳ Oracle eval | 65 s — 1.3% | similar | similar |
| `[write-results]` | <1 s — 0% | <1 s — 0% | <1 s — 0% |
| `[write-simplified-csv]` | 2 s — 0% | 1 s — 0% | 4 s — 0% |
| `[write-query-points-csv]` | 40 s — 0.8% | 23 s — 0.5% | (rolled into eval) |
| `[trajectory-length-loss]` | 14 s — 0.3% | 8 s — 0.2% | 22 s — 0% |

**Headline**: training is **~80% of every run**. Eval is ~13%. Everything else
combined is ≤5%. Optimisations that don't speed up training won't move the
total runtime much.

## What each phase uses

| Phase | GPU | CPU | Memory | Disk I/O |
|---|---|---|---|---|
| `[load-data]` | — | heavy (pandas CSV parse, tensor build) | heavy — 1-day ~130 MB / 6-day ~770 MB of point tensors in RAM | heavy read — 3.8 GB CSV on 6-day |
| `[generate-workloads]` | — | very heavy for kNN (haversine per query × per trajectory) and clustering (DBSCAN) | medium | — |
| `[build-datasets]` | — | light (window builder) | small | — |
| `[train-model]` | **heavy — 80% GPU utilisation, AMP + DataParallel pay off here** | medium (data load, label sample, ranking pair gen) | heavy on GPU (~2-3 GB VRAM); data tensors live on GPU | small (only checkpoint at end) |
| `[evaluate-matched]` | medium — only MLQDS forward uses GPU; uniform/DP/Oracle are pure CPU | heavy — `score_retained_mask` runs queries × methods on CPU; the MLQDS simplifier (greedy coverage) is a slow Python loop | medium spike when retained masks + full/simp boundaries held simultaneously | — |
| ↳ MLQDS within eval | GPU forward (small fraction) + CPU simplifier (the bulk) | heavy (Python O(N²·cr) greedy loop per trajectory) | medium | — |
| ↳ uniform / DP / Oracle | — | heavy (query exec only) | small | — |
| `[write-results]` | — | light (JSON dump) | small | small write |
| `[write-simplified-csv]` | — | medium (CSV format ~30-50 MB per method) | small | medium write |
| `[write-query-points-csv]` | — | medium (per-query loop, Python-level row writing) | small | medium write — bigger when query coverage is high |
| `[trajectory-length-loss]` | — | medium (haversine per trajectory in Python) | small | — |

## Bottlenecks

1. **GPU**: `[train-model]` is the only GPU-bound phase. AMP + DataParallel only
   help here. Nowhere else uses GPU meaningfully.
2. **CPU #1**: `[generate-workloads]` on multi-day — 8% of total on 6-day vs 1%
   on 1-day. Scales with `#queries × #trajectories` × per-query distance
   computation. Mostly Python in `query_generator.py` and `query_executor.py`.
3. **CPU #2**: `[evaluate-matched]` MLQDS simplifier — the greedy O(N²·cr)
   Python loop in `simplify_with_score_and_coverage`. ~95 minutes on 6-day
   similarity. Vectorising this would save ~10% of total runtime on 6-day.
4. **Memory**: `[load-data]` peaks. 6-day = ~24M rows × 8 floats ≈ 1–2 GB RAM,
   which is why 6-day sbatch scripts use `--mem=160G` to leave headroom for
   subsequent training tensors.
5. **Disk I/O**: never a bottleneck. `[load-data]` does the only sustained
   read; everything else is short writes.

## What recent optimisations actually fixed

| Change | Affected phase | Impact on total |
|---|---|---|
| AMP (`torch.cuda.amp`) | `[train-model]` | ~1.4× faster training → ~28% lower total |
| DataParallel (2 GPUs) | `[train-model]` | ~1.7× faster training → ~33% lower total |
| AMP + DataParallel | `[train-model]` | ~2.4× faster → ~47% lower total |
| Collapse trip-wire | `[train-model]` | Prevents wasted ~30-60% of runtime on collapsed kNN runs |
| (open) Vectorise MLQDS simplifier | `[evaluate-matched]` MLQDS | ~10% lower total on 6-day if implemented |

All of the v6 6-day speedup over v3 6-day comes from `[train-model]` changes;
eval and I/O phases are unchanged.

## How to use this when planning a sbatch script

- **One GPU is enough on 1-day data** — training there is well under 2 h.
- **Two GPUs only worth it on multi-day** — run `--gres=gpu:2` for 3-day and 6-day to halve `[train-model]`.
- **`--mem`**: 80 GB is fine on 1-day, 128 GB for 3-day, 160 GB for 6-day.
- **`--cpus-per-task`**: 32 is enough for 1-day. **For long multi-day runs bump to 48 or 64** — pandas CSV parse during `[load-data]`, the per-trajectory haversine loops in `[generate-workloads]`, the MLQDS greedy simplifier in `[evaluate-matched]`, and PyTorch's intra-op BLAS threads during eval all scale with cores. The biggest wins are on 6-day where `[generate-workloads]` is 8% of total and the MLQDS simplifier is ~10%. Note: most of these phases are written as Python loops today, so the speedup from extra cores is modest until they're vectorised; treat 48-64 as headroom that won't go to waste rather than a strict 2× win.
- **`--time=12:00:00`** is the partition cap; 6-day range with 75 epochs needs
  the full window even with AMP+DDP.
- **`MAX_QUERIES`**: bumping this only affects `[generate-workloads]` and the
  per-query CSV write — fastest phase on 1-day, ~8% on 6-day.
- **`WRITE_SIMPLIFIED_CSV=true`** adds ~50 MB of writes per method ≈ negligible
  runtime; only relevant for storage budget on `/ceph/project`.

## Methodology

Numbers come from `grep "^\\[[a-z-]+\\] (starting|done in|total runtime)"` in
the captured `.out` files for jobs **830437** (1-day kNN, completed 1 h 30 m),
**830440** (1-day mixed B1, completed 1 h 28 m), and **830428** (6-day kNN,
completed training in ~6 h 11 m). To regenerate from a fresh run:

```bash
JID=830437
grep -E "^\[[a-z-]+\] (starting|done in|total runtime)|eval [A-Za-z]+. (starting|done in)" \
  /ceph/home/student.aau.dk/hq11zw/P8/AI_lab_setup/Jobs/output/*/training_AI_*_${JID}.out
```
