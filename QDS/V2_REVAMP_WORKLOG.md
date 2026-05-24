# V2_Revamp Worklog

Context drop for any future Claude Code session picking up the AIS-QDS v2 thesis work
on the `V2_Revamp` git branch. Last updated 2026-05-08.

## What this project is

8th-semester kandidat thesis. ML model for AIS trajectory simplification that should
preserve **query-answer F1** under a small retained-point budget. Goal: beat
DouglasPeucker by 1–2% on aggregate AnswerF1 across 5 query types
(range, kNN, similarity, clustering, mixed). Currently at parity with DP on most
types and slightly above on similarity / clustering / mixed at cr=0.05 1-day. Big
gap to **uniform** baseline at tight compression ratios is a separate scaling problem.

## State as of 2026-05-08

- Branch: `V2_Revamp`. The interventions in this worklog are committed-on-disk but
  not necessarily git-committed; check `git status` before doing anything else.
- Phase-5 baseline (`/ceph/project/P8-1/AI_lab_setup/Jobs/output/phase5_15job_levers_sweep/`)
  was 16 jobs at cr=0.05 / 0.02. MLQDS lost to uniform on 15/16, ~tied with DP.
- Phase-8 (the new `bash_files/phase8_v3/` scripts) exercises the V2_Revamp
  interventions on the same workloads.
- Phase-8 jobs **827556–827560** were submitted at ~15:14 with `WRITE_SIMPLIFIED_CSV="false"`
  on A1–A4 and `=true` on B1. Check `sacct -j 827556,827557,827558,827559,827560` for
  current state. Earlier phase-8 batches (827442–827489) all FAILED 1:0 because of
  `/ceph/project` quota exhaustion — see "Storage gotcha" below.

## What changed in the codebase (V2_Revamp interventions)

Six edits, all in `qds_v2/`:

1. **Coverage-aware simplifier** —
   `src/simplification/simplify_trajectories.py::simplify_with_score_and_coverage`.
   Per-trajectory greedy: at each step pick `argmax(score − λ·Σ_j exp(−(i−j)²/2σ²))`
   over already-kept j. Endpoints seeded first. Replaces the `temporal_score_hybrid`
   as the default selector. Closes the coverage gap that pure top-k creates at
   tight cr.
2. **`simplification_mode` config flag** wired through `experiment_config.py`,
   `experiment_cli.py`, `run_ais_experiment.py`, `MLQDSMethod` (3 sites in
   `experiment_pipeline_helpers.py` + 1 in `run_inference.py`), and the validation
   path in `train_model.py::_validation_query_f1`. Modes: `score_coverage`
   (default), `hybrid`, `topk`.
3. **CLS / trajectory-summary token** in `src/models/trajectory_qds_model.py`.
   Learnable token prepended before the local transformer. The `cls_out` is added
   as an extra K/V in cross-attention so each point can pull from a global
   trajectory summary. Per-batch-element so no cross-trajectory leakage. Toggle
   with `--use_cls_token true|false` (default true). Targets similarity (worst-
   failing type at phase-5).
4. **Per-head learnable log-temperature** in the model. `nn.Parameter(zeros(4))`,
   applied as `logits / exp(clamp(log_temp, ±2.3))`. Init zero ⇒ identity at
   init. Calibrates the four type heads' magnitudes during training (the
   inference-only rank-norm in `MLQDSMethod` was already a band-aid for this).
5. **Default flips** in `experiment_config.py`:
   `residual_label_mode: "temporal" → "none"` — required for the new
   simplifier (residual labels assume a temporal anchor that no longer exists
   under `score_coverage`).
6. **`load_state_dict(strict=False)`** in `training_pipeline.py` so older
   checkpoints (without CLS / temperature) still load. Prints missing/unexpected
   keys for diagnostic.

All 56 tests pass. See `tests/`.

## Files changed

```
src/simplification/simplify_trajectories.py   (+97 lines: greedy coverage selector)
src/models/trajectory_qds_model.py            (+24 lines: CLS token + log-temperature)
src/training/train_model.py                   (mode dispatch in _validation_query_f1)
src/training/training_pipeline.py             (strict=False checkpoint load)
src/evaluation/baselines.py                   (MLQDSMethod simplification_mode dispatch)
src/experiments/experiment_config.py          (new flags + residual_label_mode default)
src/experiments/experiment_cli.py             (new --simplification_mode etc flags)
src/experiments/run_ais_experiment.py         (pass new flags through)
src/experiments/experiment_pipeline_helpers.py (3 MLQDSMethod sites updated)
src/experiments/run_inference.py              (1 MLQDSMethod site updated)
```

## How to run a new experiment

Phase-8 scripts live at `/ceph/project/P8-1/AI_lab_setup/Bash_files/phase8_v3/`:

```
ML_phase8_v3_A1_range.sh
ML_phase8_v3_A2_knn.sh
ML_phase8_v3_A3_similarity.sh
ML_phase8_v3_A4_clustering.sh
ML_phase8_v3_B1_mixed.sh
```

Submit with `sbatch <file>`. They mirror `ML_phase5_1day_cr05_*` configs, so result
deltas are attributable to the V2_Revamp interventions. Output `.out` lands in
`AI_lab_setup/Jobs/output/phase5_15job_levers_sweep/` with `phase8_v3_` prefix
in the filename.

Key new bash variables in those scripts:

```
SIMPLIFICATION_MODE="score_coverage"   # or "hybrid" or "topk" for ablation
COVERAGE_LAMBDA=0.5                    # 0 = top-k, larger = more uniform
COVERAGE_SIGMA_FRACTION=0.5
USE_CLS_TOKEN="true"                   # "false" to ablate
RESIDUAL_LABEL_MODE="none"             # was "temporal" in phase-5
CHECKPOINT_F1_VARIANT="answer"         # was "combined" in phase-5
WRITE_SIMPLIFIED_CSV="false"           # gates --save_simplified_dir; saves ~30-50 MB/job
```

## Storage gotcha (CRITICAL)

`/ceph/project` is a **shared 50 TB quota** across all groups in `cs-26-sw-8-07`.
At time of writing it sits at ~99.9% and oscillates between 0–3 GB free as other
groups write. P8-1 itself uses only ~16 GB. When writes hit `EDQUOT`:

- `sacct` reports `FAILED 1:0` with no traceback
- `.out` and `.err` files are empty or truncated mid-line
- Edits to source files via Claude can produce **0-byte files** (the truncate
  succeeds, the rewrite fails)

Check before doing anything write-heavy:

```bash
echo "free: $(($(getfattr --only-values -n ceph.quota.max_bytes /ceph/project) - $(getfattr --only-values -n ceph.dir.rbytes /ceph/project))) bytes"
```

The `WRITE_SIMPLIFIED_CSV="false"` toggle exists because the simplified-eval CSVs
were the largest write per job. With the toggle off, only the small model
checkpoint (~760 KB) and the .out log are written.

For really big jobs, consider redirecting outputs to `/ceph/home/student.aau.dk/hq11zw/`
which has a 1 TB personal quota. The training-data CSVs in
`/ceph/project/P8-1/AISDATA/preprocessed_AIS_files/` are reads only and unaffected.

## Open questions for the next session

1. Did jobs 827556–827560 finish? Compare AnswerF1 to phase-5 baselines
   (`training_AI_08-05-26_01:11_phase5_1day_cr05_A1_range_826661.out` etc).
2. If phase-8 still loses on a type, the next intervention candidates are:
   - **Better labels** for kNN (Oracle-vs-uniform showed kNN labels are
     misaligned with what F1 rewards — Oracle 0.835 < uniform 0.872 on kNN at
     cr=0.05)
   - **Sweep `coverage_lambda`** ∈ {0, 0.25, 0.5, 1.0} — current default 0.5
     is a guess
   - **Disable CLS** for range/kNN if it doesn't help those types (CLS was
     justified primarily for similarity)
3. The dead `temporal_fraction` defaults at `train_model.py:505` and
   `baselines.py:45` are cosmetic but should be cleaned up at some point.
4. The `qds_project/` directory under `QDS/` is the older v1 codebase. Has
   diverged from `qds_v2/`; may be deprecatable.

## Pointers to the auto-memory

The Claude memory system at
`~/.claude/projects/-ceph-project-P8-1/memory/` has:

- `project_results_2026-05-08.md` — phase-5 result snapshot (replaces stale
  2026-02-08 snapshot)
- `project_v2_revamp_changes_2026-05-08.md` — same content as this section,
  in memory form
- `MEMORY.md` — index

If you're a fresh Claude session, those will auto-load. This worklog is for
when you need the on-disk source of truth, not just the cached snapshot.
