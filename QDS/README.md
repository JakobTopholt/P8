# AIS-QDS v2

AIS-QDS v2 is the current shift-aware rebuild of the AIS query-driven simplification pipeline. It loads AIS trajectories, generates typed query workloads, trains a query-conditioned ranking model, and evaluates the resulting simplifier against learned and geometric baselines under matched and shifted workloads.

## What Is In This Folder

- `requirements.txt` - Python dependencies for the v2 stack.
- `src/` - package code for loading data, building queries, training models, and running experiments.
- `tests/` - regression tests that guard the rebuild.
- `results/` - example outputs from the experiment runner.

## Quick Start

```bash
cd qds_v2
pip install -r requirements.txt
python -m src.experiments.run_ais_experiment --n_queries 128 --epochs 6 --workload mixed
```

To run on a CSV instead of synthetic data:

```bash
python -m src.experiments.run_ais_experiment \
  --csv_path "C:\path\to\ais.csv" \
  --n_queries 250 \
  --epochs 20 \
  --workload mixed \
  --compression_ratio 0.20 \
  --model_type baseline
```

To train on one CSV and evaluate/clean a different CSV without trajectory splitting:

```bash
python -m src.experiments.run_ais_experiment \
  --train_csv_path "C:\path\to\train.csv" \
  --eval_csv_path "C:\path\to\eval.csv" \
  --query_coverage 0.30 \
  --max_queries 1000 \
  --range_spatial_fraction 0.02 \
  --range_time_fraction 0.04 \
  --epochs 20 \
  --lr 0.0005 \
  --pointwise_loss_weight 0.25 \
  --gradient_clip_norm 1.0 \
  --workload mixed \
  --compression_ratio 0.20 \
  --model_type baseline
```

`--query_coverage` accepts either `0.30` or `30` for 30% point coverage. Coverage mode still emits exactly `--n_queries`; the target coverage is used to bias later query anchors toward points that have not yet been covered. `--max_queries` is accepted only as a positive legacy safety parameter. When `--eval_csv_path` is used and `--save_simplified_dir` is not set, the experiment writes only the eval-set simplified CSV under `AISDATA/ML_processed_AIS_files` as `ML_simplified_eval.csv`.

Range and kNN workloads focus query anchors on dense areas with a 70/30 sampler: 70% density-map weighted by lat/lon grid cell occupancy, 30% uniform from all points. The same sampler is used by coverage-targeted generation, so coverage still controls when generation stops while density controls where range/kNN queries are anchored.

For range-heavy runs, `--range_spatial_fraction` and `--range_time_fraction` control range-box half-widths as fractions of the dataset latitude/longitude and time spans. Lower these when you want many range queries without covering most of the dataset; for example, `0.02` spatial and `0.04` time gives smaller local boxes than the default `0.08` and `0.15`.

Use `--model_type turn_aware` to include the extra `turn_score` point feature. Workload mixes can be overridden with `--train_workload_mix` and `--eval_workload_mix` (or the `..._mix_train` / `..._mix_eval` aliases).

Training uses a ranking loss plus balanced pointwise BCE supervision. Exact final query F1 can optionally be used for checkpoint selection with `--checkpoint_selection_metric f1`; this creates a held-out validation workload and restores the epoch with the best validation query F1. If diagnostics collapse to `pred_std=0`, prefer lowering `--lr`, keeping `--gradient_clip_norm` enabled, and increasing query diversity before changing the model architecture.

## Architecture At A Glance

1. `src/data/` loads AIS CSV files or generates deterministic synthetic trajectories.
2. `src/queries/` builds typed query workloads and executes range, kNN, similarity, and clustering queries.
3. `src/training/` computes typed F1-contribution labels, trains the model, restores the selected checkpoint epoch, and persists the scaler and checkpoint artifacts.
4. `src/models/` contains the query-conditioned trajectory transformer and the turn-aware variant.
5. `src/simplification/` keeps the highest-scoring points per trajectory with deterministic tie-breaking.
6. `src/evaluation/` runs learned and baseline methods and reports aggregate and per-type F1 scores.
7. `src/experiments/` wires the full pipeline together through the CLI.
8. `src/visualization/` is currently a placeholder package for future plotting hooks.

## Outputs

The experiment runner writes three files into `results/`:

- `example_run.json` - config, workload mixes, per-method metrics, training history, and selected-checkpoint metadata.
- `matched_table.txt` - fixed-width comparison table for the evaluation workload.
- `shift_table.txt` - shift table comparing the train workload against the eval workload.

## Validation

The `tests/` folder focuses on the rebuild-specific regressions:

- `test_beats_random_in_distribution.py` - in-distribution performance guard.
- `test_no_cross_trajectory_attention_leakage.py` - attention leakage guard.
- `test_query_type_ids_required.py` - query type ID contract.
- `test_scaler_persisted.py` - scaler persistence.
- `test_topk_no_positional_bias.py` - deterministic top-k behavior.
- `test_training_does_not_collapse.py` - training stability.

For the legacy v1 system, see [qds_project/README.md](../qds_project/README.md).
