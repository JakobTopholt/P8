# Experiments Module

This module is the orchestration layer for the v2 rebuild. It turns flat CLI arguments into structured config objects, derives deterministic sub-seeds, generates workloads, trains MLQDS, evaluates baselines with higher-is-better F1, and writes the example outputs.

## Files

| File | Purpose |
| --- | --- |
| `experiment_cli.py` | `argparse` parser for `run_ais_experiment.py`. |
| `experiment_config.py` | Dataclasses for data, query, model, baseline, visualization, workload, and seed config. |
| `experiment_pipeline_helpers.py` | Trajectory split, workload generation, training/evaluation, and result dumping. |
| `run_ais_experiment.py` | Main entry point. Loads CSV or synthetic data and prints the result tables. |

## CLI

`run_ais_experiment.py` accepts:

- `--csv_path`
- `--train_csv_path` / `--train_csv`
- `--eval_csv_path` / `--eval_csv`
- `--n_ships`
- `--n_points`
- `--n_queries`
- `--query_coverage` / `--target_query_coverage`
- `--max_queries`
- `--epochs`
- `--compression_ratio`
- `--model_type {baseline,turn_aware}`
- `--workload`
- `--train_workload_mix` / `--workload_mix_train`
- `--eval_workload_mix` / `--workload_mix_eval`
- `--checkpoint_selection_metric {loss,f1,uniform_gap}`
- `--f1_diagnostic_every`
- `--checkpoint_uniform_gap_weight`
- `--checkpoint_type_penalty_weight`
- `--seed`
- `--results_dir`

If `--train_csv_path` and `--eval_csv_path` are supplied together, the training CSV is used only for training and the evaluation CSV is used only for evaluation/simplified-output writing. If `--csv_path` is supplied instead, trajectories are split at trajectory level as before. If all CSV arguments are omitted, synthetic AIS data is generated with `n_ships`, `n_points`, and `seed`.

## Config Objects

- `DataConfig` - CSV paths, synthetic data size, and legacy train/validation split fractions.
- `QueryConfig` - workload size, optional target point coverage, workload label, train/eval workload mixes, and `similarity_top_k`.
- `ModelConfig` - embedding sizes, transformer depth, chunk size, dropout, compression ratio, ranking-loss hyperparameters, and checkpoint-selection diagnostics.
- `BaselineConfig` - baseline toggles such as `include_oracle`.
- `VisualizationConfig` - current hook for optional plotting.
- `TypedQueryWorkload` - padded query features, original typed query dicts, and query type IDs.
- `ExperimentConfig` - top-level container that nests the other configs.
- `SeedBundle` - deterministic sub-seeds for split, train workload, eval workload, and torch.

## Pipeline

1. Use separate train/eval trajectory sets when provided, otherwise split one dataset into train, validation, and test sets at trajectory level.
2. Generate independent train and eval typed query workloads from the respective trajectory sets; range/kNN anchors use the 70/30 density sampler described in `src/queries`.
3. Train the query-aware model and restore the epoch with the selected checkpoint metric. The default is training loss; `checkpoint_selection_metric=f1` uses exact held-out query F1 on a validation workload. `checkpoint_selection_metric=uniform_gap` also scores the fair `newUniformTemporal` baseline on the validation workload and penalizes checkpoints that hide weak range/kNN/similarity scores behind one strong type.
4. Evaluate MLQDS and baseline methods on the test set.
5. Write `results/example_run.json`, `results/matched_table.txt`, and `results/shift_table.txt` with aggregate/per-type F1 fields, retained-point spacing metrics such as `AvgPtGap`, plus `best_epoch`, `best_loss`, and `best_f1` training metadata.

## Workload Mixes

`resolve_workload_mixes` parses comma-separated strings such as `range=0.8,knn=0.2`. When no value is provided on the CLI, the helper falls back to `range=0.8,knn=0.2` for training and `range=0.2,clustering=0.8` for evaluation.
