# QDS Repository

QDS is the current shift-aware rebuild of the AIS query-driven simplification pipeline. It loads AIS trajectories, generates typed query workloads, trains a query-conditioned ranking model, and evaluates the resulting simplifier against learned and geometric baselines under matched and shifted workloads.

## What Is In This Folder

- `requirements.txt` - Python dependencies for the v2 stack.
- `src/` - package code for loading data, building queries, training models, and running experiments.
- `tests/` - regression tests that guard the rebuild.
- `results/` - retained reference outputs and benchmark artifacts.

## Environment And Smoke Checks

The sprint environment is the repository-level virtual environment at `../.venv`
when commands are run from `QDS`. Requirements are pinned in
`requirements.txt` for the local QDS checks.

```bash
cd QDS
../.venv/bin/python -m pip install -r requirements.txt
make check-env
make test
```

## Validation

The `tests/` folder focuses on the rebuild-specific regressions:

- `test_beats_random_in_distribution.py` - in-distribution performance guard.
- `test_no_cross_trajectory_attention_leakage.py` - attention leakage guard.
- `test_query_type_ids_required.py` - query type ID contract.
- `test_scaler_persisted.py` - scaler persistence.
- `test_topk_no_positional_bias.py` - deterministic top-k behavior.
- `test_training_does_not_collapse.py` - training stability.
