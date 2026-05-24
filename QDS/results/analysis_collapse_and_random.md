# Analysis: Model Collapse, Random Baseline, and Fixes

_Extracted from chat session — April 29, 2026_

---

## Response 1: Collapse and Random Analysis

**Trigger:** *"can analyse tell why they collapse all the time and tell other issus you find and what you can see that works and inaddtion i dont understand why random is better"*

I checked the code and the new logs. Short version: the model is mostly collapsing, Random looks correctly implemented, and the tables are misleading in two places.

The model is still trained from F1-derived labels, but not by directly optimizing final query F1. It builds per-point labels from query answer-set F1 contribution in [importance_labels.py](QDS/src/training/importance_labels.py#L86-L154), rescales them, then trains a ranking loss plus small MSE in [train_model.py](QDS/src/training/train_model.py#L292-L333). So “trained on F1” means “trained on point labels derived from F1,” not “trained end-to-end against final evaluated F1.”

Main problems I found:

1. `best_loss` is selecting collapsed models.  
   In your `local_mixed` run, epoch 3 had the only positive `avg_tau=+0.028`, but training restored epoch 33 because it had the lowest loss. That happens because [train_model.py](QDS/src/training/train_model.py#L421-L438) saves by loss, and [train_model.py](QDS/src/training/train_model.py#L473-L478) restores that loss-best model. With sparse/tied labels, a near-constant model can have good loss but useless ranking. Fix: choose best checkpoint by validation query F1, or at minimum “best non-collapsed tau,” not raw training loss.

2. Collapsed MLQDS behaves almost like Random.  
   When scores are constant, simplification still adds tiny deterministic jitter before top-k selection in [simplify_trajectories.py](QDS/src/simplification/simplify_trajectories.py#L20-L25). So a collapsed model still keeps a pseudo-random 10% per trajectory. That explains why MLQDS can be close to Random even when `pred_std=0.000` and `avg_tau=0.000`.

3. Random appears to work correctly.  
   Random is just seeded random scores per point, then the same per-trajectory top-k simplifier as MLQDS in [baselines.py](QDS/src/evaluation/baselines.py#L72-L82). It is strong because evaluation F1 compares trajectory-ID answer sets in [evaluate_methods.py](QDS/src/evaluation/evaluate_methods.py#L49-L54). For range/kNN, if Random keeps any point from a queried trajectory, that trajectory is still present in the answer. At 10% retention, this often works surprisingly well.

4. The `Oracle` row is currently not comparable.  
   MLQDS/Random are evaluated on `test_points` with `eval_workload`, but Oracle is evaluated on `train_points` with `train_workload` in [experiment_pipeline_helpers.py](QDS/src/experiments/experiment_pipeline_helpers.py#L241-L261). So Oracle lower than Random does not mean Random beats the true oracle. It means the Oracle row is a different experiment. Fix: compute oracle labels on `test_points/test_boundaries/eval_workload`, or rename the current row to `TrainOracle`.

5. `query_coverage` is hurting training.  
   With coverage mode, `n_queries=100` is not treated as a minimum. Range stopped at 17 queries, local_mixed stopped at 28 queries, while pure kNN hit 1000 queries and still only covered `0.36%/0.59%`. The loop stops as soon as coverage is reached, or at max queries, in [query_generator.py](QDS/src/queries/query_generator.py#L363-L386). Fix: require both `len(queries) >= n_queries` and coverage target, or disable `--query_coverage` for these training runs.

6. kNN coverage and labels are still mismatched.  
   kNN execution now returns nearest distinct trajectories, but kNN coverage still measures nearest points around the anchor in [query_generator.py](QDS/src/queries/query_generator.py#L233-L247). The kNN label also marks every point in the returned trajectory’s time window, not the nearest point(s) that actually preserve kNN membership in [importance_labels.py](QDS/src/training/importance_labels.py#L108-L117). This makes kNN labels noisy, and explains why kNN Oracle can look weak.

My recommended fix order:

1. Fix Oracle evaluation first, so the table is trustworthy.
2. Change checkpoint selection from `best_loss` to validation query F1 or best non-collapsed tau.
3. Change coverage generation so `n_queries` is a minimum, and do not use point-coverage mode for pure kNN until kNN coverage is redefined.
4. Rewrite range/kNN labels around trajectory-hit preservation: for range, reward keeping at least one in-box point per matching trajectory; for kNN, reward nearest representative points that keep the trajectory in the top-k.
5. Add diagnostics that print `pred_std` with more decimals plus positive-label fraction and skipped-window counts.

For your next test without code changes, I would not use `--query_coverage 0.3` for pure kNN or local_mixed. Use a fixed query count, for example `--workload local_mixed --n_queries 300`, because the current coverage mode is giving you either too few range/local queries or an enormous but still low-coverage kNN workload.

---

## Response 2: Detailed Fix Plan and How Random Works

**Trigger:** *"okay fix all 5 issus and let increase the amount coverage and please fix train and please go more detail how random work. becuase in assume randomw remove random point on til same amout of datapot as ml csv or what normal pratic in ai train?"*

I can fix these, but not from this current chat mode: you are in **Ask mode**, which is read-only, so I’m not allowed to edit files. Switch to Agent/Edit mode and I can apply the patches directly. For now, here is the exact fix set I would make.

**Fix The 5 Issues**

1. **Fix Oracle evaluation**
   
   Current bug: Oracle is evaluated on train data while MLQDS/Random are evaluated on eval data in [experiment_pipeline_helpers.py](QDS/src/experiments/experiment_pipeline_helpers.py#L241-L261).

   Correct fix: compute eval oracle labels from `test_points`, `test_boundaries`, and `eval_workload`, then evaluate Oracle on the same eval set as MLQDS/Random.

   Conceptually:

   ```python
   eval_labels, _ = compute_typed_importance_labels(
       points=test_points,
       boundaries=test_boundaries,
       typed_queries=eval_workload.typed_queries,
       seed=seeds.torch_seed,
   )

   oracle = OracleMethod(labels=eval_labels, workload_mix=eval_mix)
   matched[oracle.name] = evaluate_method(
       method=oracle,
       points=test_points,
       boundaries=test_boundaries,
       typed_queries=eval_workload.typed_queries,
       workload_mix=eval_mix,
       compression_ratio=config.model.compression_ratio,
   )
   ```

2. **Stop selecting collapsed models by best training loss**
   
   The previous “best loss” change is now clearly wrong for this data. In your logs, collapsed epochs often have the best loss. The restore happens in [train_model.py](QDS/src/training/train_model.py#L421-L478).

   Fix: select checkpoint by a quality score, not raw loss. Best option is validation query F1. Simpler immediate fix: only save diagnostic epochs, penalize collapse, and prefer `avg_tau`.

   Good rule:

   ```python
   quality = avg_tau
   if stats["pred_std"] < 1e-3:
       quality -= 1.0
   ```

   Then restore the epoch with highest `quality`, not lowest `loss`. Keep `best_loss` only as logging.

3. **Increase and repair query coverage**
   
   Current script uses [ML_transformer.sh](AI_lab_setup/Bash_files/ML_transformer.sh#L44-L50):

   ```bash
   N_QUERIES=100
   QUERY_COVERAGE=0.30
   MAX_QUERIES=1000
   ```

   Current generator stops when coverage is reached, even if it made fewer than `N_QUERIES`, in [query_generator.py](QDS/src/queries/query_generator.py#L363-L386). That is why range trained with only 17 queries and local_mixed with only 28.

   Fix the generator so it stops only when both are true:

   ```python
   if len(typed) >= n_queries and current_coverage >= coverage_target:
       break
   ```

   Then increase the script to something like:

   ```bash
   N_QUERIES=250
   QUERY_COVERAGE=0.50
   MAX_QUERIES=5000
   ```

   For pure `knn`, do not use current point coverage yet. Run fixed query count instead, because kNN point coverage is currently misleadingly tiny.

4. **Fix range/kNN labels to match trajectory-hit F1**
   
   Current labels mark broad support regions in [importance_labels.py](QDS/src/training/importance_labels.py#L101-L117). That does not match the real metric well. The metric only cares whether a trajectory appears in the simplified query answer, not how many support points are retained.

   Fix labels so each query/trajectory contributes a limited amount of value. For range, distribute gain per returned trajectory across its in-box points instead of giving every support point full gain. For kNN, label the nearest representative point or a small neighborhood around it per returned trajectory, not the full time window.

   This turns the training problem from “score many duplicate points high” into “keep at least one useful point per query-hit trajectory.”

5. **Add diagnostics that show why training fails**
   
   Current log rounds `pred_std` to three decimals, so small but nonzero spread prints as `0.000` in [train_model.py](QDS/src/training/train_model.py#L427-L433).

   Add:

   ```python
   pred_std={stats['pred_std']:.6g}
   ```

   Also print per active type:

   ```text
   pos_frac_t0
   label_p95_t0
   usable_rank_pairs_t0
   skipped_zero_windows_t0
   ```

   That will tell us if training fails because labels are too sparse, all tied, or skipped in most windows.

**How Random Works**

Random is not training. It is a baseline. In [baselines.py](QDS/src/evaluation/baselines.py#L72-L82), it creates one random score for every point:

```python
scores = torch.rand((points.shape[0],), generator=g)
return simplify_with_scores(scores, boundaries, compression_ratio)
```

Then [simplify_trajectories.py](QDS/src/simplification/simplify_trajectories.py#L28-L42) keeps the top `compression_ratio` points **inside each trajectory**. So yes: Random keeps approximately the same amount of data points as MLQDS, because both use the same compression ratio. It does not remove exactly the same number every time globally, because each trajectory gets its own `ceil(compression_ratio * n)` and first/last points are always retained.

This is normal AI practice: compare a learned model against simple baselines at the same budget. Random answers: “does the model beat chance under the same compression?” UniformTemporal answers: “does the model beat regular sampling?” DouglasPeucker answers: “does the model beat geometry-only simplification?”

The reason Random is strong here is the evaluation metric. Range/kNN F1 is computed over trajectory IDs, not point IDs. If Random keeps just one point from a queried trajectory, that trajectory is still recovered. At 10% per trajectory retention, that happens often. So Random high score is not necessarily a code bug; it means the current metric is forgiving and the model needs trajectory-hit-aware labels to beat it reliably.

Recommended next run after patches:

```bash
WORKLOAD="local_mixed"
N_QUERIES=250
QUERY_COVERAGE=0.50
MAX_QUERIES=5000
PATIENCE=8
```

For pure kNN, use fixed query count first:

```bash
WORKLOAD="knn"
N_QUERIES=500
# omit --query_coverage until kNN coverage is redefined
```
