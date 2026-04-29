# Task Board And Sprint Plan

This is the active execution board for AIS-Contextual-QDS. It should stay short and operational.

For stable project framing, use:

- `ContextOutline.md` for the methodology narrative and research framing.
- `DefinedChoices-AIS-QueryDrivenSimplification.md` for locked decisions, semantics, metrics, and deferred choices.
- This file for current status, next tasks, and sprint acceptance criteria.

## Board Rules

Use four columns:

- Backlog
- Next
- In progress
- Done

Use three priorities:

- P0: required for the current MVP.
- P1: useful if the core method is stable.
- P2: extension work after the current study is reproducible.

## Current Status

The project is past the initial setup and baseline stage. The current work is preparing the `query_witness` method: a query-driven, context-unaware simplifier that can be compared against uniform and Douglas-Peucker before `context_aware_query_witness` adds maritime-context priors.

Done:

- Great Belt / Storebaelt first iteration scope is fixed.
- The default time window is the 10-day cargo-vessel iteration.
- Study-region, zone, and corridor context geometries have been audited and documented.
- Raw trajectory construction, dev/eval splitting, and hardcase subset creation are implemented.
- `optimized` and `segment_exact` query-semantics modes are implemented.
- Truth labels can coexist by `label_mode`.
- Segment-exact prediction uses cached adjacent simplified segments when available.
- Uniform and Douglas-Peucker reference methods run across fixed retained-point budgets.
- Summary exports include primary query metrics and stricter point/event diagnostics.
- HTML and QGIS inspection exports are available for manual review.
- Point-context feature computation exists for `context_aware_query_witness` preparation.

Current method ladder:

- Uniform subsampling: `uniform`.
- Douglas-Peucker geometry baseline: `douglas_peucker`.
- Query-witness context-free simplification: `query_witness`.
- Context-aware query-witness simplification: planned method `context_aware_query_witness`.
- Optional advanced query-context method after `context_aware_query_witness` is complete.

## Benchmark Evidence

Standard segment-exact baseline runs:

- Dev run tag: `segment_exact_full_grid_20260428T235751`
- Eval run tag: `segment_exact_eval_full_grid_20260429T010505`
- Budgets: 10%, 20%, 30%, 40%, 50%
- Result: primary trajectory-level query F1 is already saturated at these budgets.

Stress baseline runs:

- Dev run tag: `segment_exact_dev_stress_grid_20260429T013759`
- Refined dev run tag: `segment_exact_dev_refined_stress_grid_20260429T022828`
- Refined budgets: 0.5%, 1%, 1.5%, 2%, 3%, 5%
- Methods: `uniform`, `douglas_peucker`
- Evaluation mode: `segment_exact`
- Truth label mode: `segment_exact`

Refined stress findings:

- At 0.5%, primary query degradation is material.
- At 1%, uniform has one zone false positive and perfect corridor membership.
- At 1%, Douglas-Peucker has one zone false positive and one corridor false negative.
- From 1.5% upward, both reference methods preserve primary zone-entry and corridor-membership F1 on the dev split.
- Strict point-membership and event-count metrics still separate methods across 0.5% to 5%.
- Uniform is stronger than Douglas-Peucker on strict zone metrics and runtime in the refined stress run.
- Uniform shows clear diminishing returns after roughly 3% to 5%.
- Douglas-Peucker keeps improving through the tested range but remains weaker than uniform on strict zone metrics.

Current query-witness development budgets:

- 0.5% and 1% are the primary-query stress budgets.
- 1.5%, 2%, 3%, and 5% are the strict-metric comparison budgets.
- 7.5% and 10% are not current tuning budgets; keep them only for later curve-continuity reports if needed.
- `eval` should stay held back until `query_witness` scoring and comparison rules are fixed on `dev`.

Query-witness implementation run:

- Run tag: `query_witness_temporal_guard_dev_refined_stress_grid_20260429T043835Z`
- Methods: `uniform`, `douglas_peucker`, `query_witness`
- Budgets: 0.5%, 1%, 1.5%, 2%, 3%, 5%
- Split/subset: `dev`, `great_belt_iter1_10days_hardcase`
- Evaluation mode: `segment_exact`
- Truth label mode: `segment_exact`
- Result: canonical `query_witness` keeps the temporal guard and no longer uses the primary-answer fallback. `query_witness` improves strict zone diagnostics across the grid, preserves both primary query families from 3% upward, but trails the stronger primary baseline at 0.5%, 1%, 1.5%, and 2%.
- T15 note: `Context-And-Planning/Query-Witness-Failure-Inspection-T15.md`

## Next: Sprint 3

Sprint 3 creates the first query-driven method without static maritime-context priors. This is the required comparison point before claiming that maritime context helps.

### T13. Define Query-Witness Point Scoring

Priority: P0

Task:

Define a deterministic point-importance score for the `query_witness` method.

`query_witness` may use:

- First and last point anchors.
- Points adjacent to raw query state transitions.
- Points whose neighboring segment relation is query-relevant.
- Local trajectory-shape signals as tie-breakers, such as turn angle or local deviation.
- Trajectory-local evidence derived from the target query workload.

`query_witness` must not use:

- Distance to zone boundary.
- Distance to corridor boundary or centerline.
- Generic boundary-proximity boosts.
- Any static context prior; `query_witness` may use only the trajectory's own query witnesses and local shape evidence.

Acceptance criteria:

- The scoring formula is written down in `DefinedChoices-AIS-QueryDrivenSimplification.md` or a linked method note.
- The `query_witness` and `context_aware_query_witness` boundary remains clear.
- The score can be computed for every point.
- No learned model is required.
- Tie-breaking and budget handling are deterministic.

### T14. Implement Query-Witness Simplifier

Priority: P0

Task:

Implement a simplifier that keeps the highest-scoring query-witness points under each retained-point budget while always keeping first and last points.

Acceptance criteria:

- `query_witness` runs through the existing benchmark command.
- `query_witness` respects retained-point budgets.
- `query_witness` supports the refined stress budgets.
- `query_witness` can run with `EVALUATION_MODE=segment_exact` and `TRUTH_LABEL_MODE=segment_exact`.
- `query_witness` preserves primary query F1 at least as well as the stronger baseline in the key stress range, or failures are documented clearly.
- `query_witness` improves at least one strict metric or reaches the same strict quality at a lower retained-point budget.

### T15. Inspect Query-Witness Failures

Priority: P0

Task:

Manually inspect `query_witness` failures and compare them against `uniform` and `douglas_peucker` failures.

Failure categories:

- Missed zone entry.
- False zone entry.
- Missed corridor membership.
- False corridor membership.
- Simplified segment appears to cross a zone or corridor when the raw trajectory did not.
- Simplified segment removes a crossing present in the raw trajectory.
- Boundary drift without primary label failure.
- Redundant kept points.

Acceptance criteria:

- At least 25 failure or near-failure cases are categorized.
- The top failure modes are written down.
- Visual examples exist for representative errors.
- The result gives a clear reason for what `context_aware_query_witness` should add.

Sprint 3 is done when:

- `query_witness` has been benchmarked on the refined dev stress budgets.
- `query_witness` has been compared against uniform and Douglas-Peucker.
- `query_witness` failure cases have been inspected.
- The project has a defensible context-unaware query-driven baseline.

## Later: Sprint 4

Sprint 4 adds maritime context in the smallest useful way. Some feature computation already exists, so this sprint should start by auditing what is present instead of rebuilding it from scratch.

### T16. Audit And Complete Context Features

Priority: P0

Task:

Verify or complete per-point features for:

- Distance to nearest zone boundary.
- Distance to corridor boundary or centerline.
- Zone and corridor inside/outside state.
- Nearest zone identifier.
- Local shape features needed by `query_witness` and `context_aware_query_witness`.

Acceptance criteria:

- Feature outputs are stable and documented.
- At least 20 cases are spot-checked visually or numerically.
- Distances are computed in metric units.

### T17. Audit And Complete Transition Features

Priority: P0

Task:

Verify or complete per-point transition indicators derived from observed trajectory membership changes:

- Zone entry.
- Zone exit.
- Corridor entry.
- Corridor exit.
- Neighboring segment relation relevant to the query workload.

`query_witness` may use these trajectory-local query witnesses. Static boundary-distance and corridor-distance features remain `context_aware_query_witness`-only.

Acceptance criteria:

- Transition flags are deterministic.
- Edge cases are documented.
- Manual checks agree with `segment_exact` labels on representative cases.

### T18. Implement Context-Aware Query-Witness Scorer

Priority: P0

Task:

Extend `query_witness` with static maritime-context priors, for example boundary or corridor distance terms.

Acceptance criteria:

- Context weights are configurable.
- Static context features are used only in `context_aware_query_witness`, not `query_witness`.
- `context_aware_query_witness` runs end to end through the benchmark pipeline.
- The implementation remains explainable without a learned model.

### T19. Run Context-Aware Query-Witness Benchmark

Priority: P0

Task:

Compare:

- Uniform.
- Douglas-Peucker.
- `query_witness`.
- `context_aware_query_witness`.

Acceptance criteria:

- Results cover the refined stress budgets.
- Primary query metrics and strict diagnostics are reported.
- If primary F1 saturates, conclusions are based on strict metrics and lower-budget behavior.
- Eval is run only after dev choices are frozen.

### T20. Manual Truth-Check Of Wins

Priority: P0

Task:

Inspect cases where `context_aware_query_witness` appears to beat `query_witness`.

Acceptance criteria:

- At least 20 wins are manually checked.
- At least 5 representative wins are documented.
- Improvements are confirmed as real behavior, not label artifacts.

Sprint 4 is done when:

- `context_aware_query_witness` beats `query_witness` on a primary metric, strict diagnostic, or lower-budget threshold.
- The improvement survives manual inspection.
- The explanation of the improvement is tied to specific preserved points or segments.

## Later: Sprint 5

Sprint 5 tightens the method and turns the result into a reproducible MVP.

### T21. Boundary-Only Ablation

Priority: P0

Run `context_aware_query_witness` with transition terms removed.

Acceptance criteria:

- Result table exists.
- The contribution of boundary proximity can be stated clearly.

### T22. Transition-Only Ablation

Priority: P0

Run `context_aware_query_witness` with boundary terms removed.

Acceptance criteria:

- Result table exists.
- The contribution of transition features can be stated clearly.

### T23. Geography Sensitivity

Priority: P1

Split results into constrained-water and less-constrained subsets.

Acceptance criteria:

- Comparison table exists.
- The project can state where context helps most.

### T24. Error Taxonomy

Priority: P0

Create a short document ranking remaining failure types.

Acceptance criteria:

- Primary label FP/FN errors are separated from strict point/event errors.
- False-crossing and missed-crossing artifacts are separated from ordinary point-membership loss.
- Representative visual examples are linked.
- A decision is made on whether any artifact category should become a benchmark metric.

### T25. Freeze MVP

Priority: P0

Freeze code, configs, subset, and outputs for the first reproducible study.

Acceptance criteria:

- One command sequence reproduces the benchmark.
- The README explains the command sequence.
- Dev/eval usage is documented.
- The final result can explain which kept points caused the observed gain.

## Optional After Context-Aware Query-Witness

An advanced query-context method is not part of the MVP definition of done. It is worth considering only if `context_aware_query_witness` is implemented, benchmarked, manually inspected, and ablated with time left.

Possible directions:

- Adaptive `context_aware_query_witness` weights instead of fixed configured weights.
- A learned scorer trained on `query_witness` and `context_aware_query_witness` feature signals.
- Explicit query-context interaction terms.
- Global budget allocation across trajectories.
- Failure-recovery logic based on the `context_aware_query_witness` error taxonomy.

Acceptance criteria:

- The advanced method is compared directly against `context_aware_query_witness`.
- The advanced method does not replace the `query_witness` to `context_aware_query_witness` comparison.
- Any added complexity gives a measurable benefit on dev and survives eval confirmation.
- The result remains explainable enough to support the project argument.

## Backlog After MVP

P1:

- Add route-similarity query.
- Add time-of-entry error metric.
- Promote entry-count exactness from strict diagnostic to a target query.
- Add per-zone entry sequence or order query.
- Add corridor dwell or distance-inside-corridor query.
- Add a second region.
- Add a second vessel class.
- Add context sensitivity by geography if not completed in Sprint 5.
- Try adaptive or query-context interaction scoring if `context_aware_query_witness` is already complete.

P2:

- Learned scorer.
- Global uniqueness or global-importance term.
- Streaming version.
- Vessel-interaction context.
- Weather or other dynamic context.

## MVP Definition Of Done

The MVP is done when:

- Raw zone and corridor queries are trusted.
- Uniform, Douglas-Peucker, `query_witness`, and `context_aware_query_witness` run at fixed budgets.
- `context_aware_query_witness` improves over `query_witness` on at least one primary metric, strict diagnostic, or lower-budget threshold.
- Results survive manual inspection.
- Code and configs are reproducible.
- The result explains which preserved points or segments caused the gain.

The advanced query-context method is explicitly outside the MVP definition of done.

## Guardrails

- Do not add new context layers before `context_aware_query_witness` has been benchmarked and inspected.
- Do not tune on `eval`; use it for confirmation after dev choices are fixed.
- Do not claim a context benefit until `context_aware_query_witness` is compared against `query_witness`.
- Do not rely only on trajectory-level F1 when it is saturated.
- Do not move to learned models before the simple explainable scorer has been ablated.
- Do not start advanced query-context work until `context_aware_query_witness` has a clean result and error analysis.
