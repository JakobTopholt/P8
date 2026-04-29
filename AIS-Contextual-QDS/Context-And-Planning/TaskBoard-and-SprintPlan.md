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

The project is past the initial setup and baseline stage. The current work is preparing the B3 method: a query-driven, context-unaware simplifier that can be compared against uniform and Douglas-Peucker before B4 adds maritime-context priors.

Done:

- Great Belt / Storebaelt first iteration scope is fixed.
- The default time window is the 10-day cargo-vessel iteration.
- Study-region, zone, and corridor context geometries have been audited and documented.
- Raw trajectory construction, dev/eval splitting, and hardcase subset creation are implemented.
- `optimized` and `segment_exact` query-semantics modes are implemented.
- Truth labels can coexist by `label_mode`.
- Segment-exact prediction uses cached adjacent simplified segments when available.
- Uniform and Douglas-Peucker baselines run across fixed retained-point budgets.
- Summary exports include primary query metrics and stricter point/event diagnostics.
- HTML and QGIS inspection exports are available for manual review.
- Point-context feature computation exists for B4 preparation.

Current method ladder:

- B1: uniform subsampling.
- B2: Douglas-Peucker geometry baseline.
- B3: query-driven, context-unaware simplification.
- B4: query-driven, context-aware simplification.

## Baseline Evidence

Standard segment-exact baseline runs:

- Dev run tag: `segment_exact_full_grid_20260428T235751`
- Eval run tag: `segment_exact_eval_full_grid_20260429T010505`
- Budgets: 10%, 20%, 30%, 40%, 50%
- Result: primary trajectory-level query F1 is already saturated at these budgets.

Stress baseline runs:

- Dev run tag: `segment_exact_dev_stress_grid_20260429T013759`
- Refined dev run tag: `segment_exact_dev_refined_stress_grid_20260429T022828`
- Refined budgets: 0.5%, 1%, 1.5%, 2%, 3%, 5%
- Methods: `uniform`, `dp`
- Evaluation mode: `segment_exact`
- Truth label mode: `segment_exact`

Refined stress findings:

- At 0.5%, primary query degradation is material.
- At 1%, uniform has one zone false positive and perfect corridor membership.
- At 1%, Douglas-Peucker has one zone false positive and one corridor false negative.
- From 1.5% upward, both baselines preserve primary zone-entry and corridor-membership F1 on the dev split.
- Strict point-membership and event-count metrics still separate methods across 0.5% to 5%.
- Uniform is stronger than Douglas-Peucker on strict zone metrics and runtime in the refined stress run.
- Uniform shows clear diminishing returns after roughly 3% to 5%.
- Douglas-Peucker keeps improving through the tested range but remains weaker than uniform on strict zone metrics.

Current B3 development budgets:

- 0.5% and 1% are the primary-query stress budgets.
- 1.5%, 2%, 3%, and 5% are the strict-metric comparison budgets.
- 7.5% and 10% are not current tuning budgets; keep them only for later curve-continuity reports if needed.
- `eval` should stay held back until B3 scoring and comparison rules are fixed on `dev`.

## Next: Sprint 3

Sprint 3 creates the first query-driven method without static maritime-context priors. This is the required comparison point before claiming that maritime context helps.

### T13. Define B3 Point Scoring

Priority: P0

Task:

Define a deterministic point-importance score for B3.

B3 may use:

- First and last point anchors.
- Points adjacent to raw query state transitions.
- Points whose neighboring segment relation is query-relevant.
- Local trajectory-shape signals as tie-breakers, such as turn angle or local deviation.
- Trajectory-local evidence derived from the target query workload.

B3 must not use:

- Distance to zone boundary.
- Distance to corridor boundary or centerline.
- Generic boundary-proximity boosts.
- Any static context prior; B3 may use only the trajectory's own query witnesses and local shape evidence.

Acceptance criteria:

- The scoring formula is written down in `DefinedChoices-AIS-QueryDrivenSimplification.md` or a linked method note.
- The B3/B4 boundary remains clear.
- The score can be computed for every point.
- No learned model is required.
- Tie-breaking and budget handling are deterministic.

### T14. Implement B3 Simplifier

Priority: P0

Task:

Implement a simplifier that keeps the highest-scoring B3 points under each retained-point budget while always keeping first and last points.

Acceptance criteria:

- B3 runs through the existing benchmark command.
- B3 respects retained-point budgets.
- B3 supports the refined stress budgets.
- B3 can run with `EVALUATION_MODE=segment_exact` and `TRUTH_LABEL_MODE=segment_exact`.
- B3 preserves primary query F1 at least as well as the stronger baseline in the key stress range, or failures are documented clearly.
- B3 improves at least one strict metric or reaches the same strict quality at a lower retained-point budget.

### T15. Inspect B3 Failures

Priority: P0

Task:

Manually inspect B3 failures and compare them against B1/B2 failures.

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
- The result gives a clear reason for what B4 should add.

Sprint 3 is done when:

- B3 has been benchmarked on the refined dev stress budgets.
- B3 has been compared against uniform and Douglas-Peucker.
- B3 failure cases have been inspected.
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
- Local shape features needed by B3/B4.

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

B3 may use these trajectory-local query witnesses. Static boundary-distance and corridor-distance features remain B4-only.

Acceptance criteria:

- Transition flags are deterministic.
- Edge cases are documented.
- Manual checks agree with `segment_exact` labels on representative cases.

### T18. Implement B4 Context-Aware Scorer

Priority: P0

Task:

Extend B3 with static maritime-context priors, for example boundary or corridor distance terms.

Acceptance criteria:

- Context weights are configurable.
- Static context features are used only in B4, not B3.
- B4 runs end to end through the benchmark pipeline.
- The implementation remains explainable without a learned model.

### T19. Run Context-Aware Benchmark

Priority: P0

Task:

Compare:

- Uniform.
- Douglas-Peucker.
- B3 query-driven without context.
- B4 query-driven with context.

Acceptance criteria:

- Results cover the refined stress budgets.
- Primary query metrics and strict diagnostics are reported.
- If primary F1 saturates, conclusions are based on strict metrics and lower-budget behavior.
- Eval is run only after dev choices are frozen.

### T20. Manual Truth-Check Of Wins

Priority: P0

Task:

Inspect cases where B4 appears to beat B3.

Acceptance criteria:

- At least 20 wins are manually checked.
- At least 5 representative wins are documented.
- Improvements are confirmed as real behavior, not label artifacts.

Sprint 4 is done when:

- B4 beats B3 on a primary metric, strict diagnostic, or lower-budget threshold.
- The improvement survives manual inspection.
- The explanation of the improvement is tied to specific preserved points or segments.

## Later: Sprint 5

Sprint 5 tightens the method and turns the result into a reproducible MVP.

### T21. Boundary-Only Ablation

Priority: P0

Run B4 with transition terms removed.

Acceptance criteria:

- Result table exists.
- The contribution of boundary proximity can be stated clearly.

### T22. Transition-Only Ablation

Priority: P0

Run B4 with boundary terms removed.

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

P2:

- Learned scorer.
- Global uniqueness or global-importance term.
- Streaming version.
- Vessel-interaction context.
- Weather or other dynamic context.

## MVP Definition Of Done

The MVP is done when:

- Raw zone and corridor queries are trusted.
- Uniform, Douglas-Peucker, B3, and B4 run at fixed budgets.
- B4 improves over B3 on at least one primary metric, strict diagnostic, or lower-budget threshold.
- Results survive manual inspection.
- Code and configs are reproducible.
- The result explains which preserved points or segments caused the gain.

## Guardrails

- Do not add new context layers before B4 has been benchmarked and inspected.
- Do not tune on `eval`; use it for confirmation after dev choices are fixed.
- Do not claim a context benefit until B4 is compared against B3.
- Do not rely only on trajectory-level F1 when it is saturated.
- Do not move to learned models before the simple explainable scorer has been ablated.
