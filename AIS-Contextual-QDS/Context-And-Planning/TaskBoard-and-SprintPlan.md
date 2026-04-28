Good. Here is the task board I would actually work from.

This keeps the project centered on **query-driven simplification under a storage budget**, with maritime context used only when it helps preserve the target queries. That is the right lesson to take from the paper. 

# Board structure

Use 4 columns only:

* **Backlog**
* **Next**
* **In progress**
* **Done**

Do not add more workflow than that.

Also use 3 labels:

* **P0** must do
* **P1** should do
* **P2** nice to have

---

# Sprint 1: lock scope and make raw queries work

## T1. Lock the exact study scope

**Priority:** P0

**Task**
Choose:

* one region
* one vessel class
* one time span
* 2 query types
* 2 to 5 context zones
* 1 corridor layer

**Recommended default**

* region: one constrained Danish area
* vessel class: cargo or ferry
* time span: 2 to 4 weeks
* queries: zone entry, corridor membership

**Acceptance criteria**

* written 1-page scope note exists
* no open “maybe we also include...” items
* region polygons and query targets are fixed

---

## T2. Build raw AIS ingestion

**Priority:** P0

**Task**
Read AIS data and produce clean point records with:

* MMSI
* timestamp
* lat
* lon
* speed
* course
* nav status if available

**Acceptance criteria**

* one script loads raw data end-to-end
* output schema is fixed
* can load a sample and inspect 20 rows cleanly
* bad rows are counted and logged

---

## T3. Split AIS into trajectories

**Priority:** P0

**Task**
Define how points become trajectories.
Use simple rules first:

* sort by MMSI + time
* split on long time gaps
* optionally split on impossible jumps

**Acceptance criteria**

* one trajectory-building script exists
* output contains trajectory_id
* summary stats exist: number of trajectories, avg length, median length
* plot of 10 sample trajectories looks sane

---

## T4. Load maritime context layers

**Priority:** P0

**Task**
Load:

* coastline/land mask
* geofence polygons
* corridor polygon or buffered line

**Acceptance criteria**

* all layers are in one CRS
* can overlay trajectories and context in one map
* manual visual check passes for 20 cases

---

## T5. Implement raw query engine

**Priority:** P0

**Task**
Implement on original trajectories:

* zone entry/exit query
* corridor membership query

At first, simple yes/no per trajectory is enough.

**Acceptance criteria**

* can run both queries on raw data
* returns a set of trajectory_ids
* manual inspection of 20 trajectories matches expectations
* edge cases documented: touching boundary, grazing corridor, repeated entries

---

## T6. Freeze a test subset

**Priority:** P0

**Task**
Create a small stable development subset for fast iteration.

**Acceptance criteria**

* one saved subset exists
* contains 200 to 500 trajectories
* representative of the chosen region
* used by default in local experiments

---

### Sprint 1 done when

* you can answer both target queries on raw trajectories
* context layers are loaded and trusted
* you have a fixed scope and a stable dev subset

If Sprint 1 is not clean, do not move on.

---

# Sprint 2: build evaluation and dumb baselines

## T7. Define compression budgets

**Priority:** P0

**Task**
Choose retained-point ratios for all experiments.

**Recommended**

* 10%
* 20%
* 30%
* 40%
* 50%

**Acceptance criteria**

* one config file defines all budgets
* every simplifier uses the same budgets

---

## T8. Define evaluation metrics

**Priority:** P0

**Task**
For each budget, compute:

* zone-entry precision / recall / F1
* corridor-membership precision / recall / F1
* retained-point ratio
* simplification runtime
* false spatial artifacts count

**Acceptance criteria**

* metrics script exists
* metric outputs are saved in one consistent format
* one run can evaluate any simplifier

---

## T9. Implement uniform subsampling baseline

**Priority:** P0

**Task**
Keep points uniformly along each trajectory while always keeping first/last point.

**Acceptance criteria**

* works for all budgets
* never drops first/last point
* produces valid simplified trajectories

---

## T10. Implement geometry baseline

**Priority:** P0

**Task**
Use one standard geometry simplifier.
Douglas-Peucker is fine.

**Acceptance criteria**

* works for all budgets
* can target retained-point ratio or be tuned to approximate it
* output is comparable to other methods

---

## T11. Build visual comparison tool

**Priority:** P0

**Task**
For a chosen trajectory, show:

* raw trajectory
* simplified trajectory
* context layers
* query-relevant crossings

**Acceptance criteria**

* one script/notebook generates side-by-side plots
* can inspect at least 20 examples quickly
* output is good enough to debug errors

---

## T12. Run baseline benchmark v1

**Priority:** P0

**Task**
Compare raw baselines across all budgets.

**Acceptance criteria**

* one results table exists
* one plot per query type exists
* you can state clearly whether geometry preservation helps or hurts query fidelity

---

## T12b. Run low-budget stress benchmark

**Priority:** P0

**Task**
If the 10% to 50% grid preserves the primary query answers too well, run a lower-budget stress grid before implementing B3.

Recommended stress budgets:

* 1%
* 2%
* 3%
* 5%
* 7.5%
* 10%

Run this on `dev` first with the trusted label/evaluation mode. Use the result to identify the diminishing-returns region and choose the budgets for B3/B4 development. Run `eval` only after the stress range and comparison protocol are fixed.

**Acceptance criteria**

* one dev stress table exists
* one dev stress plot or curve summary exists
* the budget range where baselines begin to deteriorate is identified
* the first diminishing-returns point is described for each baseline
* the chosen method-development budgets are written down
* strict metrics are included: zone point F1, zone event exact rate, corridor event exact rate
* an eval confirmation run is scheduled but not used for tuning

---

### Sprint 2 done when

* evaluation pipeline is stable
* you can compare baselines at fixed budgets
* you know whether the standard 10% to 50% budgets are too easy
* if they are too easy, the low-budget stress range has been identified
* `eval` remains a confirmation split, not a tuning split

---

# Sprint 3: first query-driven method without context

## T13. Define point-level query importance

**Priority:** P0

**Task**
Assign higher importance to points that matter for preserving:

* zone entry result
* corridor membership result

Simple first version:

* high score if removing a point changes a primary query answer
* high score if removing a point changes strict event-count diagnostics
* high score for points adjacent to observed raw query state transitions, treated as query witnesses
* medium score near strong turns
* low score if redundant

Do not use static context priors in B3:

* no distance-to-zone-boundary term
* no distance-to-corridor-boundary term
* no generic boundary-proximity boost

**Acceptance criteria**

* scoring formula written down clearly
* B3/B4 feature boundary is documented
* no learned model required yet
* score can be computed for every point

---

## T14. Implement context-unaware query-driven simplifier

**Priority:** P0

**Task**
Build simplifier B3:

* compute point scores
* keep top-scoring points under budget
* always keep first/last point

This is query-driven, but not yet maritime-context-aware.

**Acceptance criteria**

* runs on all trajectories
* respects budget
* preserves primary query F1 at least as well as one dumb baseline
* improves at least one strict metric or reaches the same strict quality at a lower budget

---

## T15. Failure-case inspection for B3

**Priority:** P0

**Task**
Inspect where B3 fails:

* missed zone entry
* false corridor pass
* boundary drift
* redundant kept points

**Acceptance criteria**

* at least 25 failures categorized manually
* top 3 failure modes written down
* clear guess for what context should fix

---

### Sprint 3 done when

* you have a non-context query-driven baseline
* it has been evaluated on the stress budget range
* you understand where it breaks

This is a real milestone. Do not skip it.

---

# Sprint 4: add maritime context in the smallest useful way

## T16. Compute boundary features

**Priority:** P0

**Task**
For each point compute:

* distance to nearest zone boundary
* distance to corridor boundary or centerline
* inside/outside zone
* inside/outside corridor

**Acceptance criteria**

* features saved per point
* unit tests or spot checks on 20 cases
* distances look sane on plots

---

## T17. Compute transition features

**Priority:** P0

**Task**
For each point compute whether it is near:

* zone entry
* zone exit
* corridor entry
* corridor exit

These can be derived from neighboring membership changes.

**Acceptance criteria**

* transition flags exist
* checked on 20 manually inspected trajectories
* edge-case behavior documented

---

## T18. Implement context-aware scorer

**Priority:** P0

**Task**
Extend the score:

[
I(p)=I_{base}(p)+\alpha I_{boundary}(p)+\beta I_{transition}(p)
]

Keep it simple.

**Acceptance criteria**

* alpha and beta configurable
* static context features are used only in B4, not B3
* method runs end-to-end
* does not require retraining anything expensive

---

## T19. Run context-aware benchmark v1

**Priority:** P0

**Task**
Compare:

* uniform
* geometry
* query-driven without context
* query-driven with context

**Acceptance criteria**

* one summary table exists
* one plot per metric exists
* results include the stress budget range where baseline degradation is visible
* clear statement on whether context helps and at which budgets
* if primary query F1 is saturated, conclusion is based on strict metrics and/or lower retained-point budgets

---

## T20. Manual truth-check of wins

**Priority:** P0

**Task**
For cases where context-aware wins, verify the win is real.

**Acceptance criteria**

* manually inspect at least 20 “wins”
* confirm improvement is not due to label or query bug
* document 5 representative examples

---

### Sprint 4 done when

* the context-aware method beats the context-unaware one on at least one target query
* and you trust the result after manual inspection

---

# Sprint 5: ablations and tighten the method

## T21. Boundary-only ablation

**Priority:** P0

**Task**
Run with transition term removed.

**Acceptance criteria**

* result table exists
* can tell whether boundary proximity alone helps

---

## T22. Transition-only ablation

**Priority:** P0

**Task**
Run with boundary term removed.

**Acceptance criteria**

* result table exists
* can tell whether explicit transitions matter more than raw distance

---

## T23. Context sensitivity by geography

**Priority:** P1

**Task**
Split trajectories into:

* constrained-water subset
* less-constrained subset

**Acceptance criteria**

* comparison table exists
* can state where context helps most

---

## T24. Error taxonomy

**Priority:** P0

**Task**
Categorize remaining bad outcomes:

* missed entry
* false entry
* missed corridor pass
* false corridor pass
* shape drift near boundary
* simplifier kept redundant points

**Acceptance criteria**

* one short error document exists
* top failure modes ranked
* next-step method ideas grounded in real failures

---

## T25. Freeze MVP

**Priority:** P0

**Task**
Freeze code, configs, subset, and outputs.

**Acceptance criteria**

* one command runs the full benchmark
* one README explains how
* results reproducible on the dev subset

---

### Sprint 5 done when

* you know why the method works
* you know where it still fails
* the project is stable enough to extend later

---

# Backlog after MVP

Do these only after the above is stable.

## P1 extensions

* add route similarity query
* add simple global importance term inspired by MLSimp
* test second region
* test second vessel class
* add time-of-entry error metric
* add land-crossing penalty into scoring

## P2 extensions

* learned scorer
* GNN approximation of globality/uniqueness
* streaming version
* vessel interaction context
* weather or dynamic context

---

# Definition of done for the whole MVP

The MVP is done if all of this is true:

* raw zone/corridor queries are trustworthy
* all 4 baseline/method variants run at fixed budgets
* context-aware method improves over context-unaware query-driven simplification on at least one primary query metric, strict diagnostic, or lower-budget threshold
* results survive manual inspection
* code is reproducible
* you can explain exactly which preserved points caused the gain

If you cannot explain the last point, the project is not done.

---

# The 3 biggest traps

## Trap 1

Adding more context before proving the basic two-query setup works.

**Counter**
No new context layer until T20 is done.

## Trap 2

Jumping to a neural model too early.

**Counter**
No learned model until the simple context-aware scorer is benchmarked and ablated.

## Trap 3

Trusting aggregate metrics without visual inspection.

**Counter**
Every benchmark run must include manual inspection examples.
