Good. Then build this like an engineering plan, not a paper pitch.

The core definition should stay the one from the paper: **given a storage budget, simplify trajectories so query results on the simplified database stay close to the original database**. The paper’s version is query-driven rather than error-driven, and it adjusts point importance using query workload after estimating point importance from the trajectory itself. 

## Working thesis direction

**Geofence-aware query-driven simplification for Danish AIS**

That is small enough to finish and still clearly extends the paper.

## What you are actually building

A simplification pipeline that:

1. takes AIS trajectories,
2. assigns point importance,
3. boosts points that matter for your target maritime queries,
4. preserves those points under a compression budget,
5. evaluates whether query answers are still correct.

The maritime context is not the goal. It is just extra signal for deciding which points matter for the queries you care about.

## Scope lock

Do not change this unless you have a working baseline.

**Region**

* Pick one small Danish area only.
* Best choice: a constrained area with clear spatial semantics.
* Example types: port approach, anchorage area, narrow strait, one corridor/fairway zone.

**Vessel class**

* Pick one only.
* Cargo or ferry is fine.
* Do not mix all vessel types early.

**Time span**

* Start with 2 to 4 weeks.
* Enough to get repeated patterns, small enough to debug.

**Context layers**
Only these:

* land/coastline mask
* 2 to 5 geofenced maritime zones
* 1 corridor / fairway / TSS-like polygon or polyline buffer

**Query types**
Start with only two:

* **Q1: zone entry/exit query**
  Which trajectories entered zone Z during time interval T?
* **Q2: corridor membership query**
  Which trajectories passed through corridor C during time interval T?

Current corridor semantics use covered points or adjacent segment overlap above the configured minimum overlap distance. The active 10-day iteration uses `min_corridor_overlap_meters: 1.0`.

Only add route similarity later.

## What not to do

Do not start with:

* weather
* bathymetry
* anomaly detection
* vessel interactions
* streaming
* full Denmark
* full MLSimp reproduction including diffusion

That is how you lose the project.

## Thesis objective

Use this as the internal objective:

**Design and test a context-aware query-driven simplification method for AIS that preserves geofence and corridor query answers better than context-unaware simplification at the same compression rate.**

That is tight and testable.

## Research questions

Use only three.

**RQ1**
Can static AIS-relevant context improve query-driven simplification for geofence and corridor queries?

**RQ2**
Which context-sensitive points matter most: near-boundary points, entry/exit points, or corridor-transition points?

**RQ3**
At what compression rates does context-aware simplification outperform context-unaware simplification the most?

That is enough.

## Hypotheses

Keep them simple.

**H1**
Context-aware simplification will improve zone-entry and corridor-query fidelity over geometry-only and context-unaware baselines.

**H2**
The gain will come mainly from preserving boundary-adjacent and transition points.

**H3**
The gain will be largest in constrained waters and at medium/high compression.

## Minimal data specification

You want a dataset you can actually inspect manually.

Start with:

* 5,000 to 20,000 trajectories after cleaning
* one vessel class
* one region
* 2 to 4 weeks
* static context layers as polygons/lines

For each AIS point, keep only:

* MMSI
* timestamp
* lat/lon
* speed
* course
* maybe nav status if available

For each point, derive:

* inside which zone, if any
* inside corridor or not
* distance to nearest zone boundary
* distance to nearest coastline
* whether zone/corridor membership changes at this point relative to neighbors

That is enough for version 1.

## Method plan

Do not begin with a heavy model. Build the smallest useful selector first.

### Method v0: baseline simplification

Implement:

* uniform subsampling
* Douglas-Peucker or another geometry baseline
* one simple query-driven baseline without context

The point is to establish that query-driven evaluation changes what “good” means.

### Method v1: context-unaware query-driven scorer

Build a score per point:

[
I_{\text{base}}(p) = w_1 \cdot I_{\text{shape}}(p) + w_2 \cdot I_{\text{query}}(p)
]

Where:

* (I_{\text{shape}}): simple local importance, such as deviation/turning/curvature proxy
* (I_{\text{query}}): whether the point acts as evidence for preserving the target query answers or strict event counts

You do not need a GNN yet.

A practical version:

* high score if removing the point changes zone membership outcome
* high score if removing the point changes corridor membership outcome
* high score if removing the point changes strict entry-count diagnostics
* high score if the point is adjacent to an observed raw query state transition
* medium score if the point is a strong turn / route-shape point
* low score if redundant

B3 must stay context-minimal. It may use query witnesses and trajectory-local shape evidence, but it must not use continuous static context priors such as distance to a zone boundary or distance to a corridor boundary.

### Method v2: context-aware query-driven scorer

Extend it:

[
I(p) = I_{\text{base}}(p) + \alpha \cdot I_{\text{boundary}}(p) + \beta \cdot I_{\text{transition}}(p)
]

Where:

* (I_{\text{boundary}}(p)): higher near zone or corridor boundaries
* (I_{\text{transition}}(p)): high if the point marks entering/exiting a zone or corridor
* static context can act as a prior even when a point has not yet been proven query-critical

This is your first real method.

### Method v3: optional MLSimp-inspired upgrade

Only after v2 works:

* add a better trajectory-level importance term inspired by the paper’s local/global framing
* maybe approximate “globality” and “uniqueness” without reproducing the whole model 

Do not touch diffusion until the simpler version is already strong.

## How to make it truly query-driven

Be strict here.

A method is not query-driven just because it uses AIS context.

It is query-driven if point selection is driven by whether simplification changes answers to your target queries.

So define query-aware labels or penalties like this:

For each original trajectory:

* did it enter zone Z?
* did it pass corridor C?

For a candidate simplified trajectory:

* does it still produce the same yes/no answer?

Then reward points that preserve those answers.

That is the heart of the project.

## Evaluation plan

Use four groups of metrics.

### 1. Query fidelity

Primary metrics:

* zone-entry precision / recall / F1
* corridor-membership precision / recall / F1

Strict development metrics:

* zone point-membership F1
* zone entry-count exact rate
* corridor point-membership F1
* corridor entry-count exact rate

If the primary yes/no metrics are saturated, use these strict metrics and lower retained-point budgets to compare methods.

Do not set a fixed acceptance threshold before the stress curves are visible. First compare the full metric-vs-budget curves and identify where diminishing returns begin for each simplification strategy.

Interpret early saturation carefully. For trajectory-level yes/no queries, perfect F1 at low retained-point ratios can be normal: the simplified path only needs to preserve enough evidence for the final answer. That does not prove the simplified path preserved all query-relevant behavior, so strict diagnostics remain central.

Optional later:

* time-of-entry error
* false entry/exit count
* entry count as a primary query
* per-zone entry sequence/order
* corridor dwell/distance inside corridor
* sub-interval queries inside the trajectory window

### 2. Compression

* retained-point ratio
* points removed

### 3. Efficiency

* simplification runtime
* query runtime if you build a simple query engine

### 4. Sanity / spatial validity

* land crossing count
* false zone crossings created by simplification
* missed zone crossings caused by simplification
* false corridor passes created by simplification
* missed corridor passes caused by simplification

These sanity metrics matter. Otherwise you can get fake wins. The current benchmark already counts trajectory-level false positives and false negatives for zone entry and corridor membership. Fine-grained false crossing artifacts, such as a simplified segment visually crossing a zone that the raw trajectory never entered, should be inspected and later counted as a dedicated artifact taxonomy.

## Baselines you should actually run

You do not need many.

**B1: Uniform subsampling**
Cheap and surprisingly hard to beat in some settings.

**B2: Douglas-Peucker / geometry baseline**
Shows what happens when you optimize shape instead of query answers.

**B3: Query-driven without context**
This is the crucial baseline.

**B4: Your context-aware query-driven method**
This is the main comparison.

That is enough for a strong first stage.

## First experiment sequence

Do these in order.

### Experiment 0: raw data sanity

Goal:

* verify zone/corridor queries work on raw trajectories
* manually inspect 50 to 100 trajectories

Output:

* trusted preprocessing pipeline

### Experiment 1: baseline compression vs query fidelity

Compare B1 and B2 across compression rates:

* 10%, 20%, 30%, 40%, 50% retained points

Output:

* proof that geometry-based simplification hurts query fidelity

If the primary query metrics are already perfect at 10%, run a stress grid before implementing B3:

* 1%, 2%, 3%, 5%, 7.5%, 10% retained points

Output:

* the budget range where baselines start to lose query or strict-event fidelity
* the diminishing-returns point for each baseline
* the budgets to use for B3/B4 development

Run this stress search on `dev` first. Keep `eval` as a confirmation split after budgets and comparison rules are fixed.

### Experiment 2: query-driven without context

Add B3.

Output:

* proof that query-driven objective helps on the stress budgets
* or proof that it preserves the same query quality at lower retained-point ratios

### Experiment 3: context-aware query-driven

Add B4.

Output:

* proof that maritime context helps over pure query-driven
* if primary query F1 is saturated, show the gain through strict metrics, lower budget thresholds, or fewer spatial artifacts

### Experiment 4: ablation

Turn off one context term at a time:

* no boundary term
* no transition term
* no corridor context
* no zone context

Output:

* explanation of where the improvement comes from

### Experiment 5: regional stress test

Split trajectories into:

* constrained area subset
* less constrained/open subset

Output:

* proof that context helps more where it should

That is a clean progression.

## Deliverables by stage

### Stage 1 deliverable

A small clean AIS dataset with context overlays and working query labels.

### Stage 2 deliverable

Three baseline simplifiers and a reproducible evaluation script.

### Stage 3 deliverable

First context-aware method with results across compression budgets.

### Stage 4 deliverable

Ablation plots and error analysis.

If you do just that, you already have a real thesis core.

## 12-week execution plan

### Weeks 1–2

Lock scope and build data pipeline.

* choose region
* choose vessel class
* clean AIS
* split into trajectories
* load context polygons
* implement zone/corridor membership logic
* manually inspect outputs

Done means:

* you can answer Q1 and Q2 on raw data reliably

### Weeks 3–4

Build evaluation framework.

* define trajectory-level query labels
* define metrics
* create train/val/test split or time-based split
* add visualization for raw vs simplified trajectories

Done means:

* you can evaluate any simplifier consistently

### Weeks 5–6

Implement baselines.

* uniform subsampling
* DP or similar geometry simplifier
* simple query-driven scorer without context

Done means:

* baseline comparison table exists

### Weeks 7–8

Implement context-aware scorer.

* boundary distance feature
* transition detection feature
* combine with query-aware score
* run first comparisons

Done means:

* first evidence of gain from context

### Weeks 9–10

Run ablations and debug.

* remove one context feature at a time
* examine failure cases
* inspect wrong answers manually

Done means:

* you understand why it works or fails

### Weeks 11–12

Tighten and document.

* rerun best experiments cleanly
* freeze code
* write internal notes
* decide whether to extend with route similarity or better scoring

Done means:

* project is stable enough to become a thesis chapter or paper later

## Success criteria

Be concrete.

A good first success target:

* your context-aware method beats the context-unaware query-driven baseline on zone-entry or corridor F1
* at the same retained-point ratio
* without blowing up runtime
* and with fewer false spatial artifacts

If zone-entry and corridor F1 are already perfect for all methods, the success target shifts to:

* same primary query F1 at a lower retained-point ratio
* better strict point-membership or event-count fidelity at the same retained-point ratio
* fewer spatial artifacts at the same retained-point ratio
* better diminishing-returns behavior across the low-budget stress range

If even strict diagnostics saturate after scale-up, broaden the query workload deliberately rather than adding arbitrary context: entry counts, entry sequence, time-of-entry error, corridor dwell/distance, multiple corridors, narrower zones, or sub-interval queries.

If that happens, the project is working.

## Failure criteria

Also be concrete.

The project is failing if:

* you still do not trust preprocessing after 3 weeks
* you keep adding context layers instead of finishing evaluation
* you cannot explain which points your method preserves and why
* your method only wins at one cherry-picked compression rate
* the gains disappear after manual inspection

## What to implement first in code

In this exact order:

1. trajectory splitter
2. polygon / corridor membership checker
3. raw query evaluator
4. uniform subsampler
5. geometry simplifier
6. query-driven scorer without context
7. context-aware scorer
8. batch experiment runner
9. visual inspection notebook/script
10. ablation runner

Do not start with model architecture diagrams or thesis text.

## Recommended chapter structure for yourself

Not polished. Just practical.

1. **Problem framing**

   * what query-driven simplification means
   * why AIS context matters

2. **Data and scope**

   * region, vessel class, context layers, queries

3. **Pipeline**

   * preprocessing
   * query labeling
   * simplification methods

4. **Evaluation**

   * metrics
   * compression budgets
   * train/test setup

5. **Results**

   * baselines
   * context-aware method
   * ablations
   * failure cases

6. **Next steps**

   * similarity queries
   * stronger global importance
   * larger region

## My strongest recommendation

Do **not** make “reproduce MLSimp” your main milestone.

Use the paper for the right lesson:

* optimize for query fidelity, not just trajectory shape
* point importance should depend on downstream query behavior 

Then build the smallest maritime version of that idea that you can actually finish.
