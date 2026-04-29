# AIS-Contextual-QDS Methodology Outline

This document is the compact research/methodology outline. The locked implementation choices live in `DefinedChoices-AIS-QueryDrivenSimplification.md`; the active execution plan lives in `TaskBoard-and-SprintPlan.md`.

## Core Idea

The project follows the query-driven simplification idea from MLSimp:

> Given a storage budget, simplify trajectories so query results on the simplified database stay close to query results on the original database.

The maritime context is not the goal by itself. It is extra signal for deciding which AIS points matter for the queries we care about.

## Working Objective

Design and test a context-aware query-driven simplification method for AIS that preserves geofence and corridor query behavior better than context-unaware simplification at the same retained-point budget.

The current implementation is focused on a reproducible Great Belt cargo-vessel iteration before scaling to the broader MVP window.

## Research Questions

**RQ1:** Can static AIS-relevant context improve query-driven simplification for geofence and corridor queries?

**RQ2:** Which context-sensitive points matter most: boundary-adjacent points, entry/exit points, or corridor-transition points?

**RQ3:** At what compression rates does context-aware simplification provide the most useful gain over context-unaware baselines?

## Current Empirical Finding

The first `segment_exact` baseline runs showed that trajectory-level yes/no query F1 can saturate at very low retained-point ratios.

This is not automatically a methodology flaw. A yes/no query such as "did the vessel enter zone Z?" only needs enough retained geometry to preserve one valid query witness. Once that evidence survives, the final F1 can be perfect even if many query-relevant points or event details are gone.

Therefore:

- primary query F1 remains the gate
- strict point-membership and event-count metrics are used to compare methods after primary F1 saturates
- method comparisons should be curve-based, not based on one fixed threshold
- the immediate goal is to find where diminishing returns begin for each simplification strategy

## Scope

Current active iteration:

- Region: Great Belt / Storebaelt
- Vessel class: cargo
- Time window: `2026-01-01` through `2026-01-10`
- Context: three fixed zones and one main corridor polygon
- Queries: zone entry and corridor membership
- Truth/evaluation audit mode: `segment_exact`

Broader MVP target:

- same design, scaled to a 2-to-4-week window after the method and evaluation pipeline are stable

Out of scope until the MVP is stable:

- weather
- bathymetry
- vessel interaction context
- full Denmark
- learned GNN/diffusion models
- full MLSimp reproduction

## Query Workload

### Q1: Zone Entry

Trajectory-level yes/no query:

> Which trajectories entered zone Z during the time window?

Rules:

- starting inside a zone does not count as an entry
- boundary-only touching does not count unless the path crosses from outside to inside
- multiple entries collapse to one trajectory-level positive label

### Q2: Corridor Membership

Trajectory-level yes/no query:

> Which trajectories passed through corridor C during the time window?

Current `segment_exact` semantics:

- corridor-positive if a retained point is covered by the corridor polygon, or
- one adjacent segment overlaps the corridor polygon by at least `min_corridor_overlap_meters`
- current active value: `1.0` meter

## Method Ladder

### B1: Uniform Subsampling

Keeps points uniformly along the trajectory while always keeping first and last point.

Purpose: cheap baseline and sanity check.

### B2: Douglas-Peucker

Geometry baseline that optimizes shape rather than query behavior.

Purpose: show how geometry-preserving simplification behaves under query-driven evaluation.

### B3: Query-Driven Without Static Context

Uses query witnesses and trajectory-local evidence only.

Allowed signals:

- first and last point
- local shape importance such as turn/deviation
- points whose removal changes a primary query answer
- points whose removal changes strict event-count diagnostics
- points adjacent to observed raw query state transitions

Not allowed in B3:

- distance to zone boundary
- distance to corridor boundary or centerline
- generic boundary-proximity boost
- generic maritime-context proximity terms

Purpose: test whether query-driven selection helps before adding static maritime context.

### B4: Context-Aware Query-Driven Method

Extends B3 with static context priors.

Allowed additions:

- distance to zone boundary
- distance to corridor boundary or centerline
- inside/outside zone and corridor state
- static boundary/corridor proximity weights
- optional static context priors such as distance to land

Purpose: test whether maritime context improves over pure query-driven evidence.

### B5: Optional Advanced Query-Context Method

B5 is not part of the core MVP. It is a possible extension if B4 is implemented, benchmarked, inspected, and ablated with time left.

Possible directions:

- adaptive B4 weights instead of fixed configured weights
- learned scoring using B3/B4 features
- explicit query-context interaction terms, such as transition witness plus boundary proximity
- global budget allocation across trajectories
- failure-recovery logic based on B4 error taxonomy

Purpose: test whether a more advanced method improves beyond the explainable B4 baseline without replacing B4 as the main thesis method.

## Evaluation Strategy

### Primary Metrics

These remain the gate:

- zone-entry precision / recall / F1
- corridor-membership precision / recall / F1

### Strict Development Metrics

Used when primary F1 is saturated:

- zone point-membership F1
- zone entry-count exact rate
- corridor point-membership F1
- corridor entry-count exact rate
- per-zone event exact rates

### Sanity And Artifact Metrics

Important for preventing fake wins:

- trajectory-level false positives and false negatives
- false zone/corridor crossings created by simplification
- missed crossings caused by simplification
- land-crossing or impossible spatial artifacts

Current implementation already counts trajectory-level false positives and false negatives through metrics such as `zone_entry_fp`, `zone_entry_fn`, `corridor_membership_fp`, and `corridor_membership_fn`. Fine-grained false-crossing artifact counts are planned as part of the error taxonomy and inspection workflow.

## Budget Strategy

Standard reporting grid:

- `0.10`, `0.20`, `0.30`, `0.40`, `0.50`

Current B3 development grid:

- `0.005`, `0.010`, `0.015`, `0.020`, `0.030`, `0.050`

Reason:

- `0.005` and `0.010` stress the primary query labels
- `0.015` and above recover primary F1 on dev but still separate methods through strict metrics
- `0.030` to `0.050` show diminishing returns and curve shape

Use `dev` for budget search and method choices. Keep `eval` for confirmation after the comparison protocol is fixed.

## When To Broaden The Query Workload

Do not broaden the workload just because primary F1 saturates. First use stricter diagnostics and lower budgets.

If strict diagnostics also saturate after scale-up, broaden deliberately with query variants such as:

- entry count as a primary query
- per-zone entry sequence/order
- time-of-entry error
- corridor dwell or distance inside corridor
- multiple corridors
- narrower zone variants
- sub-interval queries within the trajectory time window

## Success Criteria

The MVP is successful if:

- raw zone/corridor query labels are trusted
- B1/B2/B3/B4 run on the fixed budget grid
- B4 improves over B3 on at least one primary metric, strict diagnostic, or lower-budget threshold
- results survive manual inspection
- the method remains simple and reproducible
- we can explain which preserved points caused the gain

B5 is explicitly optional and should not be required for the MVP success claim.

## Main Risks

- Adding more context before B3 exists
- Treating saturated yes/no F1 as proof that the method is done
- Tuning on `eval`
- Trusting aggregate metrics without visual inspection
- Jumping to learned models before the simple scoring methods are benchmarked and ablated
- Letting optional B5 work blur the core B3-to-B4 comparison
