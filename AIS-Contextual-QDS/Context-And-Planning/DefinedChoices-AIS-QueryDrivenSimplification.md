# Defined Choices: AIS Query-Driven Simplification

This document is the authoritative set of locked choices for the MVP. Change it only when a later experiment clearly justifies a revision.

## 1. Project Definition

**Name:** Geofence-aware query-driven simplification for Danish AIS

**Objective:** simplify AIS trajectories under a retained-point budget while preserving the answers and query-relevant behavior of geofence and corridor queries.

**Current active iteration:**

- Config: `configs/iteration1_10days.example.yaml`
- Region: Great Belt / Storebaelt
- Vessel class: cargo
- Time window: `2026-01-01` through `2026-01-10`
- Default local mode: `optimized`
- Audit/truth mode: `segment_exact`

**MVP scale-up target:** same design over a 2-to-4-week window after the method and evaluation pipeline are stable.

## 2. Data And Storage

**Source data**

- Historical AIS data from the Danish Maritime Authority.
- Cleaned source points are stored in PostGIS.
- `public.ais_points_cleaned` is treated as the raw source for this project.

**Core table groups**

- `trajectories_raw`
- `trajectory_points_raw`
- `context_zones`
- `context_corridors`
- `trajectory_query_labels`
- `trajectory_dev_eval_subset`
- `simplification_runs`
- `trajectories_simplified_points`
- `trajectories_simplified_segments`
- `benchmark_metrics`

## 3. Study Scope

**Region**

- Great Belt / Storebaelt study area.
- One fixed study-region polygon.
- The MVP does not start with all Danish waters.

**Vessel class**

- Cargo vessels only.
- Other vessel classes are postponed.

**Trajectory construction**

- Points ordered by `mmsi, timestamp`.
- Split on gaps greater than 30 minutes.
- Split on impossible jumps according to the configured speed sanity threshold.
- Minimum trajectory size: 20 points.
- No separate stationary-period removal in the MVP.

**Endpoint policy**

- Every simplification method must keep the first and last point of each trajectory.
- No synthetic points are generated.
- Retained points must come from the original trajectory.

## 4. Context Layers

**Zones**

The active MVP uses exactly three fixed geofence zones:

- `zone_port_approach`
- `zone_anchor_or_waiting_area`
- `zone_narrow_passage_control`

**Corridor**

- One main transit-lane corridor polygon.
- If a source corridor is a line, it is buffered once and frozen as a polygon.

**Optional sanity context**

- Coastline/land mask may be used for inspection, artifact checks, and optional features.
- Weather, bathymetry, vessel interactions, anomaly labels, learned embeddings, and dynamic context layers are excluded from the MVP.

## 5. Query Semantics

The MVP optimizes two query families only.

### Q1: Zone Entry

Question:

> Which trajectories entered zone Z during the time window?

Output unit:

- trajectory-zone yes/no label

Rules:

- A trajectory counts as entering a zone if it starts outside and later enters the zone.
- Starting inside the zone does not count as entry.
- Boundary-only touching does not count unless the path crosses from outside to inside.
- Multiple entries are collapsed to one trajectory-level positive result.

### Q2: Corridor Membership

Question:

> Which trajectories passed through corridor C during the time window?

Output unit:

- trajectory yes/no label, repeated across zone rows for reporting compatibility

Rules in `segment_exact` mode:

- positive if at least one point is covered by the corridor polygon, or
- positive if one adjacent segment overlaps the corridor polygon by at least the configured threshold
- active threshold: `min_corridor_overlap_meters: 1.0`
- boundary touch alone is not enough unless overlap reaches the threshold
- no minimum dwell time is required

## 6. Semantics Modes

**`optimized`**

- Default development mode.
- Uses line-level geometry plus point-hit aggregation.
- Faster and suitable for routine checks.

**`segment_exact`**

- Audit/truth mode.
- Uses adjacent-segment semantics.
- Benchmark runs materialize adjacent simplified segments for practical exact evaluation.
- This is the mode used for the current stress-grid baseline decisions.

Truth labels are stored by `label_mode`, so `optimized` and `segment_exact` labels can coexist.

## 7. Compression Budgets

**Standard reporting grid**

- `0.10`
- `0.20`
- `0.30`
- `0.40`
- `0.50`

**Current B3 development stress grid**

- `0.005`
- `0.010`
- `0.015`
- `0.020`
- `0.030`
- `0.050`

**Budget interpretation**

- Compression is applied per trajectory.
- The goal is not to pick one fixed acceptance threshold upfront.
- The current goal is to compare metric-vs-budget curves and identify diminishing returns.

## 8. Methods

**B1: Uniform subsampling**

- Baseline that keeps evenly spaced points.

**B2: Douglas-Peucker**

- Geometry baseline.

**B3: Query-driven without static context**

- Query-witness and trajectory-local evidence only.
- Allowed: first/last points, local shape importance, query-answer witnesses, event-count witnesses, observed raw state-transition neighbors.
- Not allowed: distance to zone boundary, distance to corridor boundary/centerline, generic boundary proximity, or other static maritime-context priors.

**B3 point-scoring formula**

B3 assigns a deterministic importance score to each raw point inside one trajectory. The first and last point are forced anchors and are always retained before scores are considered. To avoid query-witness clustering creating long shortcut segments, B3 also keeps a budget-aware temporal guard set: `ceil(0.5 * target_points)` uniformly spaced anchors, including the endpoints. These guards are trajectory-local only; they do not use static context distance or proximity.

For every non-anchor point `i`, B3 uses only raw trajectory query witnesses and local shape evidence:

- `ZE_i`: zone-entry segment witnesses. Each adjacent raw segment endpoint receives one witness per target zone when the segment satisfies the current `segment_exact` zone-entry relation: outside-to-inside endpoint movement or `ST_Crosses(segment, zone)`.
- `ZH_i`: zone point-membership witnesses. The point receives one witness per target zone that covers the point.
- `ZT_i`: strict zone event witnesses. Each endpoint of an adjacent raw segment receives one witness per target zone whose covered/not-covered point state changes.
- `ZI_i`: strict zone entry-event witnesses. Each endpoint of an adjacent raw segment receives one witness per target zone whose point state changes from outside to inside.
- `CS_i`: corridor segment witnesses. Each endpoint of an adjacent raw segment receives one witness when that segment overlaps the configured corridor by at least `min_corridor_overlap_meters`.
- `CH_i`: corridor point-membership witness. The point receives one witness when the corridor covers the point.
- `CT_i`: strict corridor event witness. Each endpoint of an adjacent raw segment receives one witness when covered/not-covered corridor point state changes.
- `CI_i`: strict corridor entry-event witness. Each endpoint of an adjacent raw segment receives one witness when corridor point state changes from outside to inside.
- `S_i`: local shape score, `0.65 * normalized_local_deviation + 0.35 * normalized_turn`, where local deviation is normalized by the maximum local deviation within the trajectory and turn is normalized by 180 degrees.

The scalar B3 score is:

```text
primary_zone_entry_i = 3 * ZE_i
primary_corridor_membership_i = 3 * CS_i + 2 * CH_i
strict_event_count_i = 2 * ZI_i + ZT_i + 2 * CI_i + CT_i
point_membership_i = ZH_i + CH_i

B3_score_i =
    1000 * primary_zone_entry_i
  + 1000 * primary_corridor_membership_i
  +  100 * strict_event_count_i
  +   10 * point_membership_i
  +        S_i
```

Budget handling is per trajectory. After forced endpoints and temporal guards, B3 fills remaining slots by ranking interior points by descending `(B3_score_i, primary_query_i, strict_event_count_i, point_membership_i, S_i, -point_seq)`, where `primary_query_i = primary_zone_entry_i + primary_corridor_membership_i`. This gives deterministic tie-breaking under tight budgets while keeping query witnesses ahead of shape-only points. B3 does not inspect its own benchmark outcome and does not fall back to another method when it makes a primary-query error.

**B4: Context-aware query-driven method**

- Extends B3 with static context priors.
- Allowed: boundary proximity, corridor proximity, inside/outside state, and configurable static context weights.
- B4 may reuse B3 transition witnesses, but observed trajectory transitions are not what makes B4 context-aware.

**B5: Optional advanced query-context method**

- Not part of the MVP success criteria.
- Only allowed after B4 has been benchmarked, manually inspected, and ablated.
- Possible forms: adaptive context weights, learned scorer, explicit query-context interaction terms, global cross-trajectory budget allocation, or failure-recovery logic based on the B4 error taxonomy.
- B5 must be compared against B4, not used as a substitute for the B3-to-B4 thesis comparison.

**Model complexity rule**

- No GNN, diffusion model, or full MLSimp reproduction in the MVP.
- Learned methods are deferred until simple B3/B4 scoring is benchmarked, inspected, and ablated. If used at all, they belong under optional B5 work.

## 9. Features

Feature candidates already in scope:

- inside zone identity
- inside corridor flag
- nearest zone boundary distance
- corridor boundary or centerline distance
- optional distance to land
- zone transition flag
- corridor transition flag
- local turn/deviation proxy

B3 may use only trajectory-local and query-witness features. B4 may use static context features.

Transition flags are allowed in B3 only when they are derived from observed trajectory membership changes for the target query workload. Static distance and proximity features remain B4-only.

All metric distance calculations must use an appropriate projected CRS or a metric PostGIS/geography operation, not raw lat/lon distances.

## 10. Evaluation Guardrails

**Development and holdout**

- Use `dev` for stress-budget search, method design, and scoring-weight choices.
- Use `eval` only for confirmation after budgets, method definitions, and comparison rules are fixed.
- Do not repeatedly tune B3 or B4 after seeing `eval`.

**Primary metrics**

- zone-entry precision / recall / F1
- corridor-membership precision / recall / F1

Primary F1 remains the gate: a method that damages the main query labels must not be treated as better just because strict diagnostics improve.

**Strict diagnostics**

Used when primary F1 saturates:

- zone point-membership precision / recall / F1
- zone entry-count exact rate
- corridor point-membership precision / recall / F1
- corridor entry-count exact rate
- per-zone event exact rates

Perfect primary F1 at low budgets is not a methodology failure by itself. These are coarse trajectory-level yes/no queries, so they can saturate once the simplified trajectory retains enough evidence for the final answer.

**Artifact accounting**

Already implemented:

- trajectory-level false positives and false negatives through `zone_entry_fp`, `zone_entry_fn`, `corridor_membership_fp`, and `corridor_membership_fn`
- strict point-membership FP/FN
- event-count exact/MAE metrics

Planned:

- separate fine-grained taxonomy for false crossings, missed crossings, land crossings, and other spatial artifacts

## 11. Success Criteria

The MVP is successful if:

1. raw zone/corridor query labels are trusted
2. B1/B2/B3/B4 run on the selected budget grid
3. B4 improves over B3 on at least one primary metric, strict diagnostic, or lower-budget threshold
4. the improvement survives manual inspection
5. the implementation remains simple and reproducible
6. we can explain which preserved points caused the gain

B5 is optional extension work and is not required for MVP success.

## 12. Deferred Choices

Do not add these until the MVP is stable:

- second vessel class
- second study region
- route similarity query
- entry count as a primary query target
- per-zone entry sequence/order query
- corridor entry/exit count as a primary query target
- minimum distance or dwell-time inside corridor
- time-of-entry error as a primary query target
- multiple corridors or narrower zones
- sub-interval queries inside the current trajectory window
- global cross-trajectory budget allocation
- adaptive B4 weights or learned scoring as B5
- GNN or diffusion models
- streaming/online simplification
- weather and other dynamic context
- vessel interaction modeling

If strict diagnostics also saturate after scale-up, broaden the query workload deliberately from this deferred list rather than adding arbitrary context.
