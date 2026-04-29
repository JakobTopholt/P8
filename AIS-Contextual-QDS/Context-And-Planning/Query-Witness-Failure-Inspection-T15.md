# T15: Query-Witness Failure Inspection

Run inspected: `query_witness_temporal_guard_dev_refined_stress_grid_20260429T043835Z`

Scope:

- Split/subset: `dev`, `great_belt_iter1_10days_hardcase`
- Methods compared: `uniform`, `douglas_peucker`, `query_witness`
- Budgets: `0.005`, `0.010`, `0.015`, `0.020`, `0.030`, `0.050`
- Evaluation mode: `segment_exact`
- Truth label mode: `segment_exact`

The inspected `query_witness` method is the temporal-guarded query scorer only. It does not fall back to uniform after primary-query errors.

## Metric Summary

At `0.005`, `query_witness` has weaker primary F1 than the stronger reference method:

- `query_witness` zone-entry F1: `0.9863` with `287/2/6` TP/FP/FN.
- Uniform zone-entry F1: `0.9880` with `288/2/5`.
- Douglas-Peucker zone-entry F1: `0.9932` with `292/3/1`.
- `query_witness` corridor-membership F1: `0.9705` with `148/8/1`.
- Uniform corridor-membership F1: `0.9834` with `148/4/1`.
- Douglas-Peucker corridor-membership F1: `0.9769` with `148/6/1`.

At `0.010`, `query_witness` reaches perfect corridor membership but still has two zone-entry false positives.

At `0.015` and `0.020`, `query_witness` has one zone-entry false positive and perfect corridor membership.

At `0.030` and `0.050`, `query_witness` preserves both primary query families perfectly.

`query_witness` improves strict zone diagnostics across the grid. For example, at `0.010`, `query_witness` zone point-membership F1 is `0.9570` versus uniform `0.7375` and Douglas-Peucker `0.6510`; `query_witness` zone event-count exact rate is `0.9711` versus uniform `0.8544` and Douglas-Peucker `0.8189`.

## Primary Failure Cases

### Budget `0.005`

`query_witness` zone-entry failures:

- False negatives: `(2747, zone_anchor_or_waiting_area)`, `(2760, zone_port_approach)`, `(4472, zone_port_approach)`, `(8159, zone_port_approach)`, `(8235, zone_anchor_or_waiting_area)`, `(9145, zone_port_approach)`.
- False positives: `(8501, zone_anchor_or_waiting_area)`, `(9089, zone_anchor_or_waiting_area)`.

`query_witness` corridor-membership failures:

- False negative: `4472`.
- False positives: `1338`, `4519`, `6873`, `7747`, `8373`, `9088`, `9091`, `10386`.

Comparison:

- Uniform shares 7 of `query_witness`'s 17 primary mismatch units at `0.005`.
- Douglas-Peucker shares 9 of `query_witness`'s 17 primary mismatch units at `0.005`.
- `query_witness` has 10 mismatch units not shared with uniform and 8 not shared with Douglas-Peucker.

### Budget `0.010`

`query_witness` zone-entry failures:

- False positives: `(9089, zone_anchor_or_waiting_area)`, `(9897, zone_anchor_or_waiting_area)`.

`query_witness` corridor-membership failures:

- None.

Comparison:

- Uniform has one different zone false positive: `(7720, zone_port_approach)`.
- Douglas-Peucker shares one `query_witness` zone false positive: `(9897, zone_anchor_or_waiting_area)`.
- Douglas-Peucker also has one corridor false negative: `7273`.

### Budgets `0.015` and `0.020`

`query_witness` zone-entry failures:

- False positive: `(7720, zone_port_approach)`.

`query_witness` corridor-membership failures:

- None.

Comparison:

- Uniform and Douglas-Peucker have no primary failures at these budgets.

### Budgets `0.030` and `0.050`

`query_witness` has no primary query failures.

## Failure Mechanisms

### Shortcut False Positives

All inspected `query_witness` zone false positives are simplified shortcut segments with both endpoints outside the zone and `ST_Crosses(segment, zone) = true`.

Examples:

- Budget `0.005`, trajectory `8501`, `zone_anchor_or_waiting_area`: simplified segment source gap `482`, from source point `1` to `483`.
- Budget `0.005`, trajectory `9089`, `zone_anchor_or_waiting_area`: source gap `308`, from `1679` to `1987`.
- Budget `0.010`, trajectory `9089`: source gap `210`, from `1679` to `1889`.
- Budget `0.010`, trajectory `9897`: source gap `215`, from `1297` to `1512`.
- Budget `0.015`, trajectory `7720`, `zone_port_approach`: source gap `138`, from `1245` to `1383`.
- Budget `0.020`, trajectory `7720`: source gap `106`, from `1264` to `1370`.

This shows the temporal guard reduces but does not eliminate shortcut artifacts until about `0.030`.

The `query_witness` corridor false positives at `0.005` are also shortcut artifacts. The raw trajectories have no qualifying corridor-overlap segment and the simplified trajectories have no corridor-covered retained point; the false positive comes from a long simplified segment whose corridor overlap exceeds the threshold. Source gaps range from `424` to `601` points for the largest cases.

### Missed Entries

At `0.005`, `query_witness` misses six zone entries. Each inspected raw trajectory has two raw entry segments for the target zone, but the simplified trajectory has no simplified segment intersecting the relevant zone.

Trajectory `4472` is the clearest coupled miss:

- Zone false negative for `zone_port_approach`.
- Corridor false negative for `corridor_main_transit_lane`.
- Raw corridor evidence has `260` qualifying segments and maximum segment overlap about `348.3 m`.
- `query_witness` keeps no corridor-covered point and no qualifying simplified corridor segment at `0.005`.

This is a tight-budget witness-allocation failure: `query_witness` has the right witness types, but too few retained slots to keep all local query episodes plus enough temporal coverage.

## Implications For `context_aware_query_witness`

`context_aware_query_witness` should target shortcut false positives without hiding `query_witness` failures. Static context priors can help distinguish real boundary/corridor interactions from long straight-line artifacts by adding boundary or corridor proximity terms near candidate segments.

The `query_witness` to `context_aware_query_witness` comparison should emphasize:

- Primary recovery below `0.030`, especially zone false positives at `0.010` to `0.020`.
- Strict zone metrics, where `query_witness` already improves strongly over `uniform` and `douglas_peucker`.
- Corridor event-count exact rate, where uniform still remains stronger at several budgets.
