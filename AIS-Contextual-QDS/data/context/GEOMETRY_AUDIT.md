# Context Geometry Audit

Date: 2026-04-28

## Scope

This audit freezes the working query geometries for the Great Belt / Storebaelt
AIS-QDS iteration. The goal is not to reproduce official chart polygons, but to
define relevant, defensible static context layers for zone-entry and
corridor-membership evaluation.

## External Context Used

- Danish Maritime Authority, transit route statistics:
  `https://www.dma.dk/safety-at-sea/navigational-information/ais-data/transit-routes`
- Danish Maritime Authority, Navigation Through Danish Waters, version 15:
  `https://www.soefartsstyrelsen.dk/Media/637977139358837038/Navigation%20through%20Danish%20Water%20version%2015%20%28SEP%202022%29.pdf`
- Danish Maritime Authority, BELTREP order:
  `https://www.dma.dk/Media/637787223374336711/Order%20on%20the%20ship%20reporting%20system%20BELTREP%20and%20on%20navigation%20under%20the%20East%20Bridge%20and%20the%20West%20Bridge%20in%20the%20Great%20Belt.pdf`
- Danish Maritime Authority, VTS guidance:
  `https://www.dma.dk/safety-at-sea/safety-of-navigation/mandatory-ship-reporting-systems-msrs-and-vessel-traffic-services-vts/guidelines-on-vessel-traffic-services-vts-in-danish-waters`

The official context supports these design anchors:

- Route T is the main transit route through the Danish waters.
- BELTREP / Great Belt VTS covers central and northern Great Belt traffic.
- The East Bridge traffic route between Korsor and Sprogoe is a controlled,
  narrow-passage context.
- Kalundborg Fjord anchorage is part of the BELTREP route-reporting context.

## Local AIS Audit

The audit used the current 10-day cargo trajectory table:

- Raw points: 1,150,479
- Raw trajectories: 5,380
- Current source extent: lon 10.252878 to 11.792633, lat 54.68 to 56.139892

Problems found in the previous context layout:

- The old `zone_anchor_or_waiting_area` mostly captured fast transit traffic
  rather than waiting or anchorage behavior.
- The old `corridor_main_transit_lane` centerline was west of the highest cargo
  AIS density north of the bridge.
- The old `zone_port_approach` was too weak as a query target: only 20
  trajectory memberships and 4 entry trajectories.
- The old `zone_narrow_passage_control` was too broad, mixing the Sprogoe /
  bridge passage with the Korsor-side approach area.

## Revised Geometry Design

The final frozen layout uses:

- `zone_port_approach`: Korsor port and east-bridge approach.
- `zone_anchor_or_waiting_area`: Kalundborg / Jammerland waiting-area context.
- `zone_narrow_passage_control`: tightened Sprogoe and East Bridge passage.
- `corridor_main_transit_lane`: observed cargo transit spine through the
  Route T / BELTREP-relevant Great Belt path, buffered at 700 m on load.

Expected local counts before reload, measured against the current trajectory
table:

| geometry | area km2 | point members | trajectory members | entry trajectories | low-speed points |
| --- | ---: | ---: | ---: | ---: | ---: |
| zone_port_approach | 164.27 | 21,367 | 278 | 220 | 10,090 |
| zone_anchor_or_waiting_area | 513.15 | 17,366 | 325 | 227 | 3,915 |
| zone_narrow_passage_control | 124.34 | 70,435 | 530 | 234 | 1,660 |
| corridor_main_transit_lane, 700 m buffer | 219.86 | 273,312 | 2,161 | n/a | 1,917 |

The three revised zones have limited point overlap and no three-zone point
overlaps in the audited trajectory table.

## Rebuild Requirements

After editing these files:

```bash
make load-context \
  STUDY_REGION_FILE=data/context/study_region.geojson \
  ZONES_FILE=data/context/zones.geojson \
  CORRIDOR_FILE=data/context/corridor.geojson \
  CORRIDOR_BUFFER_METERS=700
make compute-labels
make label-balance
make create-hardcase-subset
make compute-features
```

Benchmarks should be compared only after labels and point features have been
recomputed from the revised context.

## Post-Reload Validation

The revised context was loaded with a 700 m corridor buffer and the optimized
labels, hard-case subset, and point features were recomputed.

Overall optimized label balance:

| query target | positive trajectories | positive rate |
| --- | ---: | ---: |
| zone_port_approach | 217 | 0.0403 |
| zone_anchor_or_waiting_area | 227 | 0.0422 |
| zone_narrow_passage_control | 234 | 0.0435 |
| corridor_main_transit_lane | 2,161 | 0.4017 |

Hard-case subset balance:

| split | port positives | waiting positives | narrow positives | corridor positives |
| --- | ---: | ---: | ---: | ---: |
| dev | 97 | 87 | 109 | 149 |
| eval | 80 | 53 | 90 | 148 |

Point-feature recomputation produced:

- Point features: 1,150,479
- Zone transitions: 1,220
- Corridor transitions: 3,496

Smoke benchmark:

```bash
make benchmark METHODS=uniform BUDGETS=0.02 \
  SUBSET_NAME=great_belt_iter1_10days_hardcase \
  RUN_TAG=geometry_redesign_probe
```

Selected results:

| metric | value |
| --- | ---: |
| zone_entry_f1 | 1.0000 |
| zone_point_membership_macro_f1 | 0.7989 |
| zone_entry_event_count_macro_exact_rate | 0.8989 |
| corridor_membership_f1 | 1.0000 |
| corridor_point_membership_f1 | 0.9966 |
| corridor_entry_event_count_exact_rate | 0.8967 |

The strict metrics now expose boundary/event loss that was hidden by the
trajectory-level labels, so the geometry layout is suitable for the next method
stage.
