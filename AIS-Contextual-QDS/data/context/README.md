# AIS-QDS Context Geometries

This folder contains the fixed MVP context/query geometries for the
Great Belt / Storebaelt AIS contextual query-driven simplification experiment.

## Files

- `study_region.geojson`: one polygon named `great_belt_study_area`
- `zones.geojson`: three polygon query zones named:
  - `zone_port_approach`
  - `zone_anchor_or_waiting_area`
  - `zone_narrow_passage_control`
- `corridor.geojson`: one line corridor centerline named
  `corridor_main_transit_lane`

## Loading

The corridor is intentionally stored as a centerline. Load it with a fixed
buffer distance:

```bash
cd AIS-Contextual-QDS

../.venv/bin/python -m src.cli --config configs/mvp.example.yaml load-context \
  --study-region-file data/context/study_region.geojson \
  --zones-file data/context/zones.geojson \
  --corridor-file data/context/corridor.geojson \
  --corridor-buffer-meters 700
```

## Rationale

These geometries are MVP query-workload definitions, not official nautical
chart data. They are hand-drawn in EPSG:4326 lon/lat coordinates around the
Great Belt transit area:

- The Danish Maritime Authority describes the Great Belt transit route as a
  passage over the Great Belt.
- DMA material and navigation guidance identify the Great Belt as a constrained
  traffic area with routeing / traffic separation relevance, including the
  Korsor-Sprogoe passage.

Before final thesis experiments, inspect these overlays against the cleaned AIS
tracks and freeze any refinements in this folder.
