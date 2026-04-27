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

../.venv/bin/python -m src.cli --config configs/iteration1_10days.example.yaml load-context \
  --study-region-file data/context/study_region.geojson \
  --zones-file data/context/zones.geojson \
  --corridor-file data/context/corridor.geojson \
  --corridor-buffer-meters 700
```

## Rationale

These geometries are fixed query-workload definitions, not official nautical
chart data. They are hand-drawn in EPSG:4326 lon/lat coordinates around the
Great Belt transit area and were revised after an AIS-density audit:

- `corridor_main_transit_lane` follows the observed cargo transit spine through
  Route T / BELTREP-relevant Great Belt traffic.
- `zone_port_approach` now covers the Korsor-side port and East Bridge approach
  area, where the AIS data shows repeated low-speed/manoeuvring behavior.
- `zone_anchor_or_waiting_area` now covers the Kalundborg/Jammerland
  waiting-area context rather than a fast-transit rectangle south of the bridge.
- `zone_narrow_passage_control` is tightened around Sprogoe and the East Bridge
  controlled passage.
- DMA material identifies Route T, BELTREP / Great Belt VTS, the East Bridge
  traffic route between Korsor and Sprogoe, and Kalundborg Fjord anchorage as
  relevant navigational context.

The redesign is documented in `GEOMETRY_AUDIT.md`. If these geometries are
changed again, reload context, recompute labels, recreate hard-case subsets, and
recompute point features before comparing benchmark runs.
