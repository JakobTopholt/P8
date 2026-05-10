"""GeoJSON writers for query workloads and simplified trajectories.

These outputs are designed for inspection in QGIS:
- Queries: one FeatureCollection per query type (range/knn/similarity/clustering).
- Simplified trajectories: one FeatureCollection of LineStrings with a Points
  layer for the retained samples.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import torch


def _bbox_polygon(lon_min: float, lat_min: float, lon_max: float, lat_max: float) -> list[list[list[float]]]:
    """Build a closed-ring rectangle polygon in GeoJSON [lon, lat] order."""
    return [[
        [lon_min, lat_min],
        [lon_max, lat_min],
        [lon_max, lat_max],
        [lon_min, lat_max],
        [lon_min, lat_min],
    ]]


def _seconds_to_hhmm(seconds: float) -> str:
    """Convert seconds-since-midnight to 'HH:MM' string."""
    total = int(round(seconds)) % 86400
    return f"{total // 3600:02d}:{(total % 3600) // 60:02d}"


def _query_to_feature(q: dict[str, Any]) -> dict[str, Any]:
    """Convert a typed query dict to a GeoJSON Feature.

    All query types are rendered as axis-aligned rectangles (Polygons) so
    they're consistently plottable in QGIS next to range/clustering boxes.
    - range / clustering: native lat/lon bbox.
    - knn: square around the anchor sized from `t_half_window`.
    - similarity: square around the centroid sized from `radius`.
    """
    qtype = str(q["type"]).lower()
    p = q["params"]
    if qtype in ("range", "clustering"):
        coords = _bbox_polygon(p["lon_min"], p["lat_min"], p["lon_max"], p["lat_max"])
    elif qtype == "knn":
        half = 0.05  # default half-width in degrees if not derivable
        coords = _bbox_polygon(
            p["lon"] - half, p["lat"] - half, p["lon"] + half, p["lat"] + half
        )
    elif qtype == "similarity":
        r = float(p.get("radius", 0.05))
        coords = _bbox_polygon(
            p["lon_query_centroid"] - r, p["lat_query_centroid"] - r,
            p["lon_query_centroid"] + r, p["lat_query_centroid"] + r,
        )
    else:
        raise ValueError(f"Unsupported query type for GeoJSON export: {qtype}")
    geom = {"type": "Polygon", "coordinates": coords}
    props: dict[str, Any] = {"query_type": qtype, **{k: v for k, v in p.items() if isinstance(v, (int, float, str))}}
    # Add human-readable time fields alongside the raw seconds values.
    if "t_start" in props:
        props["t_start_hm"] = _seconds_to_hhmm(float(props["t_start"]))
    if "t_end" in props:
        props["t_end_hm"] = _seconds_to_hhmm(float(props["t_end"]))
    return {
        "type": "Feature",
        "geometry": geom,
        "properties": props,
    }


def write_queries_geojson(out_dir: str, typed_queries: list[dict[str, Any]]) -> None:
    """Write one GeoJSON file per query type into out_dir."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    by_type: dict[str, list[dict[str, Any]]] = {"range": [], "knn": [], "similarity": [], "clustering": []}
    for q in typed_queries:
        qtype = str(q["type"]).lower()
        if qtype in by_type:
            by_type[qtype].append(_query_to_feature(q))
    for qtype, feats in by_type.items():
        path = out / f"queries_{qtype}.geojson"
        payload = {"type": "FeatureCollection", "features": feats}
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        print(f"  wrote {len(feats):>4d} {qtype} queries to {path}", flush=True)


def write_simplified_csv(
    out_path: str,
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
    retained_mask: torch.Tensor,
    trajectory_mmsis: list[int] | None = None,
) -> None:
    """Write retained simplified trajectories as CSV in the AIS preprocessed schema.

    Columns: MMSI, # Timestamp, Latitude, Longitude, SOG, COG.
    Matches the layout of files in AISDATA/preprocessed_AIS_files so the
    output drops directly into the same downstream tooling.
    """
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    points_np = points.detach().cpu().numpy()
    mask_np = retained_mask.detach().cpu().bool().numpy()

    rows = 0
    with open(out, "w", encoding="utf-8") as f:
        f.write("MMSI,# Timestamp,Latitude,Longitude,SOG,COG\n")
        for traj_id, (s, e) in enumerate(boundaries):
            sub_mask = mask_np[s:e]
            if not sub_mask.any():
                continue
            sub = points_np[s:e][sub_mask]
            mmsi = trajectory_mmsis[traj_id] if trajectory_mmsis is not None and traj_id < len(trajectory_mmsis) else 100000000 + traj_id
            for row in sub:
                # row = [time, lat, lon, speed, heading, ...]
                f.write(
                    f"{mmsi},{float(row[0]):.3f},{float(row[1]):.6f},"
                    f"{float(row[2]):.6f},{float(row[3]):.2f},{float(row[4]):.2f}\n"
                )
                rows += 1
    print(f"  wrote {rows} retained points across "
          f"{int(mask_np.reshape(-1).sum())} samples to {out}", flush=True)


def write_query_points_csv(
    out_dir: str,
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
    typed_queries: list[dict[str, Any]],
    support_masks_per_query: list[torch.Tensor],
    retained_masks_by_method: dict[str, torch.Tensor],
    trajectory_mmsis: list[int] | None = None,
) -> None:
    """Write one CSV per query type listing every point that participates in any query.

    Mirrors the per-type split of write_queries_geojson — query_points_<type>.csv
    has one row per (query, support_point) pair so a row appears once per query
    that touches it. Columns: query_idx, mmsi, t, lat, lon, plus a
    retained_<method> bool for each method passed in. This is the artifact the
    user inspects when cross-referencing the per-query top-15 tables to the
    actual data points each method kept or dropped.
    """
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    points_np = points.detach().cpu().numpy()
    method_names = list(retained_masks_by_method.keys())
    method_masks_np = {name: retained_masks_by_method[name].detach().cpu().bool().numpy() for name in method_names}

    point_to_traj = [-1] * int(points.shape[0])
    for traj_id, (s, e) in enumerate(boundaries):
        for i in range(s, e):
            point_to_traj[i] = traj_id

    by_type: dict[str, list[tuple[int, int]]] = {"range": [], "knn": [], "similarity": [], "clustering": []}
    for q_idx, q in enumerate(typed_queries):
        qtype = str(q["type"]).lower()
        if qtype in by_type:
            by_type[qtype].append((q_idx, q_idx))

    for qtype, entries in by_type.items():
        path = out / f"query_points_{qtype}.csv"
        rows = 0
        with open(path, "w", encoding="utf-8") as f:
            header = ["query_idx", "mmsi", "t", "lat", "lon", "sog", "cog"] + [f"retained_{m}" for m in method_names]
            f.write(",".join(header) + "\n")
            for q_idx, _ in entries:
                support_np = support_masks_per_query[q_idx].detach().cpu().bool().numpy()
                idxs = support_np.nonzero()[0]
                for pi in idxs:
                    traj_id = point_to_traj[int(pi)]
                    mmsi = trajectory_mmsis[traj_id] if (trajectory_mmsis is not None and 0 <= traj_id < len(trajectory_mmsis)) else 100000000 + max(0, traj_id)
                    row = points_np[pi]
                    cells = [str(q_idx), str(mmsi),
                             f"{float(row[0]):.3f}", f"{float(row[1]):.6f}", f"{float(row[2]):.6f}",
                             f"{float(row[3]):.2f}", f"{float(row[4]):.2f}"]
                    cells.extend(["1" if method_masks_np[m][pi] else "0" for m in method_names])
                    f.write(",".join(cells) + "\n")
                    rows += 1
        print(f"  wrote {rows:>7d} {qtype} query-point rows to {path}", flush=True)


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km between two lat/lon pairs."""
    import math
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def _trajectory_length_km(lat_lon: "torch.Tensor") -> float:
    """Sum haversine distances between consecutive (lat, lon) rows."""
    n = lat_lon.shape[0]
    if n < 2:
        return 0.0
    arr = lat_lon.detach().cpu().numpy()
    total = 0.0
    for i in range(1, n):
        total += _haversine_km(float(arr[i - 1, 0]), float(arr[i - 1, 1]),
                               float(arr[i, 0]), float(arr[i, 1]))
    return total


def report_trajectory_length_loss(
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
    retained_mask: torch.Tensor,
    top_k: int = 25,
    min_orig_km: float = 1.0,
    trajectory_mmsis: list[int] | None = None,
) -> None:
    """Print per-trajectory length-loss summary and two top-K rankings.

    For each trajectory:
        orig_len_km   = sum of haversine distances between consecutive original points
        simp_len_km   = same for retained points (preserves order)
        length_loss   = 1 - simp_len_km / orig_len_km      (0 = perfect, 1 = everything collapsed)
        points_kept   = sum(retained_mask[s:e])
        points_removed= (e - s) - points_kept

    Two ranked lists are printed:
        1. Most-distorted  : top-K by highest length_loss (largest shape damage).
        2. Least-distorted : top-K by lowest length_loss (shape best preserved).

    Averages over all non-empty trajectories are also printed.
    """
    mask = retained_mask.detach().cpu().bool()
    rows: list[tuple[int, float, float, float, int, int]] = []  # (display_id, orig, simp, loss, kept, removed)
    for traj_id, (s, e) in enumerate(boundaries):
        total_pts = e - s
        if total_pts < 2:
            continue
        sub = points[s:e]
        orig = _trajectory_length_km(sub[:, 1:3])
        sub_mask = mask[s:e]
        kept = int(sub_mask.sum().item())
        removed = total_pts - kept
        if kept >= 2:
            simp = _trajectory_length_km(sub[sub_mask][:, 1:3])
        else:
            simp = 0.0
        loss = 0.0 if orig <= 1e-9 else max(0.0, 1.0 - simp / orig)
        display_id = trajectory_mmsis[traj_id] if trajectory_mmsis is not None and traj_id < len(trajectory_mmsis) else traj_id
        rows.append((int(display_id), orig, simp, loss, kept, removed))

    if not rows:
        print("  [length-loss] no trajectories with >=2 points, skipping.", flush=True)
        return

    avg_orig = sum(r[1] for r in rows) / len(rows)
    avg_simp = sum(r[2] for r in rows) / len(rows)
    total_orig = sum(r[1] for r in rows)
    total_simp = sum(r[2] for r in rows)
    length_preserved = (total_simp / total_orig) if total_orig > 1e-9 else 1.0
    length_preserved = max(0.0, min(1.0, length_preserved))
    avg_removed = sum(r[5] for r in rows) / len(rows)
    print(
        f"  [length] {len(rows)} trajectories  "
        f"avg_orig_km={avg_orig:.2f}  avg_simp_km={avg_simp:.2f}  "
        f"length_preserved={length_preserved:.3f}  avg_points_removed={avg_removed:.1f}",
        flush=True,
    )

    # Filter out near-stationary trajectories so the top-K is meaningful.
    ranked = [r for r in rows if r[1] >= min_orig_km]
    dropped = len(rows) - len(ranked)
    if dropped:
        print(
            f"  [length-loss] filtered out {dropped} trajectories with orig_km < {min_orig_km:.2f} "
            f"(likely docked/stationary) from top-{top_k} ranking",
            flush=True,
        )
    if not ranked:
        return

    most = sorted(ranked, key=lambda r: r[3], reverse=True)[:top_k]
    least = sorted(ranked, key=lambda r: r[3])[:top_k]

    id_label = "mmsi" if trajectory_mmsis is not None else "traj_id"
    hdr = f"  {'rank':>4}  {id_label:>10}  {'orig_km':>10}  {'simp_km':>10}  {'length_loss':>11}  {'kept':>6}  {'removed':>8}"
    print(f"\n  [length-loss] Top {top_k} MOST distorted (highest length_loss):", flush=True)
    print(hdr, flush=True)
    for rank, r in enumerate(most, start=1):
        print(
            f"  {rank:>4}  {r[0]:>10d}  {r[1]:>10.2f}  {r[2]:>10.2f}  {r[3]:>11.3f}  {r[4]:>6d}  {r[5]:>8d}",
            flush=True,
        )

    print(f"\n  [length-loss] Top {top_k} LEAST distorted (lowest length_loss):", flush=True)
    print(hdr, flush=True)
    for rank, r in enumerate(least, start=1):
        print(
            f"  {rank:>4}  {r[0]:>10d}  {r[1]:>10.2f}  {r[2]:>10.2f}  {r[3]:>11.3f}  {r[4]:>6d}  {r[5]:>8d}",
            flush=True,
        )

