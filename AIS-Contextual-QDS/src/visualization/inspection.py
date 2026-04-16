"""Self-contained HTML visual inspection exports."""

from __future__ import annotations

import html
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from psycopg import Connection

from ..config import AppConfig
from ..paths import resolve_project_path

SVG_WIDTH = 980
SVG_HEIGHT = 520
SVG_PADDING = 28

ZONE_COLORS = {
    "zone_port_approach": ("#e15759", "rgba(225, 87, 89, 0.20)"),
    "zone_anchor_or_waiting_area": ("#59a14f", "rgba(89, 161, 79, 0.20)"),
    "zone_narrow_passage_control": ("#b07aa1", "rgba(176, 122, 161, 0.22)"),
}


@dataclass(frozen=True)
class PointRecord:
    """One trajectory point for visualization."""

    lon: float
    lat: float
    seq: int
    ts: str
    source_seq: int | None = None


@dataclass(frozen=True)
class ContextFeature:
    """One named context feature."""

    name: str
    kind: str
    geometry: dict[str, Any]


@dataclass
class TrajectoryView:
    """A trajectory and its query/comparison metadata."""

    trajectory_id: int
    raw_points: list[PointRecord]
    simplified_points: list[PointRecord] = field(default_factory=list)
    zone_truth: dict[str, bool] = field(default_factory=dict)
    zone_pred: dict[str, bool] = field(default_factory=dict)
    corridor_truth: bool | None = None
    corridor_pred: bool | None = None

    @property
    def raw_count(self) -> int:
        return len(self.raw_points)

    @property
    def simplified_count(self) -> int:
        return len(self.simplified_points)

    @property
    def retained_ratio(self) -> float | None:
        if not self.simplified_points or not self.raw_points:
            return None
        return len(self.simplified_points) / len(self.raw_points)

    @property
    def has_prediction(self) -> bool:
        return bool(self.zone_pred) or self.corridor_pred is not None

    @property
    def has_query_error(self) -> bool:
        if not self.has_prediction:
            return False
        for zone_name, truth_value in self.zone_truth.items():
            if self.zone_pred.get(zone_name) is not None and self.zone_pred[zone_name] != truth_value:
                return True
        if self.corridor_truth is not None and self.corridor_pred is not None:
            return self.corridor_truth != self.corridor_pred
        return False

    @property
    def has_positive_truth(self) -> bool:
        return any(self.zone_truth.values()) or bool(self.corridor_truth)

    @property
    def category(self) -> str:
        if self.has_query_error:
            return "query-error"
        if self.has_positive_truth:
            return "query-positive"
        return "query-negative"


def _esc(value: object) -> str:
    return html.escape(str(value), quote=True)


def _format_bool(value: bool | None) -> str:
    if value is None:
        return "n/a"
    return "yes" if value else "no"


def _format_ratio(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.3f}"


def _thin_points(points: list[PointRecord], max_points: int) -> list[PointRecord]:
    if max_points <= 0 or len(points) <= max_points:
        return points
    if max_points <= 2:
        return [points[0], points[-1]]

    interior_needed = max_points - 2
    step = (len(points) - 2) / (interior_needed + 1)
    chosen = [points[0]]
    used: set[int] = set()
    for rank in range(1, interior_needed + 1):
        idx = int(round(rank * step))
        idx = min(max(idx, 1), len(points) - 2)
        if idx in used:
            continue
        used.add(idx)
        chosen.append(points[idx])
    chosen.append(points[-1])
    return chosen


def _iter_geojson_lonlat(geometry: dict[str, Any]) -> list[tuple[float, float]]:
    geometry_type = str(geometry.get("type", ""))
    coordinates = geometry.get("coordinates", [])
    points: list[tuple[float, float]] = []

    def add_position(position: Any) -> None:
        if isinstance(position, list | tuple) and len(position) >= 2:
            points.append((float(position[0]), float(position[1])))

    if geometry_type == "Point":
        add_position(coordinates)
    elif geometry_type == "LineString":
        for position in coordinates:
            add_position(position)
    elif geometry_type == "MultiLineString":
        for line in coordinates:
            for position in line:
                add_position(position)
    elif geometry_type == "Polygon":
        for ring in coordinates:
            for position in ring:
                add_position(position)
    elif geometry_type == "MultiPolygon":
        for polygon in coordinates:
            for ring in polygon:
                for position in ring:
                    add_position(position)
    return points


def _bounds_for_view(
    trajectory: TrajectoryView,
    context_features: list[ContextFeature],
) -> tuple[float, float, float, float]:
    lonlat: list[tuple[float, float]] = []
    lonlat.extend((point.lon, point.lat) for point in trajectory.raw_points)
    lonlat.extend((point.lon, point.lat) for point in trajectory.simplified_points)
    for feature in context_features:
        lonlat.extend(_iter_geojson_lonlat(feature.geometry))

    if not lonlat:
        return 0.0, 0.0, 1.0, 1.0

    min_lon = min(lon for lon, _ in lonlat)
    max_lon = max(lon for lon, _ in lonlat)
    min_lat = min(lat for _, lat in lonlat)
    max_lat = max(lat for _, lat in lonlat)

    lon_pad = max((max_lon - min_lon) * 0.05, 0.01)
    lat_pad = max((max_lat - min_lat) * 0.05, 0.01)
    return min_lon - lon_pad, min_lat - lat_pad, max_lon + lon_pad, max_lat + lat_pad


def _project(
    lon: float,
    lat: float,
    bounds: tuple[float, float, float, float],
    *,
    width: int = SVG_WIDTH,
    height: int = SVG_HEIGHT,
    padding: int = SVG_PADDING,
) -> tuple[float, float]:
    min_lon, min_lat, max_lon, max_lat = bounds
    usable_w = width - padding * 2
    usable_h = height - padding * 2
    x = padding + ((lon - min_lon) / max(max_lon - min_lon, 1e-12)) * usable_w
    y = height - padding - ((lat - min_lat) / max(max_lat - min_lat, 1e-12)) * usable_h
    return x, y


def _points_attr(points: list[tuple[float, float]]) -> str:
    return " ".join(f"{x:.2f},{y:.2f}" for x, y in points)


def _render_geometry_svg(
    feature: ContextFeature,
    bounds: tuple[float, float, float, float],
) -> str:
    geometry = feature.geometry
    geometry_type = str(geometry.get("type", ""))
    coordinates = geometry.get("coordinates", [])

    stroke = "#303030"
    fill = "none"
    stroke_width = "1.6"
    opacity = "1"
    dash = ""

    if feature.kind == "study_region":
        stroke = "#4d4d4d"
        fill = "none"
        dash = " stroke-dasharray='7 5'"
    elif feature.kind == "zone":
        stroke, fill = ZONE_COLORS.get(feature.name, ("#777777", "rgba(119, 119, 119, 0.18)"))
    elif feature.kind == "corridor":
        stroke = "#edc948"
        fill = "rgba(237, 201, 72, 0.25)"
        stroke_width = "2.0"

    title = f"<title>{_esc(feature.name)}</title>"

    def render_ring(ring: list[Any]) -> str:
        points = [_project(float(pos[0]), float(pos[1]), bounds) for pos in ring if len(pos) >= 2]
        if len(points) < 2:
            return ""
        return (
            f"<polygon points='{_points_attr(points)}' fill='{fill}' stroke='{stroke}' "
            f"stroke-width='{stroke_width}' opacity='{opacity}'{dash}>{title}</polygon>"
        )

    def render_line(line: list[Any]) -> str:
        points = [_project(float(pos[0]), float(pos[1]), bounds) for pos in line if len(pos) >= 2]
        if len(points) < 2:
            return ""
        return (
            f"<polyline points='{_points_attr(points)}' fill='none' stroke='{stroke}' "
            f"stroke-width='{stroke_width}' opacity='{opacity}'{dash}>{title}</polyline>"
        )

    parts: list[str] = []
    if geometry_type == "Polygon":
        for ring in coordinates:
            parts.append(render_ring(ring))
    elif geometry_type == "MultiPolygon":
        for polygon in coordinates:
            for ring in polygon:
                parts.append(render_ring(ring))
    elif geometry_type == "LineString":
        parts.append(render_line(coordinates))
    elif geometry_type == "MultiLineString":
        for line in coordinates:
            parts.append(render_line(line))
    return "\n".join(part for part in parts if part)


def _render_trajectory_line(
    points: list[PointRecord],
    bounds: tuple[float, float, float, float],
    *,
    color: str,
    width: float,
    max_points: int,
    label: str,
) -> str:
    display_points = _thin_points(points, max_points)
    projected = [_project(point.lon, point.lat, bounds) for point in display_points]
    if len(projected) < 2:
        return ""
    return (
        f"<polyline points='{_points_attr(projected)}' fill='none' stroke='{color}' "
        f"stroke-width='{width:.1f}' stroke-linejoin='round' stroke-linecap='round'>"
        f"<title>{_esc(label)}</title></polyline>"
    )


def _render_point_marker(
    point: PointRecord,
    bounds: tuple[float, float, float, float],
    *,
    color: str,
    radius: float,
    label: str,
) -> str:
    x, y = _project(point.lon, point.lat, bounds)
    return (
        f"<circle cx='{x:.2f}' cy='{y:.2f}' r='{radius:.1f}' fill='{color}' "
        "stroke='#ffffff' stroke-width='1.2'>"
        f"<title>{_esc(label)} seq={point.seq} ts={point.ts}</title></circle>"
    )


def _render_map_svg(
    trajectory: TrajectoryView,
    context_features: list[ContextFeature],
    *,
    max_points_per_line: int,
) -> str:
    bounds = _bounds_for_view(trajectory, context_features)
    parts: list[str] = [
        f"<svg class='map' viewBox='0 0 {SVG_WIDTH} {SVG_HEIGHT}' role='img' "
        f"aria-label='Trajectory {trajectory.trajectory_id} map'>",
        "<rect x='0' y='0' width='100%' height='100%' fill='#f7f8f5' />",
    ]

    for feature in context_features:
        parts.append(_render_geometry_svg(feature, bounds))

    parts.append(
        _render_trajectory_line(
            trajectory.raw_points,
            bounds,
            color="#2f6db3",
            width=2.6,
            max_points=max_points_per_line,
            label="raw trajectory",
        )
    )

    if trajectory.simplified_points:
        parts.append(
            _render_trajectory_line(
                trajectory.simplified_points,
                bounds,
                color="#f28e2b",
                width=3.0,
                max_points=max_points_per_line,
                label="simplified trajectory",
            )
        )
        for point in _thin_points(trajectory.simplified_points, 120):
            parts.append(
                _render_point_marker(
                    point,
                    bounds,
                    color="#f28e2b",
                    radius=2.8,
                    label="kept point",
                )
            )

    if trajectory.raw_points:
        parts.append(
            _render_point_marker(
                trajectory.raw_points[0],
                bounds,
                color="#2ca02c",
                radius=5.0,
                label="start",
            )
        )
        parts.append(
            _render_point_marker(
                trajectory.raw_points[-1],
                bounds,
                color="#1f1f1f",
                radius=5.0,
                label="end",
            )
        )

    min_lon, min_lat, max_lon, max_lat = bounds
    parts.append(
        "<text x='18' y='502' font-size='12' fill='#555555'>"
        f"bounds lon {min_lon:.4f}..{max_lon:.4f}, lat {min_lat:.4f}..{max_lat:.4f}</text>"
    )
    parts.append("</svg>")
    return "\n".join(part for part in parts if part)


def _render_query_table(trajectory: TrajectoryView, zone_names: list[str]) -> str:
    rows: list[str] = []
    for zone_name in zone_names:
        truth = trajectory.zone_truth.get(zone_name)
        pred = trajectory.zone_pred.get(zone_name) if trajectory.has_prediction else None
        match = "n/a" if pred is None else ("match" if pred == truth else "mismatch")
        match_class = "na" if match == "n/a" else match
        rows.append(
            "<tr>"
            f"<td>{_esc(zone_name)}</td>"
            f"<td>{_format_bool(truth)}</td>"
            f"<td>{_format_bool(pred)}</td>"
            f"<td><span class='match {match_class}'>{_esc(match)}</span></td>"
            "</tr>"
        )

    corridor_pred = trajectory.corridor_pred if trajectory.has_prediction else None
    corridor_match = (
        "n/a"
        if corridor_pred is None or trajectory.corridor_truth is None
        else ("match" if corridor_pred == trajectory.corridor_truth else "mismatch")
    )
    corridor_match_class = "na" if corridor_match == "n/a" else corridor_match
    rows.append(
        "<tr>"
        "<td>corridor_main_transit_lane</td>"
        f"<td>{_format_bool(trajectory.corridor_truth)}</td>"
        f"<td>{_format_bool(corridor_pred)}</td>"
        f"<td><span class='match {corridor_match_class}'>{_esc(corridor_match)}</span></td>"
        "</tr>"
    )

    return (
        "<table class='query-table'>"
        "<thead><tr><th>query target</th><th>raw truth</th><th>simplified pred</th><th>status</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
    )


def _render_trajectory_card(
    trajectory: TrajectoryView,
    context_features: list[ContextFeature],
    zone_names: list[str],
    *,
    max_points_per_line: int,
) -> str:
    chip_text = {
        "query-error": "query mismatch",
        "query-positive": "query positive",
        "query-negative": "query negative",
    }[trajectory.category]
    retention = _format_ratio(trajectory.retained_ratio)
    return f"""
<section class='trajectory-card {trajectory.category}'>
  <div class='trajectory-header'>
    <div>
      <h2>Trajectory {_esc(trajectory.trajectory_id)}</h2>
      <span class='chip {trajectory.category}'>{chip_text}</span>
    </div>
    <dl class='stats'>
      <div><dt>raw points</dt><dd>{trajectory.raw_count}</dd></div>
      <div><dt>simplified points</dt><dd>{trajectory.simplified_count if trajectory.simplified_points else "n/a"}</dd></div>
      <div><dt>retained ratio</dt><dd>{retention}</dd></div>
    </dl>
  </div>
  {_render_map_svg(trajectory, context_features, max_points_per_line=max_points_per_line)}
  {_render_query_table(trajectory, zone_names)}
</section>
"""


def render_inspection_html(
    *,
    title: str,
    config: AppConfig,
    context_features: list[ContextFeature],
    trajectories: list[TrajectoryView],
    run_summary: dict[str, object] | None,
    max_points_per_line: int = 1500,
) -> str:
    """Render visual inspection HTML."""
    total = len(trajectories)
    errors = sum(1 for trajectory in trajectories if trajectory.has_query_error)
    positives = sum(1 for trajectory in trajectories if trajectory.has_positive_truth)
    raw_points = sum(trajectory.raw_count for trajectory in trajectories)
    simplified_points = sum(trajectory.simplified_count for trajectory in trajectories)
    run_label = "raw-only"
    if run_summary:
        run_label = (
            f"run_id={run_summary.get('run_id')} "
            f"method={run_summary.get('method_name')} "
            f"budget={run_summary.get('budget_ratio')}"
        )

    legend_items = [
        ("#2f6db3", "Raw trajectory"),
        ("#f28e2b", "Simplified trajectory / kept points"),
        ("#2ca02c", "Start point"),
        ("#1f1f1f", "End point"),
        ("#edc948", "Corridor buffer"),
        ("#e15759", "Port approach zone"),
        ("#59a14f", "Waiting area zone"),
        ("#b07aa1", "Narrow passage zone"),
    ]
    legend = "".join(
        f"<span class='legend-item'><span class='swatch' style='background:{color}'></span>{_esc(label)}</span>"
        for color, label in legend_items
    )

    cards = "\n".join(
        _render_trajectory_card(
            trajectory,
            context_features,
            config.context.zone_names,
            max_points_per_line=max_points_per_line,
        )
        for trajectory in trajectories
    )

    style = """
<style>
  :root {
    color-scheme: light;
    --ink: #1d2327;
    --muted: #5b6470;
    --line: #d8dde3;
    --panel: #ffffff;
    --page: #f1f3ef;
    --red: #c3423f;
    --green: #3c7d46;
    --gray: #69727d;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    background: var(--page);
    color: var(--ink);
    line-height: 1.45;
  }
  header {
    padding: 28px 32px 18px;
    border-bottom: 1px solid var(--line);
    background: #ffffff;
  }
  h1, h2 { margin: 0; letter-spacing: 0; }
  h1 { font-size: 28px; }
  h2 { font-size: 20px; }
  .subtitle { color: var(--muted); margin-top: 8px; }
  main { padding: 24px 32px 40px; max-width: 1180px; margin: 0 auto; }
  .summary-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    gap: 12px;
    margin-bottom: 18px;
  }
  .summary-card, .trajectory-card {
    background: var(--panel);
    border: 1px solid var(--line);
    border-radius: 8px;
  }
  .summary-card { padding: 14px 16px; }
  .summary-card dt { color: var(--muted); font-size: 12px; text-transform: uppercase; }
  .summary-card dd { margin: 4px 0 0; font-size: 22px; font-weight: 700; }
  .legend {
    display: flex;
    flex-wrap: wrap;
    gap: 10px 16px;
    margin: 14px 0 24px;
    color: var(--muted);
    font-size: 14px;
  }
  .legend-item { display: inline-flex; align-items: center; gap: 7px; }
  .swatch { width: 18px; height: 10px; border-radius: 3px; display: inline-block; border: 1px solid rgba(0,0,0,.16); }
  .trajectory-card {
    margin: 18px 0;
    overflow: hidden;
  }
  .trajectory-card.query-error { border-left: 6px solid var(--red); }
  .trajectory-card.query-positive { border-left: 6px solid var(--green); }
  .trajectory-card.query-negative { border-left: 6px solid var(--gray); }
  .trajectory-header {
    padding: 16px 18px;
    display: flex;
    justify-content: space-between;
    gap: 16px;
    border-bottom: 1px solid var(--line);
  }
  .chip {
    display: inline-block;
    margin-top: 8px;
    padding: 3px 8px;
    border-radius: 6px;
    color: #ffffff;
    font-size: 12px;
    font-weight: 700;
    text-transform: uppercase;
  }
  .chip.query-error { background: var(--red); }
  .chip.query-positive { background: var(--green); }
  .chip.query-negative { background: var(--gray); }
  .stats {
    display: grid;
    grid-template-columns: repeat(3, minmax(86px, auto));
    gap: 10px;
    margin: 0;
    text-align: right;
  }
  .stats dt { color: var(--muted); font-size: 12px; }
  .stats dd { margin: 2px 0 0; font-weight: 700; }
  .map {
    display: block;
    width: 100%;
    height: auto;
    border-bottom: 1px solid var(--line);
  }
  .query-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 14px;
  }
  .query-table th, .query-table td {
    padding: 9px 12px;
    border-bottom: 1px solid var(--line);
    text-align: left;
  }
  .query-table th { color: var(--muted); font-size: 12px; text-transform: uppercase; }
  .match {
    display: inline-block;
    padding: 2px 7px;
    border-radius: 6px;
    font-weight: 700;
    font-size: 12px;
  }
  .match.match { color: #245a2d; background: #e5f3e7; }
  .match.mismatch { color: #9b2d2a; background: #fae5e4; }
  .match.na { color: #5c6670; background: #eceff2; }
  @media (max-width: 760px) {
    header, main { padding-left: 16px; padding-right: 16px; }
    .trajectory-header { flex-direction: column; }
    .stats { text-align: left; grid-template-columns: repeat(3, 1fr); }
  }
</style>
"""

    return f"""<!doctype html>
<html lang='en'>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width, initial-scale=1'>
  <title>{_esc(title)}</title>
  {style}
</head>
<body>
  <header>
    <h1>{_esc(title)}</h1>
    <div class='subtitle'>Config: {_esc(config.project.name)} | {_esc(run_label)}</div>
  </header>
  <main>
    <section class='summary-grid' aria-label='summary'>
      <dl class='summary-card'><dt>trajectories</dt><dd>{total}</dd></dl>
      <dl class='summary-card'><dt>query errors</dt><dd>{errors}</dd></dl>
      <dl class='summary-card'><dt>query positives</dt><dd>{positives}</dd></dl>
      <dl class='summary-card'><dt>raw points</dt><dd>{raw_points}</dd></dl>
      <dl class='summary-card'><dt>simplified points</dt><dd>{simplified_points if simplified_points else "n/a"}</dd></dl>
    </section>
    <div class='legend'>{legend}</div>
    {cards if cards else "<p>No trajectories selected.</p>"}
  </main>
</body>
</html>
"""


def write_inspection_html(
    output_path: Path,
    *,
    title: str,
    config: AppConfig,
    context_features: list[ContextFeature],
    trajectories: list[TrajectoryView],
    run_summary: dict[str, object] | None,
    max_points_per_line: int = 1500,
) -> Path:
    """Write inspection HTML and return the absolute path."""
    resolved = resolve_project_path(output_path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    html_text = render_inspection_html(
        title=title,
        config=config,
        context_features=context_features,
        trajectories=trajectories,
        run_summary=run_summary,
        max_points_per_line=max_points_per_line,
    )
    resolved.write_text(html_text, encoding="utf-8")
    return resolved.resolve()
