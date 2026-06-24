"""AIS trajectory loading and synthetic generation utilities. See src/data/README.md for details."""

from __future__ import annotations

import math
from pathlib import Path

import pandas as pd
import torch


def _to_time_seconds(series: pd.Series) -> pd.Series:
    """Convert timestamp-like values to floating-point seconds. See src/data/README.md for details."""
    if pd.api.types.is_numeric_dtype(series):
        return series.astype(float)
    dt = pd.to_datetime(series, utc=True, errors="coerce")
    if dt.isna().all():
        return pd.Series(range(len(series)), dtype=float)
    base = dt.min()
    return (dt - base).dt.total_seconds().astype(float)


def _resolve_col(df: pd.DataFrame, aliases: list[str]) -> str:
    """Resolve a canonical column from aliases. See src/data/README.md for details."""
    lowered = {c.lower(): c for c in df.columns}
    for alias in aliases:
        if alias in lowered:
            return lowered[alias]
    raise ValueError(f"Missing required column aliases: {aliases}")


def load_ais_csv(
    csv_path: str,
    max_points_per_ship: int | None = None,
    return_mmsis: bool = False,
) -> list[torch.Tensor] | tuple[list[torch.Tensor], list[int]]:
    """Load AIS trajectories from CSV into per-trajectory tensors.

    If ``return_mmsis=True``, also return the original MMSI identifiers aligned
    with the trajectory list so downstream writers can preserve vessel IDs.
    See ``src/data/README.md`` for details.
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV path does not exist: {csv_path}")

    df = pd.read_csv(path, low_memory=False)
    df.columns = [str(c).lstrip("#").strip() for c in df.columns]
    mmsi_col = _resolve_col(df, ["mmsi", "ship_id", "vessel_id"])
    lat_col = _resolve_col(df, ["lat", "latitude"])
    lon_col = _resolve_col(df, ["lon", "longitude"])
    speed_col = _resolve_col(df, ["speed", "sog"])
    heading_col = _resolve_col(df, ["heading", "cog"])
    time_col = _resolve_col(df, ["timestamp", "time", "datetime"])

    df = df[[mmsi_col, lat_col, lon_col, speed_col, heading_col, time_col]].copy()
    df["_time"] = _to_time_seconds(df[time_col])

    # Coerce numeric columns so non-numeric entries become NaN, then drop
    # invalid rows. AIS feeds frequently contain missing heading/speed and
    # sentinel values (e.g. heading=511) that would propagate as NaN through
    # min-max normalization and collapse training to loss=NaN from epoch 1.
    for col in (lat_col, lon_col, speed_col, heading_col):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df.loc[(df[heading_col] < 0) | (df[heading_col] >= 360), heading_col] = float("nan")
    df.loc[(df[speed_col] < 0) | (df[speed_col] > 102.2), speed_col] = float("nan")
    df.loc[(df[lat_col] < -90) | (df[lat_col] > 90), lat_col] = float("nan")
    df.loc[(df[lon_col] < -180) | (df[lon_col] > 180), lon_col] = float("nan")
    df = df.dropna(subset=[lat_col, lon_col, speed_col, heading_col, "_time"])

    df = df.sort_values([mmsi_col, "_time"]).reset_index(drop=True)

    trajectories: list[torch.Tensor] = []
    mmsis: list[int] = []
    for mmsi_val, grp in df.groupby(mmsi_col, sort=False):
        if len(grp) < 4:
            continue
        if max_points_per_ship is not None and len(grp) > max_points_per_ship:
            idx = torch.linspace(0, len(grp) - 1, steps=max_points_per_ship).long().tolist()
            grp = grp.iloc[idx]

        t = torch.tensor(grp["_time"].to_numpy(), dtype=torch.float32)
        lat = torch.tensor(grp[lat_col].to_numpy(), dtype=torch.float32)
        lon = torch.tensor(grp[lon_col].to_numpy(), dtype=torch.float32)
        speed = torch.tensor(grp[speed_col].to_numpy(), dtype=torch.float32)
        heading = torch.tensor(grp[heading_col].to_numpy(), dtype=torch.float32)

        is_start = torch.zeros_like(t)
        is_end = torch.zeros_like(t)
        is_start[0] = 1.0
        is_end[-1] = 1.0

        turn_score = torch.zeros_like(t)
        if len(t) > 2:
            d = torch.abs(heading[1:] - heading[:-1])
            d = torch.minimum(d, 360.0 - d)
            turn_score[1:] = d / 180.0

        traj = torch.stack([t, lat, lon, speed, heading, is_start, is_end, turn_score], dim=1)
        trajectories.append(traj)
        try:
            mmsis.append(int(mmsi_val))
        except (TypeError, ValueError):
            mmsis.append(0)

    if not trajectories:
        raise ValueError("No valid trajectories found in CSV.")
    if return_mmsis:
        return trajectories, mmsis
    return trajectories


def generate_synthetic_ais_data(
    n_ships: int = 24,
    n_points_per_ship: int = 200,
    seed: int = 42,
) -> list[torch.Tensor]:
    """Generate synthetic AIS trajectories with realistic temporal continuity. See src/data/README.md for details."""
    g = torch.Generator()
    g.manual_seed(int(seed))

    trajectories: list[torch.Tensor] = []
    for ship_idx in range(n_ships):
        time = torch.arange(n_points_per_ship, dtype=torch.float32)
        time = time * 60.0 + 1000.0 * ship_idx

        start_lat = 30.0 + 20.0 * torch.rand(1, generator=g).item()
        start_lon = -20.0 + 40.0 * torch.rand(1, generator=g).item()

        drift_lat = (torch.rand(1, generator=g).item() - 0.5) * 0.02
        drift_lon = (torch.rand(1, generator=g).item() - 0.5) * 0.02

        wave = torch.sin(torch.linspace(0, 8.0 * math.pi, n_points_per_ship))
        lat_noise = 0.002 * torch.randn(n_points_per_ship, generator=g)
        lon_noise = 0.002 * torch.randn(n_points_per_ship, generator=g)

        lat = start_lat + drift_lat * torch.arange(n_points_per_ship) + 0.05 * wave + lat_noise
        lon = start_lon + drift_lon * torch.arange(n_points_per_ship) + 0.05 * torch.cos(wave) + lon_noise

        speed = 8.0 + 4.0 * torch.rand(n_points_per_ship, generator=g)
        heading = (torch.atan2(torch.diff(lat, prepend=lat[:1]), torch.diff(lon, prepend=lon[:1])) * 180.0 / math.pi) % 360.0

        is_start = torch.zeros(n_points_per_ship, dtype=torch.float32)
        is_end = torch.zeros(n_points_per_ship, dtype=torch.float32)
        is_start[0] = 1.0
        is_end[-1] = 1.0

        turn = torch.zeros(n_points_per_ship, dtype=torch.float32)
        if n_points_per_ship > 2:
            hd = torch.abs(heading[1:] - heading[:-1])
            hd = torch.minimum(hd, 360.0 - hd)
            turn[1:] = hd / 180.0

        traj = torch.stack([time, lat, lon, speed, heading, is_start, is_end, turn], dim=1)
        trajectories.append(traj)

    return trajectories
