"""Combine multiple per-day preprocessed AIS CSVs into one training file.

The loader groups rows by MMSI; the same vessel appearing on two days would
collapse into a single trajectory with a day-long time gap. To prevent that,
this script offsets each day's MMSIs by a unique billion-scale constant so the
loader treats per-day vessel sessions as independent trajectories.

Usage:
    python -m src.data.combine_days \\
        --input /ceph/.../preprocessed_2026-02-05.csv \\
                /ceph/.../preprocessed_2026-02-06.csv \\
                /ceph/.../preprocessed_2026-02-07.csv \\
        --output /ceph/.../preprocessed_train_combined.csv
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd


MMSI_OFFSET_PER_DAY = 1_000_000_000  # 1B; real MMSIs are 9 digits (max 999999999).


def combine(input_paths: list[Path], output_path: Path) -> None:
    if not input_paths:
        raise ValueError("at least one input CSV required")

    frames = []
    for day_idx, p in enumerate(input_paths):
        if not p.exists():
            raise FileNotFoundError(f"input CSV missing: {p}")
        df = pd.read_csv(p)
        if "MMSI" in df.columns:
            mmsi_col = "MMSI"
        elif "mmsi" in df.columns:
            mmsi_col = "mmsi"
        else:
            raise ValueError(f"no MMSI column in {p}; got {list(df.columns)}")
        df[mmsi_col] = df[mmsi_col].astype("int64") + day_idx * MMSI_OFFSET_PER_DAY
        n_traj = df[mmsi_col].nunique()
        print(f"  loaded {p.name}: {len(df):>10,} rows, {n_traj:>6,} unique MMSIs after offset {day_idx}", flush=True)
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(output_path, index=False)
    print(
        f"\nwrote {output_path}: {len(combined):,} rows, "
        f"{combined[mmsi_col].nunique():,} trajectories (post-offset)",
        flush=True,
    )


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", nargs="+", required=True, help="paths to preprocessed CSVs (one per day)")
    p.add_argument("--output", required=True, help="combined CSV output path")
    args = p.parse_args()

    combine([Path(s) for s in args.input], Path(args.output))
    return 0


if __name__ == "__main__":
    sys.exit(main())
