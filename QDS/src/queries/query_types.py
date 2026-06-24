"""Typed query schemas and feature padding helpers. See src/queries/README.md for details."""

from __future__ import annotations

from typing import Any

import torch

QUERY_TYPE_ID_RANGE = 0
QUERY_TYPE_ID_KNN = 1
QUERY_TYPE_ID_SIMILARITY = 2
QUERY_TYPE_ID_CLUSTERING = 3
NUM_QUERY_TYPES = 4

QUERY_NAME_TO_ID = {
    "range": QUERY_TYPE_ID_RANGE,
    "knn": QUERY_TYPE_ID_KNN,
    "similarity": QUERY_TYPE_ID_SIMILARITY,
    "clustering": QUERY_TYPE_ID_CLUSTERING,
}
ID_TO_QUERY_NAME = {v: k for k, v in QUERY_NAME_TO_ID.items()}


def normalize_workload_mix(workload_mix: dict[str, float]) -> dict[str, float]:
    """Normalize workload weights to sum to one. See src/queries/README.md for details."""
    filtered = {k.lower(): float(v) for k, v in workload_mix.items() if float(v) > 0.0}
    total = sum(filtered.values())
    if total <= 0.0:
        raise ValueError("Workload mix must contain at least one positive weight.")
    for name in filtered:
        if name not in QUERY_NAME_TO_ID:
            raise ValueError(f"Unknown query type in workload mix: {name}")
    return {k: v / total for k, v in filtered.items()}


def parse_workload_mix(value: str | None, default: dict[str, float]) -> dict[str, float]:
    """Parse CLI workload mix strings like 'range=0.8,knn=0.2'. See src/queries/README.md for details."""
    if value is None or value.strip() == "":
        return normalize_workload_mix(default)

    result: dict[str, float] = {}
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        name, weight = part.split("=", 1)
        result[name.strip().lower()] = float(weight)
    return normalize_workload_mix(result)


def pad_query_features(typed_queries: list[dict[str, Any]]) -> tuple[torch.Tensor, torch.Tensor]:
    """Convert heterogeneous typed queries to padded numeric features + type IDs. See src/queries/README.md for details."""
    feature_dim = 12
    feats = torch.zeros((len(typed_queries), feature_dim), dtype=torch.float32)
    type_ids = torch.zeros((len(typed_queries),), dtype=torch.long)

    for i, query in enumerate(typed_queries):
        qtype = str(query["type"]).lower()
        params = query["params"]
        if qtype == "range":
            type_ids[i] = QUERY_TYPE_ID_RANGE
            feats[i, :6] = torch.tensor(
                [
                    params["lat_min"],
                    params["lat_max"],
                    params["lon_min"],
                    params["lon_max"],
                    params["t_start"],
                    params["t_end"],
                ],
                dtype=torch.float32,
            )
        elif qtype == "knn":
            type_ids[i] = QUERY_TYPE_ID_KNN
            feats[i, :5] = torch.tensor(
                [
                    params["lat"],
                    params["lon"],
                    params["t_center"],
                    params["t_half_window"],
                    params["k"],
                ],
                dtype=torch.float32,
            )
        elif qtype == "similarity":
            type_ids[i] = QUERY_TYPE_ID_SIMILARITY
            feats[i, :5] = torch.tensor(
                [
                    params["lat_query_centroid"],
                    params["lon_query_centroid"],
                    params["t_start"],
                    params["t_end"],
                    params["radius"],
                ],
                dtype=torch.float32,
            )
            ref = query.get("reference", [])
            if ref:
                ref_t = torch.tensor(ref, dtype=torch.float32)
                feats[i, 5:8] = ref_t.mean(dim=0)
        elif qtype == "clustering":
            type_ids[i] = QUERY_TYPE_ID_CLUSTERING
            feats[i, :8] = torch.tensor(
                [
                    params["lat_min"],
                    params["lat_max"],
                    params["lon_min"],
                    params["lon_max"],
                    params["t_start"],
                    params["t_end"],
                    params["eps"],
                    params["min_samples"],
                ],
                dtype=torch.float32,
            )
        else:
            raise ValueError(f"Unsupported query type: {qtype}")
    return feats, type_ids
