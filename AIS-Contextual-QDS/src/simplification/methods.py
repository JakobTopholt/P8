"""Canonical simplification method names."""

from __future__ import annotations

METHOD_UNIFORM = "uniform"
METHOD_DOUGLAS_PEUCKER = "douglas_peucker"
METHOD_QUERY_WITNESS = "query_witness"

CANONICAL_METHODS = frozenset(
    {
        METHOD_UNIFORM,
        METHOD_DOUGLAS_PEUCKER,
        METHOD_QUERY_WITNESS,
    }
)


def normalize_method_name(method: str) -> str:
    """Validate and return a canonical method name."""
    token = method.strip().lower()
    if not token:
        raise ValueError("Method name cannot be empty.")
    if token not in CANONICAL_METHODS:
        raise ValueError(
            f"Unknown simplification method {method!r}. "
            f"Allowed canonical methods: {sorted(CANONICAL_METHODS)}."
        )
    return token


def normalize_method_names(methods: list[str]) -> list[str]:
    """Normalize method tokens while preserving their first-seen order."""
    ordered_unique: list[str] = []
    seen: set[str] = set()
    for raw_method in methods:
        method = normalize_method_name(raw_method)
        if method not in seen:
            ordered_unique.append(method)
            seen.add(method)
    if not ordered_unique:
        raise ValueError("No simplification methods configured.")
    return ordered_unique
