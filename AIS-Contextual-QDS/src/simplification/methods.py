"""Canonical simplification method names and compatibility aliases."""

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

METHOD_ALIASES = {
    METHOD_UNIFORM: METHOD_UNIFORM,
    "uniform_subsampling": METHOD_UNIFORM,
    METHOD_DOUGLAS_PEUCKER: METHOD_DOUGLAS_PEUCKER,
    "douglas-peucker": METHOD_DOUGLAS_PEUCKER,
    "douglaspeucker": METHOD_DOUGLAS_PEUCKER,
    "dp": METHOD_DOUGLAS_PEUCKER,
    METHOD_QUERY_WITNESS: METHOD_QUERY_WITNESS,
    "query-witness": METHOD_QUERY_WITNESS,
    "query_driven": METHOD_QUERY_WITNESS,
    "query-driven": METHOD_QUERY_WITNESS,
    "b3": METHOD_QUERY_WITNESS,
}

METHOD_COMPATIBILITY_ALIASES = {
    METHOD_UNIFORM: frozenset({METHOD_UNIFORM, "uniform_subsampling"}),
    METHOD_DOUGLAS_PEUCKER: frozenset(
        {METHOD_DOUGLAS_PEUCKER, "douglas-peucker", "douglaspeucker", "dp"}
    ),
    METHOD_QUERY_WITNESS: frozenset(
        {METHOD_QUERY_WITNESS, "query-witness", "query_driven", "query-driven", "b3"}
    ),
}


def normalize_method_name(method: str) -> str:
    """Return the canonical method name for a configured or CLI method token."""
    token = method.strip().lower().replace(" ", "_")
    if not token:
        raise ValueError("Method name cannot be empty.")
    try:
        return METHOD_ALIASES[token]
    except KeyError as exc:
        raise ValueError(
            f"Unknown simplification method {method!r}. "
            f"Allowed canonical methods: {sorted(CANONICAL_METHODS)}. "
            f"Compatibility aliases: {sorted(METHOD_ALIASES)}."
        ) from exc


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


def expand_method_filter(methods: list[str] | None) -> list[str] | None:
    """Expand a user filter so legacy rows remain discoverable after renames."""
    if not methods:
        return None

    expanded: set[str] = set()
    for raw_method in methods:
        canonical = normalize_method_name(raw_method)
        expanded.update(METHOD_COMPATIBILITY_ALIASES[canonical])
    return sorted(expanded)
