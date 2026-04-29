"""Compatibility wrapper for renamed query-witness evidence extraction."""

from .query_witness_evidence import fetch_b3_point_evidence, fetch_query_witness_point_evidence

__all__ = ["fetch_query_witness_point_evidence", "fetch_b3_point_evidence"]
