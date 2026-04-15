"""Metric calculations for query-fidelity evaluation."""

from __future__ import annotations


def classification_metrics(tp: int, fp: int, fn: int) -> dict[str, float]:
    """Compute precision/recall/F1 from classification counts."""
    precision = float(tp) / float(tp + fp) if (tp + fp) > 0 else 0.0
    recall = float(tp) / float(tp + fn) if (tp + fn) > 0 else 0.0
    f1 = (2.0 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0
    return {
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }
