"""General simplification benchmark runner.

This module is the neutral entrypoint for benchmark execution. The concrete
method implementations still live behind the `baselines` module name because
that file owns benchmark table writes and report metrics.
"""

from __future__ import annotations

from .baselines import run

__all__ = ["run"]
