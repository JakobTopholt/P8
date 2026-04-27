"""General simplification benchmark runner.

This module is the neutral entrypoint for benchmark execution. The current
implemented methods still live in `baselines` until query-driven methods are
added, but callers should depend on this module going forward.
"""

from __future__ import annotations

from .baselines import run

__all__ = ["run"]
