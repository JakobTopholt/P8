"""General simplification benchmark runner.

This module is the neutral entrypoint for benchmark execution. The concrete
method implementations live behind the current `baselines` module name for
backward-compatible CLI aliases and report filenames.
"""

from __future__ import annotations

from .baselines import run

__all__ = ["run"]
