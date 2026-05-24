# Visualization Module

This package is intentionally minimal in v2.

## Current State

- Only `__init__.py` is present right now.
- `VisualizationConfig.enabled` exists in the experiment config, but the current experiment runner writes JSON and fixed-width tables instead of plots.
- This folder is the extension point for future plotting or reporting utilities.

## Expected Contents

Future visualization code should live here if the rebuild grows back into charting or notebook-style analysis.
