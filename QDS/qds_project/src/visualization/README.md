# Visualization Module

Plotting utilities for AIS trajectory data, query workloads, importance scores,
and simplification results. Uses `matplotlib` with the `Agg` backend for
headless rendering (no display required).

---

## Components

### `trajectory_visualizer.py`

**`plot_trajectories(trajectories, title, save_path)`**  
Plots vessel paths as coloured lines in lat/lon space. Each trajectory is a
separate colour. Start points are marked with a green circle; end points with
a blue square.

**`plot_queries_on_trajectories(trajectories, queries, title, save_path)`**  
Overlays semi-transparent query rectangles on top of the trajectory map.
Each rectangle shows the `[lat_min, lat_max] × [lon_min, lon_max]` footprint
of a spatiotemporal query.

**`plot_typed_queries_on_trajectories(trajectories, typed_queries, title, save_path)`**  
Renders queries with type-specific visual styles:

| Query type     | Visual encoding                                                                                                                                  |
|----------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| `range`        | Solid red semi-transparent rectangle                                                                                                             |
| `intersection` | Dashed blue semi-transparent rectangle                                                                                                           |
| `aggregation`  | Dotted green semi-transparent rectangle                                                                                                          |
| `nearest`/kNN  | Orange marker at query location; **circle** for *k=1*, **star** for *k>1*; size scales with √k; *k>1* queries are annotated with their *k* value |

The legend groups mixed workloads by type and shows per-type query counts.
For kNN queries, distinct *k* values are noted in the legend label.

---

### `importance_visualizer.py`

**`plot_importance(points, importance_scores, title, save_path)`**  
Scatter plot of the full point cloud coloured by importance score (low →
blue, high → red via the `RdYlBu_r` colormap). Provides a spatial view of
which regions are ranked as most query-relevant.

**`plot_trajectories_with_importance_and_queries(trajectories, points, importance_scores, queries, title, save_path)`**  
Combined plot: trajectory lines coloured by importance + semi-transparent
query rectangles. Gives a unified view of the query workload alongside
point importance.

**`plot_simplification_results(trajectories, all_points, retained_mask, importance_scores, queries, title, save_path)`**  
Visualises retained and removed points alongside query regions. Draws all
trajectory lines for context, marks retained (green) and removed (red)
points, and overlays semi-transparent query rectangles.

**`plot_simplification_time_slices(all_points, retained_mask, importance_scores, queries, title, n_slices, save_path)`**  
Divides the time axis into `n_slices` (default: 4) equal windows and plots
retained vs. removed points for each window. Useful for inspecting how
compression varies across different temporal segments.

**`plot_turn_scores(points, retained_mask, title, save_path)`**  
Scatter plot highlighting trajectory points by turn intensity (column 7).
Requires an 8-feature point tensor; skips silently if the column is absent.

---

## Output Files

When running the full experiment, visualisations are saved to the system
temporary directory and `results/`:

| File                               | Content                                       |
|------------------------------------|-----------------------------------------------|
| `ais_trajectories.png`             | Vessel paths in lat/lon space                 |
| `ais_queries.png`                  | Trajectories + semi-transparent query boxes   |
| `ais_importance.png`               | Point cloud coloured by importance score      |
| `ais_combined.png`                 | Lines + importance colours + query boxes      |
| `results/simplification_visualization.png` | Simplification summary + query overlay|
| `results/simplification_time_slices.png`   | 4 time-window comparison panels       |
