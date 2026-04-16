# Source Layout

- `cli.py`: project command line entrypoint
- `config.py`: YAML config dataclasses + validation
- `db.py`: psycopg helpers
- `paths.py`: project-relative path resolver
- `evaluation/metrics.py`: precision/recall/F1 helpers
- `evaluation/reporting.py`: summary table + SVG plot exporters
- `visualization/inspection.py`: standalone HTML/SVG visual inspection renderer
- `visualization/qgis_package.py`: GeoJSON/QGIS project package helpers
- `simplification/`: baseline simplifiers (uniform, Douglas-Peucker)
- `pipelines/context_loader.py`: load GeoJSON/Shapefile context layers to PostGIS
- `pipelines/`: runnable pipeline steps (bootstrap, context loader, trajectories, labels, subsets, baselines, visual inspection, QGIS export, reports, status)
