# P8

Mobility project centered on AIS data, with two main workstreams:

- Data cleaning and database ingestion/query tooling (root + `ais_pipeline/`)
- Machine-learning query-driven simplification research in [`QDS/`](QDS/)

## Documentation Convention

This repository uses folder-local `README.md` files as the source of
documentation.

- Each folder with project context relevant for documentation should contain its own `README.md`.

## Documentation Map

- [`ais_pipeline/README.md`](ais_pipeline/README.md): root AIS cleaning pipeline layout.
- [`QDS/README.md`](QDS/README.md): ML simplification project overview and usage.
- [`db/README.md`](db/README.md): PostGIS setup, lifecycle, and SQL checks.
- [`ais_pipeline/environment/README.md`](ais_pipeline/environment/README.md): Java/Hadoop/Spark bootstrap helpers.
- [`ais_pipeline/tools/README.md`](ais_pipeline/tools/README.md): utility scripts and experiments.
- [`AISDATA/README.md`](AISDATA/README.md): data folder conventions and expected files.

## Project Layout (Top-Level)

- `ais_pipeline/`: root AIS Spark cleaning pipeline package.
- `db/`: local PostGIS compose, SQL assets, and DB operational scripts.
- `AISDATA/`: AIS input/output data files.
- `QDS/`: ML simplification research project.
- `frontend/`: frontend application.
- `Makefile`: shortcuts for common local commands.
- `.env.example`: example runtime/environment variables.

## Quick Start (Root Pipeline)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

Direct module entrypoint (equivalent):

```bash
python -m ais_pipeline
```

## Frontend Quick Start

```bash
cd frontend
npm install
npm run dev
```

## Makefile Shortcuts

```bash
make db-up
make db-smoke
make db-import
make db-query
make pipeline
make frontend-dev
```
