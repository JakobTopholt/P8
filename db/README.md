# Database (`db/`)

This folder contains the local PostGIS service definition and initialization SQL.

- `compose.yaml`: PostGIS container definition.
- `init.sql`: PostGIS extensions.
- `schema.sql`: AIS tables and indexes.
- `smoke_test_db.py`: DB connectivity/PostGIS sanity check.
- `import_ais_csv.py`: cleaned AIS CSV import utility.
- `run_range_query.py`: range query validation utility.
- `api_server.py`: HTTP API that serves DataPoints for the frontend.

## Database Lifecycle

Start PostGIS:

```bash
docker compose -f db/compose.yaml up -d
```

Check status/logs:

```bash
docker compose -f db/compose.yaml ps
docker compose -f db/compose.yaml logs -f postgis
```

Stop DB (keep data volume):

```bash
docker compose -f db/compose.yaml down
```

Recreate fresh DB from `init.sql` and `schema.sql` (deletes volume):

```bash
docker compose -f db/compose.yaml down -v
docker compose -f db/compose.yaml up -d
```

## Local Connection Defaults

From `compose.yaml`:

- Database: `ais`
- User: `ais`
- Password: `aisdev`
- Host port: `5433` (container `5432`)

Example URL:

```bash
export DATABASE_URL="postgresql://ais:aisdev@localhost:5433/ais"
```

## SQL Helpers

Open `psql`:

```bash
psql "$DATABASE_URL"
```

Useful checks:

```sql
SELECT COUNT(*) FROM ais_points_cleaned;
SELECT * FROM ais_import_progress;
\d+ ais_points_cleaned
\d+ ais_import_progress
```

## DataPoints API for Frontend

Start the backend API:

```bash
python db/api_server.py
```

Or from the root Makefile:

```bash
make db-api
```

The API listens on `http://127.0.0.1:8000` by default.

Endpoints:

- `GET /api/health`
- `GET /api/datapoints?limit=5000`

Optional query parameters for `/api/datapoints`:

- `limit` (max 20000)
- `mmsi` (integer)
- `t0` (ISO-8601 start timestamp)
- `t1` (ISO-8601 end timestamp)

The frontend Vite dev server proxies `/api/*` to this backend, so frontend code can call `/api/datapoints` directly during local development.

## Related Tools

- [`../ais_pipeline/README.md`](../ais_pipeline/README.md) for root pipeline documentation.
- [`../ais_pipeline/tools/README.md`](../ais_pipeline/tools/README.md) for optional utility scripts.
