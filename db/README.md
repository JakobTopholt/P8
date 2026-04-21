# Database (`db/`)

This folder contains the local PostGIS service definition and initialization SQL.

- `compose.yaml`: PostGIS container definition.
- `init.sql`: PostGIS extensions.
- `schema.sql`: AIS tables and indexes.
- `smoke_test_db.py`: DB connectivity/PostGIS sanity check.
- `import_ais_csv.py`: cleaned AIS CSV file/directory import utility.
- `run_range_query.py`: range query validation utility.

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

Import one file or an entire folder:

```bash
python db/import_ais_csv.py AISDATA/cleaned/aisdk-2026-01-01.cleaned.csv
python db/import_ais_csv.py AISDATA/cleaned
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

## Related Tools

- [`../ais_pipeline/README.md`](../ais_pipeline/README.md) for root pipeline documentation.
- [`../ais_pipeline/tools/README.md`](../ais_pipeline/tools/README.md) for optional utility scripts.
