# Terminal Commands

This file lists useful commands for work in this repository.

## Database Lifecycle

Start PostGIS:

```bash
docker compose -f db/compose.yaml up -d
```

Check container status/logs:

```bash
docker compose -f db/compose.yaml ps
docker compose -f db/compose.yaml logs -f postgis
```

Stop DB (keep data):

```bash
docker compose -f db/compose.yaml down
```

Recreate a fresh DB from init/schema SQL (deletes DB volume):

```bash
docker compose -f db/compose.yaml down -v
docker compose -f db/compose.yaml up -d
```

## Quick DB Smoke Test

```bash
python scripts/smoke_test_db.py
```

## CSV Import (Fast + Resumable)

Basic import:

```bash
python scripts/import_ais_csv.py AISDATA/aisdk-2026-02-05.cleaned.csv
```

Tune workers/chunk sizes:

```bash
python scripts/import_ais_csv.py AISDATA/aisdk-2026-02-05.cleaned.csv \
  --workers 4 \
  --chunk-rows 200000 \
  --copy-buffer-rows 50000
```

Use filename only (auto-resolves from `AISDATA/`):

```bash
python scripts/import_ais_csv.py aisdk-2026-02-05.cleaned.csv
```

Resume behavior:

- Default behavior resumes from stored progress for the same file path.
- Progress is stored in DB table `ais_import_progress`.

Start from scratch for same file path:

```bash
python scripts/import_ais_csv.py AISDATA/aisdk-2026-02-05.cleaned.csv --reset-progress
```

Ignore progress just for this run:

```bash
python scripts/import_ais_csv.py AISDATA/aisdk-2026-02-05.cleaned.csv --no-resume
```

Import only first N data rows (testing):

```bash
python scripts/import_ais_csv.py AISDATA/aisdk-2026-02-05.cleaned.csv --limit 100000
```

## Query Validation / Performance

Range query summary:

```bash
python scripts/run_range_query.py \
  --t0 2026-02-05T10:00:00Z \
  --t1 2026-02-05T11:00:00Z \
  --min-lon 10.0 --min-lat 56.5 \
  --max-lon 11.0 --max-lat 57.5
```

Include sample MMSI list:

```bash
python scripts/run_range_query.py \
  --t0 2026-02-05T10:00:00Z \
  --t1 2026-02-05T11:00:00Z \
  --min-lon 10.0 --min-lat 56.5 \
  --max-lon 11.0 --max-lat 57.5 \
  --list-mmsi --list-limit 50
```

Get query plan:

```bash
python scripts/run_range_query.py \
  --t0 2026-02-05T10:00:00Z \
  --t1 2026-02-05T11:00:00Z \
  --min-lon 10.0 --min-lat 56.5 \
  --max-lon 11.0 --max-lat 57.5 \
  --explain
```

## SQL Helpers

Open psql (see .env file for the URL):

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
