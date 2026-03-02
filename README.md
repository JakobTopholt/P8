# P8
A mobility project on AIS data performing query simplifications

## Backend MMSI lookup API

The backend now supports a fast MMSI lookup flow for frontend search:

- A dedicated lookup table (`mmsi_lookup`) stores unique MMSI values.
- MMSIs are collected from a larger source table (for example AIS datapoints imported from CSVs).
- The frontend can query only the lookup table, avoiding full scans of the raw datapoints table.

### Install dependencies

```bash
pip install -r requirements.txt
```

### Configure database connection

Environment variables (optional):

- `DATABASE_PATH` (default: `src/backend/databases/ais_index.db`)
- `SOURCE_TABLE` (default: `ais_data`)
- `SOURCE_MMSI_COLUMN` (default: `MMSI`)
- `LOOKUP_TABLE` (default: `mmsi_lookup`)
- `AUTO_REBUILD_LOOKUP_ON_STARTUP` (`true` or `false`, default: `false`)

Example PowerShell setup:

```powershell
$env:DATABASE_PATH="src/backend/databases/ais_index.db"
$env:SOURCE_TABLE="ais_data"
$env:SOURCE_MMSI_COLUMN="MMSI"
```

### Run backend

```bash
python -m uvicorn src.backend.app:app --reload --host 0.0.0.0 --port 8000
```

Or from PowerShell:

```powershell
python -m uvicorn src.backend.app:app --reload --host 0.0.0.0 --port 8000
```

### Endpoints

- `GET /api/health` - health check
- `POST /api/mmsi/rebuild` - collect all unique MMSIs from source table into lookup table
- `GET /api/mmsi/search?q=123&limit=50` - fast prefix search over lookup table
