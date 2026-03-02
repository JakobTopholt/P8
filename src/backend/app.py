import os
import re
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Generator

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


def _sanitize_identifier(raw_name: str, fallback: str) -> str:
	if not raw_name:
		return fallback
	if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", raw_name):
		return fallback
	return raw_name


BASE_DIR = Path(__file__).resolve().parent
DATABASE_PATH = Path(os.getenv("DATABASE_PATH", BASE_DIR / "databases" / "ais_index.db"))
SOURCE_TABLE = _sanitize_identifier(os.getenv("SOURCE_TABLE", "ais_data"), "ais_data")
SOURCE_MMSI_COLUMN = _sanitize_identifier(os.getenv("SOURCE_MMSI_COLUMN", "MMSI"), "MMSI")
LOOKUP_TABLE = _sanitize_identifier(os.getenv("LOOKUP_TABLE", "mmsi_lookup"), "mmsi_lookup")
AUTO_REBUILD_LOOKUP_ON_STARTUP = os.getenv("AUTO_REBUILD_LOOKUP_ON_STARTUP", "false").lower() == "true"
DEFAULT_SEARCH_LIMIT = int(os.getenv("DEFAULT_SEARCH_LIMIT", "50"))
MAX_SEARCH_LIMIT = int(os.getenv("MAX_SEARCH_LIMIT", "200"))


class MmsiSearchResponse(BaseModel):
	query: str
	count: int
	results: list[str]


class RebuildResponse(BaseModel):
	inserted: int
	total: int


class DataPointResponse(BaseModel):
	id: int
	mmsi: str
	lat: float
	lon: float
	timestamp: str


def create_connection() -> sqlite3.Connection:
	DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
	connection = sqlite3.connect(DATABASE_PATH)
	connection.row_factory = sqlite3.Row
	return connection


def get_connection() -> Generator[sqlite3.Connection, None, None]:
	connection = create_connection()
	try:
		yield connection
	finally:
		connection.close()


def initialize_lookup_table(connection: sqlite3.Connection) -> None:
	with closing(connection.cursor()) as cursor:
		cursor.execute(
			f"""
			CREATE TABLE IF NOT EXISTS {LOOKUP_TABLE} (
				mmsi TEXT PRIMARY KEY,
				last_seen_source_rowid INTEGER
			)
			"""
		)
		cursor.execute(
			f"CREATE INDEX IF NOT EXISTS idx_{LOOKUP_TABLE}_mmsi ON {LOOKUP_TABLE}(mmsi)"
		)
	connection.commit()


def _source_table_exists(connection: sqlite3.Connection) -> bool:
	with closing(connection.cursor()) as cursor:
		cursor.execute(
			"SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
			(SOURCE_TABLE,),
		)
		return cursor.fetchone() is not None


def rebuild_lookup_table(connection: sqlite3.Connection) -> RebuildResponse:
	initialize_lookup_table(connection)

	if not _source_table_exists(connection):
		raise HTTPException(
			status_code=404,
			detail=(
				f"Source table '{SOURCE_TABLE}' was not found in {DATABASE_PATH}. "
				"Set SOURCE_TABLE and SOURCE_MMSI_COLUMN to match your large AIS database schema."
			),
		)

	with closing(connection.cursor()) as cursor:
		cursor.execute(f"SELECT COUNT(*) AS total_before FROM {LOOKUP_TABLE}")
		total_before = int(cursor.fetchone()[0])

		cursor.execute(
			f"""
			INSERT OR IGNORE INTO {LOOKUP_TABLE}(mmsi, last_seen_source_rowid)
			SELECT DISTINCT CAST({SOURCE_MMSI_COLUMN} AS TEXT), MAX(rowid)
			FROM {SOURCE_TABLE}
			WHERE {SOURCE_MMSI_COLUMN} IS NOT NULL AND TRIM(CAST({SOURCE_MMSI_COLUMN} AS TEXT)) != ''
			GROUP BY CAST({SOURCE_MMSI_COLUMN} AS TEXT)
			"""
		)
		inserted = cursor.rowcount if cursor.rowcount is not None else 0

		cursor.execute(f"SELECT COUNT(*) AS total_after FROM {LOOKUP_TABLE}")
		total_after = int(cursor.fetchone()[0])

	connection.commit()

	if inserted < 0:
		inserted = max(0, total_after - total_before)

	return RebuildResponse(inserted=inserted, total=total_after)


def search_mmsi_ids(connection: sqlite3.Connection, query: str, limit: int) -> MmsiSearchResponse:
	initialize_lookup_table(connection)

	safe_query = query.strip()
	if not safe_query:
		return MmsiSearchResponse(query="", count=0, results=[])

	with closing(connection.cursor()) as cursor:
		cursor.execute(
			f"""
			SELECT mmsi
			FROM {LOOKUP_TABLE}
			WHERE mmsi LIKE ?
			ORDER BY mmsi ASC
			LIMIT ?
			""",
			(f"{safe_query}%", limit),
		)
		rows = cursor.fetchall()

	results = [str(row[0]) for row in rows]
	return MmsiSearchResponse(query=safe_query, count=len(results), results=results)


app = FastAPI(title="P8 Backend API", version="1.0.0")

app.add_middleware(
	CORSMiddleware,
	allow_origins=["*"],
	allow_credentials=True,
	allow_methods=["*"],
	allow_headers=["*"],
)


@app.on_event("startup")
def startup() -> None:
	with create_connection() as connection:
		initialize_lookup_table(connection)
		if AUTO_REBUILD_LOOKUP_ON_STARTUP:
			try:
				rebuild_lookup_table(connection)
			except HTTPException:
				pass


@app.get("/api/health")
def health_check() -> dict[str, str]:
	return {"status": "ok"}


@app.post("/api/mmsi/rebuild", response_model=RebuildResponse)
def rebuild_mmsi_lookup(
	connection: sqlite3.Connection = Depends(get_connection),
) -> RebuildResponse:
	return rebuild_lookup_table(connection)


@app.get("/api/mmsi/search", response_model=MmsiSearchResponse)
def search_mmsi(
	q: str = Query(default="", description="Prefix query for MMSI IDs"),
	limit: int = Query(default=DEFAULT_SEARCH_LIMIT, ge=1, le=MAX_SEARCH_LIMIT),
	connection: sqlite3.Connection = Depends(get_connection),
) -> MmsiSearchResponse:
	return search_mmsi_ids(connection, q, limit)


@app.get("/api/datapoints", response_model=list[DataPointResponse])
def get_datapoints_by_mmsis(
	mmsis: str = Query(default="", description="Comma-separated MMSI IDs"),
	connection: sqlite3.Connection = Depends(get_connection),
) -> list[DataPointResponse]:
	"""
	Fetch datapoints from the source table for given MMSIs.
	Query parameter: mmsis=123456789,987654321
	"""
	if not _source_table_exists(connection):
		raise HTTPException(
			status_code=404,
			detail=f"Source table '{SOURCE_TABLE}' not found in {DATABASE_PATH}",
		)

	mmsi_list = [m.strip() for m in mmsis.split(",") if m.strip()]
	if not mmsi_list:
		return []

	# Build query with proper escaping
	placeholders = ",".join(["?"] * len(mmsi_list))
	query = f"""
		SELECT 
			rowid as id,
			CAST({SOURCE_MMSI_COLUMN} AS TEXT) as mmsi,
			CAST(lat AS REAL) as lat,
			CAST(lon AS REAL) as lon,
			timestamp
		FROM {SOURCE_TABLE}
		WHERE CAST({SOURCE_MMSI_COLUMN} AS TEXT) IN ({placeholders})
		ORDER BY timestamp ASC
		LIMIT 10000
	"""

	with closing(connection.cursor()) as cursor:
		cursor.execute(query, mmsi_list)
		rows = cursor.fetchall()

	results = [
		DataPointResponse(
			id=int(row[0]),
			mmsi=str(row[1]),
			lat=float(row[2]),
			lon=float(row[3]),
			timestamp=str(row[4]),
		)
		for row in rows
	]

	return results
