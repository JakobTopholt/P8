import argparse
import csv
import io
import os
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from functools import lru_cache

from dotenv import load_dotenv
import psycopg


REQUIRED_HEADERS = [
    "MMSI",
    "Timestamp",  # normalized from "# Timestamp"
    "Type of mobile",
    "Latitude",
    "Longitude",
    "SOG",
    "COG",
    "Ship type",
]


CREATE_PROGRESS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ais_import_progress (
    source_path TEXT PRIMARY KEY,
    source_size BIGINT NOT NULL,
    source_mtime_ns BIGINT NOT NULL,
    delimiter TEXT NOT NULL,
    rows_read BIGINT NOT NULL DEFAULT 0,
    inserted BIGINT NOT NULL DEFAULT 0,
    skipped BIGINT NOT NULL DEFAULT 0,
    skip_missing_ts BIGINT NOT NULL DEFAULT 0,
    skip_missing_core BIGINT NOT NULL DEFAULT 0,
    skip_bad_coords BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

SELECT_PROGRESS_SQL = """
SELECT
    source_size,
    source_mtime_ns,
    delimiter,
    rows_read,
    inserted,
    skipped,
    skip_missing_ts,
    skip_missing_core,
    skip_bad_coords
FROM ais_import_progress
WHERE source_path = %s
"""

UPSERT_PROGRESS_SQL = """
INSERT INTO ais_import_progress (
    source_path,
    source_size,
    source_mtime_ns,
    delimiter,
    rows_read,
    inserted,
    skipped,
    skip_missing_ts,
    skip_missing_core,
    skip_bad_coords,
    updated_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (source_path) DO UPDATE SET
    source_size = EXCLUDED.source_size,
    source_mtime_ns = EXCLUDED.source_mtime_ns,
    delimiter = EXCLUDED.delimiter,
    rows_read = EXCLUDED.rows_read,
    inserted = EXCLUDED.inserted,
    skipped = EXCLUDED.skipped,
    skip_missing_ts = EXCLUDED.skip_missing_ts,
    skip_missing_core = EXCLUDED.skip_missing_core,
    skip_bad_coords = EXCLUDED.skip_bad_coords,
    updated_at = NOW()
"""

DELETE_PROGRESS_SQL = "DELETE FROM ais_import_progress WHERE source_path = %s"


def resolve_csv_path(csv_path_arg: str, aisdata_dir: str) -> str:
    if os.path.isabs(csv_path_arg):
        return csv_path_arg

    direct = os.path.abspath(csv_path_arg)
    if os.path.exists(direct):
        return direct

    in_aisdata = os.path.abspath(os.path.join(aisdata_dir, csv_path_arg))
    if os.path.exists(in_aisdata):
        return in_aisdata

    return direct


def to_int(x: str | None):
    if x is None:
        return None
    x = x.strip()
    if x == "":
        return None
    try:
        return int(float(x))
    except ValueError:
        return None


def to_float(x: str | None):
    if x is None:
        return None
    x = x.strip()
    if x == "":
        return None
    try:
        return float(x)
    except ValueError:
        return None


def to_text(x: str | None):
    if x is None:
        return None
    x = x.strip()
    if x == "":
        return None
    return x


@lru_cache(maxsize=250_000)
def parse_ts_to_iso(ts: str | None) -> str | None:
    if ts is None:
        return None
    ts = ts.strip()
    if ts == "":
        return None

    try:
        dt = datetime.strptime(ts, "%d/%m/%Y %H:%M:%S")
        dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except ValueError:
        pass

    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except ValueError:
        return None


def normalize_header(name: str) -> str:
    return name.strip().lstrip("\ufeff").lstrip("#").strip()


def safe_get(row: list[str], idx: int) -> str | None:
    if idx < len(row):
        return row[idx]
    return None


def transform_subchunk(rows: list[tuple[str | None, ...]]) -> tuple[list[list[object]], int, int, int]:
    out_rows: list[list[object]] = []
    skip_missing_ts = 0
    skip_missing_core = 0
    skip_bad_coords = 0

    for mmsi_raw, ts_raw, mobile_raw, lat_raw, lon_raw, sog_raw, cog_raw, ship_type_raw in rows:
        ts = parse_ts_to_iso(ts_raw)
        if not ts:
            skip_missing_ts += 1
            continue

        mmsi = to_int(mmsi_raw)
        lat = to_float(lat_raw)
        lon = to_float(lon_raw)
        if mmsi is None or lat is None or lon is None:
            skip_missing_core += 1
            continue

        if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
            skip_bad_coords += 1
            continue

        sog = to_float(sog_raw)
        cog = to_float(cog_raw)
        mobile_type = to_text(mobile_raw)
        ship_type = to_text(ship_type_raw)
        geom = f"SRID=4326;POINT({lon} {lat})"

        out_rows.append([mmsi, ts, lat, lon, sog, cog, mobile_type, ship_type, geom])

    return out_rows, skip_missing_ts, skip_missing_core, skip_bad_coords


def split_subchunks(rows: list[tuple[str | None, ...]], parts: int) -> list[list[tuple[str | None, ...]]]:
    if parts <= 1 or len(rows) <= 1:
        return [rows]
    step = max(1, (len(rows) + parts - 1) // parts)
    return [rows[i:i + step] for i in range(0, len(rows), step)]


def process_chunk(
    raw_rows: list[tuple[str | None, ...]],
    workers: int,
    executor: ProcessPoolExecutor | None,
) -> tuple[list[list[object]], int, int, int]:
    if not raw_rows:
        return [], 0, 0, 0

    if executor is None or workers <= 1 or len(raw_rows) < workers * 2000:
        return transform_subchunk(raw_rows)

    out_rows: list[list[object]] = []
    skip_missing_ts = 0
    skip_missing_core = 0
    skip_bad_coords = 0

    subchunks = split_subchunks(raw_rows, workers)
    for part_rows, part_ts, part_core, part_coords in executor.map(transform_subchunk, subchunks):
        out_rows.extend(part_rows)
        skip_missing_ts += part_ts
        skip_missing_core += part_core
        skip_bad_coords += part_coords

    return out_rows, skip_missing_ts, skip_missing_core, skip_bad_coords


def write_rows_to_copy(copy_obj, rows: list[list[object]], buffer_rows: int) -> None:
    if not rows:
        return

    buf = io.StringIO()
    writer = csv.writer(buf)
    pending = 0

    for row in rows:
        writer.writerow(row)
        pending += 1
        if pending >= buffer_rows:
            copy_obj.write(buf.getvalue())
            buf.seek(0)
            buf.truncate(0)
            pending = 0

    if pending:
        copy_obj.write(buf.getvalue())


def fetch_progress(cur, source_path: str):
    cur.execute(SELECT_PROGRESS_SQL, (source_path,))
    row = cur.fetchone()
    if row is None:
        return None
    return {
        "source_size": row[0],
        "source_mtime_ns": row[1],
        "delimiter": row[2],
        "rows_read": row[3],
        "inserted": row[4],
        "skipped": row[5],
        "skip_missing_ts": row[6],
        "skip_missing_core": row[7],
        "skip_bad_coords": row[8],
    }


def main():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    default_aisdata_dir = os.path.join(project_root, "AISDATA")

    parser = argparse.ArgumentParser()
    parser.add_argument("csv_path", help="CSV file path or filename inside AISDATA/")
    parser.add_argument(
        "--aisdata-dir",
        default=default_aisdata_dir,
        help="Fallback directory when csv_path is a filename (default: <repo>/AISDATA)",
    )
    parser.add_argument("--limit", type=int, default=0, help="0 = no limit")
    parser.add_argument("--chunk-rows", type=int, default=100000, help="Rows per transaction chunk")
    parser.add_argument("--copy-buffer-rows", type=int, default=25000, help="Rows per in-memory COPY write")
    parser.add_argument(
        "--workers",
        type=int,
        default=max(1, (os.cpu_count() or 2) - 1),
        help="Parallel parser worker processes (1 disables multi-core parsing)",
    )
    parser.add_argument("--no-resume", action="store_true", help="Ignore stored checkpoint and start from row 0")
    parser.add_argument("--reset-progress", action="store_true", help="Delete stored progress for this file before run")
    args = parser.parse_args()

    if args.chunk_rows <= 0:
        raise RuntimeError("--chunk-rows must be > 0")
    if args.copy_buffer_rows <= 0:
        raise RuntimeError("--copy-buffer-rows must be > 0")
    if args.workers <= 0:
        raise RuntimeError("--workers must be > 0")

    source_path = resolve_csv_path(args.csv_path, args.aisdata_dir)
    if not os.path.exists(source_path):
        raise RuntimeError(
            f"CSV file not found: {args.csv_path}. "
            f"Tried direct path and {os.path.abspath(os.path.join(args.aisdata_dir, args.csv_path))}."
        )
    source_stat = os.stat(source_path)
    source_size = source_stat.st_size
    source_mtime_ns = source_stat.st_mtime_ns

    load_dotenv()
    db_url = os.environ["DATABASE_URL"]

    rows_read = 0
    inserted = 0
    skipped = 0
    skip_missing_ts = 0
    skip_missing_core = 0
    skip_bad_coords = 0
    stored_delimiter = None

    conn = psycopg.connect(db_url, autocommit=False)
    try:
        with conn.cursor() as cur:
            cur.execute("SET TIME ZONE 'UTC';")
            cur.execute(CREATE_PROGRESS_TABLE_SQL)
        conn.commit()

        if args.reset_progress:
            with conn.cursor() as cur:
                cur.execute(DELETE_PROGRESS_SQL, (source_path,))
            conn.commit()
            print("Reset import progress for:", source_path)

        if not args.no_resume:
            with conn.cursor() as cur:
                progress = fetch_progress(cur, source_path)
            if progress:
                if (
                    progress["source_size"] != source_size
                    or progress["source_mtime_ns"] != source_mtime_ns
                ):
                    raise RuntimeError(
                        "Stored progress exists for this path but file metadata changed. "
                        "Use --reset-progress to discard old checkpoint or --no-resume to start from row 0."
                    )
                rows_read = progress["rows_read"]
                inserted = progress["inserted"]
                skipped = progress["skipped"]
                skip_missing_ts = progress["skip_missing_ts"]
                skip_missing_core = progress["skip_missing_core"]
                skip_bad_coords = progress["skip_bad_coords"]
                stored_delimiter = progress["delimiter"]
        else:
            print("Resume disabled via --no-resume; starting from row 0.")

        copy_sql = """
            COPY ais_points_cleaned
                (mmsi, ts, lat, lon, sog, cog, mobile_type, ship_type, geom)
            FROM STDIN WITH (FORMAT CSV)
        """

        processed_this_run = 0

        with open(source_path, newline="", encoding="utf-8") as f:
            sample = f.read(65536)
            f.seek(0)
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
            except csv.Error:
                dialect = csv.get_dialect("excel")

            print("Using delimiter:", repr(dialect.delimiter))
            if stored_delimiter and stored_delimiter != dialect.delimiter:
                raise RuntimeError(
                    "Stored delimiter does not match current CSV delimiter. "
                    "Use --reset-progress or --no-resume."
                )

            reader = csv.reader(f, dialect=dialect)
            raw_headers = next(reader, None)
            if raw_headers is None:
                raise RuntimeError("CSV has no header / no columns detected")
            print("Raw headers:", raw_headers)

            header_index = {
                normalize_header(name): idx for idx, name in enumerate(raw_headers)
            }
            print("Normalized headers:", sorted(header_index.keys()))

            missing_headers = [h for h in REQUIRED_HEADERS if h not in header_index]
            if missing_headers:
                raise RuntimeError(
                    "CSV missing required headers: " + ", ".join(missing_headers)
                )

            idx_mmsi = header_index["MMSI"]
            idx_ts = header_index["Timestamp"]
            idx_mobile = header_index["Type of mobile"]
            idx_lat = header_index["Latitude"]
            idx_lon = header_index["Longitude"]
            idx_sog = header_index["SOG"]
            idx_cog = header_index["COG"]
            idx_ship = header_index["Ship type"]

            if rows_read:
                print(f"Resuming at data row {rows_read:,}")
                skipped_for_resume = 0
                while skipped_for_resume < rows_read:
                    try:
                        next(reader)
                    except StopIteration:
                        raise RuntimeError(
                            "Stored progress points past end of file. Use --reset-progress."
                        ) from None
                    skipped_for_resume += 1
                    if skipped_for_resume % 500000 == 0:
                        print(f"resume-skip {skipped_for_resume:,}/{rows_read:,}")
            else:
                print("Starting from first data row.")

            executor = (
                ProcessPoolExecutor(max_workers=args.workers)
                if args.workers > 1
                else None
            )
            try:
                while True:
                    raw_chunk: list[tuple[str | None, ...]] = []
                    while len(raw_chunk) < args.chunk_rows:
                        if args.limit and processed_this_run >= args.limit:
                            break
                        try:
                            row = next(reader)
                        except StopIteration:
                            break

                        raw_chunk.append(
                            (
                                safe_get(row, idx_mmsi),
                                safe_get(row, idx_ts),
                                safe_get(row, idx_mobile),
                                safe_get(row, idx_lat),
                                safe_get(row, idx_lon),
                                safe_get(row, idx_sog),
                                safe_get(row, idx_cog),
                                safe_get(row, idx_ship),
                            )
                        )
                        processed_this_run += 1

                    if not raw_chunk:
                        break

                    valid_rows, chunk_missing_ts, chunk_missing_core, chunk_bad_coords = process_chunk(
                        raw_rows=raw_chunk,
                        workers=args.workers,
                        executor=executor,
                    )

                    chunk_inserted = len(valid_rows)
                    chunk_skipped = chunk_missing_ts + chunk_missing_core + chunk_bad_coords

                    with conn.cursor() as cur:
                        if valid_rows:
                            with cur.copy(copy_sql) as copy:
                                write_rows_to_copy(copy, valid_rows, args.copy_buffer_rows)

                        rows_read += len(raw_chunk)
                        inserted += chunk_inserted
                        skipped += chunk_skipped
                        skip_missing_ts += chunk_missing_ts
                        skip_missing_core += chunk_missing_core
                        skip_bad_coords += chunk_bad_coords

                        cur.execute(
                            UPSERT_PROGRESS_SQL,
                            (
                                source_path,
                                source_size,
                                source_mtime_ns,
                                dialect.delimiter,
                                rows_read,
                                inserted,
                                skipped,
                                skip_missing_ts,
                                skip_missing_core,
                                skip_bad_coords,
                            ),
                        )

                    conn.commit()
                    print(
                        f"progress rows_read={rows_read:,} inserted={inserted:,} "
                        f"skipped={skipped:,} chunk_rows={len(raw_chunk):,}"
                    )

                    if args.limit and processed_this_run >= args.limit:
                        break
            finally:
                if executor is not None:
                    executor.shutdown(wait=True)

        with conn.cursor() as cur:
            cur.execute("ANALYZE ais_points_cleaned;")
        conn.commit()

        print(f"DONE inserted={inserted:,} skipped={skipped:,} rows_read={rows_read:,}")
        print(
            "Skip breakdown:",
            f"missing_ts={skip_missing_ts:,}",
            f"missing_core={skip_missing_core:,}",
            f"bad_coords={skip_bad_coords:,}",
        )

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
