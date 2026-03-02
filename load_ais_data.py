#!/usr/bin/env python3
"""
Script to load AIS data from CSV into SQLite database and build MMSI lookup table.
Usage: python load_ais_data.py
"""

import os
import sqlite3
import sys
from contextlib import closing
from pathlib import Path

import pandas as pd

# Configuration
BASE_DIR = Path(__file__).resolve().parent
CSV_FILE = BASE_DIR / "AISDATA" / "aisdk-2026-02-24.cleaned.csv"
DATABASE_PATH = BASE_DIR / "src" / "backend" / "databases" / "ais_index.db"
SOURCE_TABLE = "ais_data"
MMSI_COLUMN = "MMSI"
LAT_COLUMN = "Latitude"
LON_COLUMN = "Longitude"
TIMESTAMP_COLUMN = "# Timestamp"  # Note: has # prefix
LOOKUP_TABLE = "mmsi_lookup"

# Batch size for inserts
BATCH_SIZE = 5000


def create_connection():
    """Create a database connection."""
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DATABASE_PATH)
    connection.row_factory = sqlite3.Row
    return connection


def create_ais_data_table(connection):
    """Create the main AIS data table if it doesn't exist."""
    print(f"Creating table {SOURCE_TABLE}...")
    with closing(connection.cursor()) as cursor:
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SOURCE_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                mmsi TEXT NOT NULL,
                lat REAL,
                lon REAL,
                type_of_mobile TEXT,
                navigational_status TEXT,
                sog REAL,
                cog REAL,
                heading REAL,
                name TEXT,
                ship_type TEXT,
                call_sign TEXT,
                destination TEXT
            )
            """
        )
        # Create index on MMSI for faster queries
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{SOURCE_TABLE}_mmsi ON {SOURCE_TABLE}(mmsi)")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{SOURCE_TABLE}_timestamp ON {SOURCE_TABLE}(timestamp)")
    connection.commit()
    print("✓ Table created")


def create_lookup_table(connection):
    """Create the MMSI lookup table."""
    print(f"Creating lookup table {LOOKUP_TABLE}...")
    with closing(connection.cursor()) as cursor:
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {LOOKUP_TABLE} (
                mmsi TEXT PRIMARY KEY,
                last_seen_source_rowid INTEGER
            )
            """
        )
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{LOOKUP_TABLE}_mmsi ON {LOOKUP_TABLE}(mmsi)")
    connection.commit()
    print("✓ Lookup table created")


def load_csv_to_database(connection):
    """Load CSV data into the database."""
    if not CSV_FILE.exists():
        print(f"✗ CSV file not found: {CSV_FILE}")
        return 0

    print(f"Loading CSV file: {CSV_FILE}")
    print(f"File size: {CSV_FILE.stat().st_size / (1024**3):.2f} GB")

    try:
        # Read first row to validate columns
        df_test = pd.read_csv(CSV_FILE, nrows=0)
        print(f"Found {len(df_test.columns)} columns: {list(df_test.columns)}")

        required_cols = [TIMESTAMP_COLUMN, MMSI_COLUMN, LAT_COLUMN, LON_COLUMN]
        available_cols = [c for c in required_cols if c in df_test.columns]

        if len(available_cols) != len(required_cols):
            missing = [c for c in required_cols if c not in df_test.columns]
            print(f"✗ Missing columns: {missing}")
            print(f"Available columns: {list(df_test.columns)}")
            return 0

        print(f"✓ Found required columns")

        # Read CSV in chunks
        chunks_processed = 0
        total_rows = 0

        for chunk_idx, df in enumerate(pd.read_csv(CSV_FILE, chunksize=BATCH_SIZE)):
            # Select required columns
            df_subset = df[[TIMESTAMP_COLUMN, MMSI_COLUMN, LAT_COLUMN, LON_COLUMN]].copy()
            df_subset.columns = ["timestamp", "mmsi", "lat", "lon"]

            # Remove rows with null MMSI or invalid coordinates
            df_subset = df_subset.dropna(subset=["mmsi"])
            df_subset["mmsi"] = df_subset["mmsi"].astype(str).str.strip()
            df_subset = df_subset[df_subset["mmsi"] != ""]

            if len(df_subset) == 0:
                continue

            # Insert batch into database
            with closing(connection.cursor()) as cursor:
                for _, row in df_subset.iterrows():
                    cursor.execute(
                        f"""
                        INSERT INTO {SOURCE_TABLE} (timestamp, mmsi, lat, lon)
                        VALUES (?, ?, ?, ?)
                        """,
                        (row["timestamp"], row["mmsi"], row["lat"], row["lon"]),
                    )
                connection.commit()

            chunks_processed += 1
            total_rows += len(df_subset)

            if chunks_processed % 10 == 0:
                print(f"  Processed {chunks_processed} chunks ({total_rows:,} rows)...")

        print(f"✓ Loaded {total_rows:,} rows from CSV")
        return total_rows

    except Exception as e:
        print(f"✗ Error loading CSV: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def build_mmsi_lookup(connection):
    """Build the MMSI lookup table from the main AIS data table."""
    print(f"Building {LOOKUP_TABLE} from {SOURCE_TABLE}...")

    with closing(connection.cursor()) as cursor:
        # Get count before
        cursor.execute(f"SELECT COUNT(*) FROM {SOURCE_TABLE}")
        source_count = cursor.fetchone()[0]
        print(f"  Source table has {source_count:,} total rows")

        # Clear lookup table
        cursor.execute(f"DELETE FROM {LOOKUP_TABLE}")

        # Insert unique MMSIs
        cursor.execute(
            f"""
            INSERT INTO {LOOKUP_TABLE} (mmsi, last_seen_source_rowid)
            SELECT DISTINCT mmsi, MAX(id)
            FROM {SOURCE_TABLE}
            WHERE mmsi IS NOT NULL AND TRIM(mmsi) != ''
            GROUP BY mmsi
            """
        )

        connection.commit()

        # Get count after
        cursor.execute(f"SELECT COUNT(*) FROM {LOOKUP_TABLE}")
        lookup_count = cursor.fetchone()[0]
        print(f"✓ Created {lookup_count:,} unique MMSI entries")

        return lookup_count


def main():
    """Main execution."""
    print("\n" + "=" * 60)
    print("AIS Data Loader")
    print("=" * 60)
    print(f"Database: {DATABASE_PATH}")
    print(f"CSV File: {CSV_FILE}")
    print()

    # Create connection
    connection = create_connection()

    try:
        # Create tables
        create_ais_data_table(connection)
        create_lookup_table(connection)

        # Check if data already exists
        with closing(connection.cursor()) as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {SOURCE_TABLE}")
            existing_rows = cursor.fetchone()[0]

        if existing_rows > 0:
            print(f"\n⚠ Table {SOURCE_TABLE} already has {existing_rows:,} rows")
            response = input("Clear and reload data? (y/n): ").strip().lower()
            if response == "y":
                print(f"Clearing {SOURCE_TABLE}...")
                with closing(connection.cursor()) as cursor:
                    cursor.execute(f"DELETE FROM {SOURCE_TABLE}")
                    connection.commit()
            else:
                print("Skipping CSV load")
                # Still rebuild lookup table
                unique_mmsis = build_mmsi_lookup(connection)
                print(f"\n✓ Successfully updated {LOOKUP_TABLE}")
                return

        # Load CSV
        rows_loaded = load_csv_to_database(connection)

        if rows_loaded > 0:
            # Build lookup
            unique_mmsis = build_mmsi_lookup(connection)
            print(f"\n" + "=" * 60)
            print("✓ SUCCESS")
            print("=" * 60)
            print(f"Loaded {rows_loaded:,} AIS datapoints")
            print(f"Created {unique_mmsis:,} unique MMSI entries")
            print(f"Database: {DATABASE_PATH}")
            print()

    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        connection.close()


if __name__ == "__main__":
    main()
