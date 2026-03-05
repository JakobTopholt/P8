import os
from dotenv import load_dotenv
import psycopg

load_dotenv()
db_url = os.environ["DATABASE_URL"]

with psycopg.connect(db_url) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT postgis_version();")
        print("PostGIS:", cur.fetchone()[0])

        cur.execute("SELECT COUNT(*) FROM ais_points_cleaned;")
        print("Rows in ais_points_cleaned:", cur.fetchone()[0])