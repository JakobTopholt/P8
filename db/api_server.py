import argparse
import json
import os
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv
import psycopg


SQL_BASE = """
SELECT
    id,
    mmsi,
    lat,
    lon,
    ts,
    mobile_type,
    ship_type,
    sog,
    cog
FROM ais_points_cleaned
WHERE lat IS NOT NULL
  AND lon IS NOT NULL
"""


class APIServer(ThreadingHTTPServer):
    def __init__(self, server_address, RequestHandlerClass, db_url: str):
        super().__init__(server_address, RequestHandlerClass)
        self.db_url = db_url


def build_where_clause(params: dict[str, list[str]]) -> tuple[str, list[object]]:
    where_parts: list[str] = []
    values: list[object] = []

    mmsi_param = params.get("mmsi", [""])[0].strip()
    if mmsi_param:
        try:
            mmsi = int(mmsi_param)
            where_parts.append("mmsi = %s")
            values.append(mmsi)
        except ValueError:
            raise ValueError("Query parameter 'mmsi' must be an integer") from None

    t0 = params.get("t0", [""])[0].strip()
    if t0:
        try:
            datetime.fromisoformat(t0.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("Query parameter 't0' must be ISO-8601") from None
        where_parts.append("ts >= %s")
        values.append(t0)

    t1 = params.get("t1", [""])[0].strip()
    if t1:
        try:
            datetime.fromisoformat(t1.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("Query parameter 't1' must be ISO-8601") from None
        where_parts.append("ts < %s")
        values.append(t1)

    if where_parts:
        return " AND " + " AND ".join(where_parts), values
    return "", values


def parse_limit(params: dict[str, list[str]]) -> int:
    raw = params.get("limit", ["2000"])[0].strip() or "2000"
    try:
        limit = int(raw)
    except ValueError:
        raise ValueError("Query parameter 'limit' must be an integer") from None

    if limit <= 0:
        raise ValueError("Query parameter 'limit' must be > 0")
    return min(limit, 20000)


def build_datapoints_sql(where_clause: str) -> str:
    return (
        "WITH filtered AS (\n"
        + SQL_BASE
        + where_clause
        + "\n),\n"
        + "limited_mmsi AS (\n"
        + "    SELECT DISTINCT mmsi\n"
        + "    FROM filtered\n"
        + "    ORDER BY mmsi ASC\n"
        + "    LIMIT %s\n"
        + ")\n"
        + "SELECT\n"
        + "    id,\n"
        + "    mmsi,\n"
        + "    lat,\n"
        + "    lon,\n"
        + "    ts,\n"
        + "    mobile_type,\n"
        + "    ship_type,\n"
        + "    sog,\n"
        + "    cog\n"
        + "FROM filtered\n"
        + "WHERE mmsi IN (SELECT mmsi FROM limited_mmsi)\n"
        + "ORDER BY mmsi ASC, ts ASC"
    )


def row_to_datapoint(row: tuple[object, ...]) -> dict[str, object | None]:
    point_id, mmsi, lat, lon, ts, mobile_type, ship_type, sog, cog = row
    lat_f = float(lat)
    lon_f = float(lon)

    title = ship_type or mobile_type or f"MMSI {mmsi}"
    description_parts = [
        f"mobile_type={mobile_type}" if mobile_type else None,
        f"ship_type={ship_type}" if ship_type else None,
        f"sog={float(sog):.2f} kn" if sog is not None else None,
        f"cog={float(cog):.2f} deg" if cog is not None else None,
    ]
    description = ", ".join(part for part in description_parts if part)

    return {
        "id": int(point_id),
        "mmsi": int(mmsi),
        "position": [lat_f, lon_f],
        "name": str(title),
        "location": f"{lat_f:.5f}, {lon_f:.5f}",
        "timestamp": ts.isoformat() if ts is not None else None,
        "description": description if description else None,
    }


class RequestHandler(BaseHTTPRequestHandler):
    server: APIServer

    def _send_json(self, status: int, payload: dict[str, object] | list[object]) -> None:
        data = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(data)

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)

        if parsed.path == "/api/health":
            self._send_json(HTTPStatus.OK, {"status": "ok"})
            return

        if parsed.path != "/api/datapoints":
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "Not found"})
            return

        params = parse_qs(parsed.query)
        try:
            limit = parse_limit(params)
            where_clause, values = build_where_clause(params)
        except ValueError as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return

        sql = build_datapoints_sql(where_clause)
        values.append(limit)

        try:
            with psycopg.connect(self.server.db_url) as conn:
                with conn.cursor() as cur:
                    cur.execute("SET TIME ZONE 'UTC';")
                    cur.execute(sql, values)
                    rows = cur.fetchall()
        except Exception as exc:  # pragma: no cover
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"error": "Database query failed", "detail": str(exc)},
            )
            return

        payload = [row_to_datapoint(row) for row in rows]
        self._send_json(HTTPStatus.OK, payload)


def main() -> None:
    parser = argparse.ArgumentParser(description="Serve AIS DataPoints over HTTP")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind (default: 8000)")
    args = parser.parse_args()

    load_dotenv()
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set. Add it to your environment or .env file.")

    server = APIServer((args.host, args.port), RequestHandler, db_url=db_url)
    print(f"Serving AIS API on http://{args.host}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
