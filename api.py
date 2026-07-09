"""
api.py — SentinelStream dashboard API
─────────────────────────────────────────────────────────────────────────────
A thin, read-only FastAPI layer between TimescaleDB and dashboard/index.html.

Why this exists: a static index.html (e.g. hosted on GitHub Pages) cannot
open a direct socket to Postgres — browsers can't speak the Postgres wire
protocol, and even if they could, punching your DB straight through to the
internet would be a security problem. This tiny API is the standard fix:
it exposes exactly two safe, read-only JSON endpoints that the dashboard
polls. Run it locally alongside docker-compose and producer/processor to
drive the dashboard with real, live data.

CORS is deliberately wide open (allow_origins=["*"]) because this is a
local demo project reading non-sensitive synthetic data — tighten this to
your actual portfolio domain if you ever deploy the API somewhere public.
"""

import logging

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | api | %(message)s")
log = logging.getLogger("api")

PG_DSN = dict(
    host="localhost", port=5432,
    dbname="fraud_detection", user="sentinel", password="sentinel_pw",
)

app = FastAPI(title="SentinelStream Dashboard API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def get_connection():
    try:
        return psycopg2.connect(**PG_DSN)
    except Exception as exc:
        log.exception("Database connection failed")
        raise HTTPException(status_code=503, detail=f"Database unavailable: {exc}")


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/api/stats")
def get_stats():
    """Aggregate throughput numbers for the dashboard's stat cards."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM pipeline_stats WHERE id = 1")
            row = cur.fetchone()
            return dict(row) if row else {}
    finally:
        conn.close()


@app.get("/api/alerts")
def get_recent_alerts(limit: int = 25):
    """Most recent fraud alerts, newest first, for the live alert feed."""
    limit = max(1, min(limit, 200))  # clamp to a sane range
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT transaction_id, user_id, amount, merchant, location,
                       card_type, fraud_reason, risk_score, tx_timestamp, detected_at
                FROM fraud_alerts
                ORDER BY detected_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
            # Decimal/datetime -> JSON-safe types
            for r in rows:
                r["amount"] = float(r["amount"])
                r["tx_timestamp"] = r["tx_timestamp"].isoformat()
                r["detected_at"] = r["detected_at"].isoformat()
            return rows
    finally:
        conn.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
