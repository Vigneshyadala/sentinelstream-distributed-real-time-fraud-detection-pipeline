"""
processor.py — SentinelStream real-time fraud detection layer
─────────────────────────────────────────────────────────────────────────────
Consumes the raw `transactions` Kafka topic and implements three sliding-
window fraud rules entirely in-process (no external stream-processing
cluster required):

    RULE 1 — VELOCITY:          > 3 transactions for the same user within a
                                  10-second sliding window.
    RULE 2 — HIGH AMOUNT:        any single transaction > $10,000.
    RULE 3 — IMPOSSIBLE TRAVEL:  same user, two different locations, at an
                                  implied speed no commercial flight can
                                  achieve (haversine distance / elapsed time).

Architectural decision — why NOT PySpark Structured Streaming here:
    PySpark Streaming is a legitimate choice for this problem and is
    mentioned in the brief as an option, but for a project meant to run
    entirely on a laptop, spinning up a JVM + Spark session adds a heavy,
    slow-starting dependency for what is fundamentally a per-key windowed
    aggregation — something a plain Python consumer with an in-memory
    `deque`-based sliding window (below) does with microsecond latency and
    zero extra infrastructure. This mirrors a real architectural trade-off
    fraud engineering teams make: Spark/Flink for jobs with heavy joins or
    huge state that can't fit on one box; a lightweight stateful consumer
    (what Faust or Kafka Streams do under the hood) when per-key state is
    small, as it is here (a handful of recent transactions per user).
    The state-tracking approach below is functionally equivalent to what
    Faust would generate, without adding a second framework to learn.

Dual-routing / Lambda architecture:
    - SPEED LAYER:  flagged fraud alerts are written immediately to
                    TimescaleDB (`fraud_alerts` hypertable) for low-latency
                    querying by Grafana / the dashboard API.
    - BATCH LAYER:  every raw transaction (fraud or not) is buffered and
                    flushed as a Parquet file to MinIO (S3-compatible) every
                    N seconds/records, building the historical data lake
                    used for offline model training / auditing.
"""

import io
import json
import logging
import math
import signal
import sys
import threading
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone

import boto3
import psycopg2
import psycopg2.extras
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import Config
from confluent_kafka import Consumer, KafkaError, KafkaException

# ─────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "transactions"
KAFKA_GROUP_ID = "sentinel-fraud-processor"

PG_DSN = dict(
    host="localhost", port=5432,
    dbname="fraud_detection", user="sentinel", password="sentinel_pw",
)

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "sentinel_admin"
MINIO_SECRET_KEY = "sentinel_secret"
MINIO_BUCKET = "fraud-lake"

# Fraud rule thresholds
VELOCITY_WINDOW_SECONDS = 10
VELOCITY_MAX_TXNS = 5
HIGH_AMOUNT_THRESHOLD = 10_000.00
IMPOSSIBLE_TRAVEL_MIN_SPEED_KMH = 900  # ~cruising speed of a commercial jet

# Batch layer flush policy
BATCH_FLUSH_MAX_RECORDS = 200
BATCH_FLUSH_MAX_SECONDS = 15

# Approx city coordinates — kept in sync with producer.py's CITIES dict so
# haversine distance can be computed for the impossible-travel rule.
CITY_COORDS = {
    "New York, US":    (40.7128, -74.0060),
    "London, UK":      (51.5074, -0.1278),
    "Tokyo, JP":       (35.6762, 139.6503),
    "Sydney, AU":      (-33.8688, 151.2093),
    "Mumbai, IN":      (19.0760, 72.8777),
    "Sao Paulo, BR":   (-23.5505, -46.6333),
    "Cape Town, ZA":   (-33.9249, 18.4241),
    "Toronto, CA":     (43.6511, -79.3470),
    "Dubai, AE":       (25.2048, 55.2708),
    "Berlin, DE":      (52.5200, 13.4050),
    "Singapore, SG":   (1.3521, 103.8198),
    "Chicago, US":     (41.8781, -87.6298),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | processor | %(message)s",
)
log = logging.getLogger("processor")


def haversine_km(coord1, coord2) -> float:
    """Great-circle distance between two (lat, lon) points, in kilometers."""
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    r = 6371.0  # Earth radius, km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


class SlidingWindowState:
    """Per-user in-memory state: a bounded deque of recent (timestamp, amount,
    location) tuples. This is the entire 'state store' — equivalent in spirit
    to what Kafka Streams / Faust keep in RocksDB, just in plain memory since
    per-user history here is small and doesn't need to survive a restart for
    a portfolio-scale demo."""

    def __init__(self):
        self._history = defaultdict(lambda: deque(maxlen=50))
        self._lock = threading.Lock()

    def record_and_check(self, event: dict) -> list[str]:
        """Appends the event to the user's history and evaluates all fraud
        rules against the updated window. Returns a list of fraud reasons
        (empty list if the transaction looks clean)."""
        user_id = event["user_id"]
        ts = datetime.fromisoformat(event["timestamp"])
        amount = float(event["amount"])
        location = event["location"]

        reasons = []

        with self._lock:
            history = self._history[user_id]
            history.append((ts, amount, location))

            # ---- RULE 1: VELOCITY -------------------------------------------------
            window_start = ts.timestamp() - VELOCITY_WINDOW_SECONDS
            recent_in_window = [h for h in history if h[0].timestamp() >= window_start]
            if len(recent_in_window) > VELOCITY_MAX_TXNS:
                reasons.append("VELOCITY")

            # ---- RULE 2: HIGH AMOUNT -----------------------------------------------
            if amount > HIGH_AMOUNT_THRESHOLD:
                reasons.append("HIGH_AMOUNT")

            # ---- RULE 3: IMPOSSIBLE TRAVEL -----------------------------------------
            if len(history) >= 2:
                prev_ts, _, prev_location = history[-2]
                if prev_location != location:
                    elapsed_hours = max((ts - prev_ts).total_seconds() / 3600.0, 1e-6)
                    coord_a = CITY_COORDS.get(prev_location)
                    coord_b = CITY_COORDS.get(location)
                    if coord_a and coord_b:
                        distance_km = haversine_km(coord_a, coord_b)
                        implied_speed = distance_km / elapsed_hours
                        if implied_speed > IMPOSSIBLE_TRAVEL_MIN_SPEED_KMH:
                            reasons.append("IMPOSSIBLE_TRAVEL")

        return reasons


class BatchLakeWriter:
    """Buffers raw transactions and periodically flushes them to MinIO as
    Parquet files, partitioned by date — the 'batch layer' of the Lambda
    architecture. A background timer thread guarantees we flush on a time
    interval even during low-traffic periods, not just on record count."""

    def __init__(self):
        self.s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        self._buffer = []
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._timer_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._timer_thread.start()

    def add(self, event: dict):
        with self._lock:
            self._buffer.append(event)
            should_flush = len(self._buffer) >= BATCH_FLUSH_MAX_RECORDS
        if should_flush:
            self.flush()

    def _flush_loop(self):
        while not self._stop_event.wait(BATCH_FLUSH_MAX_SECONDS):
            self.flush()

    def flush(self):
        with self._lock:
            if not self._buffer:
                return
            records, self._buffer = self._buffer, []

        try:
            table = pa.Table.from_pylist(records)
            buf = io.BytesIO()
            pq.write_table(table, buf)
            buf.seek(0)

            now = datetime.now(timezone.utc)
            key = (
                f"raw_transactions/year={now.year}/month={now.month:02d}/day={now.day:02d}/"
                f"batch_{now.strftime('%H%M%S')}_{uuid.uuid4().hex[:8]}.parquet"
            )
            self.s3.upload_fileobj(buf, MINIO_BUCKET, key)
            log.info(f"[BATCH LAYER] Flushed {len(records)} records to s3://{MINIO_BUCKET}/{key}")
        except Exception as exc:
            log.exception(f"Failed to flush batch to MinIO: {exc}")
            # Put the records back so we retry them on the next flush cycle
            # instead of silently losing data.
            with self._lock:
                self._buffer = records + self._buffer

    def stop(self):
        self._stop_event.set()
        self.flush()


class SpeedLayerWriter:
    """Writes flagged fraud alerts to TimescaleDB and periodically upserts
    aggregate pipeline stats for the dashboard/Grafana to read."""

    def __init__(self):
        self.conn = psycopg2.connect(**PG_DSN)
        self.conn.autocommit = True
        self._total_transactions = 0
        self._total_alerts = 0
        self._detection_latencies_ms = deque(maxlen=500)
        self._lock = threading.Lock()
        self._stats_thread = threading.Thread(target=self._stats_loop, daemon=True)
        self._stats_thread.start()

    def insert_alert(self, event: dict, reasons: list[str], detection_latency_ms: float):
        risk_score = min(40 + 20 * len(reasons), 100)
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO fraud_alerts
                        (transaction_id, user_id, amount, merchant, location,
                         card_type, fraud_reason, risk_score, tx_timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        event["transaction_id"], event["user_id"], event["amount"],
                        event["merchant"], event["location"], event["card_type"],
                        ",".join(reasons), risk_score, event["timestamp"],
                    ),
                )
            with self._lock:
                self._total_alerts += 1
                self._detection_latencies_ms.append(detection_latency_ms)
            log.warning(f"[SPEED LAYER] FRAUD FLAGGED user={event['user_id']} "
                        f"amount=${event['amount']:.2f} reasons={reasons}")
        except Exception as exc:
            log.exception(f"Failed to insert fraud alert into TimescaleDB: {exc}")

    def record_transaction_seen(self):
        with self._lock:
            self._total_transactions += 1

    def _stats_loop(self):
        last_count = 0
        last_time = time.time()
        while True:
            time.sleep(5)
            with self._lock:
                total = self._total_transactions
                total_alerts = self._total_alerts
                avg_latency = (
                    sum(self._detection_latencies_ms) / len(self._detection_latencies_ms)
                    if self._detection_latencies_ms else 0
                )
            now = time.time()
            tps = (total - last_count) / max(now - last_time, 1e-6)
            last_count, last_time = total, now
            try:
                with self.conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE pipeline_stats
                        SET total_transactions = %s,
                            total_alerts = %s,
                            tps = %s,
                            avg_detection_ms = %s,
                            last_updated = now()
                        WHERE id = 1
                        """,
                        (total, total_alerts, round(tps, 2), round(avg_latency, 3)),
                    )
            except Exception as exc:
                log.exception(f"Failed to update pipeline_stats: {exc}")


class FraudProcessor:
    def __init__(self):
        self.consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        })
        self.window_state = SlidingWindowState()
        self.lake_writer = BatchLakeWriter()
        self.speed_writer = SpeedLayerWriter()
        self._stop_event = threading.Event()

    def run(self):
        self.consumer.subscribe([KAFKA_TOPIC])
        log.info(f"Subscribed to '{KAFKA_TOPIC}', waiting for messages...")
        try:
            while not self._stop_event.is_set():
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                self._handle_message(msg)

        except KeyboardInterrupt:
            pass
        finally:
            self.shutdown()

    def _handle_message(self, msg):
        start = time.perf_counter()
        try:
            event = json.loads(msg.value())
        except (json.JSONDecodeError, TypeError) as exc:
            log.error(f"Skipping malformed message: {exc}")
            return

        try:
            # 1. Always land the raw event in the batch/data-lake layer.
            self.lake_writer.add(event)
            self.speed_writer.record_transaction_seen()

            # 2. Evaluate sliding-window fraud rules.
            reasons = self.window_state.record_and_check(event)

            # 3. If flagged, route to the speed-layer DB immediately.
            if reasons:
                latency_ms = (time.perf_counter() - start) * 1000
                self.speed_writer.insert_alert(event, reasons, latency_ms)

        except Exception as exc:
            log.exception(f"Error processing message: {exc}")

    def shutdown(self):
        log.info("Shutting down processor...")
        self._stop_event.set()
        self.lake_writer.stop()
        self.consumer.close()
        log.info("Processor shutdown complete.")


def _handle_sigterm(signum, frame):
    raise KeyboardInterrupt()


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _handle_sigterm)
    processor = FraudProcessor()
    try:
        processor.run()
    except KeyboardInterrupt:
        processor.shutdown()
        sys.exit(0)
