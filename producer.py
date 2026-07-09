"""
producer.py — SentinelStream mock transaction generator
─────────────────────────────────────────────────────────────────────────────
Generates a continuous, high-throughput stream of synthetic credit-card
transactions onto a Kafka topic, and deliberately injects three classes of
fraud patterns so the downstream processor.py has real signal to catch:

    1. VELOCITY BURST      — a user suddenly fires >3 transactions in a few
                              seconds (card-testing / bot attack pattern).
    2. IMPOSSIBLE TRAVEL   — the same user transacts from two geographically
                              distant locations within a time window that no
                              real person could physically travel between.
    3. AMOUNT SPIKE        — a user who normally spends small amounts suddenly
                              makes a purchase an order of magnitude larger
                              than their historical baseline.

Design decisions:
    - confluent-kafka (a thin wrapper over librdkafka, the C client Confluent
      itself uses) is chosen over kafka-python because it's faster, actively
      maintained, and is what most production Python producers use at scale.
    - A ThreadPoolExecutor drives N concurrent "virtual users" so we get
      realistic interleaving of many users' transactions rather than one
      user at a time — this is what makes the velocity/travel fraud patterns
      meaningful (they depend on *concurrent* activity across users).
    - Delivery reports are handled asynchronously (produce() is non-blocking);
      we only block briefly on flush() at shutdown to guarantee no message
      loss when the script is interrupted.
"""

import json
import logging
import random
import signal
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from confluent_kafka import Producer
from faker import Faker

# ─────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "transactions"

NUM_VIRTUAL_USERS = 300         # distinct user_ids kept "alive" and reused
NUM_PRODUCER_THREADS = 8        # concurrent worker threads generating events
TARGET_EVENTS_PER_SECOND = 200  # approximate aggregate throughput target

FRAUD_INJECTION_PROBABILITY = 0.03   # 3% chance any given tick injects a fraud burst

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | producer | %(message)s",
)
log = logging.getLogger("producer")

fake = Faker()

# A small set of real-ish cities with (lat, lon) so processor.py can compute
# genuine haversine distance/speed for "impossible travel" detection.
CITIES = {
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

CARD_TYPES = ["VISA", "MASTERCARD", "AMEX", "RUPAY", "DISCOVER"]


@dataclass
class VirtualUser:
    """Tracks per-user baseline behavior so we can generate believable
    'normal' transactions AND believable fraud deviations from that baseline."""
    user_id: str
    home_city: str
    avg_amount: float
    card_type: str
    lock: threading.Lock = field(default_factory=threading.Lock)


def build_user_pool(n: int) -> list[VirtualUser]:
    users = []
    for i in range(n):
        users.append(
            VirtualUser(
                user_id=f"user_{i:04d}",
                home_city=random.choice(list(CITIES.keys())),
                avg_amount=round(random.uniform(15, 250), 2),  # typical spend
                card_type=random.choice(CARD_TYPES),
            )
        )
    return users


def make_transaction(user: VirtualUser, *, location: str = None, amount: float = None) -> dict:
    """Builds a single transaction event matching the agreed schema."""
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user.user_id,
        "amount": round(amount if amount is not None else random.gauss(user.avg_amount, user.avg_amount * 0.2), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "merchant": fake.company(),
        "location": location or user.home_city,
        "card_type": user.card_type,
    }


class TransactionProducer:
    def __init__(self):
        self.producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "sentinel-producer",
            "acks": "all",              # don't drop events — this is financial data
            "retries": 5,
            "linger.ms": 5,              # small batching window for throughput
            "compression.type": "lz4",
        })
        self.users = build_user_pool(NUM_VIRTUAL_USERS)
        self._stop_event = threading.Event()
        self._sent_count = 0
        self._count_lock = threading.Lock()

    # ---- Kafka delivery callback -----------------------------------------
    def _delivery_report(self, err, msg):
        if err is not None:
            log.error(f"Delivery failed for record {msg.key()}: {err}")
        else:
            with self._count_lock:
                self._sent_count += 1

    # ---- Core send helper --------------------------------------------------
    def _send(self, event: dict):
        try:
            self.producer.produce(
                topic=KAFKA_TOPIC,
                key=event["user_id"],           # keying by user_id preserves
                value=json.dumps(event),        # per-user ordering in Kafka,
                callback=self._delivery_report, # which the processor relies on
            )
            # poll(0) drains the delivery-report callback queue without blocking
            self.producer.poll(0)
        except BufferError:
            log.warning("Producer queue full — backing off briefly")
            self.producer.poll(0.5)
        except Exception as exc:
            log.exception(f"Unexpected error producing event: {exc}")

    # ---- Fraud pattern injectors --------------------------------------------
    def _inject_velocity_burst(self, user: VirtualUser):
        """Fires 4-7 transactions for one user within ~2 seconds."""
        burst_size = random.randint(4, 7)
        log.warning(f"[INJECT] Velocity burst for {user.user_id} ({burst_size} txns)")
        for _ in range(burst_size):
            self._send(make_transaction(user))
            time.sleep(random.uniform(0.1, 0.4))

    def _inject_impossible_travel(self, user: VirtualUser):
        """Fires two transactions for one user from distant cities seconds apart."""
        other_city = random.choice([c for c in CITIES if c != user.home_city])
        log.warning(f"[INJECT] Impossible travel for {user.user_id}: "
                    f"{user.home_city} -> {other_city}")
        self._send(make_transaction(user, location=user.home_city))
        time.sleep(random.uniform(1, 4))
        self._send(make_transaction(user, location=other_city))

    def _inject_amount_spike(self, user: VirtualUser):
        """Fires one transaction far above the user's normal spending baseline."""
        spike_amount = round(user.avg_amount * random.uniform(40, 80), 2)  # e.g. $150 -> $9,000+
        spike_amount = max(spike_amount, 10_500)  # guarantee it clears the $10k rule too
        log.warning(f"[INJECT] Amount spike for {user.user_id}: ${spike_amount:,.2f}")
        self._send(make_transaction(user, amount=spike_amount))

    def _maybe_inject_fraud(self):
        if random.random() < FRAUD_INJECTION_PROBABILITY:
            user = random.choice(self.users)
            pattern = random.choice([
                self._inject_velocity_burst,
                self._inject_impossible_travel,
                self._inject_amount_spike,
            ])
            pattern(user)

    # ---- Worker loop --------------------------------------------------------
    def _worker_loop(self, thread_id: int):
        per_thread_delay = NUM_PRODUCER_THREADS / max(TARGET_EVENTS_PER_SECOND, 1)
        log.info(f"Worker-{thread_id} started (target delay ~{per_thread_delay:.4f}s/event)")
        while not self._stop_event.is_set():
            try:
                self._maybe_inject_fraud()
                user = random.choice(self.users)
                self._send(make_transaction(user))
                time.sleep(per_thread_delay)
            except Exception as exc:
                log.exception(f"Worker-{thread_id} error: {exc}")
                time.sleep(1)  # avoid a tight error loop taking down the box

    # ---- Lifecycle ------------------------------------------------------------
    def start(self):
        log.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}, topic '{KAFKA_TOPIC}'")
        threads = [
            threading.Thread(target=self._worker_loop, args=(i,), daemon=True)
            for i in range(NUM_PRODUCER_THREADS)
        ]
        for t in threads:
            t.start()

        # Periodic throughput reporting so you can see it's alive & how fast.
        try:
            last_report_time = time.time()
            last_report_count = 0
            while not self._stop_event.is_set():
                time.sleep(5)
                now = time.time()
                with self._count_lock:
                    delta = self._sent_count - last_report_count
                    last_report_count = self._sent_count
                tps = delta / (now - last_report_time)
                last_report_time = now
                log.info(f"Throughput: {tps:.1f} events/sec | total sent: {self._sent_count}")
        except KeyboardInterrupt:
            self.stop()
        finally:
            for t in threads:
                t.join(timeout=2)

    def stop(self):
        log.info("Shutting down producer, flushing outstanding messages...")
        self._stop_event.set()
        self.producer.flush(timeout=10)
        log.info(f"Shutdown complete. Total events sent: {self._sent_count}")


def _handle_sigterm(signum, frame):
    raise KeyboardInterrupt()


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _handle_sigterm)
    producer = TransactionProducer()
    try:
        producer.start()
    except KeyboardInterrupt:
        producer.stop()
        sys.exit(0)
