# SentinelStream — Distributed Real-Time Fraud Detection Pipeline

Kafka → Sliding-Window Detection Engine → TimescaleDB (speed layer) + MinIO (batch layer) → Grafana / Dashboard

---

## 1. Prerequisites

- Docker + Docker Compose v2
- Python 3.10+
- ~4 GB free RAM for the container stack

## 2. Project layout

```
fraud-pipeline/
├── docker-compose.yml
├── requirements.txt
├── producer.py
├── processor.py
├── api.py
├── sql/init.sql
└── dashboard/index.html
```

## 3. Bring the infrastructure up

```bash
cd fraud-pipeline

# Start Kafka, Zookeeper, TimescaleDB, MinIO, Grafana
docker compose up -d

# Watch until every service reports healthy (can take ~30-60s the first time)
docker compose ps
```

You should see `sentinel-kafka`, `sentinel-zookeeper`, `sentinel-timescaledb`,
`sentinel-minio`, and `sentinel-grafana` all show `healthy`.

## 4. Install Python dependencies

```bash
python3 -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate

pip install -r requirements.txt
```

> `confluent-kafka` ships prebuilt wheels for macOS/Linux/Windows — no manual
> librdkafka install needed on most systems. If the wheel build fails on your
> OS, install librdkafka via your package manager first (e.g. `brew install
> librdkafka` / `apt install librdkafka-dev`).

## 5. Run the pipeline (3 terminals)

**Terminal 1 — start the fraud detection processor first**, so it's
subscribed and ready before transactions start flowing:

```bash
source venv/bin/activate
python processor.py
```

**Terminal 2 — start the mock transaction producer:**

```bash
source venv/bin/activate
python producer.py
```

You'll immediately see throughput logs in Terminal 2, and `[SPEED LAYER]
FRAUD FLAGGED ...` / `[BATCH LAYER] Flushed ...` logs in Terminal 1 as fraud
patterns get caught and batches get written to MinIO.

**Terminal 3 — start the dashboard API:**

```bash
source venv/bin/activate
python api.py
# or: uvicorn api:app --reload --port 8000
```

## 6. Open the dashboard

Just open `dashboard/index.html` directly in your browser (double-click it,
or `open dashboard/index.html` / `start dashboard/index.html`). It will
detect `api.py` running on `localhost:8000` automatically and switch to
"● live · api connected". If the API isn't running, it shows a self-contained
demo animation instead — so the page never looks broken, on your machine or
hosted on GitHub Pages.

## 7. Verify the data lake

```bash
# MinIO web console
open http://localhost:9001
# login: sentinel_admin / sentinel_secret
# → browse the "fraud-lake" bucket → raw_transactions/year=.../...parquet
```

## 8. Verify TimescaleDB directly (optional)

```bash
docker exec -it sentinel-timescaledb psql -U sentinel -d fraud_detection \
  -c "SELECT fraud_reason, count(*) FROM fraud_alerts GROUP BY 1;"
```

---

## 9. Hooking Grafana up to the fraud alerts

1. Open Grafana: **http://localhost:3000** (login `admin` / `admin`, it will
   ask you to set a new password — you can skip that in a local demo).
2. **Connections → Data sources → Add data source → PostgreSQL.**
3. Fill in:
   - Host: `timescaledb:5432` (container-to-container network name — use this,
     not `localhost`, since Grafana runs inside the same Docker network)
   - Database: `fraud_detection`
   - User: `sentinel` / Password: `sentinel_pw`
   - TLS/SSL Mode: `disable` (local dev only)
   - Version: enable "TimescaleDB" toggle if shown
4. **Save & test** → should show "Database Connection OK".
5. Create a new **Dashboard → Add visualization** → select the data source,
   and use a query like:
   ```sql
   SELECT detected_at AS time, amount, user_id, fraud_reason
   FROM fraud_alerts
   WHERE $__timeFilter(detected_at)
   ORDER BY detected_at
   ```
   as a **Time series** panel, and a second panel on `pipeline_stats` for a
   single-stat TPS gauge.
6. Set the dashboard's auto-refresh (top right) to `5s` for a live feel.

---

## 10. Shutting everything down

```bash
# Ctrl+C in the producer/processor/api terminals, then:
docker compose down          # stop containers, keep data volumes
docker compose down -v       # stop containers AND wipe all data volumes
```

---

## 11. Resume bullet points

> Tailor these to the exact metrics you observe when you actually run it —
> the numbers below are realistic placeholders based on the default config.

- **Architected and built a distributed, real-time fraud detection pipeline** using Apache Kafka, a custom Python sliding-window rules engine, TimescaleDB, and MinIO, implementing a Lambda architecture that processed 150–200 synthetic transactions/sec with sub-10ms average detection latency.
- **Designed a three-rule stateful anomaly detection engine** (velocity bursts, high-value outliers, geolocation-based "impossible travel" via haversine distance) that flagged fraudulent transaction patterns in-flight, reducing manual review load by routing only ~2-5% of flagged high-risk events to the speed-layer database.
- **Engineered a dual-routing data pipeline and live monitoring stack** (Docker Compose, Grafana, a FastAPI service, and a custom real-time dashboard) that persisted flagged alerts to a TimescaleDB hypertable while archiving 100% of raw events as partitioned Parquet files in an S3-compatible data lake for downstream ML training.

---

## Notes on the dashboard + your GitHub Pages portfolio

`dashboard/index.html` is a fully static file — you can embed it directly
in your portfolio repo. Two honest caveats worth knowing as the engineer here:

- **A static page hosted on GitHub Pages cannot reach a Docker stack running
  on your laptop.** The dashboard is built to detect this automatically and
  fall back to a live-looking simulation, so it never appears broken to a
  recruiter visiting your live portfolio — but it won't show *your real*
  local data unless `api.py` is reachable from wherever the page is loaded.
- If you want the portfolio version to show **real, always-on data**, the
  standard fix is deploying `api.py` + a small Postgres instance to a free
  tier host (Railway, Render, Fly.io) and pointing the dashboard at that
  public URL via `index.html?api=https://your-api.example.com`. That's a
  natural "v2" addition to mention in an interview — it shows you understand
  the difference between a local demo and a production deployment.

For your portfolio, link to this project with a short GIF/screen-recording
of the terminal feed and Grafana dashboard in action — that's usually more
convincing to a recruiter scanning quickly than asking them to clone and run
Docker Compose themselves.
