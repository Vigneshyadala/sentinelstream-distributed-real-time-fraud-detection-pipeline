<div align="center">

```
   _____            __  _            _  _____ __
  / ___/___  ____  / /_(_)___  ___  / |/ / ___/ /_________  ____ _____ ___
  \__ \/ _ \/ __ \/ __/ / __ \/ _ \/    /\__ \/ __/ ___/ _ \/ __ `/ __ `__ \
 ___/ /  __/ / / / /_/ / / / /  __/    /___/ / /_/ /  /  __/ /_/ / / / / / /
/____/\___/_/ /_/\__/_/_/ /_/\___/_/|_//____/\__/_/   \___/\__,_/_/ /_/ /_/
```

# 🛡️ SentinelStream
### Distributed Real-Time Fraud Detection Pipeline

</div>

![Status](https://img.shields.io/badge/STATUS-LIVE%20DEMO-2dd4bf?style=for-the-badge&labelColor=1a1024)
![Python](https://img.shields.io/badge/PYTHON-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white&labelColor=1a1024)
![Kafka](https://img.shields.io/badge/KAFKA-CONFLUENT-231F20?style=for-the-badge&logo=apachekafka&logoColor=white&labelColor=1a1024)
![TimescaleDB](https://img.shields.io/badge/TIMESCALEDB-PG16-FDB515?style=for-the-badge&labelColor=1a1024)
![MinIO](https://img.shields.io/badge/MINIO-S3%20COMPATIBLE-C72E49?style=for-the-badge&logo=minio&logoColor=white&labelColor=1a1024)
![Docker](https://img.shields.io/badge/DOCKER-COMPOSE-2496ED?style=for-the-badge&logo=docker&logoColor=white&labelColor=1a1024)
![FastAPI](https://img.shields.io/badge/FASTAPI-0.111-009688?style=for-the-badge&logo=fastapi&logoColor=white&labelColor=1a1024)
![Grafana](https://img.shields.io/badge/GRAFANA-DASHBOARDS-F46800?style=for-the-badge&logo=grafana&logoColor=white&labelColor=1a1024)
![License](https://img.shields.io/badge/LICENSE-MIT-ffb088?style=for-the-badge&labelColor=1a1024)

<p align="center">
<a href="https://vigneshyadala.github.io/sentinelstream-distributed-real-time-fraud-detection-pipeline/">
<img src="https://img.shields.io/badge/🚀_LIVE_DEMO-SENTINELSTREAM-2dd4bf?style=for-the-badge&labelColor=1a1024" alt="Live Demo"/>
</a>
<a href="https://github.com/Vigneshyadala">
<img src="https://img.shields.io/badge/GITHUB-VIGNESHYADALA-000000?style=for-the-badge&logo=github&logoColor=white" alt="GitHub"/>
</a>
<a href="https://vigneshyadala.github.io/portfolio">
<img src="https://img.shields.io/badge/PORTFOLIO-VIEW-ffb088?style=for-the-badge&labelColor=1a1024" alt="Portfolio"/>
</a>
</p>

---

## 📌 About

SentinelStream is a full **Lambda-architecture** fraud detection system that
ingests a live stream of synthetic credit-card transactions, flags fraud in
real time using a stateful rules engine, and persists results down two
parallel paths — a fast, queryable speed layer and a durable batch archive —
all visualized through a live dashboard.

It's the complete pipeline, end to end:

```
Producer  →  Kafka  →  Detection Engine  →  TimescaleDB + MinIO  →  API  →  Dashboard / Grafana
```

Nothing here is simulated after ingestion — every transaction is generated,
streamed through a real Kafka broker, scored against real detection rules,
and written to a real time-series database and a real S3-compatible data
lake. The dashboard polls that live data through a thin FastAPI layer.

---

## ✨ Features

| Feature | Description |
|---|---|
| ⚡ **Real-Time Streaming Ingestion** | Kafka-backed producer streams ~150–200 synthetic transactions/sec across 300 virtual users |
| 🕵️ **Stateful Fraud Detection Engine** | Sliding-window rules catch velocity bursts, impossible-travel (haversine distance), and amount-spike anomalies |
| 🏗️ **Lambda Architecture** | Every event is dual-routed — flagged alerts hit the TimescaleDB speed layer, 100% of raw events archive as Parquet in MinIO |
| 📊 **Live Dashboard** | Static `index.html` polls `/api/stats` and `/api/alerts` for real-time counts and a live alert feed |
| 🔌 **Read-Only Dashboard API** | FastAPI service exposes exactly two safe, CORS-open, read-only endpoints — no direct DB exposure |
| 📈 **Grafana-Ready** | TimescaleDB connects directly as a Grafana data source for auto-refreshing time-series panels |
| 🐳 **One-Command Infra** | Kafka, Zookeeper, TimescaleDB, MinIO, and Grafana all boot via a single `docker compose up -d` |
| ✅ **Health-Checked Everything** | Every container has a real healthcheck — producer/processor never race a broker or DB that's still booting |

---

## 🛠️ Tech Stack

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-Confluent-231F20?style=flat-square&logo=apachekafka&logoColor=white)
![TimescaleDB](https://img.shields.io/badge/TimescaleDB-Postgres%2016-FDB515?style=flat-square)
![MinIO](https://img.shields.io/badge/MinIO-S3%20API-C72E49?style=flat-square&logo=minio&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.111-009688?style=flat-square&logo=fastapi&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-11.1-F46800?style=flat-square&logo=grafana&logoColor=white)

- **Streaming** — `confluent-kafka` (librdkafka), keyed by `user_id` for per-user ordering
- **Detection engine** — custom Python sliding-window rules engine (`processor.py`)
- **Speed layer** — TimescaleDB hypertable for low-latency alert queries
- **Batch layer** — MinIO (S3-compatible), raw events archived as partitioned Parquet via `pyarrow` + `boto3`
- **API layer** — FastAPI, two read-only endpoints, CORS-open for the static dashboard
- **Orchestration** — Docker Compose, 6 services networked together with healthchecks
- **Visualization** — static `index.html` dashboard + optional Grafana panels

---

## 🏗️ Architecture

```
                ┌───────────────┐
                │   producer.py  │   300 virtual users
                │  (Kafka producer)│  velocity / travel / spike
                └───────┬───────┘   fraud injectors
                        │ produces
                        ▼
                ┌───────────────┐
                │     Kafka      │   topic: transactions
                │  (Confluent)   │
                └───────┬───────┘
                        │ consumes
                        ▼
                ┌───────────────┐
                │  processor.py  │   sliding-window
                │ detection engine│  fraud rules
                └───────┬───────┘
                        │
            ┌───────────┴───────────┐
            ▼                       ▼
   ┌─────────────────┐     ┌─────────────────┐
   │  TimescaleDB      │     │      MinIO       │
   │  (speed layer)     │     │  (batch layer)    │
   │  fraud_alerts       │     │  raw txns → Parquet│
   └─────────┬─────────┘     └─────────────────┘
             │
             ▼
     ┌───────────────┐
     │    api.py       │   FastAPI, read-only
     │ /api/stats       │
     │ /api/alerts      │
     └───────┬───────┘
             │
    ┌────────┴────────┐
    ▼                 ▼
┌────────┐      ┌──────────┐
│index.html│      │ Grafana   │
│ dashboard │      │dashboards │
└────────┘      └──────────┘
```

| Component | Tech | Port | Role |
|---|---|---|---|
| `zookeeper` | Confluent CP | `2181` | Kafka coordination |
| `kafka` | Confluent CP | `9092` | Transaction stream broker |
| `timescaledb` | Postgres 16 + Timescale | `5432` | Speed-layer alert storage |
| `minio` | MinIO | `9000` / `9001` | Batch-layer Parquet data lake |
| `grafana` | Grafana 11.1 | `4000 → 3000` | Live time-series dashboards |
| `api.py` | FastAPI | `8000` | Read-only dashboard API |

---

## 🚀 Quick Start

**1️⃣ Clone the repo**
```bash
git clone https://github.com/Vigneshyadala/sentinelstream-distributed-real-time-fraud-detection-pipeline.git
cd sentinelstream-distributed-real-time-fraud-detection-pipeline
```

**2️⃣ Bring the infrastructure up**
```bash
docker compose up -d
docker compose ps    # wait until all services show "healthy"
```

**3️⃣ Install Python dependencies**
```bash
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

**4️⃣ Run the pipeline (3 terminals)**
```bash
# Terminal 1 — detection engine (start first)
python processor.py

# Terminal 2 — transaction producer
python producer.py

# Terminal 3 — dashboard API
python api.py
```

**5️⃣ Open the dashboard**
```bash
start index.html      # Windows
open index.html        # macOS
```

The page auto-detects `api.py` on `localhost:8000` and switches to
"● live · api connected," pulling real transaction counts, alert counts,
and a live alert feed straight from TimescaleDB.

---

## 🔒 Design Notes

✅ The API is strictly **read-only** — no endpoint can mutate the database
✅ CORS is wide open only because this is local, non-sensitive synthetic data — tighten `allow_origins` before any public deployment
✅ Kafka messages are **keyed by `user_id`**, guaranteeing per-user ordering, which the detection engine's sliding-window rules depend on
✅ Every container has a real `healthcheck` — no race conditions between producer/processor and a booting Kafka broker or database
✅ `acks=all` on the producer — this is financial data, so no message loss on retries

---

## 📁 Repository Structure

```
sentinelstream-distributed-real-time-fraud-detection-pipeline/
├── sql/
│   └── init.sql            # TimescaleDB schema + hypertable setup
├── docker-compose.yml      # Kafka, Zookeeper, TimescaleDB, MinIO, Grafana
├── producer.py              # Synthetic transaction generator + fraud injectors
├── processor.py              # Sliding-window fraud detection engine
├── api.py                    # Read-only FastAPI dashboard layer
├── index.html                 # Live dashboard (static, GitHub Pages-ready)
├── requirements.txt
└── README.md
```

---

## 📸 Live Demo

<p align="center">
<a href="https://vigneshyadala.github.io/sentinelstream-distributed-real-time-fraud-detection-pipeline/">
<img src="https://img.shields.io/badge/🚀_TRY_IT_LIVE-SENTINELSTREAM-2dd4bf?style=for-the-badge&labelColor=1a1024" alt="Try it live"/>
</a>
</p>

The hosted dashboard runs entirely as a static page. When it can reach a
local `api.py` instance it shows real live data; otherwise it falls back to
a self-contained simulated feed, so it never looks broken to a visitor.

---

## 👨‍💻 Developer

<div align="center">

| Name | Role | GitHub |
|---|---|---|
| Vignesh Yadala | Designer & Developer | [@Vigneshyadala](https://github.com/Vigneshyadala) |

🛡️ **SentinelStream** — All Rights Reserved © Vignesh Yadala 2026

Built with Kafka, TimescaleDB, MinIO, FastAPI & Docker

</div>
