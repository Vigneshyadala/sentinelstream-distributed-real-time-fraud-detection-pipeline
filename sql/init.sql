-- ─────────────────────────────────────────────────────────────────────────
-- SentinelStream speed-layer schema
-- Runs automatically on first container boot via docker-entrypoint-initdb.d
-- ─────────────────────────────────────────────────────────────────────────

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Flagged fraud alerts only (raw/all transactions go to the MinIO data lake,
-- not here — this keeps the speed-layer table small and query-fast).
CREATE TABLE IF NOT EXISTS fraud_alerts (
    id              BIGSERIAL,
    transaction_id  UUID            NOT NULL,
    user_id         TEXT            NOT NULL,
    amount          NUMERIC(12, 2)  NOT NULL,
    merchant        TEXT,
    location        TEXT,
    card_type       TEXT,
    fraud_reason    TEXT            NOT NULL,      -- e.g. 'VELOCITY', 'HIGH_AMOUNT', 'IMPOSSIBLE_TRAVEL'
    risk_score      SMALLINT        NOT NULL DEFAULT 50,
    tx_timestamp    TIMESTAMPTZ     NOT NULL,       -- when the transaction happened
    detected_at     TIMESTAMPTZ     NOT NULL DEFAULT now(), -- when the processor flagged it
    PRIMARY KEY (id, detected_at)
);

-- Convert to a hypertable partitioned on detected_at (1-day chunks).
SELECT create_hypertable('fraud_alerts', 'detected_at', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_fraud_alerts_user_id ON fraud_alerts (user_id, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_reason  ON fraud_alerts (fraud_reason);

-- Single-row "dashboard stats" table the processor upserts every few
-- seconds. Lets the API / Grafana read aggregate throughput numbers
-- without scanning Kafka or the whole alerts table.
CREATE TABLE IF NOT EXISTS pipeline_stats (
    id                  SMALLINT PRIMARY KEY DEFAULT 1,
    total_transactions  BIGINT      NOT NULL DEFAULT 0,
    total_alerts        BIGINT      NOT NULL DEFAULT 0,
    tps                 NUMERIC(8,2) NOT NULL DEFAULT 0,
    avg_detection_ms    NUMERIC(8,2) NOT NULL DEFAULT 0,
    last_updated        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT single_row CHECK (id = 1)
);

INSERT INTO pipeline_stats (id) VALUES (1) ON CONFLICT (id) DO NOTHING;

-- Convenience view Grafana can point at directly for a "last hour" panel.
CREATE OR REPLACE VIEW recent_fraud_alerts AS
SELECT * FROM fraud_alerts
WHERE detected_at > now() - INTERVAL '1 hour'
ORDER BY detected_at DESC;
