import json
import logging
import time
from kafka import KafkaConsumer
from prometheus_client import start_http_server, Counter, Gauge

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("drift-detector")

TABLE = "public.accounts"

# ── Prometheus metrics ─────────────────────────────────────────────
events_processed = Counter(
    "cdc_events_processed_total",
    "Total CDC events processed",
    ["op"]
)

null_ratio = Gauge(
    "cdc_null_ratio",
    "Rolling null ratio per column",
    ["table", "column"]
)

event_lag = Gauge(
    "cdc_event_lag_seconds",
    "Lag between DB commit and processing time",
    ["table"]
)

# rolling null window
null_windows = {}
WINDOW_SIZE = 100


def update_null_ratio(row):
    for col, value in row.items():
        window = null_windows.setdefault(col, [])

        window.append(value is None)
        if len(window) > WINDOW_SIZE:
            window.pop(0)

        ratio = sum(window) / len(window)
        null_ratio.labels(TABLE, col).set(ratio)


def process_event(payload):
    op = payload.get("op")

    if op == "r":  # snapshot event
        return

    before = payload.get("before") or {}
    after = payload.get("after") or {}

    row = after or before
    if not row:
        return

    # event lag metric
    ts_ms = payload.get("source", {}).get("ts_ms")
    if ts_ms:
        lag = time.time() - ts_ms / 1000
        event_lag.labels(TABLE).set(lag)

    update_null_ratio(row)

    events_processed.labels(op=op).inc()

    log.info(f"CDC {op} → {row}")


def main():

    consumer = KafkaConsumer(
        "account.public.accounts",
        bootstrap_servers="localhost:9092",
        group_id="drift-detector",
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    start_http_server(8000)
    log.info("Drift detector running — metrics on :8000/metrics")

    for msg in consumer:
        try:
            event = msg.value
            payload = event.get("payload")

            if not payload:
                continue

            process_event(payload)

        except Exception as e:
            log.error(f"Failed to process event: {e}", exc_info=True)


if __name__ == "__main__":
    main()