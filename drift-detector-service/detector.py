import json
import logging
import time
import base64
from decimal import Decimal
from kafka import KafkaConsumer
from prometheus_client import start_http_server, Counter, Gauge

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("drift-detector")

TABLE = "public.accounts"


ACCOUNTS_TOPIC = "account.public.accounts"
TRANSACITONS_TOPIC = "transaction.public.transactions"

INITIAL_TOTAL = Decimal("0") 
# Prometheus metrics
account_total_metric = Gauge(
    "accounts_total_money",
    "Running total of all account balances"
)

expected_total_metric = Gauge(
    "expected_total_money",
    "Expected money based on transactions"
)

money_drift_metric = Gauge(
    "money_drift_absolute",
    "Absolute difference between expected and actual totals"
)

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
# Detector state

account_total = INITIAL_TOTAL
expected_total = INITIAL_TOTAL

# track last known balances per account
balances = {}

# rolling null window
null_windows = {}
WINDOW_SIZE = 100

# Helper functions

def decode_decimal(value, scale=2):
    """Decode Debezium decimal (base64 bytes → Decimal)."""
    if value is None:
        return Decimal("0")

    raw = base64.b64decode(value)
    integer = int.from_bytes(raw, byteorder="big", signed=True)
    return Decimal(integer) / (10 ** scale)

def update_null_ratio(row):
    for col, value in row.items():
        window = null_windows.setdefault(col, [])

        window.append(value is None)
        if len(window) > WINDOW_SIZE:
            window.pop(0)

        ratio = sum(window) / len(window)
        null_ratio.labels(TABLE, col).set(ratio)


def update_account_balance(before, after):

    global account_total

    before_id = before.get("id") if before else None
    after_id = after.get("id") if after else None

    account_id = after_id or before_id
    if not account_id:
        return

    prev_balance = balances.get(account_id, Decimal("0"))

    new_balance = decode_decimal(after.get("balance")) if after else Decimal("0")

    delta = new_balance - prev_balance

    balances[account_id] = new_balance
    account_total += delta

    account_total_metric.set(float(account_total))

def process_account_event(payload):

    before = payload.get("before") or {}
    after = payload.get("after") or {}

    update_account_balance(before, after)

    ts_ms = payload.get("source", {}).get("ts_ms")
    if ts_ms:
        lag = time.time() - ts_ms / 1000
        event_lag.labels("accounts").set(lag)

    events_processed.labels("accounts").inc()

def process_transaction_event(event):

    global expected_total

    tx_type = event.get("type")
    amount = Decimal(str(event.get("amount", 0)))

    if tx_type == "deposit":
        expected_total += amount

    elif tx_type == "withdrawal":
        expected_total -= amount

    expected_total_metric.set(float(expected_total))
    events_processed.labels("transactions").inc()

def check_invariant():

    drift = abs(account_total - expected_total)

    money_drift_metric.set(float(drift))

    if drift > Decimal("0.01"):
        log.error(
            f"FINANCIAL DRIFT DETECTED "
            f"(accounts={account_total}, expected={expected_total}, drift={drift})"
        )

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

    # TO DO detect_schema_changes()
    update_null_ratio(row)
    update_checksum(row)

    events_processed.labels(op=op).inc()

    log.info(f"CDC {op} → {row}")


def main():

    consumer = KafkaConsumer(
        ACCOUNTS_TOPIC,
        TX_TOPIC,
        bootstrap_servers="localhost:9092",
        group_id="financial-drift-detector",
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    start_http_server(8000)
    log.info("Drift detector running — metrics on :8000/metrics")

    for msg in consumer:
        try:
            topic = msg.topic
            event = msg.value

            if topic == ACCOUNTS_TOPIC:

                payload = event.get("payload")
                if payload:
                    process_account_event(payload)

            elif topic == TRANSACITONS_TOPIC:

                process_transaction_event(event)

            # check invariant every few seconds
            if time.time() - last_check > 5:
                check_invariant()
                last_check = time.time()

        except Exception as e:
            log.error(f"Failed to process event: {e}", exc_info=True)


if __name__ == "__main__":
    main() 