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
TRANSACTIONS_TOPIC = "transaction.public.transactions"

snapshot_complete = False
initial_total = None

drift_detected=False

# Prometheus metrics
account_total_metric = Gauge(
    "accounts_total_money",
    "Running total of all account balances"
)


reserved_total_metric = Gauge(
    "accounts_reserved_total",
    "Running total of reserved funds"
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

account_total = Decimal("0")
reserved_total = Decimal("0")
expected_total = Decimal("0")

# track last known balances per account
balances = {}
reserved_balances={}

initial_total = None

# rolling null window
null_windows = {}
WINDOW_SIZE = 100

# Helper functions

def decode_decimal(value, scale=2):
    if value is None:
        return Decimal("0")

    raw = base64.b64decode(value)
    integer = int.from_bytes(raw, byteorder="big", signed=True)
    return Decimal(integer) / Decimal(100)

def update_account_balance(before, after):

    global account_total
    global reserved_total
    
    before_id = before.get("id") if before else None
    after_id = after.get("id") if after else None

    # account_id = after_id or before_id
    account_id = after_id if after_id is not None else before_id
    if not account_id:
        return

    prev_balance = balances.get(account_id, Decimal("0"))
    prev_reserved = reserved_balances.get(account_id, Decimal("0"))

    new_balance = decode_decimal(after.get("balance")) if after else Decimal("0")
    new_reserved = decode_decimal(after.get("reserved_amount")) if after else Decimal("0")

    balance_delta = new_balance - prev_balance
    reserved_delta = new_reserved - prev_reserved

    balances[account_id] = new_balance
    reserved_balances[account_id] = new_reserved

    account_total += balance_delta
    reserved_total += reserved_delta

    account_total_metric.set(float(account_total))
    reserved_total_metric.set(float(reserved_total))

def handle_account_delete(before):

    global account_total
    global reserved_total

    if not before:
        return

    account_id = before["id"]

    prev_balance = balances.pop(account_id, Decimal("0"))
    prev_reserved = reserved_balances.pop(account_id, Decimal("0"))

    account_total -= prev_balance
    reserved_total -= prev_reserved

def process_account_event(payload):

    global snapshot_complete
    global initial_total

    op = payload.get("op")

    before = payload.get("before")
    after = payload.get("after")

    if op == "d":
        handle_account_delete(before)
        return

    update_account_balance(before or {}, after)

    snapshot_flag = payload.get("source", {}).get("snapshot")

    if snapshot_flag == "last" and not snapshot_complete:
        snapshot_complete = True
        initial_total = account_total

        log.info(
            f"Snapshot complete. Initial system total = {initial_total}"
        )

    ts_ms = payload.get("source", {}).get("ts_ms")
    if ts_ms:
        lag = time.time() - ts_ms / 1000
        event_lag.labels("accounts").set(lag)

    events_processed.labels("accounts").inc()

# NOT USING RIGHT NOW
def process_transaction_event(event):

    global expected_total

    tx_type = event.get("type")
    amount = Decimal(str(event.get("amount", 0)))

    # expected_total == something

    expected_total_metric.set(float(expected_total))
    events_processed.labels("transactions").inc()

def check_invariant():

    global drift_detected

    if initial_total is None:
        return

    # current_total = account_total + reserved_total
    drift = abs(account_total - initial_total)

    money_drift_metric.set(float(drift))
    log.info(
            f"CURRENT STATS "
            f"(initial={initial_total}, current={account_total}, reserved_total={reserved_total}, drift={drift})"
    )
    
    if drift > Decimal("0.01") and not drift_detected:
        log.error(
            f"FINANCIAL DRIFT DETECTED "
            f"(initial={initial_total}, current={account_total}, reserved_total={reserved_total}, drift={drift})"
        )
        drift_detected = True
    elif reserved_total > Decimal("0.01"):
        log.error(
            f"FINANCIAL DRIFT DETECTED "
            f"(initial={initial_total}, current={account_total}, reserved_total={reserved_total}, drift={drift})"
        )
    elif (drift <= Decimal("0.01") or reserved_total <= Decimal("0.01")) and drift_detected:
        drift_detected=False
        log.info("Resetting drift detection")

def main():

    consumer = KafkaConsumer(
        ACCOUNTS_TOPIC,
        TRANSACTIONS_TOPIC,
        bootstrap_servers="localhost:9092",
        # group_id="financial-drift-detector",
        group_id=f"financial-drift-detector-{int(time.time())}", # for debugging
        auto_offset_reset="earliest",
        # value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        value_deserializer=lambda b: json.loads(b.decode("utf-8")) if b else None
    )

    start_http_server(8000)
    log.info("Drift detector running — metrics on :8000/metrics")
    last_check = time.time()
    for msg in consumer:
       
        try:
            topic = msg.topic
            event = msg.value

            if topic == ACCOUNTS_TOPIC:
                if event:
                    payload = event.get("payload")
                    if payload:
                        # print("ACCOUNT DB EVENT RECEIVED:", payload)
                        process_account_event(payload)
                else:
                    log.debug(f"Tombstone received for event {event}")
                    continue

            # elif topic == TRANSACTIONS_TOPIC:

                # process_transaction_event(event)

            # check invariant every few seconds
            if time.time() - last_check > 5:
                check_invariant()
                last_check = time.time()
                # sanity chekc
                log.info(
                    f"accounts tracked={len(reserved_balances)} "
                    f"sum(reserved_balances)={sum(reserved_balances.values())} "
                    f"reserved_total={reserved_total}"
                )

                recomputed = sum(reserved_balances.values())

                if abs(recomputed - reserved_total) > Decimal("0.01"):
                    log.error(
                        f"DELTA ACCOUNTING BUG "
                        f"(recomputed={recomputed}, reserved_total={reserved_total})"
                    )
                

        except Exception as e:
            log.error(f"Failed to process event: {e}", exc_info=True)


if __name__ == "__main__":
    main() 