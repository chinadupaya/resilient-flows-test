import requests
import random
import time
import signal
import sys
from threading import Event

ACCOUNT_SERVICE_URL = "http://localhost:7070/api/v1/accounts"
TRANSACTION_SERVICE_URL = "http://localhost:9090/api/v1/transactions"

SYNC_MODE = "sync"

# Ramp configuration (TPS, duration_seconds)
RAMP_STAGES = [
    (2, 60),
    (5, 60),
    (10, 60),
    (20, 60),
    (30, 60),
]

stop_event = Event()


def signal_handler(sig, frame):
    print("\nStopping ramp load generator...")
    stop_event.set()


signal.signal(signal.SIGINT, signal_handler)


def get_accounts():
    response = requests.get(ACCOUNT_SERVICE_URL)
    response.raise_for_status()
    return response.json()


def create_transaction(source_id, destination_id, amount):
    payload = {
        "sourceAccountId": source_id,
        "destinationAccountId": destination_id,
        "amount": amount,
        "type": SYNC_MODE
    }
    response = requests.post(TRANSACTION_SERVICE_URL, json=payload)
    return response.status_code


def run_stage(account_ids, tps, duration):
    print(f"\nRunning stage: {tps} TPS for {duration}s")

    delay = 1.0 / tps
    end_time = time.time() + duration

    sent = 0
    success = 0
    failures = 0

    while time.time() < end_time and not stop_event.is_set():
        start = time.time()

        source, destination = random.sample(account_ids, 2)
        amount = random.randint(10, 200)

        try:
            status = create_transaction(source, destination, amount)
            sent += 1

            if 200 <= status < 300:
                success += 1
            else:
                failures += 1

        except Exception:
            failures += 1

        elapsed = time.time() - start
        sleep_time = delay - elapsed

        if sleep_time > 0:
            time.sleep(sleep_time)

    print(f"Stage Complete → Sent: {sent}, Success: {success}, Failures: {failures}")


def main():
    print("Fetching accounts...")
    accounts = get_accounts()

    if len(accounts) < 2:
        print("Not enough accounts.")
        return

    account_ids = [a["id"] for a in accounts]
    print(f"Loaded {len(account_ids)} accounts.")

    for tps, duration in RAMP_STAGES:
        if stop_event.is_set():
            break
        run_stage(account_ids, tps, duration)

    print("\nRamp test finished.")


if __name__ == "__main__":
    main()