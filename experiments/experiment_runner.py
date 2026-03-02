import requests
import random
import time
import subprocess
import threading

# ---------------------------
# CONFIGURATION
# ---------------------------

ACCOUNT_SERVICE_URL = "http://localhost:8080/api/v1/accounts"
TRANSACTION_SERVICE_URL = "http://localhost:9090/api/v1/transactions"

# config = {
#     "sync_mode": "sync", # or async
#     "failure_stage": 2, 
#     "failure_duration": 5,
#     "failure_service": "spanner-transaction" #postgres-account, kafka, spanner-transaction
# }

SYNC_MODE = "async"

RAMP_STAGES = [
    (5, 20),
    (10, 20),
    (20, 20),
]

FAILURE_STAGE_INDEX = 2
FAILURE_DURATION = 5

FAILURE_COMMAND = ["docker", "stop", "postgres-account"]
RECOVERY_COMMAND = ["docker", "start", "postgres-account"]
# FAILURE_COMMAND = ["docker", "stop", "spanner-transaction"]
# RECOVERY_COMMAND = ["docker", "start", "spanner-transaction"]

stop_event = threading.Event()

# ---------------------------
# LOAD GENERATOR
# ---------------------------

def get_accounts_full():
    r = requests.get(ACCOUNT_SERVICE_URL)
    r.raise_for_status()
    return r.json()


def get_account_ids():
    return [a["id"] for a in get_accounts_full()]


def get_transactions():
    r = requests.get(TRANSACTION_SERVICE_URL)
    r.raise_for_status()
    return r.json()


def create_transaction(source, destination, amount):
    payload = {
        "sourceAccountId": source,
        "destinationAccountId": destination,
        "amount": amount,
        "type": SYNC_MODE
    }
    requests.post(TRANSACTION_SERVICE_URL, json=payload)


def run_stage(account_ids, tps, duration, stage_index):
    print(f"\nStage {stage_index}: {tps} TPS for {duration}s")
    delay = 1.0 / tps
    end_time = time.time() + duration

    while time.time() < end_time and not stop_event.is_set():
        start = time.time()

        source, destination = random.sample(account_ids, 2)
        amount = random.randint(10, 200)

        try:
            create_transaction(source, destination, amount)
        except Exception:
            pass

        elapsed = time.time() - start
        sleep_time = delay - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)


# ---------------------------
# COMMANDS
# ---------------------------

def inject_failure():
    print("\nInjecting failure...")
    subprocess.run(FAILURE_COMMAND)


def recover_failure():
    print("\nRecovering service...")
    subprocess.run(RECOVERY_COMMAND)

def run_sql_init():
    print("\nCleaning postgres-db service...")
    with open("init.sql", "r") as sql_file:
        result = subprocess.run(
            ["docker", "exec", "-i", "postgres-account", "psql", "-U", "postgres", "-d", "account"],
            stdin=sql_file,
            capture_output=True,
            text=True
        )
    
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {result.stderr}")
    
    return result.stdout


# ---------------------------
# CONSISTENCY CHECK
# ---------------------------

def calculate_total_money(accounts):
    return sum(a["balance"] + a["reservedAmount"] for a in accounts)


def check_consistency(initial_accounts):
    print("\nRunning consistency checks...")

    final_accounts = get_accounts_full()
    transactions = get_transactions()

    initial_total = calculate_total_money(initial_accounts)
    final_total = calculate_total_money(final_accounts)

    print(f"Initial total money: {initial_total}")
    print(f"Final total money:   {final_total}")

    money_drift = final_total - initial_total

    # Count stuck reservations
    stuck = [a for a in final_accounts if a["reservedAmount"] > 0]

    # Check transaction invariants
    completed = [t for t in transactions if t["status"] == "COMPLETED"]
    failed = [t for t in transactions if t["status"] == "FAILED"]

    print(f"\nCompleted transactions: {len(completed)}")
    print(f"Failed transactions:    {len(failed)}")
    print(f"Accounts with reserved funds: {len(stuck)}")

    print("\n==== CONSISTENCY REPORT ====")
    print(f"Money drift: {money_drift}")
    print(f"Stuck reservations: {len(stuck)}")

    if abs(money_drift) > 0.0001:
        print("❌ MONEY INCONSISTENCY DETECTED")
    else:
        print("✅ Money conserved")

    if len(stuck) > 0:
        print("❌ Stuck reservations detected")
    else:
        print("✅ No stuck reservations")

    print("=============================\n")


# ---------------------------
# EXPERIMENT
# ---------------------------

def run_experiment():
    print(f"\nStarting consistency-focused experiment of type {SYNC_MODE}")
    run_sql_init()
    time.sleep(5)
    initial_accounts = get_accounts_full()
    account_ids = [a["id"] for a in initial_accounts]

    for index, (tps, duration) in enumerate(RAMP_STAGES):

        if index == FAILURE_STAGE_INDEX:
            inject_failure()
            time.sleep(FAILURE_DURATION)
            recover_failure()

        run_stage(account_ids, tps, duration, index)

    # allow system to stabilize
    print("\nWaiting for stabilization...")
    time.sleep(10)

    check_consistency(initial_accounts)

    print("Experiment complete.")


if __name__ == "__main__":
    run_experiment()