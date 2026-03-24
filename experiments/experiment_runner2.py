import requests
import random
import time
import subprocess
import threading
import datetime
import csv
import string

PROMETHEUS_URL = "http://localhost:9095"
ACCOUNT_SERVICE_URL = "http://localhost:7070/api/v1/accounts"
TRANSACTION_SERVICE_URL = "http://localhost:9090/api/v1/transactions"

SYNC_MODE = "sync"

# RAMP_STAGES = [
#     (5, 20),
#     (10, 20),
#     (20, 20),
# ]

FAILURE_STAGE_INDEX = 2
FAILURE_DURATION = 5
stop_event = threading.Event()

FAILURE_SCENARIOS = [
    {
        "name": "Account DB failure",
        "failure_cmd": ["docker", "stop", "postgres-account"],
        "recovery_cmd": ["docker", "start", "postgres-account"],
    },
    {
        "name": "Transaction service failure",
        "failure_cmd": ["docker", "stop", "spanner-transaction"],
        "recovery_cmd": ["docker", "start", "spanner-transaction"],
    },
    {
        "name": "Kafka failure",
        "failure_cmd": ["docker", "stop", "kafka"],
        "recovery_cmd": ["docker", "start", "kafka"],
    },
    {
        "name": "Account service crash (local)",
        "failure_cmd": ["pkill", "-f", "account-service"],
        "recovery_cmd": ["bash", "-c", "cd ../account-service && mvn spring-boot:run &"],
    }
    # {
    #     "name": "Cascading: DB + Kafka",
    #     "failure_cmd": [["docker", "stop", "postgres-account"], ["docker", "stop", "kafka"]],
    #     "recovery_cmd": [["docker", "start", "postgres-account"], ["docker", "start", "kafka"]],
    #     "multi": True,
    # },
]

ACCOUNT_SERVICE_PROCESS = None
def wait_for_account_service():
    print("[CHAOS] Waiting for account service...")
    for _ in range(60):
        try:
            r = requests.get("http://localhost:7070/api/v1/accounts")
            if r.status_code == 200:
                print("[CHAOS] Account service ready")
                return
        except:
            pass
        time.sleep(1)
    raise RuntimeError("Account service did not start")
def start_account_service():
    global ACCOUNT_SERVICE_PROCESS
    print("[CHAOS] Starting account service...")

    log_file = open("account_service.log", "w")

    ACCOUNT_SERVICE_PROCESS = subprocess.Popen(
        ["mvn", "spring-boot:run"],
        cwd="../account-service",
        stdout=log_file,
        stderr=log_file,
        stdin=subprocess.DEVNULL
    )
    wait_for_account_service()
    
def stop_account_service():
    global ACCOUNT_SERVICE_PROCESS
    if ACCOUNT_SERVICE_PROCESS:
        print("[CHAOS] Stopping account service...")
        ACCOUNT_SERVICE_PROCESS.terminate()
        try:
            ACCOUNT_SERVICE_PROCESS.wait(timeout=10)
        except subprocess.TimeoutExpired:
            ACCOUNT_SERVICE_PROCESS.kill()
        ACCOUNT_SERVICE_PROCESS = None
def query_prometheus(metric: str) -> float:
    """Instant query - returns current value."""
    r = requests.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": metric})
    r.raise_for_status()
    result = r.json()["data"]["result"]
    return float(result[0]["value"][1]) if result else 0.0

def query_prometheus_range(metric: str, start: float, end: float, step: str = "5s") -> list:
    """Range query - returns time series between start and end."""
    r = requests.get(f"{PROMETHEUS_URL}/api/v1/query_range", params={
        "query": metric,
        "start": start,
        "end": end,
        "step": step,
    })
    r.raise_for_status()
    result = r.json()["data"]["result"]
    return result[0]["values"] if result else []  # [[timestamp, value], ...]
def snapshot_metrics() -> dict:
    return {
        "started":   query_prometheus("transactions_started_total"),
        "completed": query_prometheus("transactions_completed_total"),
        "failed":    query_prometheus("transactions_failed_total"),
        "active":    query_prometheus("transactions_reservations_active"),
        "stuck":    query_prometheus("transactions_stuck"),
        # "money_total":    query_prometheus("accounts_money"),
        # "reserved_total":    query_prometheus("accounts_reserved"),
        "timestamp": time.time(),
    }

def diff_snapshots(before: dict, after: dict) -> dict:
    return {
        "started": after["started"] - before["started"],
        "completed": after["completed"] - before["completed"],
        "failed":    after["failed"]    - before["failed"],
        "stuck":    after["stuck"]    - before["stuck"],
        # "money_total_drift": after["money_total"] - before["money_total"],
        # "reserved_total_drift": after["reserved_total"] - before["reserved_total"],
        "max_active": after["active"] - before["active"],
        "duration_s": after["timestamp"] - before["timestamp"],
    }

def capture_failure_window(start_time: float, end_time: float) -> dict:
    """Query time series for the failure window for each metric."""
    metrics = ["transactions_reservations_active", "transactions_started_total",
               "transactions_completed_total", "transactions_failed_total"]
    
    return {
        m: query_prometheus_range(m, start_time, end_time, step="5s")
        for m in metrics
    }

def inject_failure_after_delay(delay, scenario, stage_results):
    def _inject():
        time.sleep(delay)
        failure_start = time.time()
        print(f"\n[CHAOS] Injecting: {scenario['name']}")

        if scenario["name"] == "Account service crash (local)":
            stop_account_service()
        else:
            subprocess.run(scenario["failure_cmd"])

        time.sleep(FAILURE_DURATION)

        print(f"\n[CHAOS] Recovering: {scenario['name']}")

        if scenario["name"] == "Account service crash (local)":
            start_account_service()
        else:
            subprocess.run(scenario["recovery_cmd"])

        failure_end = time.time()
        stage_results["failure_window"] = capture_failure_window(failure_start, failure_end)

    threading.Thread(target=_inject, daemon=True).start()
def query_actual_tps(window: str = "15s") -> dict:
    return {
        "completed_tps": query_prometheus(f"rate(transactions_completed_total[{window}])"),
        "failed_tps":    query_prometheus(f"rate(transactions_failed_total[{window}])"),
        "started_tps":   query_prometheus(f"rate(transactions_started_total[{window}])"),
    }

# Commands
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
def run_spanner_init():
    print("\nCleaning Spanner transactions table...")
    HOST = "http://localhost:9020"  # exposed port from spanner-emulator container
    PROJECT = "test-project"
    INSTANCE = "test-instance"
    DATABASE = "test-database"

    base = f"{HOST}/v1/projects/{PROJECT}/instances/{INSTANCE}/databases/{DATABASE}"

    # Start a session
    session = requests.post(f"{base}/sessions").json()
    session_name = session["name"]

    # Delete all rows 
    r = requests.post(f"{HOST}/v1/{session_name}:commit", json={
        "singleUseTransaction": {"readWrite": {}},
        "mutations": [
            {
                "delete": {
                    "table": "transactions",
                    "keySet": {"all": True}  # deletes every row
                }
            }
        ]
    })

    if r.status_code != 200:
        raise RuntimeError(f"Spanner cleanup failed: {r.text}")

    # Clean up session
    requests.delete(f"{HOST}/v1/{session_name}")

# Account/Transaction Info
def get_accounts_full():
    r = requests.get(ACCOUNT_SERVICE_URL)
    r.raise_for_status()
    return r.json()

def create_transaction(source, destination, amount):
    payload = {
        "sourceAccountId": source,
        "destinationAccountId": destination,
        "amount": amount,
        "type": SYNC_MODE
    }
    headers = {
        "idempotency-key": ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(8)) 
    }
    requests.post(TRANSACTION_SERVICE_URL, json=payload, headers=headers)

def get_transactions():
    r = requests.get(TRANSACTION_SERVICE_URL)
    r.raise_for_status()
    return r.json()

def calculate_total_money(accounts):
    return {
        "balance": sum(a["balance"] for a in accounts),
        "reserved": sum(a["reservedAmount"] for a in accounts)
    }

def check_consistency(initial_accounts, before_snapshot: dict, after_snapshot: dict):
    diff = diff_snapshots(before_snapshot, after_snapshot)
    # actual_tps = query_actual_tps()

    final_accounts = get_accounts_full()
    initial_total = calculate_total_money(initial_accounts)
    final_total = calculate_total_money(final_accounts)
    print("initial_total", initial_total)
    print("final_total", final_total)
    # money_drift = diff['money_total_drift']
    money_drift=abs(final_total["balance"]-initial_total["balance"])
    money_drift_rate = (money_drift / initial_total["balance"])
    reservation_drift=abs(final_total["reserved"]-initial_total["reserved"])
    stuck = [a for a in final_accounts if a["reservedAmount"] > 0]
    transactions_stuck = abs(diff['started'] - (diff['completed'] + diff['failed']))
    transactions_stuck_rate = transactions_stuck / after_snapshot['started']

    if stuck:
        print("\nStuck reservation details:")
        for a in stuck:
            print(f"  Account {a['id']}: reserved={a['reservedAmount']}, balance={a['balance']}")

    print("\n==== PROMETHEUS METRICS ====")
    print(f"Transactions started:   {diff['started']:.0f}")
    print(f"Transactions completed: {diff['completed']:.0f}")
    print(f"Transactions failed:    {diff['failed']:.0f}")
    print(f"Current active:         {diff['max_active']}")

    print("\n==== CONSISTENCY REPORT ====")
    print(f"Money drift: {money_drift}")
    print(f"Money drift percent: {money_drift_rate:.2f}")
    print(f"Stuck reservations: {len(stuck)}")
    print(f"Reservation amount: {reservation_drift}")
    print("✅ Money conserved" if abs(money_drift) < 0.0001 else "❌ MONEY INCONSISTENCY DETECTED")
    print("✅ No stuck reservations" if not stuck else "❌ Stuck reservations detected")
    print("============================\n")

    return {
        "money_drift": money_drift,
        "money_drift_rate": money_drift_rate,
        "stuck_reservations": len(stuck),
        "reservation_total_drift": reservation_drift,
        "transactions_started": diff['started'],
        "transactions_completed": diff['completed'],
        "transactions_failed": diff['failed'],
        "transactions_active": diff['max_active'],
        "transactions_stuck":transactions_stuck,
        "transactions_stuck_rate":transactions_stuck_rate
    }

def run_stage(account_ids, tps, duration, scenario=None, stage_results=None):
    print(f"\n{tps} TPS for {duration}s")
    delay = 1.0 / tps
    end_time = time.time() + duration
    if scenario and stage_results is not None:
        inject_failure_after_delay(duration * 0.7, scenario, stage_results)  # fail halfway through

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

def run_experiment(count, scenario):
    print("===========")
    print(f"\nExperiment no. {count}. Starting consistency-focused experiment of type {SYNC_MODE}")
    run_sql_init()
    run_spanner_init()
    time.sleep(3)
    start_account_service()
    initial_accounts = get_accounts_full()
    account_ids = [a["id"] for a in initial_accounts]

    before = snapshot_metrics()
    stage_results = {}
    run_stage(account_ids, 5, 30,
                scenario=scenario,
                stage_results=stage_results)

    time.sleep(5)
    after = snapshot_metrics()
    stop_account_service()

    # Print failure window timeline if available
    if "failure_window" in stage_results:
            print("\n==== FAILURE WINDOW TIMELINE ====")
            for metric, series in stage_results["failure_window"].items():
                print(f"\n{metric}:")
                for ts, val in series:
                    print(f"  {time.strftime('%H:%M:%S', time.localtime(float(ts)))} → {float(val):.1f}")

    result = check_consistency(initial_accounts, before, after)
    result["type"] = SYNC_MODE
    result["experiment_num"] = count
    result["scenario"] =  scenario["name"]
    return result

if __name__ == "__main__":
    CSV_FILE = f"results/experiment_results_{SYNC_MODE}_{datetime.datetime.now()}.csv"
    
    TEST_SCENARIO = FAILURE_SCENARIOS[0]
    

    exp_count = 1
    all_results = []
    print(f"Running experiment {TEST_SCENARIO['name']}")
    for i in range(exp_count):
        result = run_experiment(i, TEST_SCENARIO)
        all_results.append(result)

    TEST_SCENARIO = FAILURE_SCENARIOS[2]
    print(f"Running experiment {TEST_SCENARIO['name']}")
    for i in range(exp_count):
        result = run_experiment(i, TEST_SCENARIO)
        all_results.append(result)

    TEST_SCENARIO = FAILURE_SCENARIOS[3]
    print(f"Running experiment {TEST_SCENARIO['name']}")
    for i in range(exp_count):
        result = run_experiment(i, TEST_SCENARIO)
        all_results.append(result)

    
    fieldnames = all_results[0].keys()
    with open(CSV_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_results)