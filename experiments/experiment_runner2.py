import requests
import random
import time
import subprocess
import threading
import datetime
import csv
import string
import os

PROMETHEUS_URL = "http://localhost:9095"
ACCOUNT_SERVICE_URL = "http://localhost:7070/api/v1/accounts"
TRANSACTION_SERVICE_URL = "http://localhost:9090/api/v1/transactions"

SYNC_MODE = "SYNC" # [SYNC, ASYNC, SYNCV2]

FAILURE_STAGE_INDEX = 2
FAILURE_DURATION = 5
RECOVERY_TIME = 10   # time for Restate replay / service warmup
PAUSE_TRAFFIC_DURING_FAILURE = True
stop_event = threading.Event()

traffic_pause_event = threading.Event()
traffic_pause_event.clear()

FAILURE_SCENARIOS = [
    {
        "name": "Account DB failure",
        "failure_cmd": ["docker", "stop", "postgres-account"],
        "recovery_cmd": ["docker", "start", "postgres-account"],
    },
    {
        "name": "Kafka failure",
        "failure_cmd": ["docker", "stop", "kafka"],
        "recovery_cmd": ["docker", "start", "kafka"],
    },
    {
        "name": "Account service crash (local)"
    },
    {
        "name": "Transaction service crash (local) - after reserve",
        "chaos_point": "AFTER_RESERVE",
    },
    {
        "name": "Transaction service crash (local) - after compliance",
        "chaos_point": "AFTER_COMPLIANCE",
    },
    {
        "name": "Transaction service crash (local) - after commit",
        "chaos_point": "AFTER_COMMIT",
    }
]

ACCOUNT_SERVICE_PROCESS = None
TRANSACTION_SERVICE_PROCESS = None
def wait_for_service(service):
    print(f" Waiting for {service} service...")
    for _ in range(60):
        try:
            r = None
            if service=='account':
                r = requests.get("http://localhost:7070/api/v1/accounts")
            elif service=='transaction':
                r = requests.get("http://localhost:9090/api/v1/transactions")
            if r.status_code == 200:
                print(f" {service} service ready")
                return
        except:
            pass
        time.sleep(1)
    raise RuntimeError("{service} service did not start")
def start_service(service, chaos_point=None):
    global ACCOUNT_SERVICE_PROCESS
    global TRANSACTION_SERVICE_PROCESS
    print(f" Starting {service} service...")

    log_file = open(f"{service}_service.log", "w")
    if service == 'account':
        ACCOUNT_SERVICE_PROCESS = subprocess.Popen(
            ["mvn", "spring-boot:run"],
            cwd="../account-service",
            stdout=log_file,
            stderr=log_file,
            stdin=subprocess.DEVNULL
        )
    elif service == 'transaction':
        txn_env = os.environ.copy()
        if chaos_point:
            txn_env["CHAOS_POINT"] = chaos_point
            print(f" Transaction service starting with CHAOS_POINT={chaos_point}")
        TRANSACTION_SERVICE_PROCESS = subprocess.Popen(
            ["mvn", "spring-boot:run"],
            cwd="../transaction-service",
            stdout=log_file,
            stderr=log_file,
            stdin=subprocess.DEVNULL,
            env=txn_env
        )
    wait_for_service(service)
    
def stop_service(service):
    global ACCOUNT_SERVICE_PROCESS
    global TRANSACTION_SERVICE_PROCESS
    if ACCOUNT_SERVICE_PROCESS and service == 'account':
        print(" Stopping account service...")
        ACCOUNT_SERVICE_PROCESS.terminate()
        try:
            ACCOUNT_SERVICE_PROCESS.wait(timeout=10)
        except subprocess.TimeoutExpired:
            ACCOUNT_SERVICE_PROCESS.kill()
        ACCOUNT_SERVICE_PROCESS = None
    elif TRANSACTION_SERVICE_PROCESS and service == 'transaction':
        print(" Stopping transaction service...")
        TRANSACTION_SERVICE_PROCESS.terminate()
        try:
            TRANSACTION_SERVICE_PROCESS.wait(timeout=10)
        except subprocess.TimeoutExpired:
            TRANSACTION_SERVICE_PROCESS.kill()
        TRANSACTION_SERVICE_PROCESS = None

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

def get_experiment_metrics(duration_s):
    return {
        "started": query_prometheus(f"increase(transactions_started_total[{duration_s}s])"),
        "completed": query_prometheus(f"increase(transactions_completed_total[{duration_s}s])"),
        "failed": query_prometheus(f"increase(transactions_failed_total[{duration_s}s])"),
        "active": query_prometheus("transactions_reservations_active"),
        "stuck": query_prometheus("transactions_stuck"),
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
        global RECOVERY_TIME
        time.sleep(delay)
        failure_start = time.time()
        print(f"\n Injecting: {scenario['name']}")

        # Pause traffic
        print(" Pausing traffic")
        traffic_pause_event.set()

        # inject failure
        if scenario["name"] == "Account service crash (local)":
            stop_service('account')
        elif "Transaction service crash (local)" in scenario["name"]:
            stop_service('transaction')
        else:
            subprocess.run(scenario["failure_cmd"])

        time.sleep(FAILURE_DURATION)

        print(f"\n Recovering: {scenario['name']}")
        # recovery
        if scenario["name"] == "Account service crash (local)":
            start_service('account')
        elif "Transaction service crash (local)" in scenario["name"]:
            start_service('transaction', chaos_point=scenario.get("chaos_point"))
        else:
            subprocess.run(scenario["recovery_cmd"])

        print(f" Allowing system recovery for {RECOVERY_TIME}s...")
        time.sleep(RECOVERY_TIME)
        
        # Resume
        print(" Resuming traffic")
        traffic_pause_event.clear()

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

def check_consistency(initial_accounts, before_time, after_time):
    duration = int(after_time - before_time)
    diff = get_experiment_metrics(duration)
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
    transactions_stuck = diff['stuck']
    transactions_stuck_rate = transactions_stuck / diff['started']

    if stuck:
        print("\nStuck reservation details:")
        for a in stuck:
            print(f"  Account {a['id']}: reserved={a['reservedAmount']}, balance={a['balance']}")

    print("\n==== PROMETHEUS METRICS ====")
    print(f"Transactions started:   {diff['started']}")
    print(f"Transactions completed: {diff['completed']}")
    print(f"Transactions failed:    {diff['failed']}")
    print(f"Current active:         {diff['active']}")

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
        "transactions_active": diff['active'],
        "transactions_stuck":diff['stuck'],
        "transactions_stuck_rate":transactions_stuck_rate
    }

def run_stage(account_ids, tps, duration, scenario=None, stage_results=None):
    print(f"\n{tps} TPS for {duration}s")
    traffic_pause_event.clear()

    delay = 1.0 / tps
    end_time = time.time() + duration
    if scenario and stage_results is not None:
        inject_failure_after_delay(duration * 0.3, scenario, stage_results)  # fail halfway through

    while time.time() < end_time and not stop_event.is_set():
        if traffic_pause_event.is_set():
            time.sleep(0.5)
            continue

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

    start_service('account')
    start_service('transaction')

    initial_accounts = get_accounts_full()
    account_ids = [a["id"] for a in initial_accounts]

    before_time = time.time()

    stage_results = {}
    run_stage(account_ids, 5, 60,
                scenario=scenario,
                stage_results=stage_results)

    time.sleep(5)
    after_time = time.time()

    duration = int(after_time - before_time)

    # Print failure window timeline if available
    if "failure_window" in stage_results:
            print("\n==== FAILURE WINDOW TIMELINE ====")
            for metric, series in stage_results["failure_window"].items():
                print(f"\n{metric}:")
                for ts, val in series:
                    print(f"  {time.strftime('%H:%M:%S', time.localtime(float(ts)))} → {float(val):.1f}")

    result = check_consistency(initial_accounts, before_time, after_time)
    result["type"] = SYNC_MODE
    result["experiment_num"] = count
    result["scenario"] =  scenario["name"]

    stop_service('account')
    stop_service('transaction')
    
    return result

if __name__ == "__main__":
    CSV_FILE = f"results/experiment_results_{SYNC_MODE}_{datetime.datetime.now()}.csv"
    
    exp_count = 10
    all_results = []
    # NEW_FAILURE_SCENARIOS = FAILURE_SCENARIOS[3:]
    for scenario in FAILURE_SCENARIOS:
        TEST_SCENARIO = scenario
    
        print(f"Running experiment {TEST_SCENARIO['name']}")
        for i in range(exp_count):
            result = run_experiment(i, TEST_SCENARIO)
            all_results.append(result)
    
    fieldnames = all_results[0].keys()
    with open(CSV_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_results)