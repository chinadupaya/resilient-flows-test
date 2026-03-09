import time
import json
import logging
from kafka import KafkaProducer
from google.cloud.spanner_v1 import Client

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("spanner-cdc")

KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = "transaction.public.transactions"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
)

client = Client(project="test-project")
instance = client.instance("test-instance")
database = instance.database("test-database")

last_seen = None


def poll():
    global last_seen

    with database.snapshot() as snapshot:

        if last_seen:
            sql = """
            SELECT *
            FROM transactions
            WHERE updated_at > @ts
            ORDER BY updated_at
            """
            params = {"ts": last_seen}

            rows = snapshot.execute_sql(sql, params=params)

        else:
            rows = snapshot.execute_sql(
                "SELECT * FROM transactions ORDER BY updated_at"
            )

        iterator = iter(rows)
        first_row = next(iterator, None)

        if first_row is None:
            return

        columns = [c.name for c in rows.metadata.row_type.fields]

        def publish(row):

            record = dict(zip(columns, row))

            event = {
                "source": "spanner",
                "table": "transactions",
                "op": "u",
                "after": record
            }

            producer.send("transaction.public.transactions", event)

            return record

        record = publish(first_row)
        last_seen = record["updated_at"]

        for row in iterator:
            record = publish(row)
            last_seen = record["updated_at"]

while True:
    try:
        poll()
        time.sleep(1)
    except Exception:
        log.exception("Poller failure")
        time.sleep(5)