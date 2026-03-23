"""Simple Kafka producer that streams rows from data/users.csv to a topic."""

import csv
import json
import time
from pathlib import Path

from kafka import KafkaProducer

TOPIC = "users"
BOOTSTRAP_SERVERS = "localhost:9092"
DATA_FILE = Path(__file__).resolve().parent.parent / "data" / "users.csv"


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8") if v else None,
        linger_ms=50,
    )


def stream_users(delay_seconds: float = 0.5) -> None:
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"Missing dataset: {DATA_FILE}")

    producer = build_producer()
    print(f"Producing records from {DATA_FILE} to topic '{TOPIC}'...")

    with DATA_FILE.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row["id"]
            producer.send(TOPIC, key=key, value=row)
            print(f"sent key={key} value={row}")
            time.sleep(delay_seconds)

    producer.flush()
    producer.close()
    print("Done.")


if __name__ == "__main__":
    try:
        stream_users()
    except KeyboardInterrupt:
        print("Stopped by user.")
