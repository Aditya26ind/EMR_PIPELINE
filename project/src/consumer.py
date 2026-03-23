"""Simple Kafka consumer that prints records from the users topic."""

import json
from kafka import KafkaConsumer

TOPIC = "users"
BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "users-printer"


def run() -> None:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )

    print(f"Consuming from topic '{TOPIC}'...")
    try:
        for message in consumer:
            print(
                f"offset={message.offset} key={message.key} value={message.value}"
            )
    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
