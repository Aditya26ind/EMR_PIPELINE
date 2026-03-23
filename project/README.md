# Simple Kafka Streaming Demo

Tiny, local-only pipeline that streams rows from `data/users.csv` into Kafka and prints them back out from a consumer. Designed to show the structure of a real-time data app without extra bells and whistles.

## What it does
- A KRaft (no ZooKeeper) Kafka broker runs via Docker Compose.
- `src/producer.py` reads `data/users.csv` and publishes JSON messages to the `users` topic.
- `src/consumer.py` subscribes to `users` and prints each record as it arrives.

## Prerequisites
- Docker Desktop running
- Python 3.10+ and `venv`

## Run it
```bash
# 1) Start Kafka
docker compose up -d

# 2) Install Python deps
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 3) Start the consumer (in its own shell)
python src/consumer.py

# 4) Stream the sample data (new shell)
python src/producer.py
```

You should see the consumer printing messages like `offset=0 key=1 value={'id': '1', ...}` as the producer drips them out.

## Project layout
```
docker-compose.yml      # Kafka (KRaft) broker
requirements.txt        # Python deps (kafka-python)
data/users.csv          # Local sample dataset
src/producer.py         # Push CSV rows to Kafka
src/consumer.py         # Read from Kafka and print
```

## Stop & clean up
```bash
docker compose down -v
```
