"""Create Kafka topics programmatically (Python alternative to make topics)."""
import os
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

TOPICS = [
    NewTopic("raw.trades",           num_partitions=6, replication_factor=1),
    NewTopic("processed.candles",    num_partitions=6, replication_factor=1),
    NewTopic("processed.indicators", num_partitions=6, replication_factor=1),
    NewTopic("alerts",               num_partitions=1, replication_factor=1),
]


def main():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    futures = admin.create_topics(TOPICS)
    for topic, future in futures.items():
        try:
            future.result()
            print(f"  ✓ Created: {topic}")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"  ~ Exists:  {topic}")
            else:
                print(f"  ✗ Failed:  {topic} — {e}")


if __name__ == "__main__":
    print(f"Creating Kafka topics on {BOOTSTRAP}")
    main()
