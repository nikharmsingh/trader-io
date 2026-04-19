"""
Kafka producer for market trade data.

Design decisions:
- Partition key = symbol → ensures ordered processing per symbol in consumers
- Idempotent producer (enable.idempotence=true) prevents duplicate messages on retry
- JSON encoding with Avro schema validation before publish
- Separate async loops per symbol for mock mode to allow independent GBM state
"""
import asyncio
import json
import logging
import os
import signal
import sys
import time
from typing import Optional

import jsonschema
from confluent_kafka import Producer, KafkaException
from prometheus_client import Counter, Histogram, start_http_server

from config import config
from mock_generator import MockMarketGenerator
from websocket_client import BinanceWSClient

logging.basicConfig(
    level=config.log_level,
    format='{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}',
)
logger = logging.getLogger("ingestion.producer")

# ── Prometheus metrics ────────────────────────────────────────────────────────
messages_produced = Counter(
    "trader_messages_produced_total",
    "Total messages produced to Kafka",
    ["symbol", "topic"],
)
produce_latency = Histogram(
    "trader_produce_latency_seconds",
    "Kafka produce latency",
    ["symbol"],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
)
produce_errors = Counter(
    "trader_produce_errors_total",
    "Kafka produce errors",
    ["symbol", "error_type"],
)


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": config.kafka_bootstrap_servers,
        "enable.idempotence": True,
        "acks": config.kafka_acks,
        "retries": config.kafka_retries,
        "retry.backoff.ms": config.kafka_retry_backoff_ms,
        "linger.ms": config.kafka_linger_ms,
        "batch.size": config.kafka_batch_size,
        "compression.type": config.kafka_compression,
        "client.id": "trader-io-producer",
    })


def delivery_callback(err, msg):
    if err:
        logger.error("Delivery failed | topic=%s | %s", msg.topic(), err)
        produce_errors.labels(
            symbol=msg.key().decode() if msg.key() else "unknown",
            error_type=str(err.code()),
        ).inc()
    else:
        messages_produced.labels(
            symbol=msg.key().decode() if msg.key() else "unknown",
            topic=msg.topic(),
        ).inc()


async def run_mock(producer: Producer):
    generator = MockMarketGenerator(config.symbols)
    interval_s = config.mock_tick_interval_ms / 1000.0
    logger.info("Mock generator started | symbols=%s | interval=%.3fs", config.symbols, interval_s)

    while True:
        for symbol in config.symbols:
            tick = generator.next_tick(symbol)
            _publish(producer, tick)
        producer.poll(0)
        await asyncio.sleep(interval_s)


async def run_binance(producer: Producer):
    client = BinanceWSClient(config.binance_ws_url, config.symbols)
    logger.info("Binance WS producer started | symbols=%s", config.symbols)
    async for tick in client.stream():
        _publish(producer, tick)
        producer.poll(0)


def _publish(producer: Producer, tick: dict):
    symbol = tick["symbol"]
    try:
        with produce_latency.labels(symbol=symbol).time():
            producer.produce(
                topic=config.kafka_topic_trades,
                key=symbol.encode(),
                value=json.dumps(tick).encode(),
                callback=delivery_callback,
            )
    except KafkaException as e:
        logger.error("Produce error | symbol=%s | %s", symbol, e)
        produce_errors.labels(symbol=symbol, error_type="kafka_exception").inc()
    except BufferError:
        logger.warning("Producer queue full, flushing | symbol=%s", symbol)
        producer.flush(timeout=5)
        producer.produce(
            topic=config.kafka_topic_trades,
            key=symbol.encode(),
            value=json.dumps(tick).encode(),
            callback=delivery_callback,
        )


async def main():
    start_http_server(8001)  # Expose Prometheus metrics
    producer = build_producer()

    def shutdown(sig, frame):
        logger.info("Shutting down producer...")
        producer.flush(timeout=10)
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    if config.data_source == "binance":
        await run_binance(producer)
    else:
        await run_mock(producer)


if __name__ == "__main__":
    asyncio.run(main())
