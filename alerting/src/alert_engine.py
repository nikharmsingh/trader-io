"""
Alert engine: consumes processed.indicators topic, evaluates rules,
writes triggered alerts to ClickHouse + alerts Kafka topic,
and dispatches notifications via Telegram/Discord.

Deduplication: 60-second cooldown per (symbol, alert_type) pair.
"""
import json
import logging
import os
import signal
import sys
import time
from typing import Dict, Optional, Tuple

import clickhouse_connect
from confluent_kafka import Consumer, Producer, KafkaException

from rules import evaluate, Alert
from notifiers.telegram import TelegramNotifier
from notifiers.discord import DiscordNotifier

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}',
)
logger = logging.getLogger("alerting.engine")

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_IN      = os.getenv("KAFKA_TOPIC_INDICATORS", "processed.indicators")
TOPIC_ALERTS  = os.getenv("KAFKA_TOPIC_ALERTS", "alerts")
COOLDOWN_S    = 60  # min seconds between same (symbol, type) alerts


def get_ch_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse123"),
        database=os.getenv("CLICKHOUSE_DATABASE", "trader_io"),
    )


def persist_alert(client, alert: Alert):
    client.insert(
        "alerts",
        [[
            alert.symbol, alert.alert_type, alert.message,
            alert.price, alert.indicator_value, alert.triggered_at,
        ]],
        column_names=["symbol", "alert_type", "message", "price", "indicator_value", "triggered_at"],
    )


class AlertEngine:
    def __init__(self):
        self._consumer = Consumer({
            "bootstrap.servers": KAFKA_SERVERS,
            "group.id": "alert-engine",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        })
        self._producer = Producer({"bootstrap.servers": KAFKA_SERVERS})
        self._ch = get_ch_client()
        self._telegram = TelegramNotifier()
        self._discord = DiscordNotifier()
        self._cooldowns: Dict[Tuple[str, str], float] = {}
        self._prev_records: Dict[str, dict] = {}

    def _is_cooled_down(self, symbol: str, alert_type: str) -> bool:
        key = (symbol, alert_type)
        last = self._cooldowns.get(key, 0)
        return (time.time() - last) >= COOLDOWN_S

    def _set_cooldown(self, symbol: str, alert_type: str):
        self._cooldowns[(symbol, alert_type)] = time.time()

    def _dispatch(self, alert: Alert):
        if not self._is_cooled_down(alert.symbol, alert.alert_type):
            logger.debug("Alert cooled down: %s %s", alert.symbol, alert.alert_type)
            return
        self._set_cooldown(alert.symbol, alert.alert_type)

        try:
            persist_alert(self._ch, alert)
        except Exception as e:
            logger.error("Failed to persist alert: %s", e)

        self._producer.produce(
            topic=TOPIC_ALERTS,
            key=alert.symbol.encode(),
            value=json.dumps({
                "symbol": alert.symbol,
                "alert_type": alert.alert_type,
                "message": alert.message,
                "price": alert.price,
                "indicator_value": alert.indicator_value,
                "triggered_at": alert.triggered_at,
            }).encode(),
        )
        self._producer.poll(0)

        self._telegram.send(self._telegram.format_alert(alert))
        self._discord.send(alert)

        logger.info("Alert dispatched | %s | %s | %s", alert.symbol, alert.alert_type, alert.message)

    def run(self):
        self._consumer.subscribe([TOPIC_IN])
        logger.info("Alert engine started | topic=%s", TOPIC_IN)

        def shutdown(sig, frame):
            logger.info("Shutting down alert engine...")
            self._consumer.close()
            self._producer.flush(5)
            sys.exit(0)

        signal.signal(signal.SIGTERM, shutdown)
        signal.signal(signal.SIGINT, shutdown)

        while True:
            msg = self._consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.warning("Consumer error: %s", msg.error())
                continue
            try:
                record = json.loads(msg.value().decode())
                symbol = record.get("symbol")
                prev = self._prev_records.get(symbol)
                alerts = evaluate(record, prev)
                for alert in alerts:
                    self._dispatch(alert)
                self._prev_records[symbol] = record
            except Exception as e:
                logger.error("Error processing message: %s", e)


if __name__ == "__main__":
    engine = AlertEngine()
    engine.run()
