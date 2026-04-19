import os
from dataclasses import dataclass, field
from typing import List


@dataclass
class Config:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    kafka_topic_trades: str = os.getenv("KAFKA_TOPIC_TRADES", "raw.trades")
    data_source: str = os.getenv("DATA_SOURCE", "mock")
    symbols: List[str] = field(default_factory=lambda: os.getenv(
        "SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,ADAUSDT"
    ).split(","))
    mock_tick_interval_ms: int = int(os.getenv("MOCK_TICK_INTERVAL_MS", "500"))
    binance_ws_url: str = os.getenv("BINANCE_WS_URL", "wss://stream.binance.com:9443/ws")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    # Kafka producer tuning
    kafka_linger_ms: int = 5
    kafka_batch_size: int = 65536
    kafka_compression: str = "snappy"
    kafka_acks: str = "all"
    kafka_retries: int = 5
    kafka_retry_backoff_ms: int = 500


config = Config()
