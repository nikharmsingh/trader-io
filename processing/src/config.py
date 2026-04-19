import os


class Config:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    kafka_topic_trades: str = os.getenv("KAFKA_TOPIC_TRADES", "raw.trades")
    kafka_topic_candles: str = os.getenv("KAFKA_TOPIC_CANDLES", "processed.candles")
    kafka_topic_indicators: str = os.getenv("KAFKA_TOPIC_INDICATORS", "processed.indicators")
    kafka_consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "spark-processor")
    kafka_starting_offsets: str = os.getenv("KAFKA_STARTING_OFFSETS", "latest")

    clickhouse_host: str = os.getenv("CLICKHOUSE_HOST", "localhost")
    clickhouse_http_port: str = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
    clickhouse_user: str = os.getenv("CLICKHOUSE_USER", "default")
    clickhouse_password: str = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse123")
    clickhouse_database: str = os.getenv("CLICKHOUSE_DATABASE", "trader_io")

    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    minio_bucket_raw: str = os.getenv("MINIO_BUCKET_RAW", "raw-market-data")

    candle_intervals: list[int] = [1, 5, 15]  # minutes
    indicator_window_size: int = 200           # ticks to buffer for indicators

    spark_app_name: str = "trader-io-processor"
    spark_master: str = "local[*]"
    spark_checkpoint_dir: str = "/tmp/spark-checkpoints"

    log_level: str = os.getenv("LOG_LEVEL", "INFO")


config = Config()
