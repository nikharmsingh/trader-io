"""
Spark Structured Streaming job for real-time market data processing.

Pipeline:
  Kafka raw.trades
    → parse JSON
    → write raw Parquet to MinIO (raw data lake)
    → 1-min tumbling window → OHLCV candles → ClickHouse
    → 5-min tumbling window → OHLCV candles → ClickHouse
    → per-symbol indicator computation → ClickHouse

Late data handling: 10-minute watermark on event_time.
Checkpointing: local /tmp/spark-checkpoints.
"""
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, BooleanType,
)
from config import config

logging.basicConfig(level=config.log_level)
logger = logging.getLogger("processing.spark_job")

TRADE_SCHEMA = StructType([
    StructField("event_time",      LongType(),    False),
    StructField("symbol",          StringType(),  False),
    StructField("trade_id",        StringType(),  False),
    StructField("price",           DoubleType(),  False),
    StructField("quantity",        DoubleType(),  False),
    StructField("buyer_order_id",  StringType(),  True),
    StructField("seller_order_id", StringType(),  True),
    StructField("is_buyer_maker",  BooleanType(), False),
    StructField("source",          StringType(),  False),
])

MINIO_PATH = f"s3a://{config.minio_bucket_raw}/trades"


def _ch_client():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=config.clickhouse_host,
        port=int(config.clickhouse_http_port),
        username=config.clickhouse_user,
        password=config.clickhouse_password,
        database=config.clickhouse_database,
    )


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName(config.spark_app_name)
        .master(config.spark_master)
        .config("spark.sql.shuffle.partitions", "12")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.checkpointLocation", config.spark_checkpoint_dir)
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{config.minio_endpoint}")
        .config("spark.hadoop.fs.s3a.access.key", config.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", config.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", config.kafka_topic_trades)
        .option("startingOffsets", config.kafka_starting_offsets)
        .option("kafka.group.id", config.kafka_consumer_group)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 50000)
        .load()
    )


def parse_trades(raw: DataFrame) -> DataFrame:
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("d"),
        F.col("partition"),
        F.col("offset"),
    ).select("d.*", "partition", "offset")

    return parsed.withColumn(
        "event_ts",
        F.to_timestamp(F.col("event_time") / 1000),
    ).withWatermark("event_ts", "10 minutes")


def write_raw_to_minio(trades: DataFrame):
    return (
        trades.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", MINIO_PATH)
        .option("checkpointLocation", f"{config.spark_checkpoint_dir}/raw")
        .partitionBy("symbol")
        .trigger(processingTime="30 seconds")
        .start()
    )


def build_candle_query(trades: DataFrame, interval_minutes: int):
    candles = (
        trades.groupBy(
            F.col("symbol"),
            F.window(F.col("event_ts"), f"{interval_minutes} minutes"),
        )
        .agg(
            F.first("price").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price").alias("close"),
            F.sum("quantity").alias("volume"),
            F.count("*").alias("trade_count"),
            (F.sum(F.col("price") * F.col("quantity")) / F.sum("quantity")).alias("vwap"),
        )
        .select(
            F.col("symbol"),
            F.lit(f"{interval_minutes}m").alias("interval"),
            (F.col("window.start").cast("long") * 1000).alias("open_time"),
            (F.col("window.end").cast("long") * 1000 - 1).alias("close_time"),
            F.col("open"), F.col("high"), F.col("low"), F.col("close"),
            F.col("volume"), F.col("trade_count"), F.col("vwap"),
        )
    )

    table = f"candles_{interval_minutes}m"
    columns = ["symbol", "interval", "open_time", "close_time",
               "open", "high", "low", "close", "volume", "trade_count", "vwap"]

    def write_candle_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        pdf = batch_df.toPandas()
        rows = [list(r) for r in pdf[columns].itertuples(index=False)]
        _ch_client().insert(table, rows, column_names=columns)
        logger.info("Candle batch %d → %s (%d rows)", batch_id, table, len(rows))

    return (
        candles.writeStream
        .foreachBatch(write_candle_batch)
        .outputMode("update")
        .trigger(processingTime=f"{interval_minutes} minutes")
        .option("checkpointLocation", f"{config.spark_checkpoint_dir}/candles_{interval_minutes}m")
        .start()
    )


def build_indicator_query(trades: DataFrame):
    # Row-frame windows are not supported in streaming; all indicator math
    # happens inside foreachBatch where batch_df is a regular DataFrame.
    trade_prices = trades.select("symbol", "event_time", "price")

    def write_indicator_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        import pandas as pd
        from pyspark.sql.window import Window as W
        from indicators import rsi, ema, macd as compute_macd

        window7  = W.partitionBy("symbol").orderBy("event_time").rowsBetween(-6, 0)
        window14 = W.partitionBy("symbol").orderBy("event_time").rowsBetween(-13, 0)
        window50 = W.partitionBy("symbol").orderBy("event_time").rowsBetween(-49, 0)
        enriched = (
            batch_df
            .withColumn("sma_7",  F.avg("price").over(window7))
            .withColumn("sma_14", F.avg("price").over(window14))
            .withColumn("sma_50", F.avg("price").over(window50))
        )

        result_rows = []
        pdf = enriched.toPandas()
        for symbol, group in pdf.groupby("symbol"):
            g = group.sort_values("event_time").copy()
            prices = g["price"]
            g["ema_12"] = ema(prices, 12)
            g["ema_26"] = ema(prices, 26)
            g["rsi_14"] = rsi(prices, 14)
            macd_line, signal_line, histogram = compute_macd(prices)
            g["macd"] = macd_line
            g["macd_signal"] = signal_line
            g["macd_histogram"] = histogram
            result_rows.append(g)

        if not result_rows:
            return

        out = pd.concat(result_rows)
        cols = ["symbol", "event_time", "sma_7", "sma_14", "sma_50",
                "ema_12", "ema_26", "rsi_14", "macd", "macd_signal", "macd_histogram"]
        rows = [list(r) for r in out[cols].itertuples(index=False)]
        _ch_client().insert("indicators", rows, column_names=cols)
        logger.info("Indicator batch %d written (%d rows)", batch_id, len(rows))

    return (
        trade_prices.writeStream
        .foreachBatch(write_indicator_batch)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", f"{config.spark_checkpoint_dir}/indicators")
        .start()
    )


def log_stream_metrics(spark):
    import threading, time

    def _log():
        while True:
            time.sleep(30)
            for q in spark.streams.active:
                prog = q.lastProgress
                if prog:
                    logger.info(
                        "stream_metrics | query=%s | input_rows_per_sec=%.1f | "
                        "processed_rows_per_sec=%.1f | batch_duration_ms=%d | "
                        "num_input_rows=%d",
                        q.name or q.id,
                        prog.get("inputRowsPerSecond", 0),
                        prog.get("processedRowsPerSecond", 0),
                        prog.get("batchDuration", 0),
                        prog.get("numInputRows", 0),
                    )

    threading.Thread(target=_log, daemon=True).start()


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark Structured Streaming job starting...")

    raw        = read_kafka_stream(spark)
    trades     = parse_trades(raw)

    write_raw_to_minio(trades)
    build_candle_query(trades, 1)
    build_candle_query(trades, 5)
    build_indicator_query(trades)

    log_stream_metrics(spark)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
