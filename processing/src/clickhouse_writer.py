"""
Writes processed data to ClickHouse via HTTP interface.
Uses batch inserts for efficiency; idempotent via ReplacingMergeTree dedup.
"""
import logging
from typing import List, Dict, Any

import clickhouse_connect

from config import config

logger = logging.getLogger(__name__)


def get_client():
    return clickhouse_connect.get_client(
        host=config.clickhouse_host,
        port=int(config.clickhouse_http_port),
        username=config.clickhouse_user,
        password=config.clickhouse_password,
        database=config.clickhouse_database,
    )


def insert_trades(trades: List[Dict[str, Any]]):
    if not trades:
        return
    client = get_client()
    rows = [
        [
            t["symbol"],
            t["trade_id"],
            t["price"],
            t["quantity"],
            t["is_buyer_maker"],
            t["source"],
            t["event_time"],
        ]
        for t in trades
    ]
    client.insert(
        "trades",
        rows,
        column_names=["symbol", "trade_id", "price", "quantity", "is_buyer_maker", "source", "event_time"],
    )
    logger.debug("Inserted %d trades", len(trades))


def insert_candles(candles: List[Dict[str, Any]], interval: str):
    if not candles:
        return
    table = f"candles_{interval}"
    client = get_client()
    rows = [
        [
            c["symbol"],
            c["interval"],
            c["open_time"],
            c["close_time"],
            c["open"],
            c["high"],
            c["low"],
            c["close"],
            c["volume"],
            c["trade_count"],
            c.get("vwap"),
        ]
        for c in candles
    ]
    client.insert(
        table,
        rows,
        column_names=[
            "symbol", "interval", "open_time", "close_time",
            "open", "high", "low", "close", "volume", "trade_count", "vwap",
        ],
    )
    logger.debug("Inserted %d candles into %s", len(candles), table)


def insert_indicators(indicators: List[Dict[str, Any]]):
    if not indicators:
        return
    client = get_client()
    rows = [
        [
            i["symbol"],
            i["event_time"],
            i.get("sma_7"),
            i.get("sma_14"),
            i.get("sma_50"),
            i.get("ema_12"),
            i.get("ema_26"),
            i.get("rsi_14"),
            i.get("macd"),
            i.get("macd_signal"),
            i.get("macd_histogram"),
        ]
        for i in indicators
    ]
    client.insert(
        "indicators",
        rows,
        column_names=[
            "symbol", "event_time",
            "sma_7", "sma_14", "sma_50",
            "ema_12", "ema_26",
            "rsi_14", "macd", "macd_signal", "macd_histogram",
        ],
    )
    logger.debug("Inserted %d indicator rows", len(indicators))
