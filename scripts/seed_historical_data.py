"""
Seed script: generates 7 days of mock historical OHLCV data and inserts
directly into ClickHouse. Run once after `make up` to have data visible
in the dashboard immediately.

Usage: python scripts/seed_historical_data.py
"""
import os
import sys
import time
import random
import uuid
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../processing/src"))

import clickhouse_connect

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_HTTP_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse123")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "trader_io")

SYMBOLS_INIT = {
    "BTCUSDT":  65000.0,
    "ETHUSDT":   3400.0,
    "SOLUSDT":   175.0,
    "BNBUSDT":   580.0,
    "ADAUSDT":     0.58,
}
DAYS = 7
INTERVAL_MINUTES = 1


def gbm_price(price: float, volatility: float = 0.0008) -> float:
    return price * (1 + random.gauss(0, volatility))


def generate_1m_candles(symbol: str, start_price: float, days: int) -> list:
    rows = []
    price = start_price
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days)
    ts = int(start.timestamp() * 1000)
    step_ms = 60 * 1000  # 1 minute

    total_candles = days * 24 * 60
    for _ in range(total_candles):
        open_p = price
        prices = [open_p]
        for _ in range(10):
            prices.append(gbm_price(prices[-1]))
        high_p  = max(prices)
        low_p   = min(prices)
        close_p = prices[-1]
        volume  = random.lognormvariate(2, 1) * 0.5
        rows.append([
            symbol, "1m",
            ts, ts + step_ms - 1,
            open_p, high_p, low_p, close_p,
            volume,
            random.randint(10, 500),
            (open_p + close_p) / 2,
        ])
        price = close_p
        ts += step_ms
    return rows


def main():
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_HTTP_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )
    print(f"Connected to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}")

    columns = ["symbol", "interval", "open_time", "close_time",
               "open", "high", "low", "close", "volume", "trade_count", "vwap"]

    for symbol, init_price in SYMBOLS_INIT.items():
        print(f"Seeding {symbol} ({DAYS} days of 1m candles)...")
        rows = generate_1m_candles(symbol, init_price, DAYS)
        client.insert("candles_1m", rows, column_names=columns)

        # Also seed a few raw trades for the latest_prices_mv
        trade_rows = [
            [symbol, str(uuid.uuid4()), rows[-1][7], 0.1, 0, "MOCK", rows[-1][2]]
        ]
        client.insert(
            "trades",
            trade_rows,
            column_names=["symbol", "trade_id", "price", "quantity", "is_buyer_maker", "source", "event_time"],
        )
        print(f"  ✓ {len(rows)} candles inserted for {symbol}")

    # Aggregate 1m candles → 5m and 15m
    for interval_minutes, table in [(5, "candles_5m"), (15, "candles_15m")]:
        print(f"Aggregating 1m → {interval_minutes}m candles...")
        client.command(f"""
            INSERT INTO {table}
                (symbol, interval, open_time, close_time, open, high, low, close,
                 volume, trade_count, vwap)
            SELECT
                symbol, interval, open_time, close_time,
                open, high, low, close, vol, trade_count,
                if(vol > 0, total_pv / vol, NULL) AS vwap
            FROM (
                SELECT
                    symbol,
                    '{interval_minutes}m' AS interval,
                    toUnixTimestamp(toStartOfInterval(
                        toDateTime(open_time / 1000),
                        INTERVAL {interval_minutes} MINUTE
                    )) * 1000                                AS open_time,
                    toUnixTimestamp(toStartOfInterval(
                        toDateTime(open_time / 1000),
                        INTERVAL {interval_minutes} MINUTE
                    )) * 1000 + {interval_minutes * 60 * 1000} - 1 AS close_time,
                    argMin(open,  open_time)                 AS open,
                    max(high)                                AS high,
                    min(low)                                 AS low,
                    argMax(close, close_time)                AS close,
                    sum(volume)                              AS vol,
                    sum(trade_count)                         AS trade_count,
                    sum(volume * vwap)                       AS total_pv
                FROM candles_1m
                GROUP BY symbol, interval, open_time, close_time
            )
        """)
        rows_inserted = client.query(f"SELECT count() FROM {table}").result_rows[0][0]
        print(f"  ✓ {rows_inserted:,} rows in {table}")

    # Trigger daily aggregation for seeded data
    print("Running daily aggregation on seeded data...")
    for day_offset in range(DAYS):
        date = (datetime.now(timezone.utc) - timedelta(days=day_offset)).strftime("%Y-%m-%d")
        client.command(f"""
            INSERT INTO daily_ohlcv
                (symbol, date, open, high, low, close, volume,
                 trade_count, avg_price, price_change, price_change_pct)
            SELECT symbol,
                   toDate(toDateTime(open_time / 1000)) AS date,
                   argMin(open, open_time), max(high), min(low),
                   argMax(close, close_time), sum(volume), sum(trade_count),
                   avg(vwap),
                   argMax(close, close_time) - argMin(open, open_time),
                   (argMax(close, close_time) - argMin(open, open_time))
                       / argMin(open, open_time) * 100
            FROM candles_1m
            WHERE toDate(toDateTime(open_time / 1000)) = toDate('{date}')
            GROUP BY symbol, toDate(toDateTime(open_time / 1000))
        """)
    # Seed indicators from candles using ClickHouse window functions
    print("Computing indicators from seeded candles...")
    client.command("""
        INSERT INTO indicators
            (symbol, event_time, sma_7, sma_14, sma_50, ema_12, ema_26)
        SELECT
            symbol,
            open_time AS event_time,
            avg(close) OVER (
                PARTITION BY symbol ORDER BY open_time
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) AS sma_7,
            avg(close) OVER (
                PARTITION BY symbol ORDER BY open_time
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) AS sma_14,
            avg(close) OVER (
                PARTITION BY symbol ORDER BY open_time
                ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
            ) AS sma_50,
            avg(close) OVER (
                PARTITION BY symbol ORDER BY open_time
                ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
            ) AS ema_12,
            avg(close) OVER (
                PARTITION BY symbol ORDER BY open_time
                ROWS BETWEEN 25 PRECEDING AND CURRENT ROW
            ) AS ema_26
        FROM candles_1m
        ORDER BY symbol, open_time
    """)
    print("  ✓ Indicators seeded")
    print("Seed complete. Dashboard should now show historical data.")


if __name__ == "__main__":
    main()
