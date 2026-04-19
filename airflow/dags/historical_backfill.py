"""
Historical backfill DAG.
Fetches historical klines from Binance REST API and loads into ClickHouse.
Parameterized: trigger with { "symbol": "BTCUSDT", "start_date": "2024-01-01", "end_date": "2024-03-01" }
"""
import time
from datetime import datetime, timedelta
from typing import List, Dict

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import sys

sys.path.insert(0, "/opt/airflow/plugins")
from clickhouse_hook import ClickHouseHook

DEFAULT_ARGS = {
    "owner": "trader-io",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
BATCH_SIZE = 1000


dag = DAG(
    dag_id="historical_backfill",
    default_args=DEFAULT_ARGS,
    description="Backfill historical OHLCV from Binance REST API",
    schedule=None,  # triggered manually or via API
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["trader-io", "backfill"],
    params={
        "symbol": "BTCUSDT",
        "interval": "1m",
        "start_date": "2024-01-01",
        "end_date": "2024-01-07",
    },
)


def fetch_klines(symbol: str, interval: str, start_ms: int, end_ms: int) -> List[Dict]:
    rows = []
    current = start_ms
    while current < end_ms:
        resp = requests.get(
            BINANCE_KLINES_URL,
            params={
                "symbol": symbol,
                "interval": interval,
                "startTime": current,
                "endTime": end_ms,
                "limit": BATCH_SIZE,
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        for k in data:
            rows.append({
                "symbol": symbol,
                "interval": interval,
                "open_time": k[0],
                "close_time": k[6],
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
                "trade_count": int(k[8]),
                "vwap": float(k[5]) and float(k[7]) / float(k[5]) if float(k[5]) else None,
            })
        current = data[-1][6] + 1
        time.sleep(0.1)  # Binance rate limit courtesy sleep
    return rows


def backfill_klines(**context):
    params = context["params"]
    symbol = params["symbol"]
    interval = params["interval"]
    start_ms = int(datetime.fromisoformat(params["start_date"]).timestamp() * 1000)
    end_ms = int(datetime.fromisoformat(params["end_date"]).timestamp() * 1000)

    print(f"Backfilling {symbol} {interval} from {params['start_date']} to {params['end_date']}")
    klines = fetch_klines(symbol, interval, start_ms, end_ms)
    print(f"Fetched {len(klines)} klines")

    if not klines:
        print("No data fetched — Binance may not have data for this range")
        return

    table = f"candles_{interval.replace('m', 'm')}"
    hook = ClickHouseHook()
    rows = [
        [k["symbol"], k["interval"], k["open_time"], k["close_time"],
         k["open"], k["high"], k["low"], k["close"], k["volume"],
         k["trade_count"], k["vwap"]]
        for k in klines
    ]
    hook.insert(
        table,
        rows,
        ["symbol", "interval", "open_time", "close_time", "open", "high", "low", "close", "volume", "trade_count", "vwap"],
    )
    print(f"Inserted {len(rows)} rows into {table}")


def backfill_daily_from_candles(**context):
    params = context["params"]
    hook = ClickHouseHook()
    start = params["start_date"]
    end = params["end_date"]
    hook.execute(f"""
        INSERT INTO daily_ohlcv
            (symbol, date, open, high, low, close, volume,
             trade_count, avg_price, price_change, price_change_pct)
        SELECT
            symbol,
            toDate(toDateTime(open_time / 1000)) AS date,
            argMin(open, open_time), max(high), min(low), argMax(close, close_time),
            sum(volume), sum(trade_count), avg(vwap),
            argMax(close, close_time) - argMin(open, open_time),
            (argMax(close, close_time) - argMin(open, open_time)) / argMin(open, open_time) * 100
        FROM candles_1m
        WHERE symbol = '{params["symbol"]}'
          AND toDate(toDateTime(open_time / 1000)) BETWEEN '{start}' AND '{end}'
        GROUP BY symbol, toDate(toDateTime(open_time / 1000))
    """)
    print("Daily aggregation from backfill complete")


with dag:
    start = EmptyOperator(task_id="start")

    fetch = PythonOperator(
        task_id="fetch_and_load_klines",
        python_callable=backfill_klines,
    )

    daily = PythonOperator(
        task_id="aggregate_daily",
        python_callable=backfill_daily_from_candles,
    )

    end = EmptyOperator(task_id="end")

    start >> fetch >> daily >> end
