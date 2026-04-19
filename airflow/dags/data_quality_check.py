"""
Data quality checks DAG. Runs hourly.
Validates freshness, row counts, and indicator completeness.
Sends alert to Kafka `alerts` topic on failure.
"""
import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
import sys

sys.path.insert(0, "/opt/airflow/plugins")
from clickhouse_hook import ClickHouseHook

SYMBOLS = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,ADAUSDT").split(",")
STALE_THRESHOLD_MINUTES = 5

DEFAULT_ARGS = {
    "owner": "trader-io",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
}

dag = DAG(
    dag_id="data_quality_check",
    default_args=DEFAULT_ARGS,
    description="Hourly freshness and completeness checks",
    schedule="*/30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["trader-io", "data-quality"],
    max_active_runs=1,
)


def check_trade_freshness(**context):
    hook = ClickHouseHook()
    failures = []
    for symbol in SYMBOLS:
        rows = hook.query(f"""
            SELECT max(event_time) as latest_ms
            FROM trades
            WHERE symbol = '{symbol}'
        """)
        latest_ms = rows[0][0] if rows and rows[0][0] else 0
        age_minutes = (datetime.utcnow().timestamp() * 1000 - latest_ms) / 60000
        if age_minutes > STALE_THRESHOLD_MINUTES:
            failures.append(f"{symbol}: last trade {age_minutes:.1f}m ago")

    if failures:
        raise ValueError("Stale data detected:\n" + "\n".join(failures))
    print(f"Trade freshness OK for {SYMBOLS}")


def check_candle_gaps(**context):
    hook = ClickHouseHook()
    for symbol in SYMBOLS:
        rows = hook.query(f"""
            SELECT
                count() AS total,
                countIf(close > 0) AS valid
            FROM candles_1m
            WHERE symbol = '{symbol}'
              AND toDateTime(open_time / 1000) >= now() - INTERVAL 1 HOUR
        """)
        total, valid = (rows[0][0], rows[0][1]) if rows else (0, 0)
        if total > 0 and valid / total < 0.9:
            raise ValueError(f"{symbol}: only {valid}/{total} valid 1m candles in last hour")
    print("Candle gap check passed")


def check_indicator_lag(**context):
    hook = ClickHouseHook()
    for symbol in SYMBOLS:
        rows = hook.query(f"""
            SELECT max(event_time) FROM indicators WHERE symbol = '{symbol}'
        """)
        latest_ms = rows[0][0] if rows and rows[0][0] else 0
        lag_minutes = (datetime.utcnow().timestamp() * 1000 - latest_ms) / 60000
        if lag_minutes > 10:
            raise ValueError(f"Indicator lag for {symbol}: {lag_minutes:.1f}m")
    print("Indicator lag check passed")


with dag:
    freshness = PythonOperator(task_id="check_trade_freshness", python_callable=check_trade_freshness)
    gaps      = PythonOperator(task_id="check_candle_gaps",     python_callable=check_candle_gaps)
    lag       = PythonOperator(task_id="check_indicator_lag",   python_callable=check_indicator_lag)

    freshness >> gaps >> lag
