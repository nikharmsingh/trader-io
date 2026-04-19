"""
Daily OHLCV aggregation DAG.
Runs at 00:05 UTC each day to aggregate prior day's candles into daily_ohlcv.
Idempotent: uses INSERT ... SELECT with ReplacingMergeTree dedup.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import sys, os

sys.path.insert(0, "/opt/airflow/plugins")
from clickhouse_hook import ClickHouseHook

SYMBOLS = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,ADAUSDT").split(",")

DEFAULT_ARGS = {
    "owner": "trader-io",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

dag = DAG(
    dag_id="daily_aggregation",
    default_args=DEFAULT_ARGS,
    description="Aggregate 1-min candles into daily OHLCV",
    schedule="5 0 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["trader-io", "aggregation"],
    max_active_runs=1,
)


def aggregate_daily(ds: str, **context):
    """Aggregate candles_1m into daily_ohlcv for a given date."""
    hook = ClickHouseHook()
    target_date = ds  # YYYY-MM-DD

    sql = f"""
    INSERT INTO daily_ohlcv
        (symbol, date, open, high, low, close, volume,
         trade_count, avg_price, price_change, price_change_pct)
    SELECT
        symbol,
        toDate(toDateTime(open_time / 1000))  AS date,
        argMin(open,  open_time)              AS open,
        max(high)                             AS high,
        min(low)                              AS low,
        argMax(close, close_time)             AS close,
        sum(volume)                           AS volume,
        sum(trade_count)                      AS trade_count,
        avg(vwap)                             AS avg_price,
        argMax(close, close_time) - argMin(open, open_time)  AS price_change,
        (argMax(close, close_time) - argMin(open, open_time))
            / argMin(open, open_time) * 100   AS price_change_pct
    FROM candles_1m
    WHERE toDate(toDateTime(open_time / 1000)) = toDate('{target_date}')
    GROUP BY symbol, toDate(toDateTime(open_time / 1000))
    """
    hook.execute(sql)
    print(f"Daily aggregation complete for {target_date}")


def validate_daily(ds: str, **context):
    hook = ClickHouseHook()
    rows = hook.query(f"""
        SELECT symbol, count() as candle_count
        FROM daily_ohlcv
        WHERE date = toDate('{ds}')
        GROUP BY symbol
        ORDER BY symbol
    """)
    if not rows:
        raise ValueError(f"No daily_ohlcv rows found for {ds} — aggregation may have failed")
    for symbol, count in rows:
        print(f"  {symbol}: {count} daily rows")


def optimize_tables(**context):
    hook = ClickHouseHook()
    for table in ["candles_1m", "candles_5m", "indicators", "daily_ohlcv"]:
        hook.execute(f"OPTIMIZE TABLE {table} FINAL")
        print(f"Optimized {table}")


with dag:
    start = EmptyOperator(task_id="start")

    agg = PythonOperator(
        task_id="aggregate_daily_ohlcv",
        python_callable=aggregate_daily,
    )

    validate = PythonOperator(
        task_id="validate_aggregation",
        python_callable=validate_daily,
    )

    optimize = PythonOperator(
        task_id="optimize_tables",
        python_callable=optimize_tables,
    )

    end = EmptyOperator(task_id="end")

    start >> agg >> validate >> optimize >> end
