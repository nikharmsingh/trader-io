"""
Custom Airflow hook for ClickHouse operations.
Wraps clickhouse-connect for use in DAG tasks.
"""
import os
from typing import Any, List, Dict

import clickhouse_connect
from airflow.hooks.base import BaseHook


class ClickHouseHook(BaseHook):
    conn_name_attr = "clickhouse_conn_id"
    default_conn_name = "clickhouse_default"
    hook_name = "ClickHouse"

    def __init__(self, clickhouse_conn_id: str = default_conn_name):
        super().__init__()
        self.clickhouse_conn_id = clickhouse_conn_id

    def get_client(self):
        host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
        port = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
        user = os.getenv("CLICKHOUSE_USER", "default")
        password = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse123")
        database = os.getenv("CLICKHOUSE_DATABASE", "trader_io")
        return clickhouse_connect.get_client(
            host=host, port=port, username=user, password=password, database=database
        )

    def execute(self, sql: str) -> Any:
        return self.get_client().command(sql)

    def query(self, sql: str) -> List[List]:
        return self.get_client().query(sql).result_rows

    def insert(self, table: str, rows: List[List], column_names: List[str]):
        self.get_client().insert(table, rows, column_names=column_names)
