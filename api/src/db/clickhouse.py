import os
from functools import lru_cache
from typing import Any, List, Optional

import clickhouse_connect
from clickhouse_connect.driver import Client


@lru_cache(maxsize=1)
def get_client() -> Client:
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse123"),
        database=os.getenv("CLICKHOUSE_DATABASE", "trader_io"),
        connect_timeout=10,
        query_retries=2,
    )


def query(sql: str, parameters: Optional[dict] = None) -> List[List[Any]]:
    return get_client().query(sql, parameters=parameters).result_rows


def query_df(sql: str, parameters: Optional[dict] = None):
    return get_client().query_df(sql, parameters=parameters)
