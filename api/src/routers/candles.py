from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query

from db.clickhouse import query
from models.schemas import CandleResponse, PaginatedResponse

router = APIRouter(prefix="/candles", tags=["Candles"])

VALID_SYMBOLS = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "ADAUSDT"}
VALID_INTERVALS = {"1m", "5m", "15m"}
INTERVAL_TABLE = {"1m": "candles_1m", "5m": "candles_5m", "15m": "candles_15m"}


@router.get("/{symbol}", response_model=PaginatedResponse)
def get_candles(
    symbol: str,
    interval: str = Query("1m", description="Candle interval: 1m, 5m, 15m"),
    start_ms: Optional[int] = Query(None),
    end_ms: Optional[int] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
):
    symbol = symbol.upper()
    if symbol not in VALID_SYMBOLS:
        raise HTTPException(400, f"Unknown symbol: {symbol}")
    if interval not in VALID_INTERVALS:
        raise HTTPException(400, f"Invalid interval. Use: {VALID_INTERVALS}")

    table = INTERVAL_TABLE[interval]
    where = f"symbol = '{symbol}'"
    if start_ms:
        where += f" AND open_time >= {start_ms}"
    if end_ms:
        where += f" AND open_time <= {end_ms}"

    total_rows = query(f"SELECT count() FROM {table} WHERE {where}")
    total = total_rows[0][0] if total_rows else 0

    offset = (page - 1) * page_size
    rows = query(f"""
        SELECT symbol, interval, open_time, close_time,
               open, high, low, close, volume, trade_count, vwap
        FROM {table}
        WHERE {where}
        ORDER BY open_time DESC
        LIMIT {page_size} OFFSET {offset}
    """)

    data = [
        CandleResponse(
            symbol=r[0], interval=r[1], open_time=r[2], close_time=r[3],
            open=r[4], high=r[5], low=r[6], close=r[7],
            volume=r[8], trade_count=r[9], vwap=r[10],
        )
        for r in rows
    ]
    return PaginatedResponse(
        data=[d.model_dump() for d in data],
        total=total,
        page=page,
        page_size=page_size,
        has_more=(offset + page_size) < total,
    )
