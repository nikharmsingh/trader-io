from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from db.clickhouse import query
from models.schemas import IndicatorResponse, PaginatedResponse

router = APIRouter(prefix="/indicators", tags=["Indicators"])

VALID_SYMBOLS = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "ADAUSDT"}


@router.get("/latest/{symbol}", response_model=IndicatorResponse)
def get_latest_indicators(symbol: str):
    symbol = symbol.upper()
    if symbol not in VALID_SYMBOLS:
        raise HTTPException(400, f"Unknown symbol: {symbol}")

    rows = query(f"""
        SELECT symbol, event_time,
               sma_7, sma_14, sma_50,
               ema_12, ema_26,
               rsi_14, macd, macd_signal, macd_histogram
        FROM indicators
        WHERE symbol = '{symbol}'
        ORDER BY event_time DESC
        LIMIT 1
    """)
    if not rows:
        # Fall back to latest price with null indicators — indicators populate once Spark runs
        price_rows = query(f"""
            SELECT symbol, event_time FROM latest_prices_mv_target
            WHERE symbol = '{symbol}' ORDER BY event_time DESC LIMIT 1
        """)
        event_time = price_rows[0][1] if price_rows else 0
        return IndicatorResponse(symbol=symbol, event_time=event_time)

    r = rows[0]
    return IndicatorResponse(
        symbol=r[0], event_time=r[1],
        sma_7=r[2], sma_14=r[3], sma_50=r[4],
        ema_12=r[5], ema_26=r[6],
        rsi_14=r[7], macd=r[8], macd_signal=r[9], macd_histogram=r[10],
    )


@router.get("/{symbol}", response_model=PaginatedResponse)
def get_indicator_history(
    symbol: str,
    start_ms: Optional[int] = Query(None),
    end_ms: Optional[int] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
):
    symbol = symbol.upper()
    if symbol not in VALID_SYMBOLS:
        raise HTTPException(400, f"Unknown symbol: {symbol}")

    where = f"symbol = '{symbol}'"
    if start_ms:
        where += f" AND event_time >= {start_ms}"
    if end_ms:
        where += f" AND event_time <= {end_ms}"

    total_rows = query(f"SELECT count() FROM indicators WHERE {where}")
    total = total_rows[0][0] if total_rows else 0
    offset = (page - 1) * page_size

    rows = query(f"""
        SELECT symbol, event_time,
               sma_7, sma_14, sma_50, ema_12, ema_26,
               rsi_14, macd, macd_signal, macd_histogram
        FROM indicators
        WHERE {where}
        ORDER BY event_time DESC
        LIMIT {page_size} OFFSET {offset}
    """)

    data = [
        IndicatorResponse(
            symbol=r[0], event_time=r[1],
            sma_7=r[2], sma_14=r[3], sma_50=r[4],
            ema_12=r[5], ema_26=r[6],
            rsi_14=r[7], macd=r[8], macd_signal=r[9], macd_histogram=r[10],
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
