from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query

from db.clickhouse import query
from models.schemas import PriceResponse, PaginatedResponse

router = APIRouter(prefix="/prices", tags=["Prices"])

VALID_SYMBOLS = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "ADAUSDT"}


@router.get("/latest/{symbol}", response_model=PriceResponse)
def get_latest_price(symbol: str):
    symbol = symbol.upper()
    if symbol not in VALID_SYMBOLS:
        raise HTTPException(400, f"Unknown symbol: {symbol}")

    rows = query(f"""
        SELECT symbol, price, event_time
        FROM latest_prices_mv_target
        WHERE symbol = '{symbol}'
        ORDER BY event_time DESC
        LIMIT 1
    """)
    if not rows:
        raise HTTPException(404, f"No price data for {symbol}")

    sym, price, event_time = rows[0]
    return PriceResponse(
        symbol=sym,
        price=price,
        event_time=event_time,
        timestamp=datetime.utcfromtimestamp(event_time / 1000),
    )


@router.get("/latest", response_model=List[PriceResponse])
def get_all_latest_prices():
    rows = query("""
        SELECT symbol, price, event_time
        FROM latest_prices_mv_target
        WHERE (symbol, event_time) IN (
            SELECT symbol, max(event_time)
            FROM latest_prices_mv_target
            GROUP BY symbol
        )
        ORDER BY symbol
    """)
    return [
        PriceResponse(
            symbol=r[0], price=r[1], event_time=r[2],
            timestamp=datetime.utcfromtimestamp(r[2] / 1000),
        )
        for r in rows
    ]


@router.get("/history/{symbol}", response_model=PaginatedResponse)
def get_price_history(
    symbol: str,
    start_ms: Optional[int] = Query(None, description="Start timestamp (ms)"),
    end_ms: Optional[int] = Query(None, description="End timestamp (ms)"),
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

    total_rows = query(f"SELECT count() FROM trades WHERE {where}")
    total = total_rows[0][0] if total_rows else 0

    offset = (page - 1) * page_size
    rows = query(f"""
        SELECT symbol, price, event_time
        FROM trades
        WHERE {where}
        ORDER BY event_time DESC
        LIMIT {page_size} OFFSET {offset}
    """)

    data = [
        PriceResponse(
            symbol=r[0], price=r[1], event_time=r[2],
            timestamp=datetime.utcfromtimestamp(r[2] / 1000),
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
