from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field


class PriceResponse(BaseModel):
    symbol: str
    price: float
    event_time: int
    timestamp: datetime


class CandleResponse(BaseModel):
    symbol: str
    interval: str
    open_time: int
    close_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    trade_count: int
    vwap: Optional[float] = None


class IndicatorResponse(BaseModel):
    symbol: str
    event_time: int
    sma_7: Optional[float] = None
    sma_14: Optional[float] = None
    sma_50: Optional[float] = None
    ema_12: Optional[float] = None
    ema_26: Optional[float] = None
    rsi_14: Optional[float] = None
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_histogram: Optional[float] = None


class AlertResponse(BaseModel):
    id: str
    symbol: str
    alert_type: str
    message: str
    price: float
    indicator_value: Optional[float] = None
    triggered_at: int


class DailyOHLCV(BaseModel):
    symbol: str
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    trade_count: int
    avg_price: float
    price_change: float
    price_change_pct: float


class PaginatedResponse(BaseModel):
    data: list
    total: int
    page: int
    page_size: int
    has_more: bool


class HealthResponse(BaseModel):
    status: str
    clickhouse: str
    version: str = "1.0.0"
