"""
Builds OHLCV candles from tick data.
Used both in Spark window aggregations and in batch backfill.
"""
import pandas as pd
from dataclasses import dataclass
from typing import Optional


@dataclass
class Candle:
    symbol: str
    interval: str
    open_time: int       # ms epoch — start of window
    close_time: int      # ms epoch — end of window
    open: float
    high: float
    low: float
    close: float
    volume: float
    trade_count: int
    vwap: Optional[float] = None


def build_candles(ticks: pd.DataFrame, interval_minutes: int) -> pd.DataFrame:
    """
    Build OHLCV candles from a DataFrame of ticks.

    ticks must have columns: event_time (ms), symbol, price, quantity
    Returns a DataFrame of candles, one row per (symbol, window).
    """
    df = ticks.copy()
    df["ts"] = pd.to_datetime(df["event_time"], unit="ms", utc=True)
    df = df.set_index("ts").sort_index()

    rule = f"{interval_minutes}min"
    result = []

    for symbol, group in df.groupby("symbol"):
        ohlcv = group["price"].resample(rule).ohlc()
        volume = group["quantity"].resample(rule).sum()
        count = group["price"].resample(rule).count()
        vwap_val = (group["price"] * group["quantity"]).resample(rule).sum() / volume

        combined = ohlcv.join(volume.rename("volume")).join(
            count.rename("trade_count")
        ).join(vwap_val.rename("vwap"))
        combined["symbol"] = symbol
        combined["interval"] = f"{interval_minutes}m"
        combined["open_time"] = combined.index.astype("int64") // 10**6
        combined["close_time"] = combined["open_time"] + interval_minutes * 60 * 1000 - 1
        combined = combined.dropna(subset=["open", "close"])
        result.append(combined.reset_index(drop=True))

    if not result:
        return pd.DataFrame()
    return pd.concat(result, ignore_index=True)
