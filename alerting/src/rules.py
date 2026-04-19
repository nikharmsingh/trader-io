"""
Alert rule definitions.
Each rule is a pure function: (indicator_record) → Alert | None
"""
import os
import time
from dataclasses import dataclass
from typing import Optional


RSI_OVERBOUGHT = float(os.getenv("ALERT_RSI_OVERBOUGHT", "70"))
RSI_OVERSOLD   = float(os.getenv("ALERT_RSI_OVERSOLD",   "30"))


@dataclass
class Alert:
    symbol: str
    alert_type: str
    message: str
    price: float
    indicator_value: Optional[float]
    triggered_at: int


def check_rsi_overbought(record: dict) -> Optional[Alert]:
    rsi = record.get("rsi_14")
    if rsi is not None and rsi > RSI_OVERBOUGHT:
        return Alert(
            symbol=record["symbol"],
            alert_type="RSI_OVERBOUGHT",
            message=f"RSI {rsi:.1f} above overbought threshold ({RSI_OVERBOUGHT})",
            price=record.get("price", 0),
            indicator_value=rsi,
            triggered_at=int(time.time() * 1000),
        )
    return None


def check_rsi_oversold(record: dict) -> Optional[Alert]:
    rsi = record.get("rsi_14")
    if rsi is not None and rsi < RSI_OVERSOLD:
        return Alert(
            symbol=record["symbol"],
            alert_type="RSI_OVERSOLD",
            message=f"RSI {rsi:.1f} below oversold threshold ({RSI_OVERSOLD})",
            price=record.get("price", 0),
            indicator_value=rsi,
            triggered_at=int(time.time() * 1000),
        )
    return None


def check_ma_crossover(record: dict, prev_record: Optional[dict]) -> Optional[Alert]:
    if prev_record is None:
        return None
    sma14_curr = record.get("sma_14")
    sma50_curr = record.get("sma_50")
    sma14_prev = prev_record.get("sma_14")
    sma50_prev = prev_record.get("sma_50")

    if None in (sma14_curr, sma50_curr, sma14_prev, sma50_prev):
        return None

    # Golden cross: SMA14 crosses above SMA50
    if sma14_prev <= sma50_prev and sma14_curr > sma50_curr:
        return Alert(
            symbol=record["symbol"],
            alert_type="MA_CROSSOVER",
            message=f"Golden cross: SMA14 ({sma14_curr:.2f}) crossed above SMA50 ({sma50_curr:.2f})",
            price=record.get("price", 0),
            indicator_value=sma14_curr,
            triggered_at=int(time.time() * 1000),
        )

    # Death cross: SMA14 crosses below SMA50
    if sma14_prev >= sma50_prev and sma14_curr < sma50_curr:
        return Alert(
            symbol=record["symbol"],
            alert_type="MA_CROSSOVER",
            message=f"Death cross: SMA14 ({sma14_curr:.2f}) crossed below SMA50 ({sma50_curr:.2f})",
            price=record.get("price", 0),
            indicator_value=sma14_curr,
            triggered_at=int(time.time() * 1000),
        )
    return None


ALL_RULES = [check_rsi_overbought, check_rsi_oversold]


def evaluate(record: dict, prev_record: Optional[dict] = None) -> list[Alert]:
    alerts = []
    for rule in ALL_RULES:
        result = rule(record)
        if result:
            alerts.append(result)
    ma_alert = check_ma_crossover(record, prev_record)
    if ma_alert:
        alerts.append(ma_alert)
    return alerts
