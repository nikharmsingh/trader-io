"""
HTTP client for the Trader.IO FastAPI backend.
All methods return plain dicts/lists for easy Streamlit consumption.
"""
import os
from typing import Optional

import requests

BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
TIMEOUT = 5


def _get(path: str, params: Optional[dict] = None) -> dict | list:
    resp = requests.get(f"{BASE_URL}{path}", params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def get_all_prices() -> list:
    return _get("/prices/latest")


def get_latest_price(symbol: str) -> dict:
    return _get(f"/prices/latest/{symbol}")


def get_candles(symbol: str, interval: str = "1m", page_size: int = 200) -> list:
    data = _get(f"/candles/{symbol}", {"interval": interval, "page_size": page_size})
    return data.get("data", [])


def get_latest_indicators(symbol: str) -> dict:
    return _get(f"/indicators/latest/{symbol}")


def get_indicator_history(symbol: str, page_size: int = 100) -> list:
    data = _get(f"/indicators/{symbol}", {"page_size": page_size})
    return data.get("data", [])


def get_alert_history(symbol: Optional[str] = None, page_size: int = 20) -> list:
    params = {"page_size": page_size}
    if symbol:
        params["symbol"] = symbol
    data = _get("/alerts/history", params)
    return data.get("data", [])


def health_check() -> dict:
    return _get("/health")
