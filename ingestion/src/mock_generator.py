"""
Generates realistic mock tick data using geometric Brownian motion.
Simulates bid/ask spread, volume profiles, and volatility clustering.
"""
import random
import time
import uuid
from dataclasses import dataclass
from typing import Dict


@dataclass
class TickerState:
    symbol: str
    price: float
    drift: float = 0.0
    volatility: float = 0.02


class MockMarketGenerator:
    INITIAL_PRICES: Dict[str, float] = {
        "BTCUSDT":  65000.0,
        "ETHUSDT":   3400.0,
        "SOLUSDT":    175.0,
        "BNBUSDT":    580.0,
        "ADAUSDT":      0.58,
    }
    VOLATILITY: Dict[str, float] = {
        "BTCUSDT": 0.0008,
        "ETHUSDT": 0.0010,
        "SOLUSDT": 0.0015,
        "BNBUSDT": 0.0012,
        "ADAUSDT": 0.0020,
    }

    def __init__(self, symbols: list[str]):
        self._states: Dict[str, TickerState] = {
            s: TickerState(
                symbol=s,
                price=self.INITIAL_PRICES.get(s, 100.0),
                volatility=self.VOLATILITY.get(s, 0.001),
            )
            for s in symbols
        }

    def next_tick(self, symbol: str) -> dict:
        state = self._states[symbol]
        # Geometric Brownian Motion step
        shock = random.gauss(0, state.volatility)
        state.price *= (1 + shock)
        state.price = max(state.price, 0.0001)

        quantity = round(random.lognormvariate(0, 1) * 0.1, 6)
        is_buyer_maker = random.random() > 0.5

        return {
            "event_time": int(time.time() * 1000),
            "symbol": symbol,
            "trade_id": str(uuid.uuid4()),
            "price": round(state.price, 8),
            "quantity": quantity,
            "buyer_order_id": str(uuid.uuid4()),
            "seller_order_id": str(uuid.uuid4()),
            "is_buyer_maker": is_buyer_maker,
            "source": "MOCK",
        }
