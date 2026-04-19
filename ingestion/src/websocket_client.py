"""
Binance WebSocket client for real-time trade stream.
Implements exponential backoff reconnection and heartbeat monitoring.
"""
import asyncio
import json
import logging
import time
from typing import AsyncIterator, List, Callable

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

logger = logging.getLogger(__name__)


class BinanceWSClient:
    MAX_RECONNECT_DELAY = 60
    HEARTBEAT_TIMEOUT = 30

    def __init__(self, base_url: str, symbols: List[str]):
        self._base_url = base_url
        self._symbols = symbols
        self._streams = "/".join(f"{s.lower()}@trade" for s in symbols)
        self._url = f"{base_url}/stream?streams={self._streams}"
        self._reconnect_delay = 1.0

    async def stream(self) -> AsyncIterator[dict]:
        while True:
            try:
                async with websockets.connect(
                    self._url,
                    ping_interval=20,
                    ping_timeout=self.HEARTBEAT_TIMEOUT,
                    close_timeout=10,
                ) as ws:
                    logger.info("Connected to Binance WebSocket: %d symbols", len(self._symbols))
                    self._reconnect_delay = 1.0
                    async for raw in ws:
                        try:
                            envelope = json.loads(raw)
                            data = envelope.get("data", envelope)
                            yield self._normalize(data)
                        except (json.JSONDecodeError, KeyError) as e:
                            logger.warning("Malformed message: %s | %s", e, raw[:200])

            except (ConnectionClosed, WebSocketException, OSError) as e:
                logger.warning("WS disconnected: %s. Reconnecting in %.1fs", e, self._reconnect_delay)
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, self.MAX_RECONNECT_DELAY)

    def _normalize(self, data: dict) -> dict:
        return {
            "event_time": data.get("T", int(time.time() * 1000)),
            "symbol": data.get("s", "UNKNOWN"),
            "trade_id": str(data.get("t", "")),
            "price": float(data.get("p", 0)),
            "quantity": float(data.get("q", 0)),
            "buyer_order_id": str(data.get("b", "")),
            "seller_order_id": str(data.get("a", "")),
            "is_buyer_maker": bool(data.get("m", False)),
            "source": "BINANCE",
        }
