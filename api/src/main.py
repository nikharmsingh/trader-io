"""
Trader.IO FastAPI service.

Endpoints:
  GET  /health
  GET  /prices/latest/{symbol}
  GET  /prices/latest
  GET  /prices/history/{symbol}
  GET  /candles/{symbol}?interval=1m
  GET  /indicators/latest/{symbol}
  GET  /indicators/{symbol}
  GET  /alerts/history
  WS   /ws/live/{symbol}          ← live price stream via Kafka consumer
  GET  /metrics                   ← Prometheus scrape endpoint
"""
import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from starlette.requests import Request

from db.clickhouse import get_client
from middleware.metrics import PrometheusMiddleware, metrics_endpoint
from models.schemas import HealthResponse
from routers import prices, candles, indicators, alerts

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}',
)
logger = logging.getLogger("api.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Trader.IO API starting up...")
    try:
        get_client().command("SELECT 1")
        logger.info("ClickHouse connection OK")
    except Exception as e:
        logger.error("ClickHouse connection failed: %s", e)
    yield
    logger.info("Trader.IO API shutting down...")


app = FastAPI(
    title="Trader.IO Market Data API",
    description="Real-time and historical stock/crypto market data",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(PrometheusMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(prices.router)
app.include_router(candles.router)
app.include_router(indicators.router)
app.include_router(alerts.router)
app.add_route("/metrics", metrics_endpoint)


@app.get("/health", response_model=HealthResponse, tags=["System"])
def health_check():
    try:
        get_client().command("SELECT 1")
        ch_status = "ok"
    except Exception:
        ch_status = "error"
    return HealthResponse(status="ok", clickhouse=ch_status)


# ── WebSocket: live price stream ─────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self._connections: dict[str, list[WebSocket]] = {}

    async def connect(self, symbol: str, ws: WebSocket):
        await ws.accept()
        self._connections.setdefault(symbol, []).append(ws)

    def disconnect(self, symbol: str, ws: WebSocket):
        if symbol in self._connections:
            self._connections[symbol].discard(ws) if hasattr(
                self._connections[symbol], "discard"
            ) else self._connections[symbol].remove(ws)

    async def broadcast(self, symbol: str, message: dict):
        for ws in list(self._connections.get(symbol, [])):
            try:
                await ws.send_json(message)
            except Exception:
                self._connections[symbol].remove(ws)


manager = ConnectionManager()


@app.websocket("/ws/live/{symbol}")
async def websocket_live(websocket: WebSocket, symbol: str):
    symbol = symbol.upper()
    await manager.connect(symbol, websocket)
    logger.info("WS client connected for %s", symbol)
    try:
        from confluent_kafka import Consumer
        consumer = Consumer({
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            "group.id": f"ws-live-{symbol}-{id(websocket)}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        })
        consumer.subscribe(["raw.trades"])
        loop = asyncio.get_event_loop()

        while True:
            msg = await loop.run_in_executor(None, lambda: consumer.poll(timeout=1.0))
            if msg is None:
                continue
            if msg.error():
                continue
            if msg.key() and msg.key().decode() == symbol:
                data = json.loads(msg.value().decode())
                await websocket.send_json(data)

    except WebSocketDisconnect:
        manager.disconnect(symbol, websocket)
        logger.info("WS client disconnected from %s", symbol)
    except Exception as e:
        logger.error("WS error for %s: %s", symbol, e)
        manager.disconnect(symbol, websocket)
