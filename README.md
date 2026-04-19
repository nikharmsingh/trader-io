# Trader.IO — Real-Time Market Data Platform

A production-grade, end-to-end data engineering platform for real-time stock and crypto market data. Built as a portfolio project to demonstrate streaming pipelines, OLAP storage, REST APIs, and full observability.

---

## Architecture

```
  Binance WebSocket / Mock Generator
            │
            ▼
    ┌──────────────┐
    │    Kafka     │  raw.trades · processed.candles · processed.indicators · alerts
    │  (6 partitions per topic)
    └──────┬───────┘
           │
    ┌──────▼────────────────┐        ┌─────────────────┐
    │  Spark Structured     │        │   MinIO (S3)    │
    │  Streaming            │───────▶│  Raw Parquet    │
    │  SMA · EMA · RSI      │        │  Data Lake      │
    │  MACD · OHLCV Candles │        └─────────────────┘
    └──────┬────────────────┘
           │
    ┌──────▼────────────────┐
    │     ClickHouse        │  trades · candles_1m/5m/15m · indicators · alerts
    │  ReplacingMergeTree   │
    └──────┬────────────────┘
           │
    ┌──────▼──────┐   ┌──────────────┐   ┌──────────────────┐
    │  FastAPI    │   │   Airflow    │   │  Alert Engine    │
    │  REST + WS  │   │  Daily Agg   │   │  RSI · MA Cross  │
    │  /docs      │   │  Backfill    │   │  Telegram/Discord│
    └──────┬──────┘   └──────────────┘   └──────────────────┘
           │
    ┌──────▼──────────┐   ┌──────────────────────────────┐
    │   Streamlit     │   │  Prometheus · Grafana · Loki  │
    │   Dashboard     │   │  Pipeline metrics · Logs      │
    └─────────────────┘   └──────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python, Kafka (Confluent), Binance WebSocket |
| Stream Processing | Apache Spark Structured Streaming |
| Data Lake | MinIO (S3-compatible) — Parquet |
| OLAP Storage | ClickHouse (ReplacingMergeTree) |
| Orchestration | Apache Airflow |
| API | FastAPI + WebSocket |
| Dashboard | Streamlit + Plotly |
| Alerting | Telegram Bot, Discord Webhook |
| Monitoring | Prometheus, Grafana, Loki, Promtail |
| Infrastructure | Docker, Docker Compose |

---

## Quick Start

**Prerequisites:** Docker Desktop installed and running.

```bash
git clone https://github.com/nikharmsingh/trader-io.git
cd trader-io

# 1. Copy env config
cp .env.example .env

# 2. Start full stack (~2 min first run)
make up

# 3. Create Kafka topics
make topics

# 4. Seed 7 days of historical mock data
make seed

# 5. (Optional) Start monitoring stack
make up-mon
```

---

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| Dashboard | http://localhost:8501 | — |
| API Docs (Swagger) | http://localhost:8000/docs | — |
| Airflow | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| Kafka UI | http://localhost:8090 | — |
| Grafana | http://localhost:3000 | admin / admin123 |
| Prometheus | http://localhost:9090 | — |

---

## Features

### Data Ingestion
- Real-time tick data via **Binance WebSocket** (or built-in mock generator)
- **Kafka producer** with idempotent delivery, retry logic, and snappy compression
- Partitioned by symbol for parallel processing
- Prometheus metrics on produce rate and latency

### Stream Processing (Spark)
- **1-min and 5-min OHLCV candles** via tumbling windows
- **Technical indicators:** SMA (7/14/50), EMA (12/26), RSI (14), MACD (12/26/9)
- 10-minute watermark for late data handling
- Raw trades written to MinIO as Parquet (data lake)

### Storage
- **ClickHouse** with `ReplacingMergeTree` for idempotent inserts
- Partition by month, TTL-based retention per table
- Materialized view for sub-millisecond latest-price queries

### Batch / Airflow
- `daily_aggregation` DAG — runs at 00:05 UTC, aggregates 1m candles into daily OHLCV
- `historical_backfill` DAG — triggered manually, fetches Binance historical klines
- `data_quality_check` DAG — hourly freshness and gap validation

### REST API (FastAPI)
```
GET  /prices/latest/{symbol}
GET  /prices/latest
GET  /prices/history/{symbol}?start_ms=&end_ms=&page=&page_size=
GET  /candles/{symbol}?interval=1m|5m|15m
GET  /indicators/latest/{symbol}
GET  /indicators/{symbol}
GET  /alerts/history
WS   /ws/live/{symbol}
GET  /metrics   ← Prometheus scrape
GET  /health
```

### Dashboard
- Live candlestick chart with SMA overlays
- RSI and MACD panels
- Real-time alert feed
- Auto-refresh every 5 seconds

### Alerting
- **RSI overbought** (> 70) and **oversold** (< 30) alerts
- **Golden cross / Death cross** (SMA14 × SMA50)
- 60-second cooldown per symbol/type to avoid spam
- Dispatches to **Telegram** and/or **Discord webhook**

---

## Project Structure

```
trader-io/
├── ingestion/        # Kafka producer, Binance WS client, mock generator
├── processing/       # Spark Structured Streaming job, indicators
├── storage/          # ClickHouse DDL (init.sql)
├── airflow/          # DAGs + ClickHouse hook plugin
├── api/              # FastAPI service (routers, models, middleware)
├── dashboard/        # Streamlit app + Plotly components
├── alerting/         # Rule engine + Telegram/Discord notifiers
├── monitoring/       # Prometheus, Grafana, Loki configs
├── scripts/          # Seed data, topic creator, health check
├── docker-compose.yml
├── docker-compose.monitoring.yml
├── Makefile
└── .env.example
```

---

## Configuration

All configuration is via `.env` (copy from `.env.example`). Key settings:

```bash
DATA_SOURCE=mock          # mock | binance
SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,ADAUSDT
BINANCE_API_KEY=...       # only needed for DATA_SOURCE=binance
TELEGRAM_BOT_TOKEN=...    # optional alerting
DISCORD_WEBHOOK_URL=...   # optional alerting
```

---

## Make Commands

```bash
make up        # Start full stack
make up-mon    # Start + monitoring stack
make down      # Stop all containers
make topics    # Create Kafka topics
make seed      # Load 7 days of mock historical data
make health    # Check all service health
make logs s=api  # Tail logs for a specific service
make clean     # Remove all volumes and networks
```

---

## Cloud Deployment

The stack is cloud-ready with minor config changes:

| Component | AWS | GCP |
|---|---|---|
| Kafka | MSK | Confluent Cloud |
| MinIO | S3 | GCS |
| ClickHouse | ClickHouse Cloud | ClickHouse Cloud |
| Airflow | MWAA | Cloud Composer |
| API + Dashboard | ECS Fargate | Cloud Run |

---

## License

MIT
