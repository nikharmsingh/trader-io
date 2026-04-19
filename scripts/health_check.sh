#!/usr/bin/env bash
# Health check for all Trader.IO services

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}  ✓ $1${NC}"; }
fail() { echo -e "${RED}  ✗ $1${NC}"; }
warn() { echo -e "${YELLOW}  ~ $1${NC}"; }

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Trader.IO Health Check"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Kafka
echo -e "\n[Kafka]"
if docker exec trader-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &>/dev/null; then
  pass "Kafka broker reachable"
  topics=$(docker exec trader-kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null | tr '\n' ' ')
  if echo "$topics" | grep -q "raw.trades"; then
    pass "Topics created: $topics"
  else
    fail "Topics missing! Run: make topics"
  fi
else
  fail "Kafka unreachable"
fi

# MinIO
echo -e "\n[MinIO]"
if curl -sf http://localhost:9000/minio/health/live > /dev/null; then
  pass "MinIO healthy"
else
  fail "MinIO unhealthy"
fi

# ClickHouse
echo -e "\n[ClickHouse]"
if curl -sf "http://localhost:8123/ping" > /dev/null; then
  pass "ClickHouse HTTP ping OK"
  tables=$(curl -sf "http://localhost:8123/?query=SHOW+TABLES+FROM+trader_io&user=default&password=clickhouse123" 2>/dev/null | tr '\n' ' ')
  pass "Tables: $tables"
else
  fail "ClickHouse unreachable"
fi

# FastAPI
echo -e "\n[FastAPI]"
health=$(curl -sf http://localhost:8000/health 2>/dev/null || echo '{}')
if echo "$health" | grep -q '"status":"ok"'; then
  pass "API healthy"
else
  fail "API health check failed: $health"
fi

# Airflow
echo -e "\n[Airflow]"
if curl -sf http://localhost:8080/health > /dev/null; then
  pass "Airflow webserver reachable"
else
  warn "Airflow not reachable (may still be initializing)"
fi

# Streamlit
echo -e "\n[Dashboard]"
if curl -sf http://localhost:8501/_stcore/health > /dev/null; then
  pass "Streamlit dashboard healthy"
else
  warn "Dashboard not ready yet"
fi

# Kafka UI
echo -e "\n[Kafka UI]"
if curl -sf http://localhost:8090/ > /dev/null; then
  pass "Kafka UI reachable at http://localhost:8090"
else
  warn "Kafka UI not ready"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Endpoints:"
echo "  Dashboard  → http://localhost:8501"
echo "  API Docs   → http://localhost:8000/docs"
echo "  Airflow    → http://localhost:8080  (admin/admin)"
echo "  MinIO      → http://localhost:9001  (minioadmin/minioadmin123)"
echo "  Kafka UI   → http://localhost:8090"
echo "  Grafana    → http://localhost:3000  (admin/admin123)"
echo "  Prometheus → http://localhost:9090"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
