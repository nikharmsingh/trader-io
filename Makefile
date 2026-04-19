.PHONY: help up down restart logs ps build clean topics seed health

COMPOSE=docker compose
COMPOSE_MON=docker compose -f docker-compose.monitoring.yml

help:
	@echo ""
	@echo "  TRADER.IO — Dev Commands"
	@echo "  ─────────────────────────────────────────"
	@echo "  make up          Start full stack"
	@echo "  make up-mon      Start + monitoring stack"
	@echo "  make down        Stop and remove containers"
	@echo "  make restart     Restart all services"
	@echo "  make build       Rebuild all images"
	@echo "  make logs s=api  Tail logs for a service"
	@echo "  make ps          Show running containers"
	@echo "  make topics      Create Kafka topics"
	@echo "  make seed        Seed historical mock data"
	@echo "  make health      Run health checks"
	@echo "  make clean       Remove volumes + networks"
	@echo ""

up:
	@cp -n .env.example .env 2>/dev/null || true
	$(COMPOSE) up -d
	@echo "Stack running. Airflow: http://localhost:8080 | MinIO: http://localhost:9001 | API: http://localhost:8000"

up-mon: up
	$(COMPOSE_MON) up -d
	@echo "Monitoring: Grafana http://localhost:3000 | Prometheus http://localhost:9090"

down:
	$(COMPOSE) down
	$(COMPOSE_MON) down 2>/dev/null || true

restart:
	$(COMPOSE) restart

build:
	$(COMPOSE) build --no-cache

logs:
	$(COMPOSE) logs -f $(s)

ps:
	$(COMPOSE) ps

topics:
	docker exec trader-kafka sh -c \
		"kafka-topics --create --bootstrap-server localhost:9092 \
		 --partitions 6 --replication-factor 1 \
		 --topic raw.trades --if-not-exists && \
		 kafka-topics --create --bootstrap-server localhost:9092 \
		 --partitions 6 --replication-factor 1 \
		 --topic processed.candles --if-not-exists && \
		 kafka-topics --create --bootstrap-server localhost:9092 \
		 --partitions 6 --replication-factor 1 \
		 --topic processed.indicators --if-not-exists && \
		 kafka-topics --create --bootstrap-server localhost:9092 \
		 --partitions 1 --replication-factor 1 \
		 --topic alerts --if-not-exists && \
		 kafka-topics --list --bootstrap-server localhost:9092"

seed:
	docker exec trader-api python /scripts/seed_historical_data.py

health:
	@bash scripts/health_check.sh

clean:
	$(COMPOSE) down -v --remove-orphans
	$(COMPOSE_MON) down -v --remove-orphans 2>/dev/null || true
	@echo "All volumes and networks removed."
