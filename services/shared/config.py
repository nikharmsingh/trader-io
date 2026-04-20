from pathlib import Path
from dotenv import load_dotenv
import os

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / '..' / '.env')

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'redpanda:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'market-ticks')
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
MINIO_BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME', 'trader-raw')
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', '8123'))
CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DB', 'default')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')
DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL', '')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')
BINANCE_POLL = os.getenv('BINANCE_POLL', 'false').lower() == 'true'
SYMBOLS = [symbol.strip() for symbol in os.getenv('SYMBOLS', 'BTCUSDT').split(',')]
PRODUCER_BATCH_SIZE = int(os.getenv('PRODUCER_BATCH_SIZE', '20'))
PROCESSOR_BATCH_INTERVAL = int(os.getenv('PROCESSOR_BATCH_INTERVAL', '30'))
API_HOST = os.getenv('API_HOST', '0.0.0.0')
API_PORT = int(os.getenv('API_PORT', '8000'))
PROMETHEUS_PORT = int(os.getenv('PROMETHEUS_PORT', '8000'))
DASHBOARD_PORT = int(os.getenv('DASHBOARD_PORT', '8501'))
