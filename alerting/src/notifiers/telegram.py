import logging
import os
from typing import Optional

import requests

logger = logging.getLogger(__name__)


class TelegramNotifier:
    def __init__(self):
        self._token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self._chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        self._enabled = bool(self._token and self._chat_id)
        if not self._enabled:
            logger.info("Telegram notifier disabled (no token/chat_id configured)")

    def send(self, message: str) -> bool:
        if not self._enabled:
            return False
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{self._token}/sendMessage",
                json={"chat_id": self._chat_id, "text": message, "parse_mode": "Markdown"},
                timeout=5,
            )
            resp.raise_for_status()
            return True
        except Exception as e:
            logger.error("Telegram send failed: %s", e)
            return False

    def format_alert(self, alert) -> str:
        icon = {"RSI_OVERBOUGHT": "🔴", "RSI_OVERSOLD": "🟢", "MA_CROSSOVER": "🟡"}.get(
            alert.alert_type, "⚠️"
        )
        return (
            f"{icon} *Trader.IO Alert*\n"
            f"Symbol: `{alert.symbol}`\n"
            f"Type: `{alert.alert_type}`\n"
            f"Message: {alert.message}\n"
            f"Price: `${alert.price:,.4f}`"
        )
