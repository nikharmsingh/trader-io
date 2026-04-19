import logging
import os

import requests

logger = logging.getLogger(__name__)

ALERT_COLORS = {
    "RSI_OVERBOUGHT": 0xef5350,
    "RSI_OVERSOLD":   0x26a69a,
    "MA_CROSSOVER":   0xffd700,
}


class DiscordNotifier:
    def __init__(self):
        self._webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "")
        self._enabled = bool(self._webhook_url)
        if not self._enabled:
            logger.info("Discord notifier disabled (no webhook URL configured)")

    def send(self, alert) -> bool:
        if not self._enabled:
            return False
        try:
            embed = {
                "title": f"Trader.IO Alert — {alert.symbol}",
                "description": alert.message,
                "color": ALERT_COLORS.get(alert.alert_type, 0x90a4ae),
                "fields": [
                    {"name": "Type",  "value": alert.alert_type, "inline": True},
                    {"name": "Price", "value": f"${alert.price:,.4f}", "inline": True},
                ],
            }
            resp = requests.post(
                self._webhook_url,
                json={"embeds": [embed]},
                timeout=5,
            )
            resp.raise_for_status()
            return True
        except Exception as e:
            logger.error("Discord send failed: %s", e)
            return False
