"""Alert feed component."""
from datetime import datetime
import streamlit as st


ALERT_COLORS = {
    "RSI_OVERBOUGHT": "#ef5350",
    "RSI_OVERSOLD":   "#26a69a",
    "MA_CROSSOVER":   "#ffd700",
    "DEFAULT":        "#90a4ae",
}

ALERT_ICONS = {
    "RSI_OVERBOUGHT": "🔴",
    "RSI_OVERSOLD":   "🟢",
    "MA_CROSSOVER":   "🟡",
    "DEFAULT":        "⚪",
}


def render_alert_feed(alerts: list):
    st.subheader("Recent Alerts")
    if not alerts:
        st.caption("No alerts triggered yet.")
        return

    for alert in alerts[:10]:
        icon = ALERT_ICONS.get(alert.get("alert_type", ""), ALERT_ICONS["DEFAULT"])
        color = ALERT_COLORS.get(alert.get("alert_type", ""), ALERT_COLORS["DEFAULT"])
        ts = datetime.utcfromtimestamp(alert["triggered_at"] / 1000).strftime("%H:%M:%S UTC")

        st.markdown(
            f"""
            <div style="
                border-left: 4px solid {color};
                padding: 8px 12px;
                margin-bottom: 8px;
                background: rgba(255,255,255,0.05);
                border-radius: 4px;
            ">
                {icon} <b>{alert['symbol']}</b> — {alert['message']}<br>
                <small style="color:#90a4ae">
                    {alert['alert_type']} | Price: ${alert['price']:,.2f} | {ts}
                </small>
            </div>
            """,
            unsafe_allow_html=True,
        )
