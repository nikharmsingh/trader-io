"""
Trader.IO — Streamlit Dashboard

Layout:
  Sidebar  → symbol selector, interval, refresh toggle
  Row 1    → KPI cards (price, 24h change, RSI, volume)
  Row 2    → Candlestick chart with SMA overlay
  Row 3    → RSI panel | MACD panel
  Row 4    → Alert feed
"""
import time
import os

import streamlit as st

from api_client import (
    get_all_prices, get_latest_price, get_candles,
    get_latest_indicators, get_indicator_history,
    get_alert_history, health_check,
)
from components.price_chart import render_candlestick
from components.indicator_panel import render_rsi, render_macd
from components.alert_feed import render_alert_feed

st.set_page_config(
    page_title="Trader.IO Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("📈 Trader.IO")
    st.caption("Real-time market data platform")
    st.divider()

    symbol = st.selectbox(
        "Symbol",
        ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "ADAUSDT"],
        index=0,
    )
    interval = st.selectbox("Candle Interval", ["1m", "5m", "15m"], index=0)
    refresh_interval = int(os.getenv("DASHBOARD_REFRESH_INTERVAL", "5"))
    auto_refresh = st.toggle("Auto-refresh", value=True)

    st.divider()
    try:
        health = health_check()
        st.success(f"API: {health['status'].upper()}")
        st.caption(f"ClickHouse: {health['clickhouse']}")
    except Exception:
        st.error("API unreachable")

    st.divider()
    st.caption("Stack: Kafka · Spark · ClickHouse · FastAPI")

# ── Main layout ───────────────────────────────────────────────────────────────
st.title(f"Market Dashboard — {symbol}")

placeholder = st.empty()

def fetch_and_render():
    with placeholder.container():
        # ── KPI Row ──────────────────────────────────────────────────────────
        try:
            price_data  = get_latest_price(symbol)
            indicators  = get_latest_indicators(symbol)
            all_prices  = get_all_prices()
        except Exception as e:
            st.error(f"Failed to fetch data: {e}")
            return

        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            price = price_data.get("price", 0)
            st.metric("Price", f"${price:,.4f}")

        with col2:
            rsi = indicators.get("rsi_14")
            rsi_label = "—" if rsi is None else f"{rsi:.1f}"
            rsi_status = "" if rsi is None else ("🔴 OB" if rsi > 70 else "🟢 OS" if rsi < 30 else "")
            st.metric("RSI (14)", rsi_label, rsi_status)

        with col3:
            sma14 = indicators.get("sma_14")
            st.metric("SMA 14", f"${sma14:,.2f}" if sma14 else "—")

        with col4:
            ema12 = indicators.get("ema_12")
            st.metric("EMA 12", f"${ema12:,.2f}" if ema12 else "—")

        with col5:
            macd_val = indicators.get("macd")
            st.metric("MACD", f"{macd_val:.4f}" if macd_val else "—")

        st.divider()

        # ── Charts Row ───────────────────────────────────────────────────────
        chart_col, alert_col = st.columns([3, 1])

        with chart_col:
            try:
                candles = get_candles(symbol, interval=interval, page_size=200)
                ind_history = get_indicator_history(symbol, page_size=200)
                fig = render_candlestick(candles, symbol, sma_data=ind_history)
                st.plotly_chart(fig, use_container_width=True, key="candlestick")
            except Exception as e:
                st.warning(f"Chart unavailable: {e}")

        with alert_col:
            try:
                alerts = get_alert_history(symbol=symbol, page_size=10)
                render_alert_feed(alerts)
            except Exception as e:
                st.caption(f"Alerts unavailable: {e}")

        # ── Indicator Row ────────────────────────────────────────────────────
        rsi_col, macd_col = st.columns(2)

        with rsi_col:
            try:
                ind_h = get_indicator_history(symbol, page_size=100)
                st.plotly_chart(render_rsi(ind_h), use_container_width=True, key="rsi")
            except Exception as e:
                st.caption(f"RSI unavailable: {e}")

        with macd_col:
            try:
                st.plotly_chart(render_macd(ind_h), use_container_width=True, key="macd")
            except Exception as e:
                st.caption(f"MACD unavailable: {e}")

        # ── All Prices Table ─────────────────────────────────────────────────
        with st.expander("All Prices Snapshot"):
            if all_prices:
                import pandas as pd
                df = pd.DataFrame(all_prices)[["symbol", "price", "event_time"]]
                df["price"] = df["price"].map("${:,.4f}".format)
                df["event_time"] = pd.to_datetime(df["event_time"], unit="ms", utc=True)
                st.dataframe(df, use_container_width=True)


fetch_and_render()

if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
