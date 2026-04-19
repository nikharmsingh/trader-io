"""
Pipeline Monitor — shows live data flow metrics queried directly from ClickHouse.

Metrics shown:
  - Total rows per table (trades, candles_1m, indicators)
  - Rows inserted in last 1 min / 5 min / 1 hour  (throughput)
  - Data freshness per symbol (seconds since last record)
  - Volume by symbol (bar chart)
  - Candlestick count timeline (when data was written)
"""
import os
import time

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
import requests

API_BASE = os.getenv("API_BASE_URL", "http://localhost:8000")
REFRESH  = int(os.getenv("DASHBOARD_REFRESH_INTERVAL", "5"))

st.set_page_config(
    page_title="Pipeline Monitor — Trader.IO",
    page_icon="⚡",
    layout="wide",
)

st.title("⚡ Pipeline Monitor")
st.caption("Live data flow metrics — refreshed every 5 seconds from ClickHouse")


# ── Helper ────────────────────────────────────────────────────────────────────
def ch_query(sql: str) -> list:
    """Run a raw ClickHouse SQL query via the API's health endpoint passthrough."""
    try:
        import clickhouse_connect
        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse123"),
            database=os.getenv("CLICKHOUSE_DATABASE", "trader_io"),
        )
        return client.query(sql).result_rows
    except Exception as e:
        st.warning(f"ClickHouse query failed: {e}")
        return []


placeholder = st.empty()


def render():
    with placeholder.container():

        # ── Row counts ───────────────────────────────────────────────────────
        st.subheader("Table Row Counts")
        counts_data = []
        for table in ["trades", "candles_1m", "candles_5m", "candles_15m", "indicators", "daily_ohlcv", "alerts"]:
            rows = ch_query(f"SELECT count() FROM {table}")
            counts_data.append({"Table": table, "Total Rows": rows[0][0] if rows else 0})

        counts_df = pd.DataFrame(counts_data)
        cols = st.columns(len(counts_data))
        for i, row in counts_df.iterrows():
            with cols[i]:
                st.metric(row["Table"], f"{row['Total Rows']:,}")

        st.divider()

        # ── Throughput ───────────────────────────────────────────────────────
        st.subheader("Ingestion Throughput")
        t1, t2, t3 = st.columns(3)

        def throughput(table, minutes):
            rows = ch_query(f"""
                SELECT count()
                FROM {table}
                WHERE toDateTime(
                    {'open_time' if 'candle' in table else 'event_time'} / 1000
                ) >= now() - INTERVAL {minutes} MINUTE
            """)
            return rows[0][0] if rows else 0

        with t1:
            st.metric("Trades — last 1 min",  f"{throughput('trades', 1):,}")
            st.metric("Trades — last 5 min",  f"{throughput('trades', 5):,}")
            st.metric("Trades — last 1 hour", f"{throughput('trades', 60):,}")
        with t2:
            st.metric("1m Candles — last 5 min",  f"{throughput('candles_1m', 5):,}")
            st.metric("1m Candles — last 1 hour", f"{throughput('candles_1m', 60):,}")
        with t3:
            st.metric("Indicators — last 5 min",  f"{throughput('indicators', 5):,}")
            st.metric("Indicators — last 1 hour", f"{throughput('indicators', 60):,}")

        st.divider()

        # ── Freshness per symbol ─────────────────────────────────────────────
        st.subheader("Data Freshness (seconds since last record)")
        freshness_rows = ch_query("""
            SELECT
                symbol,
                dateDiff('second', toDateTime(max(event_time) / 1000), now()) AS lag_seconds
            FROM trades
            GROUP BY symbol
            ORDER BY symbol
        """)

        if freshness_rows:
            fr_df = pd.DataFrame(freshness_rows, columns=["Symbol", "Lag (s)"])
            fig = px.bar(
                fr_df, x="Symbol", y="Lag (s)",
                color="Lag (s)",
                color_continuous_scale=["#26a69a", "#ffd700", "#ef5350"],
                title="Seconds since last trade per symbol",
                template="plotly_dark",
                height=280,
            )
            fig.update_coloraxes(showscale=False)
            st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # ── Volume by symbol ─────────────────────────────────────────────────
        col_vol, col_timeline = st.columns(2)

        with col_vol:
            st.subheader("Volume by Symbol (last 1 hour)")
            vol_rows = ch_query("""
                SELECT symbol, sum(quantity) AS volume
                FROM trades
                WHERE toDateTime(event_time / 1000) >= now() - INTERVAL 1 HOUR
                GROUP BY symbol
                ORDER BY volume DESC
            """)
            if vol_rows:
                vol_df = pd.DataFrame(vol_rows, columns=["Symbol", "Volume"])
                fig = px.bar(
                    vol_df, x="Symbol", y="Volume",
                    color="Symbol",
                    title="Trade Volume (last 1h)",
                    template="plotly_dark",
                    height=300,
                )
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

        with col_timeline:
            st.subheader("Candles Written per Minute (last 30 min)")
            timeline_rows = ch_query("""
                SELECT
                    toStartOfMinute(toDateTime(open_time / 1000)) AS minute,
                    count() AS candles
                FROM candles_1m
                WHERE toDateTime(open_time / 1000) >= now() - INTERVAL 30 MINUTE
                GROUP BY minute
                ORDER BY minute
            """)
            if timeline_rows:
                tl_df = pd.DataFrame(timeline_rows, columns=["Minute", "Candles"])
                fig = px.area(
                    tl_df, x="Minute", y="Candles",
                    title="1m Candles written per minute",
                    template="plotly_dark",
                    height=300,
                    color_discrete_sequence=["#7c4dff"],
                )
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # ── Spark UI link ─────────────────────────────────────────────────────
        st.subheader("Spark Streaming UI")
        st.info(
            "The Spark Streaming UI shows live batch statistics, input rate, "
            "processing rate, and watermark lag for each streaming query.\n\n"
            "**Open:** [http://localhost:4040/StreamingQuery](http://localhost:4040/StreamingQuery)"
        )
        st.caption(
            "If Spark is processing data you will see active queries with "
            "input rows/sec and batch duration metrics."
        )

        st.caption(f"Last updated: {pd.Timestamp.utcnow().strftime('%H:%M:%S UTC')}")


render()

time.sleep(REFRESH)
st.rerun()
