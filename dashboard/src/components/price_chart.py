"""Candlestick + volume chart component using Plotly."""
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def render_candlestick(candles: list, symbol: str, sma_data: list = None) -> go.Figure:
    if not candles:
        fig = go.Figure()
        fig.add_annotation(text="No candle data available", x=0.5, y=0.5, showarrow=False)
        return fig

    df = pd.DataFrame(candles)
    df["ts"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df = df.sort_values("ts")

    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.75, 0.25],
    )

    # Candlestick
    fig.add_trace(
        go.Candlestick(
            x=df["ts"],
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
            name=symbol,
            increasing_line_color="#26a69a",
            decreasing_line_color="#ef5350",
        ),
        row=1, col=1,
    )

    # SMA overlays
    if sma_data:
        sma_df = pd.DataFrame(sma_data)
        sma_df["ts"] = pd.to_datetime(sma_df["event_time"], unit="ms", utc=True)
        sma_df = sma_df.sort_values("ts")
        for period, color in [(7, "#ffd700"), (14, "#ff8c00"), (50, "#ff4500")]:
            col = f"sma_{period}"
            if col in sma_df.columns:
                fig.add_trace(
                    go.Scatter(
                        x=sma_df["ts"], y=sma_df[col],
                        name=f"SMA {period}",
                        line=dict(color=color, width=1, dash="dot"),
                        opacity=0.8,
                    ),
                    row=1, col=1,
                )

    # Volume bars
    colors = ["#26a69a" if c >= o else "#ef5350" for c, o in zip(df["close"], df["open"])]
    fig.add_trace(
        go.Bar(x=df["ts"], y=df["volume"], name="Volume", marker_color=colors, opacity=0.6),
        row=2, col=1,
    )

    fig.update_layout(
        title=f"{symbol} — Price Chart",
        template="plotly_dark",
        xaxis_rangeslider_visible=False,
        height=550,
        showlegend=True,
        margin=dict(l=0, r=0, t=40, b=0),
    )
    fig.update_yaxes(title_text="Price (USDT)", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)

    return fig
