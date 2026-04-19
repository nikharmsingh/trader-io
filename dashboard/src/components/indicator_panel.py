"""RSI and MACD indicator panels."""
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def render_rsi(indicator_history: list) -> go.Figure:
    if not indicator_history:
        return go.Figure()

    df = pd.DataFrame(indicator_history)
    df["ts"] = pd.to_datetime(df["event_time"], unit="ms", utc=True)
    df = df.sort_values("ts").dropna(subset=["rsi_14"])

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["ts"], y=df["rsi_14"],
        name="RSI (14)",
        line=dict(color="#7c4dff", width=2),
        fill="tozeroy",
        fillcolor="rgba(124,77,255,0.1)",
    ))
    fig.add_hline(y=70, line_dash="dash", line_color="#ef5350", annotation_text="Overbought (70)")
    fig.add_hline(y=30, line_dash="dash", line_color="#26a69a", annotation_text="Oversold (30)")
    fig.add_hline(y=50, line_dash="dot", line_color="gray", opacity=0.4)

    fig.update_layout(
        title="RSI (14)",
        template="plotly_dark",
        height=220,
        yaxis=dict(range=[0, 100], title="RSI"),
        showlegend=False,
        margin=dict(l=0, r=0, t=35, b=0),
    )
    return fig


def render_macd(indicator_history: list) -> go.Figure:
    if not indicator_history:
        return go.Figure()

    df = pd.DataFrame(indicator_history)
    df["ts"] = pd.to_datetime(df["event_time"], unit="ms", utc=True)
    df = df.sort_values("ts").dropna(subset=["macd"])

    colors = ["#26a69a" if v >= 0 else "#ef5350" for v in df["macd_histogram"].fillna(0)]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["ts"], y=df["macd_histogram"],
        name="Histogram", marker_color=colors, opacity=0.7,
    ))
    fig.add_trace(go.Scatter(
        x=df["ts"], y=df["macd"],
        name="MACD", line=dict(color="#2196f3", width=1.5),
    ))
    fig.add_trace(go.Scatter(
        x=df["ts"], y=df["macd_signal"],
        name="Signal", line=dict(color="#ff9800", width=1.5),
    ))

    fig.update_layout(
        title="MACD (12, 26, 9)",
        template="plotly_dark",
        height=220,
        showlegend=True,
        margin=dict(l=0, r=0, t=35, b=0),
    )
    return fig
