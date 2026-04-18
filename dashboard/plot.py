"""
Dashboard generator — produces a self-contained HTML file with:
  - One row per company (primary target shown first)
  - Left subplot: bar chart of QoQ CapEx % change; bars ≥40% in red
  - Right subplot: QFG flag (0/1) as a step line + scatter
  - Anomaly quarters (Spike=1 AND Flag=0) marked with a star annotation
"""

from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def generate_dashboard(
    results: dict[str, dict],
    spike_threshold: float,
    output_path: str | Path,
) -> Path:
    """
    results structure:
      {
        ticker: {
          "name": str,
          "capex": pd.DataFrame,   # columns: period_end, value_usd, qoq_pct, spike
          "qfg":   dict,           # {period_end: {"flag": 0|1, ...}}
        }
      }
    Returns the resolved output path.
    """
    tickers = list(results.keys())
    n       = len(tickers)

    fig = make_subplots(
        rows=n, cols=2,
        subplot_titles=_subplot_titles(results),
        horizontal_spacing=0.10,
        vertical_spacing=0.12,
    )

    threshold_pct = spike_threshold * 100

    for row_idx, ticker in enumerate(tickers, start=1):
        entry  = results[ticker]
        name   = entry["name"]
        df     = entry["capex"]
        qfg    = entry["qfg"]

        if df.empty:
            _add_no_data(fig, row_idx, ticker)
            continue

        # Merge QFG flags into df
        df = df.copy()
        df["qfg_flag"] = df["period_end"].map(
            lambda d: qfg.get(d, {}).get("flag", None)
        )
        df["anomaly"] = df["spike"] & (df["qfg_flag"] == 0)

        labels = df["period_end"].dt.strftime("%Y-Q%q") if hasattr(df["period_end"].dt, "quarter") else [
            _quarter_label(d) for d in df["period_end"]
        ]

        # --- CapEx bar chart ---
        bar_colors = [
            "#d62728" if s else "#1f77b4"   # red = spike, blue = normal
            for s in df["spike"]
        ]
        fig.add_trace(
            go.Bar(
                x=labels,
                y=df["qoq_pct"].round(1),
                marker_color=bar_colors,
                name=f"{ticker} CapEx QoQ%",
                showlegend=False,
                hovertemplate="<b>%{x}</b><br>QoQ: %{y:.1f}%<extra></extra>",
            ),
            row=row_idx, col=1,
        )
        # Threshold reference line
        fig.add_hline(
            y=threshold_pct,
            line_dash="dash",
            line_color="rgba(200,0,0,0.4)",
            annotation_text=f"{int(threshold_pct)}% threshold",
            annotation_position="top right",
            row=row_idx, col=1,
        )

        # --- QFG flag step chart ---
        valid_qfg = df.dropna(subset=["qfg_flag"])
        fig.add_trace(
            go.Scatter(
                x=[_quarter_label(d) for d in valid_qfg["period_end"]],
                y=valid_qfg["qfg_flag"],
                mode="lines+markers",
                line={"shape": "hv", "color": "#2ca02c", "width": 2},
                marker={"size": 10},
                name=f"{ticker} QFG",
                showlegend=False,
                hovertemplate="<b>%{x}</b><br>QFG Flag: %{y}<extra></extra>",
            ),
            row=row_idx, col=2,
        )

        # --- Anomaly annotations ---
        for _, arow in df[df["anomaly"]].iterrows():
            ql = _quarter_label(arow["period_end"])
            fig.add_annotation(
                x=ql, y=arow["qoq_pct"] + 5,
                text="★ ANOMALY",
                showarrow=True, arrowhead=2,
                font={"color": "red", "size": 12},
                arrowcolor="red",
                row=row_idx, col=1,
            )

        # Y-axis labels
        fig.update_yaxes(title_text="QoQ % Change", row=row_idx, col=1)
        fig.update_yaxes(
            title_text="QFG Flag",
            tickvals=[0, 1],
            ticktext=["0 (No Justification)", "1 (Justified)"],
            range=[-0.2, 1.2],
            row=row_idx, col=2,
        )

    fig.update_layout(
        title={
            "text": "Sprint Dashboard — CapEx Spike & QFG Flag Analysis",
            "font": {"size": 20},
        },
        height=350 * n + 100,
        template="plotly_white",
        margin={"t": 100, "b": 60, "l": 80, "r": 40},
    )

    out = Path(output_path)
    out.parent.mkdir(exist_ok=True)
    fig.write_html(str(out), include_plotlyjs="cdn")
    return out


def _quarter_label(d: datetime) -> str:
    q = (d.month - 1) // 3 + 1
    return f"{d.year}-Q{q}"


def _subplot_titles(results: dict) -> list[str]:
    titles = []
    for ticker, entry in results.items():
        titles.append(f"{entry['name']} ({ticker}) — CapEx QoQ %")
        titles.append(f"{entry['name']} ({ticker}) — QFG Flag")
    return titles


def _add_no_data(fig: go.Figure, row: int, ticker: str) -> None:
    for col in (1, 2):
        fig.add_annotation(
            text=f"No XBRL data found for {ticker}",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font={"color": "gray", "size": 14},
            row=row, col=col,
        )
