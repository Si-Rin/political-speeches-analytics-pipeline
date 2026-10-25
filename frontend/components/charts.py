"""Shared altair chart helpers for the dashboard pages."""
import altair as alt
import pandas as pd
import streamlit as st


def hbar(df: pd.DataFrame, label: str, value: str, height_per_row: int = 26, fmt: str = ",.2f", color: str = "#4c78a8"):
    """Horizontal bars with value labels (sorted)"""
    if df is None or df.empty:
        st.caption("No data.")
        return

    is_dark = st.context.theme.type == "dark"
    text_color = "#e6edf3" if is_dark else "#1f2328"  # a text color for light and dark mode

    chart = (
        alt.Chart(df)
        .mark_bar(color=color)
        .encode(
            x=alt.X(f"{value}:Q", title=None),
            y=alt.Y(f"{label}:N", sort="-x", title=None),
            tooltip=[label, alt.Tooltip(f"{value}:Q", format=fmt)],
        )
        .properties(height=max(120, height_per_row * len(df)))
    )
    text = chart.mark_text(align="left", dx=3, color=text_color).encode(text=alt.Text(f"{value}:Q", format=fmt))
    st.altair_chart(chart + text, width="stretch")


def donut(df: pd.DataFrame, label: str, value: str, height: int = 260, fmt: str = ",d"):
    """Donut chart with value labels"""
    if df is None or df.empty:
        st.caption("No data.")
        return
    st.altair_chart(
        alt.Chart(df).mark_arc(innerRadius=55).encode(
            theta=f"{value}:Q",
            color=alt.Color(f"{label}:N", legend=alt.Legend(orient="bottom", title=None)),
            tooltip=[label, alt.Tooltip(f"{value}:Q", format=fmt)],
        ).properties(height=height),
        width="stretch",
    )
