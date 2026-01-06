from __future__ import annotations

from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

DATA_PATH = Path("data/crypto_us_yields.parquet")

st.set_page_config(page_title="Crypto vs US Yields", layout="wide")
st.title("Crypto vs US Treasury Yields")

if not DATA_PATH.exists():
    st.error(f"Missing data file: {DATA_PATH}")
    st.stop()

df = pd.read_parquet(DATA_PATH)
if "date" in df.columns:
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
df = df.sort_values("date")

rate_type = st.selectbox("Aave rate", ["supply", "borrow"])
tenor = st.selectbox("Treasury tenor", ["6m", "2y", "5y", "10y"])

aave_col = f"aave_{rate_type}_apy"
yield_col = f"yield_{tenor}"
spread_col = f"{rate_type}_minus_yield_{tenor}"

plot_df = df.set_index("date")[[aave_col, yield_col, spread_col]].copy()
plot_df[spread_col] = (
    plot_df[spread_col].rolling(window=10, min_periods=1).mean()
)
plot_df = plot_df.rename(
    columns={
        aave_col: f"Aave {rate_type} APY",
        yield_col: f"Treasury {tenor}",
        spread_col: "Spread (10d MA)",
    }
).dropna(how="all")

long_df = plot_df.reset_index().melt(
    id_vars="date", var_name="series", value_name="value"
)
long_df = long_df.dropna(subset=["value"])

selection = alt.selection_multi(fields=["series"], bind="legend")

base_chart = (
    alt.Chart(long_df)
    .mark_line()
    .encode(
        x=alt.X("date:T", title="Date"),
        y=alt.Y("value:Q", title="Percent"),
        color=alt.Color("series:N", title="Series"),
        opacity=alt.condition(selection, alt.value(1.0), alt.value(0.15)),
        tooltip=["date:T", "series:N", "value:Q"],
    )
    .add_selection(selection)
    .properties(height=450)
)

zero_rule = (
    alt.Chart(pd.DataFrame({"y": [0]}))
    .mark_rule(color="#E53935", size=2.5)
    .encode(y="y:Q")
)

chart = zero_rule + base_chart

st.altair_chart(chart, use_container_width=True)
st.caption(f"Rows: {len(plot_df)} | Last date: {plot_df.index.max().date()}")
