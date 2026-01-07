from __future__ import annotations

from math import ceil, floor
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

DATA_PATH = Path("data/crypto_us_yields.parquet")
BTC_DATA_PATH = Path("data/btcusd_1-min_data.csv")


def to_utc_ts(value: pd.Timestamp) -> int:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return int(ts.timestamp())


@st.cache_data(show_spinner="Loading BTC price data...")
def load_btc_daily_median(
    csv_path: Path, start_ts: int | None, end_ts: int | None
) -> pd.DataFrame:
    usecols = ["Timestamp", "Close"]
    df = pd.read_csv(csv_path, usecols=usecols)
    df["Timestamp"] = pd.to_numeric(df["Timestamp"], errors="coerce")
    df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
    df = df.dropna(subset=["Timestamp", "Close"])

    if start_ts is not None:
        df = df[df["Timestamp"] >= start_ts]
    if end_ts is not None:
        df = df[df["Timestamp"] <= end_ts]

    df["date"] = (
        pd.to_datetime(df["Timestamp"], unit="s", utc=True)
        .dt.tz_convert(None)
        .dt.normalize()
    )
    daily = (
        df.groupby("date", as_index=False)["Close"]
        .median()
        .rename(columns={"Close": "median_price"})
    )
    return daily.sort_values("date")


def add_forward_returns(daily_df: pd.DataFrame, forward_days: int) -> pd.DataFrame:
    out = daily_df.sort_values("date").copy()
    out["forward_return"] = (
        out["median_price"].shift(-forward_days) / out["median_price"] - 1
    )
    return out


def build_fixed_width_bins(series: pd.Series, width: float) -> pd.Series:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return pd.Series(dtype="object")

    min_val = float(clean.min())
    max_val = float(clean.max())
    if min_val == max_val:
        edges = [min_val - width / 2, max_val + width / 2]
    else:
        start = floor(min_val / width) * width
        end = ceil(max_val / width) * width
        if start == end:
            end = start + width
        steps = int(round((end - start) / width))
        edges = [start + i * width for i in range(steps + 1)]
        if edges[-1] < max_val:
            edges.append(edges[-1] + width)

    bins = pd.cut(series, bins=edges, include_lowest=True, right=False)
    labels = bins.cat.categories
    label_map = {
        interval: f"{interval.left:.1f} to {interval.right:.1f}" for interval in labels
    }
    return bins.map(label_map)


st.set_page_config(page_title="Crypto vs US Yields", layout="wide")
st.title("Crypto vs US Treasury Yields")

if not DATA_PATH.exists():
    st.error(f"Missing data file: {DATA_PATH}")
    st.stop()

df = pd.read_parquet(DATA_PATH)
if "date" in df.columns:
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
df = df.sort_values("date")

controls = st.columns(4)
with controls[0]:
    rate_type = st.selectbox("Aave rate", ["supply", "borrow"])
with controls[1]:
    tenor = st.selectbox("Treasury tenor", ["6m", "2y", "5y", "10y"])
with controls[2]:
    ma_window = st.selectbox("Spread MA window (days)", [10, 21, 55, 100, 200], index=0)
with controls[3]:
    forward_days = st.number_input(
        "BTC forward return days", min_value=1, max_value=365, value=30, step=1
    )

aave_col = f"aave_{rate_type}_apy"
yield_col = f"yield_{tenor}"
spread_col = f"{rate_type}_minus_yield_{tenor}"

rates_df = df.set_index("date")[[aave_col, yield_col, spread_col]].copy()
rates_df[spread_col] = (
    rates_df[spread_col].rolling(window=ma_window, min_periods=1).mean()
)
plot_df = rates_df.copy()
plot_df = plot_df.rename(
    columns={
        aave_col: f"Aave {rate_type} APY",
        yield_col: f"Treasury {tenor}",
        spread_col: f"Spread ({ma_window}d MA)",
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
last_date = plot_df.index.max().date() if not plot_df.empty else "n/a"
st.caption(f"Rows: {len(plot_df)} | Last date: {last_date}")

st.subheader("BTC Forward Returns vs Rate Spread")
if not BTC_DATA_PATH.exists():
    st.warning(f"Missing BTC data file: {BTC_DATA_PATH}")
else:
    spread_series = rates_df[[spread_col]].dropna()
    if spread_series.empty:
        st.warning("No spread data available for the selected configuration.")
    else:
        spread_start = spread_series.index.min()
        spread_end = spread_series.index.max()
        start_ts = to_utc_ts(spread_start)
        end_ts = to_utc_ts(spread_end + pd.Timedelta(days=1) - pd.Timedelta(seconds=1))

        btc_daily = load_btc_daily_median(BTC_DATA_PATH, start_ts, end_ts)
        btc_returns = add_forward_returns(btc_daily, int(forward_days))

        spread_ma_df = spread_series.rename(columns={spread_col: "spread_ma"}).reset_index()
        scatter_df = spread_ma_df.merge(
            btc_returns[["date", "forward_return"]], on="date", how="inner"
        ).dropna()

        if scatter_df.empty:
            st.warning("No overlapping BTC forward returns for the selected window.")
        else:
            x_title = f"{rate_type} - {tenor} spread ({ma_window}d MA, pp)"
            y_title = f"BTC {forward_days}d forward return (median daily price)"

            scatter = (
                alt.Chart(scatter_df)
                .mark_circle(size=60, opacity=0.7)
                .encode(
                    x=alt.X("spread_ma:Q", title=x_title, axis=alt.Axis(format=".2f")),
                    y=alt.Y("forward_return:Q", title=y_title, axis=alt.Axis(format=".2%")),
                    tooltip=[
                        alt.Tooltip("date:T", title="Date"),
                        alt.Tooltip("spread_ma:Q", title="Spread (pp)", format=".2f"),
                        alt.Tooltip(
                            "forward_return:Q", title="Forward return", format=".2%"
                        ),
                    ],
                )
                .properties(height=450)
            )

            scatter_zero = (
                alt.Chart(pd.DataFrame({"y": [0]}))
                .mark_rule(color="#E53935", opacity=0.5, size=2)
                .encode(y="y:Q")
            )

            st.altair_chart(scatter_zero + scatter, use_container_width=True)

        st.subheader("Conditional BTC Forward Returns by Borrow-10y Spread")
        bin_width = st.selectbox("Spread bin width (pp)", [1.0, 2.0], index=0)

        borrow_spread = (
            df.set_index("date")["borrow_minus_yield_10y"]
            .dropna()
            .rename("borrow_minus_10y")
        )
        if borrow_spread.empty:
            st.warning("No borrow - 10y spread data available.")
        else:
            borrow_df = borrow_spread.reset_index()
            borrow_df["spread_bin"] = build_fixed_width_bins(
                borrow_df["borrow_minus_10y"], float(bin_width)
            )
            distribution_df = borrow_df.merge(
                btc_returns[["date", "forward_return"]], on="date", how="inner"
            ).dropna(subset=["spread_bin", "forward_return"])

            if distribution_df.empty:
                st.warning("No overlapping BTC forward returns for the selected bins.")
            else:
                def bin_sort_key(label: str) -> float:
                    try:
                        return float(label.split(" to ")[0])
                    except (AttributeError, ValueError):
                        return 0.0

                order = sorted(
                    distribution_df["spread_bin"].dropna().unique(), key=bin_sort_key
                )
                box = (
                    alt.Chart(distribution_df)
                    .mark_boxplot(size=20, extent="min-max")
                    .encode(
                        x=alt.X(
                            "spread_bin:N",
                            title="Borrow - 10y spread (pp bins)",
                            sort=order,
                        ),
                        y=alt.Y(
                            "forward_return:Q",
                            title=f"BTC {forward_days}d forward return",
                            axis=alt.Axis(format=".2%"),
                        ),
                        tooltip=[
                            alt.Tooltip("spread_bin:N", title="Spread bin"),
                            alt.Tooltip(
                                "forward_return:Q", title="Forward return", format=".2%"
                            ),
                        ],
                    )
                    .properties(height=420)
                )

                win_table = (
                    distribution_df.groupby("spread_bin")["forward_return"]
                    .agg(
                        median="median",
                        p25=lambda s: s.quantile(0.25),
                        p75=lambda s: s.quantile(0.75),
                        win_rate=lambda s: (s > 0).mean(),
                        count="count",
                    )
                    .reset_index()
                )
                win_table = win_table.sort_values(
                    "spread_bin", key=lambda col: col.map(bin_sort_key)
                )

                st.altair_chart(box, use_container_width=True)
                st.caption("Summary stats per bin (returns in decimal form).")
                st.dataframe(win_table, use_container_width=True)
