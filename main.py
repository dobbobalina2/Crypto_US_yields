from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from dotenv import load_dotenv

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
FRED_SERIES = {
    "DGS6MO": "yield_6m",
    "DGS2": "yield_2y",
    "DGS5": "yield_5y",
    "DGS10": "yield_10y",
}


def pick_column(df: pd.DataFrame, preferred: Iterable[str], required_terms: Iterable[str]) -> str | None:
    for name in preferred:
        if name in df.columns:
            return name
    required_terms = [term.lower() for term in required_terms]
    matches = [
        col
        for col in df.columns
        if all(term in col.lower() for term in required_terms)
    ]
    if matches:
        return sorted(matches, key=len)[0]
    return None


def pick_date_column(df: pd.DataFrame) -> str | None:
    preferred = ["date", "day", "dt", "timestamp", "block_date"]
    for name in preferred:
        if name in df.columns:
            return name
    for col in df.columns:
        lowered = col.lower()
        if "date" in lowered or lowered.endswith("day"):
            return col
    return None


def maybe_convert_percent(series: pd.Series, label: str) -> pd.Series:
    series = pd.to_numeric(series, errors="coerce")
    max_val = series.dropna().abs().max()
    if pd.isna(max_val):
        return series
    if max_val <= 1.5:
        print(f"{label} looks like decimal; converting to percent.")
        return series * 100
    return series


def fetch_aave_apy(dune_api_key: str, query_id: int) -> pd.DataFrame:
    dune = DuneClient(dune_api_key)
    query = QueryBase(query_id=query_id)
    df = dune.run_query_dataframe(query)

    print("Dune columns:", df.columns.tolist())
    print(df.head(3))
    print(df.dtypes)

    date_col = pick_date_column(df)
    supply_col = pick_column(
        df,
        preferred=["aave_supply_apy", "supply_apy", "supply_apr", "supply_rate", "supply"],
        required_terms=["supply"],
    )
    borrow_col = pick_column(
        df,
        preferred=[
            "aave_borrow_apy",
            "borrow_apy",
            "borrow_apr",
            "borrow_rate",
            "avg_variableRate",
            "variable_rate",
            "variable",
            "borrow",
        ],
        required_terms=["borrow"],
    )
    if borrow_col is None:
        borrow_col = pick_column(
            df,
            preferred=["avg_variableRate", "variable_rate", "variable"],
            required_terms=["variable"],
        )

    if not date_col or not supply_col or not borrow_col:
        raise ValueError(
            "Unable to find required columns. "
            f"date={date_col}, supply={supply_col}, borrow={borrow_col}."
        )

    out = df[[date_col, supply_col, borrow_col]].copy()
    out = out.rename(
        columns={
            date_col: "date",
            supply_col: "aave_supply_apy",
            borrow_col: "aave_borrow_apy",
        }
    )
    out["date"] = (
        pd.to_datetime(out["date"], errors="coerce", utc=True)
        .dt.tz_convert(None)
        .dt.normalize()
    )
    out["aave_supply_apy"] = maybe_convert_percent(out["aave_supply_apy"], "aave_supply_apy")
    out["aave_borrow_apy"] = maybe_convert_percent(out["aave_borrow_apy"], "aave_borrow_apy")

    return out.dropna(subset=["date"])


def fetch_fred_series(series_id: str, api_key: str, observation_start: str) -> pd.DataFrame:
    limit = 100000
    offset = 0
    observations: list[dict[str, str]] = []
    total_count = None

    while True:
        params = {
            "series_id": series_id,
            "api_key": api_key,
            "file_type": "json",
            "observation_start": observation_start,
            "offset": offset,
            "limit": limit,
        }
        response = requests.get(FRED_BASE, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()

        if total_count is None:
            total_count = int(payload.get("count", 0))
            print(
                f"{series_id} count={total_count} offset={payload.get('offset')} "
                f"limit={payload.get('limit')}"
            )

        page_obs = payload.get("observations", [])
        observations.extend(page_obs)
        print(f"{series_id} page offset={offset} limit={limit} rows={len(page_obs)}")

        if total_count is None or offset + limit >= total_count:
            break
        offset += limit

    print(f"{series_id} rows_collected={len(observations)}")
    if not observations:
        return pd.DataFrame(columns=["date", "value"])

    df = pd.DataFrame(observations)[["date", "value"]]
    df["date"] = (
        pd.to_datetime(df["date"], errors="coerce", utc=True)
        .dt.tz_convert(None)
        .dt.normalize()
    )
    df["value"] = pd.to_numeric(df["value"].replace(".", pd.NA), errors="coerce")
    df = df.sort_values("date")
    return df


def build_yield_frame(
    api_key: str, observation_start: str
) -> pd.DataFrame:
    merged = None
    for series_id, col_name in FRED_SERIES.items():
        series_df = fetch_fred_series(series_id, api_key, observation_start)
        max_date = series_df["date"].max()
        null_count = series_df["value"].isna().sum()
        print(f"{series_id} max_date={max_date} nulls={null_count}")

        series_df = series_df.rename(columns={"value": col_name})
        if merged is None:
            merged = series_df
        else:
            merged = merged.merge(series_df, on="date", how="outer")

    if merged is None:
        return pd.DataFrame(columns=["date"] + list(FRED_SERIES.values()))
    return merged.sort_values("date")


def add_spreads(df: pd.DataFrame) -> pd.DataFrame:
    for tenor in ["6m", "2y", "5y", "10y"]:
        yield_col = f"yield_{tenor}"
        df[f"supply_minus_yield_{tenor}"] = df["aave_supply_apy"] - df[yield_col]
        df[f"borrow_minus_yield_{tenor}"] = df["aave_borrow_apy"] - df[yield_col]
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Aave APY vs US Treasury yield series.")
    parser.add_argument("--query-id", type=int, default=4280536)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/crypto_us_yields.parquet"),
    )
    parser.add_argument(
        "--no-ffill-yields",
        action="store_true",
        help="Use inner-joined business days only.",
    )
    args = parser.parse_args()

    load_dotenv()
    dune_api_key = os.getenv("DUNE_API_KEY")
    fred_api_key = os.getenv("FRED_API_KEY")
    if not dune_api_key or not fred_api_key:
        raise RuntimeError("Missing DUNE_API_KEY or FRED_API_KEY in environment.")

    aave_df = fetch_aave_apy(dune_api_key, args.query_id)
    if aave_df.empty:
        raise RuntimeError("Dune query returned no rows.")

    start_date = aave_df["date"].min().date().isoformat()
    yields_df = build_yield_frame(fred_api_key, start_date)

    joined_inner = aave_df.merge(yields_df, on="date", how="inner")
    print(f"Inner-joined rows: {len(joined_inner)}")

    if args.no_ffill_yields:
        joined = joined_inner
    else:
        all_dates = pd.date_range(
            start=aave_df["date"].min(), end=aave_df["date"].max(), freq="D"
        )
        yields_daily = (
            yields_df.set_index("date").reindex(all_dates).sort_index().ffill()
        )
        yields_daily = yields_daily.reset_index().rename(columns={"index": "date"})
        joined = aave_df.merge(yields_daily, on="date", how="left")

    joined = add_spreads(joined)
    print(f"Final rows: {len(joined)}")
    print(f"Min date: {joined['date'].min()} | Max date: {joined['date'].max()}")
    for tenor in ["6m", "2y", "5y", "10y"]:
        for side in ["supply", "borrow"]:
            col = f"{side}_minus_yield_{tenor}"
            col_min = joined[col].min()
            col_max = joined[col].max()
            print(f"{col} min={col_min} max={col_max}")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    joined.to_parquet(args.output, index=False)
    print(f"Wrote {len(joined)} rows to {args.output}")


if __name__ == "__main__":
    main()
