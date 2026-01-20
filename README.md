# Crypto_US_yields

Compare Aave lending/borrowing APY to US Treasury yields and visualize the rate
spread against BTC forward returns.

**What this repo does**
- Pulls Aave supply/borrow APY from a Dune query.
- Pulls US Treasury yields (6m, 2y, 5y, 10y) from FRED.
- Joins the daily series, optionally forward-filling Treasury yields.
- Computes Aave minus Treasury spreads.
- Streamlit app plots the time series and a scatter of BTC forward returns vs
  the spread moving average.

**Repo layout**
- `main.py` fetches Aave + FRED data, builds spreads, writes
  `data/crypto_us_yields.parquet`.
- `streamlit_app.py` reads the parquet + BTC 1-min CSV and renders the UI.
- `data/btcusd_1-min_data.csv` is the BTC price input.
- `btcReturnsSpec.md` documents the BTC forward returns view requirements.

**Setup (uv)**
- `uv sync`
- Create a `.env` file (optional) or export environment variables:
  - `DUNE_API_KEY`
  - `FRED_API_KEY`

Example `.env`:
```bash
DUNE_API_KEY=...
FRED_API_KEY=...
```

**Run all scripts**
1. Build the dataset (writes `data/crypto_us_yields.parquet`):
   ```bash
   uv run python main.py
   ```
   Options:
   - `--query-id 4280536` (override Dune query id)
   - `--output data/crypto_us_yields.parquet` (custom output path)
   - `--no-ffill-yields` (use inner-joined business days only)

2. Launch the Streamlit app:
   ```bash
   uv run streamlit run streamlit_app.py
   ```

**How the data flows**
- `main.py` reads Aave rates from Dune, auto-detects the date/supply/borrow
  columns, and converts decimals to percent when needed.
- FRED series are requested from the earliest Aave date, merged by day, then
  spreads are computed for each tenor.
- `streamlit_app.py` loads the parquet, lets you choose rate type + tenor +
  MA window, and overlays BTC forward returns computed from daily median price.
