# BTC Forward Returns Scatterplot Spec

## Goal
Add a Streamlit view that plots a scatterplot where:
- X axis = moving average (MA) of the spread between an Aave rate and a US rate.
- Y axis = X days forward returns of BTC.

## Data Sources
- BTC price data: `data/btcusd_1-min_data.csv`
  - 6 columns: `Timestamp`, `Open`, `High`, `Low`, `Close`, `Volume`
  - `Timestamp` is Unix seconds for each 60s window
- Aave rate data and US rate data are existing sources in the app (confirm current data pipeline).

## Core Calculations
1) **BTC daily median price**
   - Convert `Timestamp` to UTC datetime.
   - Slice BTC data to the overlap window with the chosen rate series (Aave/US) before resampling.
   - Resample to daily frequency.
   - Use median of the selected price field (recommend `Close`) per day.

2) **BTC forward returns**
   - Let `median_price[d]` be daily median price.
   - For configurable `forward_days` (integer):
     - `forward_return[d] = (median_price[d + forward_days] / median_price[d]) - 1`
   - Align dates so only valid forward returns are plotted.

3) **Rate spread**
   - Spread = `aave_rate[tenor] - us_rate[tenor]`.
   - Tenors/rates used in the spread must be configurable.

4) **Moving average of spread**
   - MA window is configurable, choose from: 10, 21, 55, 100, 200 (days).
   - `spread_ma = rolling_mean(spread, window=ma_window)`.

5) **Join for plotting**
   - Align BTC forward returns with `spread_ma` on date.
   - Keep only the overlapping date range (Aave likely starts ~2020).
   - Drop rows with missing values before plotting.

## Streamlit UI Requirements
- Controls:
  - Aave rate selector (tenor / rate series).
  - US rate selector (tenor / rate series).
  - MA window selector (10, 21, 55, 100, 200).
  - Forward days selector (integer, e.g., 1–365).
- Plot:
  - Scatterplot with x = `spread_ma`, y = `forward_return`.
  - Add axis labels that reflect the selected tenors and window.

## Implementation Notes
- Confirm the current dataframes and column names for Aave and US rates in `streamlit_app.py`.
- Use daily alignment for rate data (ensure same timezone and date index as BTC daily series).
- If rate data is at higher frequency, resample to daily (e.g., last or mean).
- Add guardrails for insufficient data (e.g., when MA window or forward_days exceeds available history).

## Acceptance Criteria
- Scatterplot renders with configurable tenors, MA window, and forward days.
- Forward returns use daily median BTC price from `data/btcusd_1-min_data.csv`.
- The spread MA updates when any control changes.
- Missing data is handled gracefully without app crashes.
