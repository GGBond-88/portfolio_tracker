# Portfolio Tracker

A personal multi-asset portfolio tracking pipeline in Python.

The current project focus is:

- standardize raw trade lists
- replay transactions into daily holdings
- fetch market prices from Yahoo Finance
- calculate daily market value and basic P&L

## What This Project Does

The project turns raw broker/exported transaction files into analysis-ready outputs.

Current pipeline:

1. `t0`: standardize the raw tradelist and map Yahoo tickers
2. `t1`: replay trades into daily holdings snapshots
3. `t2`: fetch prices and calculate market value / unrealized P&L / total P&L

## Project Structure

- `data/`
  Raw input files, intermediate CSVs, and output CSVs
- `tools/t0_tradelist_standardizer.py`
  Standardizes tradelist input for downstream tools
- `tools/t1_holdings_builder.py`
  Replays transactions into daily holdings and exited positions
- `tools/t2_price_fetcher.py`
  Fetches Yahoo Finance prices and creates priced holdings
- `tests/`
  Pytest test suite
- `pipeline.py`
  Single entry point to run `t0 -> t1 -> t2`

## Input Files

The pipeline currently expects files under `data/`.

Main raw inputs:

- `data/*Tradelist*.csv`
  Raw tradelist export. The latest matching file is used by `t0`.
- `data/*Securities bookings*.csv`
  Reference file used by `t0` to enrich ISIN mappings.
- `data/ticker_overrides.csv`
  Optional manual mapping file for Bloomberg reference -> Yahoo ticker.

Example `ticker_overrides.csv`:

```csv
bloomberg_reference,yahoo_ticker
CLAR SP EQUITY,A17U.SI
11 HK EQUITY,0011_OL.HK
```

## Output Files

### `data/t0_standardized_tradelist.csv`

Output of Tool 0.

Purpose:

- cleans and standardizes raw tradelist columns
- adds `Order type`, `Booking date`, `ISIN`, `Quantity`, `Execution price`
- adds `Symbol` and `Yahoo Ticker`

### `data/daily_holdings.csv`

Output of Tool 1.

Each row represents a daily snapshot of one open position.

Important columns:

- `date`
- `entry_date`
- `Portfolio`
- `Ticker / ISIN / Reference`
- `Yahoo Ticker`
- `quantity`
- `avg_cost`
- `cost_basis`
- `realized_pnl`

### `data/exited_positions.csv`

Output of Tool 1.

Each row represents one fully exited position lifecycle.

Important columns:

- `symbol`
- `Portfolio`
- `entry_date`
- `exit_date`
- `total_realized_pnl`

### `data/priced_holdings.csv`

Output of Tool 2.

This is `daily_holdings.csv` plus market prices and valuation fields.

Important columns:

- `market_price`
- `market_value`
- `unrealized_pnl`
- `total_pnl`

### `data/prices_cache.csv`

Price cache used by Tool 2 to avoid repeated Yahoo Finance downloads.

## Installation

Recommended Python version:

- Python `3.11+`

Install dependencies:

```bash
python -m pip install -e ".[dev]"
```

This installs:

- runtime dependencies from `pyproject.toml`
- development dependencies such as `pytest`

## How To Run

### Run the full pipeline

```bash
python pipeline.py
```

This runs:

1. `t0`
2. `t1`
3. `t2`

### Run tools individually

```bash
python -m tools.t0_tradelist_standardizer
python -m tools.t1_holdings_builder
python -m tools.t2_price_fetcher
```

## How To Test

Run all tests:

```bash
python -m pytest -q
```

Current tests cover:

- Yahoo ticker standardization and manual overrides
- long / short transaction replay logic
- multi-portfolio separation in holdings replay
- price fetching and cache reuse

## Current Known Limitations

- Some Yahoo tickers still require manual override entries in `data/ticker_overrides.csv`
- Yahoo Finance may not provide prices for every instrument or warrant
- Current outputs are in instrument currency; portfolio-level FX conversion is not built yet
- Return calculations such as IRR, money-weighted return, and time-weighted return are not built yet

## Suggested Next Steps

Recommended next development stages:

1. Add a return engine for IRR / MWR / TWR
2. Add FX conversion and base-currency NAV
3. Store outputs in SQL tables
4. Connect Power BI for visualization
