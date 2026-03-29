# Portfolio Tracker

A personal multi-asset portfolio tracking pipeline in Python.

The project converts raw broker transaction exports into analysis-ready portfolio data, with a modular tool-by-tool workflow and CSV outputs that are easy to inspect manually.

## Current Scope

The current implemented pipeline focuses on:

- standardizing raw tradelist exports
- replaying transactions into daily holdings
- fetching market prices from Yahoo Finance
- converting holdings into USD using daily FX rates
- calculating basic market value and P&L fields

The next planned stage is:

- building a proper cash flow layer
- computing portfolio-level MWR / IRR and TWR
- exporting consolidated outputs into SQLite
- creating BI-friendly summary tables for Power BI

## Current Pipeline

The pipeline currently runs these tools in sequence:

1. `t0`: standardize the raw tradelist and map Yahoo tickers
2. `t1`: replay trades into daily holdings snapshots and exited positions
3. `t2`: fetch market prices and calculate market value / unrealized P&L / total P&L
4. `t3`: fetch daily FX rates and convert priced holdings into USD
5. `t4`: aggregate daily portfolio NAV summary (currently FGI + Equities scope)
6. `t5`: build transaction-level portfolio cash flows (currently FGI equity BUY/SELL scope)

## Project Structure

- `data/`
  Raw input files, intermediate CSVs, caches, and output CSVs
- `tools/t0_tradelist_standardizer.py`
  Standardizes tradelist input for downstream tools
- `tools/t1_holdings_builder.py`
  Replays transactions into daily holdings and exited positions
- `tools/t2_price_fetcher.py`
  Fetches Yahoo Finance prices and creates priced holdings
- `tools/t3_fx_converter.py`
  Fetches daily FX rates and creates USD-valued priced holdings
- `tools/t4_portfolio_nav.py`
  Aggregates daily portfolio-level NAV summary and demo daily return
- `tools/t5_cash_flow_builder.py`
  Builds signed transaction-level cash flow rows from standardized tradelist + FX cache
- `tests/`
  Pytest test suite for the tool modules
- `pipeline.py`
  Single entry point to run `t0 -> t1 -> t2 -> t3 -> t4 -> t5`

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
- `Asset class`
- `Ticker / ISIN / Reference`
- `Security name`
- `Currency`
- `Yahoo Ticker`
- `quantity`
- `avg_cost`
- `cost_basis`
- `realized_pnl`

Notes:

- positions are tracked by `Portfolio + symbol + currency`
- `isin` is treated as metadata, not as the identity key
- this avoids splitting the same portfolio position when source ISIN data is inconsistent

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
- `realized_pnl`
- `total_pnl`

Definitions:

- `market_value = quantity * market_price`
- `unrealized_pnl = market_value - cost_basis`
- `total_pnl = unrealized_pnl + realized_pnl`

### `data/prices_cache.csv`

Price cache used by Tool 2 to avoid repeated Yahoo Finance downloads.

### `data/priced_holdings_usd.csv`

Output of Tool 3.

This is `priced_holdings.csv` plus daily FX conversion into USD.

Important columns:

- `fx_rate_to_usd`
- `cost_basis_usd`
- `market_value_usd`
- `unrealized_pnl_usd`
- `realized_pnl_usd`
- `total_pnl_usd`

For rows where instrument `Currency` is already `USD`, `fx_rate_to_usd = 1.0`.

### `data/fx_cache.csv`

FX cache used by Tool 3 to avoid repeated Yahoo Finance FX downloads.

### `data/portfolio_nav.csv`

Output of Tool 4.

This is a daily portfolio-level summary aggregated from `priced_holdings_usd.csv`.

Current default scope in pipeline:

- `portfolio = FGI`
- `asset class = Equities`
- `scope = fgi_equities`

Important columns:

- `date`
- `portfolio`
- `scope`
- `position_count`
- `total_market_value_usd`
- `total_cost_basis_usd`
- `total_unrealized_pnl_usd`
- `total_realized_pnl_usd`
- `total_pnl_usd`
- `daily_return_pct`

`daily_return_pct` is currently a demo-level day-over-day return:

- `daily_return_pct = (NAV_today - NAV_yesterday) / NAV_yesterday`
- NAV here is `total_market_value_usd`
- on first available date (or if previous NAV is 0), `daily_return_pct` is empty
- this is not yet true TWR; proper TWR will be built after the cash flow layer

### `data/portfolio_cash_flows.csv`

Output of Tool 5.

This is a transaction-level cash flow table (no daily aggregation) built from `t0_standardized_tradelist.csv` and `fx_cache.csv`.

Current default scope in pipeline:

- `portfolio = FGI`
- `scope = equity_sub`
- includes only `Asset Type = Equities` with `Order type in (BUY, SELL)`

Important columns:

- `date`
- `portfolio`
- `scope`
- `cf_type` (`BUY` or `SELL`)
- `ticker`
- `asset_class`
- `currency`
- `amount_local`
- `fx_rate_to_usd`
- `amount_usd`

Sign convention:

- `BUY` -> negative cash flow (cash out into positions)
- `SELL` -> positive cash flow (cash returned from positions)

FX conversion notes:

- `amount_usd = amount_local * fx_rate_to_usd`
- non-USD currencies use `fx_cache.csv` pair format `{CURRENCY}USD=X`
- rates are forward-filled by date when transaction date is a non-trading day
- USD transactions always use `fx_rate_to_usd = 1.0`

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
4. `t3`
5. `t4`
6. `t5`

### Run tools individually

```bash
python -m tools.t0_tradelist_standardizer
python -m tools.t1_holdings_builder
python -m tools.t2_price_fetcher
python -m tools.t3_fx_converter
python -m tools.t4_portfolio_nav
python -m tools.t5_cash_flow_builder
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
- inconsistent-ISIN position merge behavior
- price fetching and price cache reuse
- FX conversion and FX cache reuse
- portfolio NAV aggregation with configurable scope filters
- demo daily return calculation on portfolio NAV
- equity BUY/SELL cash flow extraction and FX conversion into USD

## Current Known Limitations

- Some Yahoo tickers still require manual override entries in `data/ticker_overrides.csv`
- Yahoo Finance may not provide prices for every instrument, warrant, or delisted security
- Tool 1 currently focuses on equity holdings replay, not full multi-asset portfolio accounting
- Portfolio-level return calculations such as IRR, MWR, and TWR are not built yet
- Tool 5 currently builds only equity BUY/SELL transaction cash flows (not yet external deposits, withdrawals, dividends, fees, FD, PE, or full multi-asset scope)
- The current storage layer is CSV-first; SQLite and BI-ready summary tables are still pending

## Suggested Next Steps

Recommended next development stages:

1. Extend the cash flow layer beyond equity BUY/SELL (deposits, withdrawals, dividends, fees, FD, PE, and other asset classes)
2. Build portfolio-level MWR / IRR and TWR, starting with `Portfolio = FGI` and `Asset class = Equities`
3. Add SQLite export while still preserving CSV outputs for manual inspection
4. Build BI-friendly summary tables for Power BI

## Design Direction For The Next Stage

The next returns engine should likely follow this structure:

1. `cash flow layer`
   A normalized daily table of portfolio cash movements such as deposits, withdrawals, fees, dividends, and trading cash flows
2. `portfolio NAV layer`
   A daily portfolio-level table that aggregates holdings market value and cash balances
3. `return engine`
   Portfolio-level daily TWR and MWR / IRR outputs
4. `database export`
   SQLite tables that preserve both detailed rows and BI-friendly summary views

The recommended first implementation scope is:

- `Portfolio = FGI`
- `Asset class = Equities`
- keep the design extensible so bonds, PE, fixed deposits, and other asset classes can be added later
