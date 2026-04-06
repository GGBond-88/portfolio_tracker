# Portfolio Tracker

A personal multi-asset portfolio tracking pipeline in Python.

The project converts raw broker transaction exports into analysis-ready portfolio data, with a modular tool-by-tool workflow and CSV outputs that are easy to inspect manually.

## Current Scope

The pipeline **implements Tools `t0`–`t7` end to end**:

- **`t0`–`t1`**: standardize tradelist exports and replay into daily holdings (and exited positions)
- **`t2`–`t3`**: Yahoo Finance prices, local-currency valuation, then daily FX into USD (`priced_holdings_usd.csv`)
- **`t4`**: daily portfolio NAV aggregate (`portfolio_nav.csv`) for the configured portfolio and asset-class filter
- **`t5`**: transaction-level equity BUY/SELL cash flows in USD (`portfolio_cash_flows.csv`)
- **`t6`**: daily TWR-style sub-period returns, period-to-date links, and annualized IRR (`portfolio_returns.csv`)
- **`t7`**: fundamentals snapshot (Yahoo `info` + RSI-14) for latest-date live holdings (`fundamentals_snapshot.csv`)

**Still missing / next priorities** (not implemented yet):

- **SQLite** (or similar) export while keeping inspectable CSVs
- **Full-asset cash flows** (dividends, fees, deposits/withdrawals, FD, PE, non-equity classes) beyond equity BUY/SELL
- **BI dashboard / Power BI–ready summary tables** and a richer reporting layer
- **Returns and NAV** extended to whole-portfolio, multi-asset accounting (today defaults to FGI + Equities + `equity_sub` scope)

## Current Pipeline

The pipeline currently runs these tools in sequence:

1. `t0`: standardize the raw tradelist and map Yahoo tickers
2. `t1`: replay trades into daily holdings snapshots and exited positions
3. `t2`: fetch market prices and calculate market value / unrealized P&L / total P&L
4. `t3`: fetch daily FX rates and convert priced holdings into USD
5. `t4`: aggregate daily portfolio NAV summary (default: FGI + Equities, `scope = equity_sub`)
6. `t5`: build transaction-level portfolio cash flows (currently FGI equity BUY/SELL scope)
7. `t6`: calculate portfolio returns (daily TWR + period returns + annualized IRR)
8. `t7`: fetch fundamentals snapshot for latest-date current holdings

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
  Aggregates daily portfolio-level NAV summary and a simple NAV day-over-day return column
- `tools/t5_cash_flow_builder.py`
  Builds signed transaction-level cash flow rows from standardized tradelist + FX cache
- `tools/t6_return_calculator.py`
  Calculates return series from NAV + cash flows (TWR and annualized MWR/IRR)
- `tools/t7_fundamentals_snapshot.py`
  Fetches Yahoo fundamentals and RSI-14 for latest-date holdings snapshot
- `tests/`
  Pytest test suite for the tool modules
- `pipeline.py`
  Single entry point to run `t0 -> t1 -> t2 -> t3 -> t4 -> t5 -> t6 -> t7`

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
- `unreatlized_pnl` — deprecated typo alias of `unrealized_pnl`, still written for CSV compatibility (`# DEPRECATED: remove after migration` in Tool 2)

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

Current default scope in pipeline (see `pipeline.py`):

- `portfolio = FGI`
- `asset class = Equities`
- `scope = equity_sub` (same string as `t5` / `t6`; labels the equity sub-portfolio slice)

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

`daily_return_pct` is a **simple day-over-day NAV change**, not time-weighted return:

- `daily_return_pct = (NAV_today - NAV_yesterday) / NAV_yesterday`
- NAV here is `total_market_value_usd`
- on the first available date, or if previous NAV is 0 or missing, `daily_return_pct` is empty
- **For cash-flow-adjusted daily TWR and linked cumulative series, use `t6` output `portfolio_returns.csv`** (`daily_return_twr`, `cumulative_twr`, etc.)

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

### `data/portfolio_returns.csv`

Output of Tool 6.

This table combines `portfolio_nav.csv` and `portfolio_cash_flows.csv` to produce daily TWR, period-to-date returns, and full-period annualized MWR/IRR.

Current default scope in pipeline:

- `portfolio = FGI`
- `scope = equity_sub`

Important columns:

- `date`
- `portfolio`
- `scope`
- `nav_usd`
- `daily_net_cf_usd`
- `daily_return_twr`
- `cumulative_twr`
- `mtd_return`
- `qtd_return`
- `ytd_return`
- `itd_return`
- `irr_annualized_full`
- `irr_annualized_itd`

Method notes:

- daily TWR uses a modified-Dietz style sub-period formula with start-of-day cash flow handling
- when prior-day NAV ≤ 0, `daily_return_twr` is NaN (undefined); chain-linked cumulative TWR skips those days (gross factor 1.0)
- `daily_net_cf_usd` is summed by date from transaction-level cash flow rows
- `itd_return` equals `cumulative_twr`
- `irr_annualized_full` is a single full-sample annualized IRR (same value on every row), solved from timed cash flows and annualized with `(1 + daily_irr)^365 - 1`
- `irr_annualized_itd` is a rolling inception-to-date IRR: recomputed on month-ends, Sundays, the second row, and the last row, with forward-fill between those dates; row 0 is NaN
- IRR cash flows: NAV is booked as an inflow on the last date; the offset-0 outflow is `daily_net_cf_usd` on the first date when it is non-zero (investor perspective: BUY negative). If that day has no net flow (`0`), the solver uses `-nav_usd` on day 0 instead (existing portfolio with no day-0 contribution in the cash-flow file). If `|daily_net_cf_usd|` on day 0 is below half of `nav_usd` on that day (and NAV is positive), the solver still uses `-nav_usd` at offset 0 so a small top-up on an existing book does not dominate the IRR

### `data/fundamentals_snapshot.csv`

Output of Tool 7.

This is a point-in-time fundamentals snapshot for latest-date holdings in the selected portfolio.

Current default scope in pipeline:

- `portfolio = FGI`
- latest available holdings date from `priced_holdings_usd.csv`
- includes only rows with `quantity > 0` and non-empty Yahoo ticker

Important columns:

- `snapshot_date`
- `portfolio`
- `yahoo_ticker`
- `security_name`
- `currency`
- `quantity`
- `market_value_usd`
- `weight_pct`
- `sector`
- `industry`
- `pe_ttm`
- `pb_ratio`
- `eps_ttm`
- `book_value_per_share`
- `dividend_yield`
- `annual_dividend_rate`
- `market_cap`
- `week_52_high`
- `week_52_low`
- `rsi_14`

Notes:

- fundamentals come from `yfinance.Ticker(ticker).info`
- RSI is computed from 1-month price history using a 14-period close-based RSI
- this tool is snapshot-only and does not maintain a persistent cache

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
7. `t6`
8. `t7`

### Run tools individually

```bash
python -m tools.t0_tradelist_standardizer
python -m tools.t1_holdings_builder
python -m tools.t2_price_fetcher
python -m tools.t3_fx_converter
python -m tools.t4_portfolio_nav
python -m tools.t5_cash_flow_builder
python -m tools.t6_return_calculator
python -m tools.t7_fundamentals_snapshot
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
- simple NAV day-over-day `daily_return_pct` on portfolio NAV (distinct from `t6` TWR)
- equity BUY/SELL cash flow extraction and FX conversion into USD
- portfolio return calculations (TWR period series + annualized IRR)
- fundamentals snapshot extraction with mocked Yahoo API tests

## Known Limitations

- **`t4` `daily_return_pct`** is a plain ratio of consecutive NAVs. It is **not** time-weighted return. **Formal TWR** (with daily cash-flow adjustment in the numerator) is in **`t6` → `portfolio_returns.csv`** (`daily_return_twr`, `cumulative_twr`, …).
- **`t5` cash flows** include **only equity `BUY` / `SELL`** (plus FX into USD). They omit **dividends, interest, fees, and external cash in/out**. **`t6` IRR / MWR** is therefore **economically incomplete** until the cash-flow layer matches how you define investor contributions and distributions.
- **`t6` TWR**: when **prior-day NAV ≤ 0**, **`daily_return_twr` is NaN** (undefined); cumulative TWR does not compound those days.
- Some Yahoo tickers still require manual overrides in `data/ticker_overrides.csv`
- Yahoo Finance may not provide prices or FX for every instrument, warrant, or delisted security
- **Tool 1** focuses on **equity** holdings replay, not full multi-asset portfolio accounting
- **Default pipeline scope** is **FGI + Equities + `equity_sub`** for NAV, cash flows, and returns; other portfolios and asset classes need explicit parameter changes or future extensions
- **Storage is CSV-first**; SQLite export and BI-ready summary tables are not implemented yet

## Suggested Next Steps

1. **Enrich cash flows** beyond equity BUY/SELL (deposits, withdrawals, dividends, interest, fees, FD, PE, other asset classes) and align them with IRR / MWR definitions
2. **Broaden NAV and returns** from the current FGI + Equities + `equity_sub` slice to full-portfolio views where needed
3. **Add SQLite export** (and optional BI staging tables) while **keeping CSV outputs** for manual inspection
4. **Dashboard / Power BI**: curated summary tables and refresh workflow on top of the existing CSVs or SQLite

## Design Direction For Extensions

The codebase already has a **first cash-flow slice** (`t5`), **NAV** (`t4`), and a **return engine** (`t6`). Further work should **extend** rather than replace that shape:

1. **Cash flow layer** — normalized external and internal movements (still separate from pure trading rows where that matters for returns)
2. **Portfolio NAV layer** — daily market value (and eventually cash) consistent with the return definition
3. **Return engine** — same TWR / IRR patterns as `t6`, fed by the expanded flows and NAV
4. **Database export** — SQLite (or similar) with both detail and BI-friendly aggregates

Keep the design **extensible** beyond **FGI + Equities** so bonds, PE, fixed deposits, and other asset classes can be added without a full rewrite.
