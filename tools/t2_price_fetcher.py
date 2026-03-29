"""Tool 2: fetch market prices and produce priced daily holdings."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import logging
from pathlib import Path
from typing import Any

import pandas as pd
import yfinance as yf

LOGGER = logging.getLogger(__name__)

_CACHE_COLUMNS = ["yahoo_ticker", "date", "market_price", "updated_at_utc"]


def build_priced_holdings(
    data_dir: Path,
    holdings_path: Path,
    output_path: Path,
    cache_path: Path,
) -> pd.DataFrame:
    """Fetch prices for holdings history and output priced holdings CSV."""
    if not holdings_path.exists():
        raise FileNotFoundError(f"Holdings file not found: {holdings_path}")

    holdings_df = pd.read_csv(holdings_path, dtype=str)
    if holdings_df.empty:
        empty_df = holdings_df.copy()
        for column in ["market_price", "market_value", "unrealized_pnl", "unreatlized_pnl", "total_pnl"]:
            empty_df[column] = pd.Series(dtype=float)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        empty_df.to_csv(output_path, index=False)
        _empty_cache(cache_path=cache_path)
        return empty_df

    ticker_column = _pick_ticker_column(holdings_df=holdings_df)
    if ticker_column is None:
        raise ValueError("Missing ticker columns. Expected one of: Yahoo Ticker, symbol, Symbol")
    if "date" not in holdings_df.columns:
        raise ValueError("Holdings file must contain 'date' column.")

    if "entry_date" not in holdings_df.columns:
        LOGGER.warning("Holdings missing 'entry_date'. Please regenerate via revised Tool 1.")

    priced_df = holdings_df.copy()
    priced_df["date"] = priced_df["date"].map(_parse_date)
    priced_df["yahoo_ticker"] = priced_df[ticker_column].fillna("").astype(str).str.strip().str.upper()
    if "symbol" in priced_df.columns:
        priced_df["yahoo_ticker"] = priced_df["yahoo_ticker"].replace("", pd.NA).fillna(
            priced_df["symbol"].fillna("").astype(str).str.strip().str.upper()
        )
    priced_df["yahoo_ticker"] = priced_df["yahoo_ticker"].fillna("").astype(str).str.strip().str.upper()

    priced_df["quantity"] = priced_df.get("quantity", 0.0).map(_parse_number)
    priced_df["cost_basis"] = priced_df.get("cost_basis", 0.0).map(_parse_number)
    priced_df["realized_pnl"] = priced_df.get("realized_pnl", 0.0).map(_parse_number)

    tickers = sorted(t for t in priced_df["yahoo_ticker"].unique().tolist() if t)
    date_min = priced_df["date"].min()
    date_max = priced_df["date"].max()

    cache_df = _load_cache(cache_path=cache_path)
    for ticker in tickers:
        cache_df = _ensure_cache_coverage(
            cache_df=cache_df,
            ticker=ticker,
            start_date=date_min,
            end_date=date_max,
        )
    cache_df = _dedupe_cache(cache_df=cache_df)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_df.to_csv(cache_path, index=False)

    price_map_df = _build_daily_price_map(
        holdings_df=priced_df[["date", "yahoo_ticker"]],
        cache_df=cache_df,
    )
    priced_df = priced_df.merge(price_map_df, how="left", on=["date", "yahoo_ticker"])
    priced_df["market_price"] = pd.to_numeric(priced_df["market_price"], errors="coerce")

    priced_df["market_value"] = (priced_df["quantity"] * priced_df["market_price"]).round(8)
    priced_df["unrealized_pnl"] = (priced_df["market_value"] - priced_df["cost_basis"]).round(8)
    priced_df["unreatlized_pnl"] = priced_df["unrealized_pnl"]
    priced_df["total_pnl"] = (priced_df["unrealized_pnl"] + priced_df["realized_pnl"]).round(8)

    priced_df = priced_df.sort_values(by=["date", "yahoo_ticker"], kind="stable").reset_index(drop=True)
    priced_df["date"] = priced_df["date"].map(lambda d: d.isoformat())

    output_path.parent.mkdir(parents=True, exist_ok=True)
    priced_df.to_csv(output_path, index=False)

    missing_prices = int(priced_df["market_price"].isna().sum())
    LOGGER.info("Loaded holdings from %s (%s rows).", holdings_path, len(priced_df))
    LOGGER.info("Saved priced holdings to %s", output_path)
    LOGGER.info("Saved prices cache to %s", cache_path)
    if missing_prices:
        LOGGER.warning("Missing market prices on %s rows.", missing_prices)
    return priced_df


def _empty_cache(cache_path: Path) -> None:
    """Create an empty cache file when no holdings exist."""
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(columns=_CACHE_COLUMNS).to_csv(cache_path, index=False)


def _pick_ticker_column(holdings_df: pd.DataFrame) -> str | None:
    """Pick the most preferred ticker column from holdings output."""
    for candidate in ["Yahoo Ticker", "yahoo_ticker", "symbol", "Symbol"]:
        if candidate in holdings_df.columns:
            return candidate
    return None


def _load_cache(cache_path: Path) -> pd.DataFrame:
    """Load existing prices cache or return empty cache DataFrame."""
    if not cache_path.exists():
        return pd.DataFrame(columns=_CACHE_COLUMNS)
    cache_df = pd.read_csv(cache_path, dtype=str)
    for column in _CACHE_COLUMNS:
        if column not in cache_df.columns:
            cache_df[column] = ""
    cache_df["yahoo_ticker"] = cache_df["yahoo_ticker"].fillna("").astype(str).str.strip().str.upper()
    cache_df["date"] = cache_df["date"].map(_parse_date)
    cache_df["market_price"] = cache_df["market_price"].map(_parse_number)
    cache_df["updated_at_utc"] = cache_df["updated_at_utc"].fillna("").astype(str)
    return cache_df[_CACHE_COLUMNS].copy()


def _ensure_cache_coverage(
    cache_df: pd.DataFrame,
    ticker: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Fetch ticker history when cache does not fully cover required range."""
    if not ticker:
        return cache_df
    ticker_cache = cache_df[cache_df["yahoo_ticker"] == ticker]
    need_fetch = ticker_cache.empty
    if not need_fetch:
        cache_min = ticker_cache["date"].min()
        cache_max = ticker_cache["date"].max()
        need_fetch = bool(cache_min > start_date or cache_max < end_date)
    if not need_fetch:
        return cache_df

    fetch_start = start_date - timedelta(days=7)
    fetch_end = end_date + timedelta(days=1)
    fetched_df = _fetch_yahoo_history(ticker=ticker, start_date=fetch_start, end_date=fetch_end)
    if fetched_df.empty:
        LOGGER.warning("No price data fetched for %s", ticker)
        return cache_df

    if cache_df.empty:
        return _dedupe_cache(cache_df=fetched_df)
    merged = pd.concat([cache_df, fetched_df], ignore_index=True)
    return _dedupe_cache(cache_df=merged)


def _fetch_yahoo_history(ticker: str, start_date: date, end_date: date) -> pd.DataFrame:
    """Download daily close prices from Yahoo Finance via yfinance."""
    history = yf.download(
        tickers=ticker,
        start=start_date.isoformat(),
        end=end_date.isoformat(),
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )
    if history is None or history.empty:
        return pd.DataFrame(columns=_CACHE_COLUMNS)

    close_series = history["Close"]
    if isinstance(close_series, pd.DataFrame):
        if ticker in close_series.columns:
            close_series = close_series[ticker]
        else:
            close_series = close_series.iloc[:, 0]

    close_series = close_series.dropna()
    if close_series.empty:
        return pd.DataFrame(columns=_CACHE_COLUMNS)

    updated_at = datetime.now(tz=timezone.utc).isoformat()
    fetched_df = pd.DataFrame(
        {
            "yahoo_ticker": ticker,
            "date": pd.to_datetime(close_series.index).date,
            "market_price": close_series.astype(float).round(8).values,
            "updated_at_utc": updated_at,
        }
    )
    return fetched_df[_CACHE_COLUMNS]


def _dedupe_cache(cache_df: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate cache on (ticker, date), keeping most recent record."""
    if cache_df.empty:
        return pd.DataFrame(columns=_CACHE_COLUMNS)
    ordered = cache_df.copy()
    ordered["yahoo_ticker"] = ordered["yahoo_ticker"].fillna("").astype(str).str.strip().str.upper()
    ordered["date"] = ordered["date"].map(_parse_date)
    ordered["market_price"] = ordered["market_price"].map(_parse_number)
    ordered["updated_at_utc"] = ordered["updated_at_utc"].fillna("").astype(str)
    ordered = ordered.sort_values(by=["yahoo_ticker", "date", "updated_at_utc"], kind="stable")
    deduped = ordered.drop_duplicates(subset=["yahoo_ticker", "date"], keep="last").reset_index(drop=True)
    return deduped[_CACHE_COLUMNS]


def _build_daily_price_map(holdings_df: pd.DataFrame, cache_df: pd.DataFrame) -> pd.DataFrame:
    """Build per-day market price map by forward-filling trading-day closes."""
    rows: list[dict[str, Any]] = []
    for ticker, group in holdings_df.groupby("yahoo_ticker"):
        ticker = str(ticker).strip().upper()
        if not ticker:
            continue
        ticker_dates = sorted(group["date"].map(_parse_date).unique().tolist())
        ticker_cache = cache_df[cache_df["yahoo_ticker"] == ticker]
        if ticker_cache.empty:
            for ticker_date in ticker_dates:
                rows.append({"date": ticker_date, "yahoo_ticker": ticker, "market_price": pd.NA})
            continue
        price_series = (
            ticker_cache.sort_values(by="date", kind="stable")
            .drop_duplicates(subset=["date"], keep="last")
            .set_index("date")["market_price"]
        )
        aligned = price_series.reindex(ticker_dates, method="ffill")
        for ticker_date, market_price in zip(ticker_dates, aligned.values, strict=False):
            rows.append({"date": ticker_date, "yahoo_ticker": ticker, "market_price": market_price})

    if not rows:
        return pd.DataFrame(columns=["date", "yahoo_ticker", "market_price"])
    return pd.DataFrame(rows, columns=["date", "yahoo_ticker", "market_price"])


def _parse_date(value: Any) -> date:
    """Parse input into date object."""
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        raise ValueError("Date value is empty.")
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d.%m.%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return pd.to_datetime(text).date()


def _parse_number(value: Any) -> float:
    """Parse potentially formatted numeric values into float."""
    text = str(value).strip() if value is not None else ""
    if not text or text.lower() == "nan":
        return 0.0
    cleaned = text.replace(",", "")
    try:
        return round(float(cleaned), 8)
    except ValueError:
        return 0.0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build_priced_holdings(
        data_dir=Path("data"),
        holdings_path=Path("data/daily_holdings.csv"),
        output_path=Path("data/priced_holdings.csv"),
        cache_path=Path("data/prices_cache.csv"),
    )
