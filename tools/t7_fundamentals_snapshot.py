"""Tool 7: fetch a fundamentals snapshot for latest-date current holdings."""

from __future__ import annotations

from datetime import date, datetime
import logging
from pathlib import Path
from typing import Any

import pandas as pd
import yfinance as yf

LOGGER = logging.getLogger(__name__)

_OUTPUT_COLUMNS = [
    "snapshot_date",
    "portfolio",
    "yahoo_ticker",
    "security_name",
    "currency",
    "quantity",
    "market_value_usd",
    "weight_pct",
    "sector",
    "industry",
    "pe_ttm",
    "pb_ratio",
    "eps_ttm",
    "book_value_per_share",
    "dividend_yield",
    "annual_dividend_rate",
    "market_cap",
    "week_52_high",
    "week_52_low",
    "rsi_14",
]

_FUNDAMENTAL_KEYS = {
    "sector": "sector",
    "industry": "industry",
    "pe_ttm": "trailingPE",
    "pb_ratio": "priceToBook",
    "eps_ttm": "trailingEps",
    "book_value_per_share": "bookValue",
    "dividend_yield": "dividendYield",
    "annual_dividend_rate": "trailingAnnualDividendRate",
    "market_cap": "marketCap",
    "week_52_high": "fiftyTwoWeekHigh",
    "week_52_low": "fiftyTwoWeekLow",
}


def build_fundamentals_snapshot(
    data_dir: Path,
    priced_holdings_usd_path: Path,
    output_path: Path,
    portfolio_filter: str | None = "FGI",
) -> pd.DataFrame:
    """Build a point-in-time fundamentals snapshot for latest-date holdings."""
    _ = data_dir  # Keep interface consistent with other tools.
    if not priced_holdings_usd_path.exists():
        raise FileNotFoundError(f"Priced holdings USD file not found: {priced_holdings_usd_path}")

    holdings_df = pd.read_csv(priced_holdings_usd_path, dtype=str)
    if holdings_df.empty:
        empty_df = pd.DataFrame(columns=_OUTPUT_COLUMNS)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        empty_df.to_csv(output_path, index=False)
        return empty_df

    date_column = _pick_existing_column(holdings_df, ["date"])
    portfolio_column = _pick_existing_column(holdings_df, ["Portfolio", "portfolio"])
    ticker_column = _pick_existing_column(holdings_df, ["yahoo_ticker", "Yahoo Ticker"])
    quantity_column = _pick_existing_column(holdings_df, ["quantity", "Quantity"])
    market_value_usd_column = _pick_existing_column(holdings_df, ["market_value_usd"])
    currency_column = _pick_existing_column(holdings_df, ["Currency", "currency"])
    security_name_column = _pick_existing_column(holdings_df, ["Security name", "name"])
    missing_labels = []
    for column_name, label in [
        (date_column, "date"),
        (portfolio_column, "Portfolio/portfolio"),
        (ticker_column, "yahoo_ticker/Yahoo Ticker"),
        (quantity_column, "quantity/Quantity"),
        (market_value_usd_column, "market_value_usd"),
        (currency_column, "Currency/currency"),
        (security_name_column, "Security name/name"),
    ]:
        if column_name is None:
            missing_labels.append(label)
    if missing_labels:
        raise ValueError(f"Input file missing required columns: {', '.join(missing_labels)}")

    snapshot_df = holdings_df.copy()
    snapshot_df["date"] = snapshot_df[date_column].map(_parse_date)
    snapshot_df["portfolio"] = snapshot_df[portfolio_column].fillna("").astype(str).str.strip()
    snapshot_df["yahoo_ticker"] = snapshot_df[ticker_column].fillna("").astype(str).str.strip()
    snapshot_df["security_name"] = snapshot_df[security_name_column].fillna("").astype(str).str.strip()
    snapshot_df["currency"] = snapshot_df[currency_column].fillna("").astype(str).str.strip().str.upper()
    snapshot_df["quantity"] = snapshot_df[quantity_column].map(_parse_number)
    snapshot_df["market_value_usd"] = snapshot_df[market_value_usd_column].map(_parse_number)

    if portfolio_filter is not None:
        normalized = str(portfolio_filter).strip().casefold()
        snapshot_df = snapshot_df[snapshot_df["portfolio"].str.casefold() == normalized].copy()
    if snapshot_df.empty:
        empty_df = pd.DataFrame(columns=_OUTPUT_COLUMNS)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        empty_df.to_csv(output_path, index=False)
        LOGGER.info("Saved fundamentals snapshot to %s (0 rows after filters).", output_path)
        return empty_df

    latest_date = snapshot_df["date"].max()
    snapshot_df = snapshot_df[snapshot_df["date"] == latest_date].copy()
    snapshot_df = snapshot_df[snapshot_df["quantity"] > 0].copy()
    snapshot_df = snapshot_df[snapshot_df["yahoo_ticker"] != ""].copy()
    if snapshot_df.empty:
        empty_df = pd.DataFrame(columns=_OUTPUT_COLUMNS)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        empty_df.to_csv(output_path, index=False)
        LOGGER.info("Saved fundamentals snapshot to %s (0 current holdings on latest date).", output_path)
        return empty_df

    positions = (
        snapshot_df.sort_values(by=["date", "portfolio", "yahoo_ticker"], kind="stable")
        .groupby("yahoo_ticker", as_index=False, dropna=False)
        .agg(
            portfolio=("portfolio", "first"),
            security_name=("security_name", "first"),
            currency=("currency", "first"),
            quantity=("quantity", "sum"),
            market_value_usd=("market_value_usd", "sum"),
        )
        .reset_index(drop=True)
    )
    total_market_value = float(positions["market_value_usd"].sum())
    if total_market_value > 0:
        positions["weight_pct"] = (positions["market_value_usd"] / total_market_value).round(10)
    else:
        positions["weight_pct"] = 0.0

    fundamentals_rows: list[dict[str, Any]] = []
    for _, row in positions.iterrows():
        ticker = str(row["yahoo_ticker"]).strip()
        LOGGER.info("Fetching fundamentals for ticker %s", ticker)
        fundamentals = _fetch_ticker_fundamentals(ticker=ticker)
        fundamentals_rows.append(
            {
                "snapshot_date": latest_date.isoformat(),
                "portfolio": row["portfolio"],
                "yahoo_ticker": ticker,
                "security_name": row["security_name"],
                "currency": row["currency"],
                "quantity": float(row["quantity"]),
                "market_value_usd": float(row["market_value_usd"]),
                "weight_pct": float(row["weight_pct"]),
                **fundamentals,
            }
        )

    output_df = pd.DataFrame(fundamentals_rows, columns=_OUTPUT_COLUMNS)
    output_df = output_df.sort_values(by=["weight_pct"], ascending=False, kind="stable").reset_index(drop=True)
    for column in _OUTPUT_COLUMNS:
        if column in {"snapshot_date", "portfolio", "yahoo_ticker", "security_name", "currency", "sector", "industry"}:
            continue
        output_df[column] = pd.to_numeric(output_df[column], errors="coerce")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_df.to_csv(output_path, index=False)
    LOGGER.info("Saved fundamentals snapshot to %s", output_path)
    LOGGER.info("Fundamentals snapshot rows: %s", len(output_df))
    return output_df


def _fetch_ticker_fundamentals(ticker: str) -> dict[str, Any]:
    """Fetch Yahoo fundamentals and 1-month history to compute RSI-14."""
    fallback = {column: pd.NA for column in list(_FUNDAMENTAL_KEYS.keys()) + ["rsi_14"]}
    try:
        ticker_obj = yf.Ticker(ticker)
        info = ticker_obj.info if isinstance(ticker_obj.info, dict) else {}
        history = ticker_obj.history(period="1mo")
        result = {}
        for output_column, info_key in _FUNDAMENTAL_KEYS.items():
            result[output_column] = info.get(info_key, pd.NA)
        result["rsi_14"] = _compute_rsi_14(history=history)
        return result
    except Exception as exc:  # pragma: no cover - defensive for API/runtime failures
        LOGGER.warning("Failed to fetch fundamentals for %s: %s", ticker, exc)
        return fallback


def _compute_rsi_14(history: pd.DataFrame) -> float | pd._libs.missing.NAType:
    """Compute RSI-14 from close prices using average gains/losses over 14 periods."""
    if history is None or history.empty or "Close" not in history.columns:
        return pd.NA
    close_series = pd.to_numeric(history["Close"], errors="coerce").dropna()
    if len(close_series) < 15:
        return pd.NA
    delta = close_series.diff().dropna()
    gains = delta.clip(lower=0.0)
    losses = -delta.clip(upper=0.0)
    avg_gain = gains.tail(14).mean()
    avg_loss = losses.tail(14).mean()
    if pd.isna(avg_gain) or pd.isna(avg_loss):
        return pd.NA
    if avg_loss == 0:
        if avg_gain == 0:
            return 50.0
        return 100.0
    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return round(float(rsi), 8)


def _pick_existing_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Return the first matching column name from candidates."""
    for column in candidates:
        if column in df.columns:
            return column
    return None


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
        return round(float(cleaned), 10)
    except ValueError:
        return 0.0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build_fundamentals_snapshot(
        data_dir=Path("data"),
        priced_holdings_usd_path=Path("data/priced_holdings_usd.csv"),
        output_path=Path("data/fundamentals_snapshot.csv"),
        portfolio_filter="FGI",
    )
