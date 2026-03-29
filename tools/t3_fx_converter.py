"""Tool 3: convert priced holdings into USD using daily FX rates."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import logging
from pathlib import Path
from typing import Any

import pandas as pd
import yfinance as yf

LOGGER = logging.getLogger(__name__)

_FX_CACHE_COLUMNS = ["fx_pair", "date", "fx_rate_to_usd", "updated_at_utc"]
_USD_NUMERIC_SOURCE_COLUMNS = [
    "cost_basis",
    "market_value",
    "unrealized_pnl",
    "realized_pnl",
    "total_pnl",
]


def build_fx_converted_holdings(
    data_dir: Path,
    priced_holdings_path: Path,
    output_path: Path,
    fx_cache_path: Path,
) -> pd.DataFrame:
    """Read priced holdings, attach FX rates, and write USD-enriched output."""
    _ = data_dir  # Keep consistent interface with other tools.
    if not priced_holdings_path.exists():
        raise FileNotFoundError(f"Priced holdings file not found: {priced_holdings_path}")

    priced_df = pd.read_csv(priced_holdings_path, dtype=str)
    if priced_df.empty:
        empty_df = priced_df.copy()
        empty_df["fx_rate_to_usd"] = pd.Series(dtype=float)
        for column in _USD_NUMERIC_SOURCE_COLUMNS:
            empty_df[f"{column}_usd"] = pd.Series(dtype=float)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        empty_df.to_csv(output_path, index=False)
        _empty_cache(cache_path=fx_cache_path)
        return empty_df

    if "date" not in priced_df.columns:
        raise ValueError("Priced holdings file must contain 'date' column.")
    currency_column = _pick_currency_column(priced_df=priced_df)
    if currency_column is None:
        raise ValueError("Missing currency column. Expected one of: Currency, currency")

    enriched_df = priced_df.copy()
    enriched_df["date"] = enriched_df["date"].map(_parse_date)
    enriched_df["_currency_norm"] = (
        enriched_df[currency_column].fillna("").astype(str).str.strip().str.upper()
    )

    for source_column in _USD_NUMERIC_SOURCE_COLUMNS:
        enriched_df[source_column] = enriched_df.get(source_column, 0.0).map(_parse_number)

    currencies = sorted(c for c in enriched_df["_currency_norm"].unique().tolist() if c and c != "USD")
    date_min = enriched_df["date"].min()
    date_max = enriched_df["date"].max()

    cache_df = _load_cache(cache_path=fx_cache_path)
    for currency in currencies:
        cache_df = _ensure_fx_cache_coverage(
            cache_df=cache_df,
            fx_pair=_to_fx_pair(currency=currency),
            start_date=date_min,
            end_date=date_max,
        )
    cache_df = _dedupe_cache(cache_df=cache_df)
    fx_cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_df.to_csv(fx_cache_path, index=False)

    enriched_df["fx_pair"] = enriched_df["_currency_norm"].map(_to_fx_pair)
    fx_map = _build_daily_fx_map(holdings_df=enriched_df[["date", "fx_pair"]], cache_df=cache_df)
    enriched_df = enriched_df.merge(fx_map, how="left", on=["date", "fx_pair"])
    enriched_df["fx_rate_to_usd"] = pd.to_numeric(enriched_df["fx_rate_to_usd"], errors="coerce")
    enriched_df.loc[enriched_df["_currency_norm"] == "USD", "fx_rate_to_usd"] = 1.0

    for source_column in _USD_NUMERIC_SOURCE_COLUMNS:
        usd_column = f"{source_column}_usd"
        enriched_df[usd_column] = (enriched_df[source_column] * enriched_df["fx_rate_to_usd"]).round(8)

    enriched_df = enriched_df.drop(columns=["fx_pair", "_currency_norm"])
    enriched_df = enriched_df.sort_values(by=["date"], kind="stable").reset_index(drop=True)
    enriched_df["date"] = enriched_df["date"].map(lambda value: value.isoformat())

    output_path.parent.mkdir(parents=True, exist_ok=True)
    enriched_df.to_csv(output_path, index=False)

    missing_fx = int(enriched_df["fx_rate_to_usd"].isna().sum())
    LOGGER.info("Loaded priced holdings from %s (%s rows).", priced_holdings_path, len(enriched_df))
    LOGGER.info("Saved FX cache to %s", fx_cache_path)
    LOGGER.info("Saved USD priced holdings to %s", output_path)
    if missing_fx:
        LOGGER.warning("Missing fx_rate_to_usd on %s rows.", missing_fx)
    return enriched_df


def _pick_currency_column(priced_df: pd.DataFrame) -> str | None:
    """Pick currency column from priced holdings."""
    for candidate in ["Currency", "currency"]:
        if candidate in priced_df.columns:
            return candidate
    return None


def _to_fx_pair(currency: str) -> str:
    """Map instrument currency code to Yahoo FX pair symbol against USD."""
    normalized = str(currency).strip().upper()
    if not normalized or normalized == "USD":
        return "USDUSD=X"
    return f"{normalized}USD=X"


def _empty_cache(cache_path: Path) -> None:
    """Create an empty FX cache file when no holdings exist."""
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(columns=_FX_CACHE_COLUMNS).to_csv(cache_path, index=False)


def _load_cache(cache_path: Path) -> pd.DataFrame:
    """Load existing FX cache or return empty cache DataFrame."""
    if not cache_path.exists():
        return pd.DataFrame(columns=_FX_CACHE_COLUMNS)
    cache_df = pd.read_csv(cache_path, dtype=str)
    for column in _FX_CACHE_COLUMNS:
        if column not in cache_df.columns:
            cache_df[column] = ""
    cache_df["fx_pair"] = cache_df["fx_pair"].fillna("").astype(str).str.strip().str.upper()
    cache_df["date"] = cache_df["date"].map(_parse_date)
    cache_df["fx_rate_to_usd"] = cache_df["fx_rate_to_usd"].map(_parse_number)
    cache_df["updated_at_utc"] = cache_df["updated_at_utc"].fillna("").astype(str)
    return cache_df[_FX_CACHE_COLUMNS].copy()


def _ensure_fx_cache_coverage(
    cache_df: pd.DataFrame,
    fx_pair: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Fetch FX history when cache does not fully cover required range."""
    if not fx_pair or fx_pair == "USDUSD=X":
        return cache_df
    pair_cache = cache_df[cache_df["fx_pair"] == fx_pair]
    need_fetch = pair_cache.empty
    if not need_fetch:
        cache_min = pair_cache["date"].min()
        cache_max = pair_cache["date"].max()
        need_fetch = bool(cache_min > start_date or cache_max < end_date)
    if not need_fetch:
        return cache_df

    fetch_start = start_date - timedelta(days=7)
    fetch_end = end_date + timedelta(days=1)
    fetched_df = _fetch_yahoo_fx_history(fx_pair=fx_pair, start_date=fetch_start, end_date=fetch_end)
    if fetched_df.empty:
        LOGGER.warning("No FX data fetched for %s", fx_pair)
        return cache_df

    if cache_df.empty:
        return _dedupe_cache(cache_df=fetched_df)
    merged = pd.concat([cache_df, fetched_df], ignore_index=True)
    return _dedupe_cache(cache_df=merged)


def _fetch_yahoo_fx_history(fx_pair: str, start_date: date, end_date: date) -> pd.DataFrame:
    """Download daily close FX rates from Yahoo Finance via yfinance."""
    history = yf.download(
        tickers=fx_pair,
        start=start_date.isoformat(),
        end=end_date.isoformat(),
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )
    if history is None or history.empty:
        return pd.DataFrame(columns=_FX_CACHE_COLUMNS)

    close_series = history["Close"]
    if isinstance(close_series, pd.DataFrame):
        if fx_pair in close_series.columns:
            close_series = close_series[fx_pair]
        else:
            close_series = close_series.iloc[:, 0]

    close_series = close_series.dropna()
    if close_series.empty:
        return pd.DataFrame(columns=_FX_CACHE_COLUMNS)

    updated_at = datetime.now(tz=timezone.utc).isoformat()
    fetched_df = pd.DataFrame(
        {
            "fx_pair": fx_pair,
            "date": pd.to_datetime(close_series.index).date,
            "fx_rate_to_usd": close_series.astype(float).round(10).values,
            "updated_at_utc": updated_at,
        }
    )
    return fetched_df[_FX_CACHE_COLUMNS]


def _dedupe_cache(cache_df: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate FX cache on (pair, date), keeping most recent record."""
    if cache_df.empty:
        return pd.DataFrame(columns=_FX_CACHE_COLUMNS)
    ordered = cache_df.copy()
    ordered["fx_pair"] = ordered["fx_pair"].fillna("").astype(str).str.strip().str.upper()
    ordered["date"] = ordered["date"].map(_parse_date)
    ordered["fx_rate_to_usd"] = ordered["fx_rate_to_usd"].map(_parse_number)
    ordered["updated_at_utc"] = ordered["updated_at_utc"].fillna("").astype(str)
    ordered = ordered.sort_values(by=["fx_pair", "date", "updated_at_utc"], kind="stable")
    deduped = ordered.drop_duplicates(subset=["fx_pair", "date"], keep="last").reset_index(drop=True)
    return deduped[_FX_CACHE_COLUMNS]


def _build_daily_fx_map(holdings_df: pd.DataFrame, cache_df: pd.DataFrame) -> pd.DataFrame:
    """Build per-day FX map by forward-filling latest available daily close."""
    rows: list[dict[str, Any]] = []
    for fx_pair, group in holdings_df.groupby("fx_pair"):
        normalized_pair = str(fx_pair).strip().upper()
        pair_dates = sorted(group["date"].map(_parse_date).unique().tolist())
        if normalized_pair == "USDUSD=X":
            for holding_date in pair_dates:
                rows.append({"date": holding_date, "fx_pair": normalized_pair, "fx_rate_to_usd": 1.0})
            continue
        pair_cache = cache_df[cache_df["fx_pair"] == normalized_pair]
        if pair_cache.empty:
            for holding_date in pair_dates:
                rows.append({"date": holding_date, "fx_pair": normalized_pair, "fx_rate_to_usd": pd.NA})
            continue
        fx_series = (
            pair_cache.sort_values(by="date", kind="stable")
            .drop_duplicates(subset=["date"], keep="last")
            .set_index("date")["fx_rate_to_usd"]
        )
        aligned = fx_series.reindex(pair_dates, method="ffill")
        for holding_date, fx_rate in zip(pair_dates, aligned.values, strict=False):
            rows.append({"date": holding_date, "fx_pair": normalized_pair, "fx_rate_to_usd": fx_rate})

    if not rows:
        return pd.DataFrame(columns=["date", "fx_pair", "fx_rate_to_usd"])
    return pd.DataFrame(rows, columns=["date", "fx_pair", "fx_rate_to_usd"])


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
    build_fx_converted_holdings(
        data_dir=Path("data"),
        priced_holdings_path=Path("data/priced_holdings.csv"),
        output_path=Path("data/priced_holdings_usd.csv"),
        fx_cache_path=Path("data/fx_cache.csv"),
    )
