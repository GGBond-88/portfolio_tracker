"""Tool 5: build transaction-level portfolio cash flows for equity sub-scope."""

from __future__ import annotations

from datetime import date, datetime
import logging
from pathlib import Path
from typing import Any

import pandas as pd

LOGGER = logging.getLogger(__name__)

_OUTPUT_COLUMNS = [
    "date",
    "portfolio",
    "scope",
    "cf_type",
    "ticker",
    "asset_class",
    "currency",
    "amount_local",
    "fx_rate_to_usd",
    "amount_usd",
]

_REQUIRED_TRADELIST_COLUMNS = [
    "Asset Type",
    "Portfolio",
    "Order type",
    "Booking date",
    "Quantity",
    "Execution price",
    "Currency",
    "Yahoo Ticker",
    "Asset class",
]

_REQUIRED_FX_COLUMNS = ["fx_pair", "date", "fx_rate_to_usd", "updated_at_utc"]


def build_cash_flows(
    data_dir: Path,
    tradelist_path: Path,
    fx_cache_path: Path,
    output_path: Path,
    portfolio_filter: str | None = "FGI",
    asset_class_filter: str | None = "Equities",
    scope: str = "equity_sub",
) -> pd.DataFrame:
    """Build signed transaction-level cash flows for equity BUY/SELL rows."""
    _ = data_dir  # Keep interface consistent with other tools.
    if not tradelist_path.exists():
        raise FileNotFoundError(f"Standardized tradelist file not found: {tradelist_path}")
    if not fx_cache_path.exists():
        raise FileNotFoundError(f"FX cache file not found: {fx_cache_path}")

    tradelist_df = pd.read_csv(tradelist_path, dtype=str)
    if tradelist_df.empty:
        empty_df = pd.DataFrame(columns=_OUTPUT_COLUMNS)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        empty_df.to_csv(output_path, index=False)
        return empty_df

    _assert_required_columns(tradelist_df, _REQUIRED_TRADELIST_COLUMNS, "tradelist")

    cash_df = tradelist_df.copy()
    cash_df["Asset Type"] = cash_df["Asset Type"].fillna("").astype(str).str.strip()
    cash_df["Portfolio"] = cash_df["Portfolio"].fillna("").astype(str).str.strip()
    cash_df["Order type"] = cash_df["Order type"].fillna("").astype(str).str.strip().str.upper()

    cash_df = cash_df[cash_df["Asset Type"].str.casefold() == "equities".casefold()].copy()
    cash_df = cash_df[cash_df["Order type"].isin(["BUY", "SELL"])].copy()
    if portfolio_filter is not None:
        normalized_portfolio = str(portfolio_filter).strip().casefold()
        cash_df = cash_df[cash_df["Portfolio"].str.casefold() == normalized_portfolio].copy()
    if asset_class_filter is not None:
        normalized_asset_class = str(asset_class_filter).strip().casefold()
        cash_df = cash_df[
            cash_df["Asset class"].fillna("").astype(str).str.strip().str.casefold() == normalized_asset_class
        ].copy()

    if cash_df.empty:
        empty_df = pd.DataFrame(columns=_OUTPUT_COLUMNS)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        empty_df.to_csv(output_path, index=False)
        LOGGER.info("Saved cash flows to %s (0 rows after filters).", output_path)
        return empty_df

    cash_df["date"] = cash_df["Booking date"].map(_parse_date)
    cash_df["portfolio"] = cash_df["Portfolio"]
    cash_df["scope"] = str(scope).strip() or "equity_sub"
    cash_df["cf_type"] = cash_df["Order type"]
    cash_df["ticker"] = cash_df["Yahoo Ticker"].fillna("").astype(str).str.strip()
    cash_df["asset_class"] = cash_df["Asset class"].fillna("").astype(str).str.strip()
    cash_df["currency"] = cash_df["Currency"].fillna("").astype(str).str.strip().str.upper()
    cash_df["quantity"] = cash_df["Quantity"].map(_parse_number)
    cash_df["execution_price"] = cash_df["Execution price"].map(_parse_number)

    base_amount = (cash_df["quantity"] * cash_df["execution_price"]).round(8)
    cash_df["amount_local"] = base_amount
    cash_df.loc[cash_df["cf_type"] == "BUY", "amount_local"] = -base_amount
    cash_df.loc[cash_df["cf_type"] == "SELL", "amount_local"] = base_amount

    cash_df["fx_pair"] = cash_df["currency"].map(_to_fx_pair)
    fx_cache_df = _load_fx_cache(fx_cache_path=fx_cache_path)
    fx_map_df = _build_daily_fx_map(cash_flows_df=cash_df[["date", "fx_pair"]], cache_df=fx_cache_df)

    cash_df = cash_df.merge(fx_map_df, how="left", on=["date", "fx_pair"])
    cash_df["fx_rate_to_usd"] = pd.to_numeric(cash_df["fx_rate_to_usd"], errors="coerce")
    cash_df.loc[cash_df["currency"] == "USD", "fx_rate_to_usd"] = 1.0
    cash_df["amount_usd"] = (cash_df["amount_local"] * cash_df["fx_rate_to_usd"]).round(8)

    _raise_if_missing_fx_to_usd(cash_df)

    output_df = cash_df[_OUTPUT_COLUMNS].copy()
    output_df["date"] = output_df["date"].map(lambda value: value.isoformat())
    output_df = output_df.sort_values(by=["date", "portfolio", "ticker"], kind="stable").reset_index(drop=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_df.to_csv(output_path, index=False)

    LOGGER.info("Saved cash flows to %s", output_path)
    LOGGER.info("Cash flow rows: %s", len(output_df))
    return output_df


def _assert_required_columns(df: pd.DataFrame, required_columns: list[str], label: str) -> None:
    """Raise ValueError if required columns are missing."""
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required {label} columns: {', '.join(missing)}")


def _raise_if_missing_fx_to_usd(df: pd.DataFrame) -> None:
    """Log and fail if any row lacks fx_rate_to_usd (avoids silent NaN amount_usd downstream)."""
    missing_mask = df["fx_rate_to_usd"].isna()
    if not missing_mask.any():
        return
    bad = df.loc[missing_mask, ["date", "currency"]].drop_duplicates()
    for _, row in bad.iterrows():
        LOGGER.error("Missing fx_rate_to_usd for date=%s, currency=%s.", row["date"], row["currency"])
    details = [(row["date"], str(row["currency"])) for _, row in bad.iterrows()]
    raise ValueError(
        "Missing FX rate to USD for one or more cash flow rows; cannot compute amount_usd. "
        f"Affected (date, currency): {details}"
    )


def _to_fx_pair(currency: str) -> str:
    """Map a currency code to Yahoo-style FX pair against USD."""
    normalized = str(currency).strip().upper()
    if not normalized or normalized == "USD":
        return "USDUSD=X"
    return f"{normalized}USD=X"


def _load_fx_cache(fx_cache_path: Path) -> pd.DataFrame:
    """Load and normalize FX cache rows used for FX lookup."""
    cache_df = pd.read_csv(fx_cache_path, dtype=str)
    _assert_required_columns(cache_df, _REQUIRED_FX_COLUMNS, "fx cache")
    if cache_df.empty:
        return pd.DataFrame(columns=_REQUIRED_FX_COLUMNS)

    normalized = cache_df.copy()
    normalized["fx_pair"] = normalized["fx_pair"].fillna("").astype(str).str.strip().str.upper()
    normalized["date"] = normalized["date"].map(_parse_date)
    normalized["fx_rate_to_usd"] = normalized["fx_rate_to_usd"].map(_parse_number)
    normalized["updated_at_utc"] = normalized["updated_at_utc"].fillna("").astype(str)
    normalized = normalized.sort_values(by=["fx_pair", "date", "updated_at_utc"], kind="stable")
    normalized = normalized.drop_duplicates(subset=["fx_pair", "date"], keep="last").reset_index(drop=True)
    return normalized[_REQUIRED_FX_COLUMNS].copy()


def _build_daily_fx_map(cash_flows_df: pd.DataFrame, cache_df: pd.DataFrame) -> pd.DataFrame:
    """Build per-transaction-day FX map using forward-filled cache rates."""
    rows: list[dict[str, Any]] = []
    for fx_pair, group in cash_flows_df.groupby("fx_pair"):
        normalized_pair = str(fx_pair).strip().upper()
        txn_dates = sorted(group["date"].map(_parse_date).unique().tolist())
        if normalized_pair == "USDUSD=X":
            for txn_date in txn_dates:
                rows.append({"date": txn_date, "fx_pair": normalized_pair, "fx_rate_to_usd": 1.0})
            continue

        pair_cache = cache_df[cache_df["fx_pair"] == normalized_pair]
        if pair_cache.empty:
            for txn_date in txn_dates:
                rows.append({"date": txn_date, "fx_pair": normalized_pair, "fx_rate_to_usd": pd.NA})
            continue

        fx_series = (
            pair_cache.sort_values(by="date", kind="stable")
            .drop_duplicates(subset=["date"], keep="last")
            .set_index("date")["fx_rate_to_usd"]
        )
        aligned = fx_series.reindex(txn_dates, method="ffill")
        for txn_date, fx_rate in zip(txn_dates, aligned.values, strict=False):
            rows.append({"date": txn_date, "fx_pair": normalized_pair, "fx_rate_to_usd": fx_rate})

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
    build_cash_flows(
        data_dir=Path("data"),
        tradelist_path=Path("data/t0_standardized_tradelist.csv"),
        fx_cache_path=Path("data/fx_cache.csv"),
        output_path=Path("data/portfolio_cash_flows.csv"),
        portfolio_filter="FGI",
        asset_class_filter="Equities",
        scope="equity_sub",
    )
