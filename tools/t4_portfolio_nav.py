"""Tool 4: aggregate priced holdings into daily portfolio NAV summary."""

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
    "position_count",
    "total_market_value_usd",
    "total_cost_basis_usd",
    "total_unrealized_pnl_usd",
    "total_realized_pnl_usd",
    "total_pnl_usd",
    "daily_return_pct",
]


def build_portfolio_nav(
    data_dir: Path,
    priced_holdings_usd_path: Path,
    output_path: Path,
    portfolio_filter: str | None = "FGI",
    asset_class_filter: str | None = "Equities",
    scope: str = "fgi_equities",
) -> pd.DataFrame:
    """Build daily portfolio NAV summary from USD-priced holdings."""
    _ = data_dir  # Keep interface consistent with other tool modules.
    if not priced_holdings_usd_path.exists():
        raise FileNotFoundError(f"Priced holdings USD file not found: {priced_holdings_usd_path}")

    holdings_df = pd.read_csv(priced_holdings_usd_path, dtype=str)
    if holdings_df.empty:
        empty_df = pd.DataFrame(columns=_OUTPUT_COLUMNS)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        empty_df.to_csv(output_path, index=False)
        return empty_df

    if "date" not in holdings_df.columns:
        raise ValueError("Input file must contain 'date' column.")
    portfolio_column = _pick_existing_column(holdings_df, ["Portfolio", "portfolio"])
    if portfolio_column is None:
        raise ValueError("Input file must contain one of: Portfolio, portfolio")
    asset_class_column = _pick_existing_column(holdings_df, ["Asset class", "Asset Type", "asset_class"])
    if asset_class_column is None:
        raise ValueError("Input file must contain one of: Asset class, Asset Type, asset_class")

    nav_df = holdings_df.copy()
    nav_df["date"] = nav_df["date"].map(_parse_date)
    nav_df["portfolio"] = nav_df[portfolio_column].fillna("").astype(str).str.strip()
    nav_df["_asset_class_norm"] = nav_df[asset_class_column].fillna("").astype(str).str.strip().str.casefold()

    if portfolio_filter is not None:
        normalized_portfolio = str(portfolio_filter).strip().casefold()
        nav_df = nav_df[nav_df["portfolio"].str.casefold() == normalized_portfolio].copy()
    if asset_class_filter is not None:
        normalized_asset_class = str(asset_class_filter).strip().casefold()
        nav_df = nav_df[nav_df["_asset_class_norm"] == normalized_asset_class].copy()

    for column in [
        "market_value_usd",
        "cost_basis_usd",
        "unrealized_pnl_usd",
        "realized_pnl_usd",
        "total_pnl_usd",
    ]:
        nav_df[column] = nav_df.get(column, 0.0).map(_parse_number)

    if nav_df.empty:
        empty_df = pd.DataFrame(columns=_OUTPUT_COLUMNS)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        empty_df.to_csv(output_path, index=False)
        LOGGER.info("Saved portfolio NAV to %s (0 rows after filters).", output_path)
        return empty_df

    grouped = (
        nav_df.groupby(["date", "portfolio"], as_index=False, dropna=False)
        .agg(
            position_count=("portfolio", "size"),
            total_market_value_usd=("market_value_usd", "sum"),
            total_cost_basis_usd=("cost_basis_usd", "sum"),
            total_unrealized_pnl_usd=("unrealized_pnl_usd", "sum"),
            total_realized_pnl_usd=("realized_pnl_usd", "sum"),
            total_pnl_usd=("total_pnl_usd", "sum"),
        )
        .reset_index(drop=True)
    )

    grouped["scope"] = str(scope).strip() or "fgi_equities"
    for value_column in [
        "total_market_value_usd",
        "total_cost_basis_usd",
        "total_unrealized_pnl_usd",
        "total_realized_pnl_usd",
        "total_pnl_usd",
    ]:
        grouped[value_column] = grouped[value_column].astype(float).round(8)
    grouped = grouped.sort_values(by=["portfolio", "date"], kind="stable").reset_index(drop=True)
    prev_nav = grouped.groupby("portfolio", dropna=False)["total_market_value_usd"].shift(1)
    grouped["daily_return_pct"] = ((grouped["total_market_value_usd"] - prev_nav) / prev_nav).round(8)
    grouped.loc[prev_nav.isna() | (prev_nav == 0), "daily_return_pct"] = pd.NA

    grouped = grouped[_OUTPUT_COLUMNS].sort_values(by=["date", "portfolio"], kind="stable").reset_index(drop=True)
    grouped["date"] = grouped["date"].map(lambda value: value.isoformat())

    output_path.parent.mkdir(parents=True, exist_ok=True)
    grouped.to_csv(output_path, index=False)

    LOGGER.info("Saved portfolio NAV to %s", output_path)
    LOGGER.info("Portfolio NAV rows: %s", len(grouped))
    return grouped


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
        return round(float(cleaned), 8)
    except ValueError:
        return 0.0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build_portfolio_nav(
        data_dir=Path("data"),
        priced_holdings_usd_path=Path("data/priced_holdings_usd.csv"),
        output_path=Path("data/portfolio_nav.csv"),
        portfolio_filter="FGI",
        asset_class_filter="Equities",
        scope="fgi_equities",
    )
