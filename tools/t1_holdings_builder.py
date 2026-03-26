"""Tool 1: replay equity transactions into daily holdings snapshots."""

from __future__ import annotations

from datetime import date, datetime, timedelta
import logging
from pathlib import Path
from typing import Any

import pandas as pd

LOGGER = logging.getLogger(__name__)

_OUTPUT_EXCLUDE_NAMES = {
    "daily_holdings.csv",
    "prices_cache.csv",
    "normalized_transactions.csv",
}


def inspect_csv(data_dir: Path) -> pd.DataFrame:
    """Find latest CSV, print schema diagnostics, and return raw DataFrame."""
    latest_csv = _find_latest_csv(data_dir=data_dir)
    raw_df = pd.read_csv(latest_csv, dtype=str)

    LOGGER.info("Inspecting CSV: %s", latest_csv)
    LOGGER.info("All column names (%s): %s", len(raw_df.columns), list(raw_df.columns))
    LOGGER.info("First 5 rows:\n%s", raw_df.head(5).to_string(index=False))

    asset_type_column = _pick_first_existing_column(raw_df, ["Asset Type", "AssetType"])
    if asset_type_column is not None:
        unique_asset_types = (
            raw_df[asset_type_column]
            .fillna("")
            .astype(str)
            .str.strip()
            .replace("", pd.NA)
            .dropna()
            .unique()
            .tolist()
        )
        LOGGER.info("Unique values of %s: %s", asset_type_column, sorted(unique_asset_types))
    else:
        LOGGER.warning("Asset Type column not found.")

    return raw_df


def load_and_filter_transactions(data_dir: Path) -> pd.DataFrame:
    """Load latest CSV, filter to equities, and normalize transaction columns."""
    latest_csv = _find_latest_csv(data_dir=data_dir)
    raw_df = pd.read_csv(latest_csv, dtype=str)

    asset_type_column = _require_column(raw_df, ["Asset Type", "AssetType"])
    order_type_column = _require_column(raw_df, ["Order type", "Order Type"])
    date_column = _require_column(raw_df, ["Booking date", "Booking Date", "Trade Date", "Date/Time"])
    isin_column = _require_column(raw_df, ["ISIN"])
    quantity_column = _require_column(raw_df, ["Quantity"])
    price_column = _require_column(raw_df, ["Execution price", "Price", "Execution Price"])
    currency_column = _require_column(raw_df, ["Currency"])
    name_column = _pick_first_existing_column(raw_df, ["Description", "Name", "Position"])
    symbol_column = _pick_first_existing_column(raw_df, ["Symbol", "Ticker"])

    equities_df = raw_df[
        raw_df[asset_type_column]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.casefold()
        .eq("equities")
    ].copy()

    if equities_df.empty:
        LOGGER.warning("No equities rows found in %s.", latest_csv)
        return pd.DataFrame(
            columns=[
                "date",
                "symbol",
                "isin",
                "name",
                "currency",
                "order_type",
                "quantity",
                "price",
            ]
        )

    equities_df["date"] = equities_df[date_column].map(_parse_date)
    equities_df["isin"] = equities_df[isin_column].fillna("").astype(str).str.strip()
    equities_df["name"] = (
        equities_df[name_column].fillna("").astype(str).map(_first_line) if name_column else ""
    )
    equities_df["symbol"] = (
        equities_df[symbol_column].fillna("").astype(str).str.strip() if symbol_column else ""
    )
    equities_df["currency"] = equities_df[currency_column].fillna("").astype(str).str.strip().str.upper()
    equities_df["order_type"] = (
        equities_df[order_type_column].fillna("").astype(str).str.strip().str.upper()
    )
    equities_df["raw_quantity"] = equities_df[quantity_column].map(_parse_number)
    equities_df["price"] = equities_df[price_column].map(_parse_number)

    equities_df["symbol"] = equities_df.apply(
        lambda row: _fallback_symbol(
            symbol=str(row.get("symbol", "")),
            isin=str(row.get("isin", "")),
            name=str(row.get("name", "")),
        ),
        axis=1,
    )
    equities_df["name"] = equities_df["name"].replace("", pd.NA).fillna(equities_df["symbol"])
    equities_df["quantity"] = equities_df.apply(_normalize_signed_quantity, axis=1)

    normalized_df = equities_df[
        ["date", "symbol", "isin", "name", "currency", "order_type", "quantity", "price"]
    ].copy()
    normalized_df = normalized_df[normalized_df["quantity"] != 0].reset_index(drop=True)

    return _aggregate_same_day_transactions(normalized_df)


def replay_transactions(transactions: pd.DataFrame) -> pd.DataFrame:
    """Core logic: replay day-by-day and return daily holdings DataFrame."""
    if transactions.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "symbol",
                "isin",
                "name",
                "quantity",
                "avg_cost",
                "cost_basis",
                "currency",
                "realized_pnl",
            ]
        )

    tx_df = transactions.copy()
    tx_df["date"] = tx_df["date"].map(_parse_date)
    tx_df = tx_df.sort_values(by=["date", "symbol", "isin", "order_type"], kind="stable")

    start_date = tx_df["date"].min()
    end_date = date.today()
    all_days = pd.date_range(start=start_date, end=end_date, freq="D").date

    positions: dict[tuple[str, str, str], dict[str, Any]] = {}
    snapshots: list[dict[str, Any]] = []

    for current_day in all_days:
        day_rows = tx_df[tx_df["date"] == current_day]
        for _, row in day_rows.iterrows():
            key = (str(row["symbol"]), str(row["isin"]), str(row["currency"]))
            state = positions.setdefault(
                key,
                {
                    "symbol": str(row["symbol"]),
                    "isin": str(row["isin"]),
                    "name": str(row["name"]),
                    "currency": str(row["currency"]),
                    "quantity": 0.0,
                    "avg_cost": 0.0,
                    "realized_pnl": 0.0,
                },
            )
            state["name"] = str(row["name"]) or state["name"]

            qty_change = float(row["quantity"])
            price = float(row["price"])
            old_qty = float(state["quantity"])
            old_avg = float(state["avg_cost"])

            if qty_change > 0:
                new_qty = old_qty + qty_change
                if abs(new_qty) < 1e-12:
                    state["quantity"] = 0.0
                    state["avg_cost"] = 0.0
                else:
                    weighted_cost = (old_qty * old_avg) + (qty_change * price)
                    state["quantity"] = round(new_qty, 8)
                    state["avg_cost"] = round(weighted_cost / new_qty, 8)
            elif qty_change < 0:
                sell_qty = abs(qty_change)
                state["realized_pnl"] = round(
                    float(state["realized_pnl"]) + ((price - old_avg) * sell_qty),
                    8,
                )
                state["quantity"] = round(old_qty + qty_change, 8)
            else:
                continue

        # Remove flat positions, keep long and short.
        flat_keys = [key for key, state in positions.items() if abs(float(state["quantity"])) < 1e-12]
        for key in flat_keys:
            positions.pop(key)

        for state in positions.values():
            quantity = float(state["quantity"])
            avg_cost = float(state["avg_cost"])
            snapshots.append(
                {
                    "date": current_day.isoformat(),
                    "symbol": state["symbol"],
                    "isin": state["isin"],
                    "name": state["name"],
                    "quantity": round(quantity, 8),
                    "avg_cost": round(avg_cost, 8),
                    "cost_basis": round(quantity * avg_cost, 8),
                    "currency": state["currency"],
                    "realized_pnl": round(float(state["realized_pnl"]), 8),
                }
            )

    result_df = pd.DataFrame(snapshots)
    if result_df.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "symbol",
                "isin",
                "name",
                "quantity",
                "avg_cost",
                "cost_basis",
                "currency",
                "realized_pnl",
            ]
        )

    return result_df.sort_values(by=["date", "symbol", "isin"], kind="stable").reset_index(drop=True)


def build_holdings(data_dir: Path, output_path: Path) -> pd.DataFrame:
    """Main entry point: inspect, load/filter, replay, save, and return holdings."""
    inspect_csv(data_dir=data_dir)
    transactions = load_and_filter_transactions(data_dir=data_dir)
    daily_holdings = replay_transactions(transactions=transactions)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    daily_holdings.to_csv(output_path, index=False)

    total_symbols = transactions["symbol"].nunique() if not transactions.empty else 0
    latest_date = daily_holdings["date"].max() if not daily_holdings.empty else "N/A"
    latest_positions = (
        daily_holdings[daily_holdings["date"] == latest_date] if not daily_holdings.empty else pd.DataFrame()
    )
    open_positions = int((latest_positions["quantity"] > 0).sum()) if not latest_positions.empty else 0
    date_range = (
        f"{transactions['date'].min()} -> {transactions['date'].max()}"
        if not transactions.empty
        else "N/A"
    )

    LOGGER.info("Saved daily holdings to %s", output_path)
    LOGGER.info("Total unique symbols traded: %s", total_symbols)
    LOGGER.info("Currently open positions (quantity > 0 on latest date): %s", open_positions)
    LOGGER.info("Date range covered: %s", date_range)

    return daily_holdings


def _find_latest_csv(data_dir: Path) -> Path:
    csv_paths = [path for path in data_dir.glob("*.csv") if path.name not in _OUTPUT_EXCLUDE_NAMES]
    if not csv_paths:
        raise FileNotFoundError(f"No CSV files found in: {data_dir}")
    return max(csv_paths, key=lambda path: path.stat().st_mtime)


def _pick_first_existing_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for column in candidates:
        if column in df.columns:
            return column
    return None


def _require_column(df: pd.DataFrame, candidates: list[str]) -> str:
    column = _pick_first_existing_column(df=df, candidates=candidates)
    if column is None:
        raise ValueError(f"Missing required column. Expected one of: {candidates}")
    return column


def _parse_number(value: Any) -> float:
    text = str(value).strip() if value is not None else ""
    if not text or text.lower() == "nan":
        return 0.0
    cleaned = text.replace(",", "")
    try:
        return round(float(cleaned), 8)
    except ValueError:
        return 0.0


def _parse_date(value: Any) -> date:
    text = str(value).strip()
    if not text:
        raise ValueError("Date value is empty.")
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return pd.to_datetime(text).date()


def _first_line(value: str) -> str:
    lines = [line.strip() for line in str(value).splitlines() if line.strip()]
    return lines[0] if lines else ""


def _fallback_symbol(symbol: str, isin: str, name: str) -> str:
    if symbol.strip():
        return symbol.strip().upper()
    if isin.strip():
        return isin.strip().upper()
    short_name = name.strip()
    return short_name[:24].upper() if short_name else "UNKNOWN"


def _normalize_signed_quantity(row: pd.Series) -> float:
    order_type = str(row.get("order_type", "")).strip().upper()
    raw_qty = float(row.get("raw_quantity", 0.0))
    if order_type == "BUY":
        return abs(raw_qty)
    if order_type == "SELL":
        return -abs(raw_qty)
    # Unknown order types are skipped later because quantity becomes 0.
    return 0.0


def _aggregate_same_day_transactions(transactions: pd.DataFrame) -> pd.DataFrame:
    tx = transactions.copy()
    tx["turnover"] = tx["quantity"].abs() * tx["price"]
    grouped = (
        tx.groupby(
            ["date", "symbol", "isin", "name", "currency", "order_type"],
            as_index=False,
            dropna=False,
        )
        .agg(quantity=("quantity", "sum"), turnover=("turnover", "sum"))
        .reset_index(drop=True)
    )
    grouped["price"] = grouped.apply(
        lambda row: round(
            float(row["turnover"]) / max(abs(float(row["quantity"])), 1e-12),
            8,
        ),
        axis=1,
    )
    return grouped[["date", "symbol", "isin", "name", "currency", "order_type", "quantity", "price"]]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    df = build_holdings(
        data_dir=Path("data"),
        output_path=Path("data/daily_holdings.csv"),
    )
    print(df.tail(20))
