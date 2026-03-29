"""Tool 0: parse tradelist CSV and standardize it for Tool 1 input."""

from __future__ import annotations

from datetime import date, datetime
import logging
import re
from pathlib import Path
from typing import Any

import pandas as pd

LOGGER = logging.getLogger(__name__)

_TRADELIST_EXCLUDE_NAMES = {
    "daily_holdings.csv",
    "prices_cache.csv",
    "normalized_transactions.csv",
    "t0_standardized_tradelist.csv",
}
_DEFAULT_TICKER_OVERRIDES_NAME = "ticker_overrides.csv"

_ISIN_PATTERN = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")
_BLOOMBERG_EQUITY_PATTERN = re.compile(r"^(.+?)\s+([A-Z]{1,4})\s+EQUITY$", re.IGNORECASE)

_YAHOO_SUFFIX_BY_MARKET = {
    "HK": ".HK",
    "LN": ".L",
    "TB": ".BK",
    "SP": ".SI",
    "JT": ".T",
    "GY": ".DE",
    "GR": ".DE",
    "ID": ".JK",
    "LI": ".MI",
}


def standardize_tradelist(
    data_dir: Path,
    output_path: Path,
    reference_bookings_path: Path | None = None,
) -> pd.DataFrame:
    """Standardize latest tradelist file and append Yahoo ticker column."""
    tradelist_path = _find_latest_tradelist_csv(data_dir=data_dir)
    tradelist_df = pd.read_csv(tradelist_path, dtype=str)
    name_to_isin = _load_name_to_isin_map(data_dir=data_dir, reference_bookings_path=reference_bookings_path)
    ticker_overrides = _load_manual_ticker_overrides(data_dir=data_dir)

    LOGGER.info("Standardizing tradelist: %s", tradelist_path)
    LOGGER.info("Loaded %s rows from tradelist.", len(tradelist_df))
    if ticker_overrides:
        LOGGER.info("Loaded %s manual Yahoo ticker overrides.", len(ticker_overrides))

    required_columns = [
        "Action",
        "Ticker / ISIN / Reference",
        "Security name",
        "Currency",
        "Trade date",
        " Executed unit price ",
        " Executed quantity ",
    ]
    for column in required_columns:
        if column not in tradelist_df.columns:
            raise ValueError(f"Missing required column in tradelist: {column}")

    standardized_rows: list[dict[str, Any]] = []
    for _, row in tradelist_df.iterrows():
        reference = str(row.get("Ticker / ISIN / Reference", "")).strip()
        security_name = str(row.get("Security name", "")).strip()
        action_text = str(row.get("Action", "")).strip().upper()
        currency = str(row.get("Currency", "")).strip().upper()
        order_type = _normalize_order_type(action_text=action_text)
        yahoo_ticker = _resolve_yahoo_ticker(reference=reference, ticker_overrides=ticker_overrides)
        is_equity_reference = bool(_BLOOMBERG_EQUITY_PATTERN.match(reference.strip().upper()))
        booking_date = _parse_date(value=row.get("Trade date", ""))
        quantity = abs(_parse_number(value=row.get(" Executed quantity ", "")))
        execution_price = _parse_number(value=row.get(" Executed unit price ", ""))
        normalized_name = _normalize_security_name(security_name)
        inferred_isin = reference if _is_isin(reference) else name_to_isin.get(normalized_name, "")

        standardized = {
            **{column: row.get(column, "") for column in tradelist_df.columns},
            # Keep equity classification independent from Yahoo mapping success.
            "Asset Type": "Equities" if is_equity_reference else "Other",
            "Order type": order_type,
            "Booking date": booking_date.isoformat(),
            "ISIN": inferred_isin,
            "Quantity": round(quantity, 8),
            "Execution price": round(execution_price, 8),
            "Description": security_name,
            "Symbol": yahoo_ticker,
            "Yahoo Ticker": yahoo_ticker,
        }
        standardized_rows.append(standardized)

    standardized_df = pd.DataFrame(standardized_rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    standardized_df.to_csv(output_path, index=False)

    yahoo_count = int((standardized_df["Yahoo Ticker"].astype(str).str.strip() != "").sum())
    LOGGER.info("Saved standardized tradelist to %s", output_path)
    LOGGER.info("Rows with Yahoo ticker: %s / %s", yahoo_count, len(standardized_df))

    return standardized_df


def _load_manual_ticker_overrides(data_dir: Path) -> dict[str, str]:
    """Load manual Bloomberg-to-Yahoo overrides from data/ticker_overrides.csv."""
    override_path = data_dir / _DEFAULT_TICKER_OVERRIDES_NAME
    if not override_path.exists():
        return {}

    override_df = pd.read_csv(override_path, dtype=str)
    source_col = _pick_first_existing_column(
        override_df,
        ["bloomberg_reference", "reference", "Ticker / ISIN / Reference"],
    )
    target_col = _pick_first_existing_column(
        override_df,
        ["yahoo_ticker", "Yahoo Ticker", "Symbol"],
    )
    if source_col is None or target_col is None:
        LOGGER.warning("Ignoring %s because required columns are missing.", override_path)
        return {}

    overrides: dict[str, str] = {}
    for _, row in override_df.iterrows():
        source = _normalize_reference_key(str(row.get(source_col, "")))
        target = str(row.get(target_col, "")).strip().upper()
        if source and target:
            overrides[source] = target
    return overrides


def _find_latest_tradelist_csv(data_dir: Path) -> Path:
    csv_paths = [path for path in data_dir.glob("*.csv") if path.name not in _TRADELIST_EXCLUDE_NAMES]
    tradelist_candidates = [path for path in csv_paths if "tradelist" in path.name.lower()]
    candidates = tradelist_candidates or csv_paths
    if not candidates:
        raise FileNotFoundError(f"No input CSV files found in: {data_dir}")
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _find_latest_bookings_csv(data_dir: Path) -> Path | None:
    bookings_candidates = sorted(
        [path for path in data_dir.glob("*Securities bookings*.csv")],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return bookings_candidates[0] if bookings_candidates else None


def _pick_first_existing_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Return first existing column name from candidates."""
    for column in candidates:
        if column in df.columns:
            return column
    return None


def _load_name_to_isin_map(data_dir: Path, reference_bookings_path: Path | None) -> dict[str, str]:
    source_path = reference_bookings_path or _find_latest_bookings_csv(data_dir=data_dir)
    if source_path is None or not source_path.exists():
        LOGGER.warning("No reference Securities bookings CSV found for ISIN enrichment.")
        return {}

    bookings_df = pd.read_csv(source_path, dtype=str)
    if "ISIN" not in bookings_df.columns or "Position" not in bookings_df.columns:
        LOGGER.warning("Reference file %s missing Position/ISIN columns.", source_path)
        return {}

    filtered_df = bookings_df.dropna(subset=["Position", "ISIN"]).copy()
    mapping: dict[str, str] = {}
    for _, row in filtered_df.iterrows():
        isin = str(row["ISIN"]).strip().upper()
        if not _is_isin(isin):
            continue
        name = _first_line(str(row["Position"]))
        key = _normalize_security_name(name)
        if key and key not in mapping:
            mapping[key] = isin
    LOGGER.info("Loaded %s Position->ISIN mappings from %s", len(mapping), source_path)
    return mapping


def _normalize_order_type(action_text: str) -> str:
    if action_text == "BUY":
        return "BUY"
    if action_text == "SELL":
        return "SELL"
    return action_text


def _resolve_yahoo_ticker(reference: str, ticker_overrides: dict[str, str]) -> str:
    """Resolve Yahoo ticker by manual override first, then automatic conversion."""
    reference_key = _normalize_reference_key(reference)
    if reference_key and reference_key in ticker_overrides:
        return ticker_overrides[reference_key]
    return _to_yahoo_ticker(reference=reference)


def _to_yahoo_ticker(reference: str) -> str:
    text = str(reference).strip().upper()
    if not text:
        return ""

    match = _BLOOMBERG_EQUITY_PATTERN.match(text)
    if not match:
        return ""

    ticker_root = match.group(1).strip().upper()
    market = match.group(2).strip().upper()
    if not ticker_root:
        return ""

    if market == "US":
        return ticker_root
    if market in {"CN", "CH"}:
        return _format_mainland_china_ticker(ticker_root=ticker_root)
    if market == "HK":
        return _format_hk_ticker(ticker_root=ticker_root)

    suffix = _YAHOO_SUFFIX_BY_MARKET.get(market, "")
    return f"{ticker_root}{suffix}" if suffix else ticker_root


def _normalize_reference_key(reference: str) -> str:
    """Normalize Bloomberg reference for stable dictionary matching."""
    text = str(reference).strip().upper()
    return re.sub(r"\s+", " ", text)


def _format_hk_ticker(ticker_root: str) -> str:
    cleaned = ticker_root.strip().upper()
    if cleaned.isdigit():
        return f"{cleaned.zfill(4)}.HK"
    return f"{cleaned}.HK"


def _format_mainland_china_ticker(ticker_root: str) -> str:
    cleaned = ticker_root.strip().upper()
    if not cleaned.isdigit():
        return f"{cleaned}.SS"
    if cleaned.startswith(("0", "2", "3")):
        return f"{cleaned.zfill(6)}.SZ"
    return f"{cleaned.zfill(6)}.SS"


def _is_isin(value: str) -> bool:
    return bool(_ISIN_PATTERN.match(str(value).strip().upper()))


def _first_line(value: str) -> str:
    lines = [line.strip() for line in str(value).splitlines() if line.strip()]
    return lines[0] if lines else ""


def _normalize_security_name(value: str) -> str:
    text = str(value).strip().upper()
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _parse_number(value: Any) -> float:
    text = str(value).strip() if value is not None else ""
    if not text:
        return 0.0
    cleaned = text.replace(",", "").replace(" ", "")
    cleaned = cleaned.replace("(", "-").replace(")", "")
    if cleaned in {"-", "--", "---"}:
        return 0.0
    try:
        return round(float(cleaned), 8)
    except ValueError:
        return 0.0


def _parse_date(value: Any) -> date:
    text = str(value).strip()
    if not text:
        raise ValueError("Trade date is empty.")
    for fmt in ("%d/%m/%Y", "%d.%m.%Y", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return pd.to_datetime(text).date()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    output = standardize_tradelist(
        data_dir=Path("data"),
        output_path=Path("data/t0_standardized_tradelist.csv"),
    )
    print(output.tail(20))
