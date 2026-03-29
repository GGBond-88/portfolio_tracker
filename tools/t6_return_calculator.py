"""Tool 6: calculate portfolio returns (TWR + annualized MWR/IRR)."""

from __future__ import annotations

from datetime import date, datetime
import logging
import math
from pathlib import Path
from typing import Any

import pandas as pd
from scipy.optimize import brentq

LOGGER = logging.getLogger(__name__)

_OUTPUT_COLUMNS = [
    "date",
    "portfolio",
    "scope",
    "nav_usd",
    "daily_net_cf_usd",
    "daily_return_twr",
    "cumulative_twr",
    "mtd_return",
    "qtd_return",
    "ytd_return",
    "itd_return",
    "irr_annualized",
]

_REQUIRED_NAV_COLUMNS = ["date", "portfolio", "total_market_value_usd"]
_REQUIRED_CF_COLUMNS = ["date", "portfolio", "amount_usd"]


def build_portfolio_returns(
    data_dir: Path,
    nav_path: Path,
    cash_flows_path: Path,
    output_path: Path,
    portfolio_filter: str | None = "FGI",
    scope: str = "equity_sub",
) -> pd.DataFrame:
    """Build daily portfolio return series with TWR and full-period annualized IRR."""
    _ = data_dir  # Keep interface consistent with other tools.
    if not nav_path.exists():
        raise FileNotFoundError(f"Portfolio NAV file not found: {nav_path}")
    if not cash_flows_path.exists():
        raise FileNotFoundError(f"Portfolio cash flow file not found: {cash_flows_path}")

    nav_df = pd.read_csv(nav_path, dtype=str)
    try:
        cf_df = pd.read_csv(cash_flows_path, dtype=str)
    except pd.errors.EmptyDataError:
        cf_df = pd.DataFrame(columns=_REQUIRED_CF_COLUMNS)
    _assert_required_columns(nav_df, _REQUIRED_NAV_COLUMNS, "portfolio NAV")
    _assert_required_columns(cf_df, _REQUIRED_CF_COLUMNS, "portfolio cash flows")

    nav_series = _prepare_nav_series(nav_df=nav_df, portfolio_filter=portfolio_filter)
    if nav_series.empty:
        empty_df = pd.DataFrame(columns=_OUTPUT_COLUMNS)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        empty_df.to_csv(output_path, index=False)
        LOGGER.info("Saved portfolio returns to %s (0 NAV rows after filters).", output_path)
        return empty_df

    daily_cf = _prepare_daily_cash_flows(cf_df=cf_df, portfolio_filter=portfolio_filter)
    returns_df = nav_series.merge(daily_cf, how="left", on="date")
    returns_df["daily_net_cf_usd"] = pd.to_numeric(returns_df["daily_net_cf_usd"], errors="coerce").fillna(0.0)

    returns_df = returns_df.sort_values(by=["date"], kind="stable").reset_index(drop=True)
    returns_df["scope"] = str(scope).strip() or "equity_sub"

    daily_returns = _compute_daily_twr(
        nav_series=returns_df["nav_usd"],
        net_cf_series=returns_df["daily_net_cf_usd"],
    )
    returns_df["daily_return_twr"] = daily_returns
    gross_daily = (1.0 + returns_df["daily_return_twr"].fillna(0.0)).astype(float)
    returns_df["cumulative_twr"] = gross_daily.cumprod() - 1.0
    returns_df["itd_return"] = returns_df["cumulative_twr"]
    returns_df["mtd_return"] = _compute_period_returns(
        dates=returns_df["date"],
        gross_daily_returns=gross_daily,
        period_key="m",
    )
    returns_df["qtd_return"] = _compute_period_returns(
        dates=returns_df["date"],
        gross_daily_returns=gross_daily,
        period_key="q",
    )
    returns_df["ytd_return"] = _compute_period_returns(
        dates=returns_df["date"],
        gross_daily_returns=gross_daily,
        period_key="y",
    )

    annualized_irr = _compute_annualized_irr(
        dates=returns_df["date"],
        nav_usd=returns_df["nav_usd"],
        daily_net_cf_usd=returns_df["daily_net_cf_usd"],
    )
    returns_df["irr_annualized"] = annualized_irr

    returns_df["date"] = returns_df["date"].map(lambda value: value.isoformat())
    returns_df = returns_df[_OUTPUT_COLUMNS].copy()
    for column in _OUTPUT_COLUMNS:
        if column in {"date", "portfolio", "scope"}:
            continue
        returns_df[column] = pd.to_numeric(returns_df[column], errors="coerce").round(10)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    returns_df.to_csv(output_path, index=False)

    LOGGER.info("Saved portfolio returns to %s", output_path)
    LOGGER.info("Portfolio returns rows: %s", len(returns_df))
    if pd.isna(annualized_irr):
        LOGGER.warning("Could not solve annualized IRR for the selected period.")
    return returns_df


def _prepare_nav_series(nav_df: pd.DataFrame, portfolio_filter: str | None) -> pd.DataFrame:
    """Prepare daily NAV time series for return calculations."""
    working = nav_df.copy()
    working["date"] = working["date"].map(_parse_date)
    working["portfolio"] = working["portfolio"].fillna("").astype(str).str.strip()
    working["nav_usd"] = working["total_market_value_usd"].map(_parse_number)
    if portfolio_filter is not None:
        normalized = str(portfolio_filter).strip().casefold()
        working = working[working["portfolio"].str.casefold() == normalized].copy()
    if working.empty:
        return pd.DataFrame(columns=["date", "portfolio", "nav_usd"])
    result = (
        working.sort_values(by=["date", "portfolio"], kind="stable")
        .drop_duplicates(subset=["date"], keep="last")
        .reset_index(drop=True)
    )
    return result[["date", "portfolio", "nav_usd"]]


def _prepare_daily_cash_flows(cf_df: pd.DataFrame, portfolio_filter: str | None) -> pd.DataFrame:
    """Aggregate daily USD cash flows from transaction-level cash flow rows."""
    if cf_df.empty:
        return pd.DataFrame(columns=["date", "daily_net_cf_usd"])
    working = cf_df.copy()
    working["date"] = working["date"].map(_parse_date)
    working["portfolio"] = working["portfolio"].fillna("").astype(str).str.strip()
    working["amount_usd"] = working["amount_usd"].map(_parse_number)
    if portfolio_filter is not None:
        normalized = str(portfolio_filter).strip().casefold()
        working = working[working["portfolio"].str.casefold() == normalized].copy()
    if working.empty:
        return pd.DataFrame(columns=["date", "daily_net_cf_usd"])
    grouped = (
        working.groupby("date", as_index=False, dropna=False)
        .agg(daily_net_cf_usd=("amount_usd", "sum"))
        .reset_index(drop=True)
    )
    return grouped


def _compute_daily_twr(nav_series: pd.Series, net_cf_series: pd.Series) -> pd.Series:
    """Compute daily modified-Dietz style sub-period return series."""
    returns: list[Any] = [pd.NA]
    for idx in range(1, len(nav_series)):
        nav_t = float(nav_series.iloc[idx])
        nav_prev = float(nav_series.iloc[idx - 1])
        cf_t = float(net_cf_series.iloc[idx])
        numerator = nav_t - nav_prev + cf_t
        if cf_t < 0:
            denominator = nav_prev - cf_t
            if denominator <= 0:
                returns.append(0.0)
                continue
            returns.append(numerator / denominator)
        else:
            denominator = nav_prev
            if denominator <= 0:
                returns.append(0.0)
                continue
            returns.append(numerator / denominator)
    return pd.Series(returns, dtype="Float64")


def _compute_period_returns(
    dates: pd.Series,
    gross_daily_returns: pd.Series,
    period_key: str,
) -> pd.Series:
    """Compute chain-linked period-to-date returns for calendar periods."""
    if period_key == "m":
        periods = dates.map(lambda d: (d.year, d.month))
    elif period_key == "q":
        periods = dates.map(lambda d: (d.year, (d.month - 1) // 3 + 1))
    elif period_key == "y":
        periods = dates.map(lambda d: d.year)
    else:
        raise ValueError(f"Unsupported period key: {period_key}")
    cumulative = gross_daily_returns.groupby(periods).cumprod()
    return cumulative - 1.0


def _compute_annualized_irr(
    dates: pd.Series,
    nav_usd: pd.Series,
    daily_net_cf_usd: pd.Series,
) -> float:
    """Compute full-period annualized IRR using daily-discounted cash flow timings."""
    if len(dates) < 2:
        return float("nan")
    start_date = dates.iloc[0]
    end_date = dates.iloc[-1]
    if end_date <= start_date:
        return float("nan")

    offset_to_cf: dict[int, float] = {}
    offset_to_cf[0] = -float(nav_usd.iloc[0])
    for idx in range(1, len(dates)):
        cf_value = float(daily_net_cf_usd.iloc[idx])
        if cf_value == 0.0:
            continue
        offset = (dates.iloc[idx] - start_date).days
        offset_to_cf[offset] = offset_to_cf.get(offset, 0.0) + cf_value

    last_offset = (end_date - start_date).days
    offset_to_cf[last_offset] = offset_to_cf.get(last_offset, 0.0) + float(nav_usd.iloc[-1])

    offsets = sorted(offset_to_cf.keys())
    cash_flows = [offset_to_cf[offset] for offset in offsets]
    if not any(value > 0 for value in cash_flows) or not any(value < 0 for value in cash_flows):
        return float("nan")

    def npv(rate_daily: float) -> float:
        total = 0.0
        for offset, value in zip(offsets, cash_flows, strict=False):
            total += value / ((1.0 + rate_daily) ** offset)
        return total

    brackets = [(-0.50, 2.0), (-0.90, 10.0), (-0.99, 100.0)]
    daily_irr: float | None = None
    for lower, upper in brackets:
        try:
            f_lower = npv(lower)
            f_upper = npv(upper)
            if not math.isfinite(f_lower) or not math.isfinite(f_upper):
                continue
            if f_lower == 0:
                daily_irr = lower
                break
            if f_upper == 0:
                daily_irr = upper
                break
            if f_lower * f_upper > 0:
                continue
            daily_irr = brentq(npv, lower, upper)
            break
        except Exception:  # pragma: no cover - defensive branch
            continue

    if daily_irr is None:
        LOGGER.warning("IRR solver did not converge for full period.")
        return float("nan")
    return (1.0 + daily_irr) ** 365 - 1.0


def _assert_required_columns(df: pd.DataFrame, required_columns: list[str], label: str) -> None:
    """Raise ValueError if required columns are missing."""
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required {label} columns: {', '.join(missing)}")


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
    build_portfolio_returns(
        data_dir=Path("data"),
        nav_path=Path("data/portfolio_nav.csv"),
        cash_flows_path=Path("data/portfolio_cash_flows.csv"),
        output_path=Path("data/portfolio_returns.csv"),
        portfolio_filter="FGI",
        scope="equity_sub",
    )
