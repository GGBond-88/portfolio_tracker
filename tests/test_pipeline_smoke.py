"""End-to-end pipeline smoke test with network calls mocked."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from pipeline import run_pipeline


def _d(days_offset: int) -> str:
    return (date.today() + timedelta(days=days_offset)).isoformat()


def _raw_tradelist_rows() -> list[dict[str, str]]:
    return [
        {
            "Action": "Buy",
            "Portfolio": "FGI",
            "Broker & acct": "",
            "Asset class": "Equities",
            "Ticker / ISIN / Reference": "AAPL US EQUITY",
            "Security name": "Apple Inc",
            "Currency": "USD",
            "Trade date": _d(-2),
            " Executed unit price ": "100.00",
            " Executed quantity ": "10",
            "Transaction cost": "0",
        },
        {
            "Action": "Buy",
            "Portfolio": "FGI",
            "Broker & acct": "",
            "Asset class": "Equities",
            "Ticker / ISIN / Reference": "AAPL US EQUITY",
            "Security name": "Apple Inc",
            "Currency": "USD",
            "Trade date": _d(-1),
            " Executed unit price ": "100.00",
            " Executed quantity ": "5",
            "Transaction cost": "0",
        },
    ]


def _fake_yf_download(*args, **kwargs):  # type: ignore[no-untyped-def]
    """Return a daily Close series for any yfinance equity or FX download in t2/t3."""
    start_raw = kwargs.get("start")
    end_raw = kwargs.get("end")
    start = pd.to_datetime(start_raw).normalize()
    end = pd.to_datetime(end_raw).normalize()
    idx = pd.date_range(start=start, end=end, freq="D")
    if len(idx) == 0:
        idx = pd.DatetimeIndex([start])
    return pd.DataFrame({"Close": [150.0] * len(idx)}, index=idx)


class _FakeTicker:
    """Minimal yfinance.Ticker stand-in for Tool 7."""

    _history = pd.DataFrame({"Close": list(range(1, 25))})
    _info: dict[str, object] = {
        "sector": "Technology",
        "industry": "Consumer Electronics",
        "trailingPE": 22.0,
        "priceToBook": 35.0,
        "trailingEps": 6.0,
        "bookValue": 4.0,
        "dividendYield": 0.005,
        "trailingAnnualDividendRate": 0.96,
        "marketCap": 1_000_000_000,
        "fiftyTwoWeekHigh": 220.0,
        "fiftyTwoWeekLow": 160.0,
    }

    def __init__(self, ticker: str) -> None:
        self._ticker = ticker

    @property
    def info(self) -> dict[str, object]:
        return dict(_FakeTicker._info)

    def history(self, period: str = "1mo") -> pd.DataFrame:
        assert period == "1mo"
        return _FakeTicker._history.copy()


@pytest.fixture
def pipeline_data_dir(tmp_path: Path) -> Path:
    """tmp_path with a minimal raw tradelist discoverable by Tool 0."""
    tradelist = pd.DataFrame(_raw_tradelist_rows())
    tradelist.to_csv(tmp_path / "smoke_tradelist.csv", index=False)
    return tmp_path


@patch("tools.t7_fundamentals_snapshot.yf.Ticker", _FakeTicker)
@patch("tools.t3_fx_converter.yf.download", _fake_yf_download)
@patch("tools.t2_price_fetcher.yf.download", _fake_yf_download)
def test_run_pipeline_end_to_end_smoke(
    pipeline_data_dir: Path,
) -> None:
    """Full t0–t7 run: outputs exist, key tables non-empty, core numeric columns free of NaN."""
    data_dir = pipeline_data_dir
    result = run_pipeline(data_dir=data_dir)

    expected_files = [
        data_dir / "t0_standardized_tradelist.csv",
        data_dir / "daily_holdings.csv",
        data_dir / "exited_positions.csv",
        data_dir / "priced_holdings.csv",
        data_dir / "prices_cache.csv",
        data_dir / "priced_holdings_usd.csv",
        data_dir / "fx_cache.csv",
        data_dir / "portfolio_nav.csv",
        data_dir / "portfolio_cash_flows.csv",
        data_dir / "portfolio_returns.csv",
        data_dir / "fundamentals_snapshot.csv",
    ]
    for path in expected_files:
        assert path.is_file(), f"missing output: {path}"

    assert len(result["t0_standardized_tradelist"]) >= 2
    assert len(result["t1_daily_holdings"]) > 0
    assert len(result["t2_priced_holdings"]) > 0
    assert len(result["t3_priced_holdings_usd"]) > 0

    nav_df = pd.read_csv(data_dir / "portfolio_nav.csv")
    returns_df = pd.read_csv(data_dir / "portfolio_returns.csv")
    cf_df = pd.read_csv(data_dir / "portfolio_cash_flows.csv")
    fund_df = pd.read_csv(data_dir / "fundamentals_snapshot.csv")

    assert len(nav_df) > 0
    assert len(returns_df) > 0
    assert len(cf_df) > 0
    assert len(fund_df) > 0

    nav_value_cols = [
        "total_market_value_usd",
        "total_cost_basis_usd",
        "total_unrealized_pnl_usd",
        "total_realized_pnl_usd",
        "total_pnl_usd",
    ]
    for col in nav_value_cols:
        assert col in nav_df.columns
        assert pd.to_numeric(nav_df[col], errors="coerce").notna().all(), f"NaN in portfolio_nav.{col}"

    assert pd.to_numeric(returns_df["nav_usd"], errors="coerce").notna().all()
    assert pd.to_numeric(returns_df["daily_net_cf_usd"], errors="coerce").notna().all()
    assert pd.to_numeric(returns_df["cumulative_twr"], errors="coerce").notna().all()

    assert pd.to_numeric(cf_df["amount_usd"], errors="coerce").notna().all()
    assert pd.to_numeric(cf_df["fx_rate_to_usd"], errors="coerce").notna().all()

    fund_numeric = [
        "quantity",
        "market_value_usd",
        "weight_pct",
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
    for col in fund_numeric:
        assert col in fund_df.columns
        assert pd.to_numeric(fund_df[col], errors="coerce").notna().all(), f"NaN in fundamentals_snapshot.{col}"
