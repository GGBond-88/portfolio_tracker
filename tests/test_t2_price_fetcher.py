"""Tests for Tool 2 price fetcher and cache behavior."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from tools.t2_price_fetcher import build_priced_holdings


def _d(days_offset: int) -> str:
    return (date.today() + timedelta(days=days_offset)).isoformat()


def test_build_priced_holdings_adds_pnl_columns(monkeypatch, tmp_path: Path) -> None:
    """Tool 2 should enrich holdings with market and pnl columns."""
    holdings_path = tmp_path / "daily_holdings.csv"
    output_path = tmp_path / "priced_holdings.csv"
    cache_path = tmp_path / "prices_cache.csv"
    holdings_df = pd.DataFrame(
        [
            {
                "date": _d(-2),
                "entry_date": _d(-5),
                "Portfolio": "FGI",
                "Asset class": "EQUITY",
                "Ticker / ISIN / Reference": "700 HK EQUITY",
                "Security name": "Tencent",
                "Currency": "HKD",
                "Yahoo Ticker": "0700.HK",
                "symbol": "0700.HK",
                "isin": "KYG875721634",
                "name": "Tencent",
                "quantity": "10",
                "avg_cost": "100",
                "cost_basis": "1000",
                "realized_pnl": "50",
            },
            {
                "date": _d(-1),
                "entry_date": _d(-5),
                "Portfolio": "FGI",
                "Asset class": "EQUITY",
                "Ticker / ISIN / Reference": "700 HK EQUITY",
                "Security name": "Tencent",
                "Currency": "HKD",
                "Yahoo Ticker": "0700.HK",
                "symbol": "0700.HK",
                "isin": "KYG875721634",
                "name": "Tencent",
                "quantity": "10",
                "avg_cost": "100",
                "cost_basis": "1000",
                "realized_pnl": "50",
            },
        ]
    )
    holdings_df.to_csv(holdings_path, index=False)

    idx = pd.to_datetime([_d(-2), _d(-1)])
    fake_history = pd.DataFrame({"Close": [120.0, 130.0]}, index=idx)

    def fake_download(*args, **kwargs):  # type: ignore[no-untyped-def]
        return fake_history

    monkeypatch.setattr("tools.t2_price_fetcher.yf.download", fake_download)

    priced_df = build_priced_holdings(
        data_dir=tmp_path,
        holdings_path=holdings_path,
        output_path=output_path,
        cache_path=cache_path,
    )

    assert output_path.exists()
    assert cache_path.exists()
    assert "unrealized_pnl" in priced_df.columns
    assert {"market_price", "market_value", "total_pnl"}.issubset(priced_df.columns)
    assert "unreatlized_pnl" in priced_df.columns
    assert priced_df["unreatlized_pnl"].astype(float).tolist() == priced_df["unrealized_pnl"].astype(float).tolist()
    assert float(priced_df.loc[0, "market_price"]) == 120.0
    assert float(priced_df.loc[1, "market_price"]) == 130.0
    assert float(priced_df.loc[1, "market_value"]) == 1300.0
    assert float(priced_df.loc[1, "unrealized_pnl"]) == 300.0
    assert float(priced_df.loc[1, "total_pnl"]) == 350.0


def test_build_priced_holdings_reuses_cache_without_fetch(monkeypatch, tmp_path: Path) -> None:
    """Tool 2 should skip yfinance call when cache fully covers requested range."""
    holdings_path = tmp_path / "daily_holdings.csv"
    output_path = tmp_path / "priced_holdings.csv"
    cache_path = tmp_path / "prices_cache.csv"

    holdings_df = pd.DataFrame(
        [
            {
                "date": _d(-2),
                "entry_date": _d(-3),
                "Portfolio": "FGI",
                "Asset class": "EQUITY",
                "Ticker / ISIN / Reference": "AAPL US EQUITY",
                "Security name": "Apple",
                "Currency": "USD",
                "Yahoo Ticker": "AAPL",
                "symbol": "AAPL",
                "isin": "US0378331005",
                "name": "Apple",
                "quantity": "2",
                "avg_cost": "100",
                "cost_basis": "200",
                "realized_pnl": "0",
            },
            {
                "date": _d(-1),
                "entry_date": _d(-3),
                "Portfolio": "FGI",
                "Asset class": "EQUITY",
                "Ticker / ISIN / Reference": "AAPL US EQUITY",
                "Security name": "Apple",
                "Currency": "USD",
                "Yahoo Ticker": "AAPL",
                "symbol": "AAPL",
                "isin": "US0378331005",
                "name": "Apple",
                "quantity": "2",
                "avg_cost": "100",
                "cost_basis": "200",
                "realized_pnl": "0",
            },
        ]
    )
    holdings_df.to_csv(holdings_path, index=False)

    cache_df = pd.DataFrame(
        {
            "yahoo_ticker": ["AAPL", "AAPL"],
            "date": [_d(-2), _d(-1)],
            "market_price": [140.0, 150.0],
            "updated_at_utc": ["2026-01-01T00:00:00+00:00", "2026-01-01T00:00:00+00:00"],
        }
    )
    cache_df.to_csv(cache_path, index=False)

    def fail_download(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("yfinance should not be called when cache is sufficient")

    monkeypatch.setattr("tools.t2_price_fetcher.yf.download", fail_download)

    priced_df = build_priced_holdings(
        data_dir=tmp_path,
        holdings_path=holdings_path,
        output_path=output_path,
        cache_path=cache_path,
    )

    assert "unrealized_pnl" in priced_df.columns
    assert float(priced_df.loc[0, "market_price"]) == 140.0
    assert float(priced_df.loc[1, "market_price"]) == 150.0
