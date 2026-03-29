"""Tests for Tool 5 cash flow builder."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from tools.t5_cash_flow_builder import build_cash_flows


def _d(days_offset: int) -> str:
    return (date.today() + timedelta(days=days_offset)).isoformat()


def test_build_cash_flows_applies_buy_sell_signs(tmp_path: Path) -> None:
    """BUY should be negative cash flow and SELL should be positive."""
    tradelist_path = tmp_path / "t0_standardized_tradelist.csv"
    fx_cache_path = tmp_path / "fx_cache.csv"
    output_path = tmp_path / "portfolio_cash_flows.csv"

    tradelist_df = pd.DataFrame(
        [
            {
                "Asset Type": "Equities",
                "Portfolio": "FGI",
                "Order type": "BUY",
                "Booking date": _d(-2),
                "Quantity": "10",
                "Execution price": "50",
                "Currency": "USD",
                "Yahoo Ticker": "AAPL",
                "Asset class": "Equities",
            },
            {
                "Asset Type": "Equities",
                "Portfolio": "FGI",
                "Order type": "SELL",
                "Booking date": _d(-1),
                "Quantity": "8",
                "Execution price": "60",
                "Currency": "USD",
                "Yahoo Ticker": "AAPL",
                "Asset class": "Equities",
            },
            {
                "Asset Type": "Equities",
                "Portfolio": "SINOWELL",
                "Order type": "BUY",
                "Booking date": _d(-1),
                "Quantity": "1",
                "Execution price": "100",
                "Currency": "USD",
                "Yahoo Ticker": "TSLA",
                "Asset class": "Equities",
            },
        ]
    )
    tradelist_df.to_csv(tradelist_path, index=False)

    fx_cache_df = pd.DataFrame(
        {
            "fx_pair": ["HKDUSD=X"],
            "date": [_d(-2)],
            "fx_rate_to_usd": [0.128],
            "updated_at_utc": ["2026-01-01T00:00:00+00:00"],
        }
    )
    fx_cache_df.to_csv(fx_cache_path, index=False)

    output_df = build_cash_flows(
        data_dir=tmp_path,
        tradelist_path=tradelist_path,
        fx_cache_path=fx_cache_path,
        output_path=output_path,
    )

    assert output_path.exists()
    assert len(output_df) == 2
    buy_row = output_df[output_df["cf_type"] == "BUY"].iloc[0]
    sell_row = output_df[output_df["cf_type"] == "SELL"].iloc[0]
    assert float(buy_row["amount_local"]) == -500.0
    assert float(sell_row["amount_local"]) == 480.0
    assert float(buy_row["fx_rate_to_usd"]) == 1.0
    assert float(sell_row["fx_rate_to_usd"]) == 1.0


def test_build_cash_flows_excludes_non_equity_rows(tmp_path: Path) -> None:
    """Only Asset Type=Equities BUY/SELL rows should be included."""
    tradelist_path = tmp_path / "t0_standardized_tradelist.csv"
    fx_cache_path = tmp_path / "fx_cache.csv"
    output_path = tmp_path / "portfolio_cash_flows.csv"

    tradelist_df = pd.DataFrame(
        [
            {
                "Asset Type": "Equities",
                "Portfolio": "FGI",
                "Order type": "BUY",
                "Booking date": _d(-2),
                "Quantity": "100",
                "Execution price": "2",
                "Currency": "USD",
                "Yahoo Ticker": "WJG",
                "Asset class": "PE",
            },
            {
                "Asset Type": "Bonds",
                "Portfolio": "FGI",
                "Order type": "BUY",
                "Booking date": _d(-2),
                "Quantity": "10",
                "Execution price": "10",
                "Currency": "USD",
                "Yahoo Ticker": "BOND1",
                "Asset class": "Bonds",
            },
            {
                "Asset Type": "Equities",
                "Portfolio": "FGI",
                "Order type": "DIVIDEND",
                "Booking date": _d(-2),
                "Quantity": "1",
                "Execution price": "1",
                "Currency": "USD",
                "Yahoo Ticker": "AAPL",
                "Asset class": "Equities",
            },
        ]
    )
    tradelist_df.to_csv(tradelist_path, index=False)
    pd.DataFrame(columns=["fx_pair", "date", "fx_rate_to_usd", "updated_at_utc"]).to_csv(
        fx_cache_path,
        index=False,
    )

    output_df = build_cash_flows(
        data_dir=tmp_path,
        tradelist_path=tradelist_path,
        fx_cache_path=fx_cache_path,
        output_path=output_path,
    )

    assert len(output_df) == 1
    row = output_df.iloc[0]
    assert row["ticker"] == "WJG"
    assert row["asset_class"] == "PE"
    assert row["cf_type"] == "BUY"


def test_build_cash_flows_hkd_uses_forward_filled_fx(tmp_path: Path) -> None:
    """HKD rows should use forward-filled FX from latest available cache date."""
    tradelist_path = tmp_path / "t0_standardized_tradelist.csv"
    fx_cache_path = tmp_path / "fx_cache.csv"
    output_path = tmp_path / "portfolio_cash_flows.csv"

    tradelist_df = pd.DataFrame(
        [
            {
                "Asset Type": "Equities",
                "Portfolio": "FGI",
                "Order type": "BUY",
                "Booking date": "2026-01-03",
                "Quantity": "100",
                "Execution price": "20",
                "Currency": "HKD",
                "Yahoo Ticker": "0005.HK",
                "Asset class": "Equities",
            }
        ]
    )
    tradelist_df.to_csv(tradelist_path, index=False)

    fx_cache_df = pd.DataFrame(
        {
            "fx_pair": ["HKDUSD=X", "HKDUSD=X"],
            "date": ["2026-01-01", "2026-01-02"],
            "fx_rate_to_usd": [0.127, 0.128],
            "updated_at_utc": ["2026-01-02T00:00:00+00:00", "2026-01-02T00:00:00+00:00"],
        }
    )
    fx_cache_df.to_csv(fx_cache_path, index=False)

    output_df = build_cash_flows(
        data_dir=tmp_path,
        tradelist_path=tradelist_path,
        fx_cache_path=fx_cache_path,
        output_path=output_path,
    )

    assert len(output_df) == 1
    row = output_df.iloc[0]
    assert float(row["amount_local"]) == -2000.0
    assert float(row["fx_rate_to_usd"]) == 0.128
    assert float(row["amount_usd"]) == -256.0


def test_build_cash_flows_usd_rows_use_fx_rate_one(tmp_path: Path) -> None:
    """USD rows should always receive fx_rate_to_usd equal to 1.0."""
    tradelist_path = tmp_path / "t0_standardized_tradelist.csv"
    fx_cache_path = tmp_path / "fx_cache.csv"
    output_path = tmp_path / "portfolio_cash_flows.csv"

    tradelist_df = pd.DataFrame(
        [
            {
                "Asset Type": "Equities",
                "Portfolio": "FGI",
                "Order type": "SELL",
                "Booking date": _d(-1),
                "Quantity": "3",
                "Execution price": "100",
                "Currency": "USD",
                "Yahoo Ticker": "MSFT",
                "Asset class": "Equities",
            }
        ]
    )
    tradelist_df.to_csv(tradelist_path, index=False)
    pd.DataFrame(columns=["fx_pair", "date", "fx_rate_to_usd", "updated_at_utc"]).to_csv(
        fx_cache_path,
        index=False,
    )

    output_df = build_cash_flows(
        data_dir=tmp_path,
        tradelist_path=tradelist_path,
        fx_cache_path=fx_cache_path,
        output_path=output_path,
    )

    assert len(output_df) == 1
    row = output_df.iloc[0]
    assert float(row["fx_rate_to_usd"]) == 1.0
    assert float(row["amount_local"]) == 300.0
    assert float(row["amount_usd"]) == 300.0
