"""Tests for Tool 3 FX converter and FX cache behavior."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from tools.t3_fx_converter import build_fx_converted_holdings


def _d(days_offset: int) -> str:
    return (date.today() + timedelta(days=days_offset)).isoformat()


def test_build_fx_converted_holdings_adds_usd_columns_for_multi_currency(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Tool 3 should apply FX rates and add *_usd columns."""
    priced_holdings_path = tmp_path / "priced_holdings.csv"
    output_path = tmp_path / "priced_holdings_usd.csv"
    fx_cache_path = tmp_path / "fx_cache.csv"

    priced_df = pd.DataFrame(
        [
            {
                "date": _d(-2),
                "Currency": "HKD",
                "cost_basis": "1000",
                "market_value": "1200",
                "unrealized_pnl": "200",
                "realized_pnl": "50",
                "total_pnl": "250",
            },
            {
                "date": _d(-1),
                "Currency": "GBP",
                "cost_basis": "500",
                "market_value": "700",
                "unrealized_pnl": "200",
                "realized_pnl": "10",
                "total_pnl": "210",
            },
            {
                "date": _d(-1),
                "Currency": "USD",
                "cost_basis": "200",
                "market_value": "220",
                "unrealized_pnl": "20",
                "realized_pnl": "5",
                "total_pnl": "25",
            },
        ]
    )
    priced_df.to_csv(priced_holdings_path, index=False)

    hkd_history = pd.DataFrame({"Close": [0.1280, 0.1290]}, index=pd.to_datetime([_d(-2), _d(-1)]))
    gbp_history = pd.DataFrame({"Close": [1.2500, 1.2600]}, index=pd.to_datetime([_d(-2), _d(-1)]))

    def fake_download(*args, **kwargs):  # type: ignore[no-untyped-def]
        ticker = kwargs["tickers"]
        if ticker == "HKDUSD=X":
            return hkd_history
        if ticker == "GBPUSD=X":
            return gbp_history
        raise AssertionError(f"Unexpected ticker fetch: {ticker}")

    monkeypatch.setattr("tools.t3_fx_converter.yf.download", fake_download)

    output_df = build_fx_converted_holdings(
        data_dir=tmp_path,
        priced_holdings_path=priced_holdings_path,
        output_path=output_path,
        fx_cache_path=fx_cache_path,
    )

    assert output_path.exists()
    assert fx_cache_path.exists()
    assert {
        "fx_rate_to_usd",
        "cost_basis_usd",
        "market_value_usd",
        "unrealized_pnl_usd",
        "realized_pnl_usd",
        "total_pnl_usd",
    }.issubset(output_df.columns)

    hkd_row = output_df[output_df["Currency"] == "HKD"].iloc[0]
    assert float(hkd_row["fx_rate_to_usd"]) == 0.128
    assert float(hkd_row["market_value_usd"]) == 153.6
    assert float(hkd_row["total_pnl_usd"]) == 32.0

    gbp_row = output_df[output_df["Currency"] == "GBP"].iloc[0]
    assert float(gbp_row["fx_rate_to_usd"]) == 1.26
    assert float(gbp_row["total_pnl_usd"]) == 264.6

    usd_row = output_df[output_df["Currency"] == "USD"].iloc[0]
    assert float(usd_row["fx_rate_to_usd"]) == 1.0
    assert float(usd_row["market_value_usd"]) == 220.0


def test_build_fx_converted_holdings_reuses_fx_cache_without_fetch(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Tool 3 should skip yfinance call when FX cache fully covers range."""
    priced_holdings_path = tmp_path / "priced_holdings.csv"
    output_path = tmp_path / "priced_holdings_usd.csv"
    fx_cache_path = tmp_path / "fx_cache.csv"

    priced_df = pd.DataFrame(
        [
            {
                "date": _d(-2),
                "Currency": "HKD",
                "cost_basis": "1000",
                "market_value": "1200",
                "unrealized_pnl": "200",
                "realized_pnl": "50",
                "total_pnl": "250",
            },
            {
                "date": _d(-1),
                "Currency": "HKD",
                "cost_basis": "1000",
                "market_value": "1300",
                "unrealized_pnl": "300",
                "realized_pnl": "50",
                "total_pnl": "350",
            },
        ]
    )
    priced_df.to_csv(priced_holdings_path, index=False)

    cache_df = pd.DataFrame(
        {
            "fx_pair": ["HKDUSD=X", "HKDUSD=X"],
            "date": [_d(-2), _d(-1)],
            "fx_rate_to_usd": [0.128, 0.129],
            "updated_at_utc": ["2026-01-01T00:00:00+00:00", "2026-01-01T00:00:00+00:00"],
        }
    )
    cache_df.to_csv(fx_cache_path, index=False)

    def fail_download(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("yfinance should not be called when FX cache is sufficient")

    monkeypatch.setattr("tools.t3_fx_converter.yf.download", fail_download)

    output_df = build_fx_converted_holdings(
        data_dir=tmp_path,
        priced_holdings_path=priced_holdings_path,
        output_path=output_path,
        fx_cache_path=fx_cache_path,
    )

    assert float(output_df.loc[0, "fx_rate_to_usd"]) == 0.128
    assert float(output_df.loc[1, "fx_rate_to_usd"]) == 0.129
