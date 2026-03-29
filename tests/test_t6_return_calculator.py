"""Tests for Tool 6 portfolio return calculator."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from tools.t6_return_calculator import build_portfolio_returns


def _make_nav_df(rows: list[dict[str, str]]) -> pd.DataFrame:
    """Create NAV test DataFrame with required Tool 4 schema."""
    defaults = {
        "scope": "fgi_equities",
        "position_count": "1",
        "total_cost_basis_usd": "0",
        "total_unrealized_pnl_usd": "0",
        "total_realized_pnl_usd": "0",
        "total_pnl_usd": "0",
        "daily_return_pct": "",
    }
    normalized: list[dict[str, str]] = []
    for row in rows:
        merged = defaults.copy()
        merged.update(row)
        normalized.append(merged)
    return pd.DataFrame(normalized)


def _make_cf_df(rows: list[dict[str, str]]) -> pd.DataFrame:
    """Create cash flow test DataFrame with required Tool 5 schema."""
    defaults = {
        "scope": "equity_sub",
        "cf_type": "BUY",
        "ticker": "TEST",
        "asset_class": "Equities",
        "currency": "USD",
        "amount_local": "0",
        "fx_rate_to_usd": "1.0",
    }
    normalized: list[dict[str, str]] = []
    for row in rows:
        merged = defaults.copy()
        merged.update(row)
        normalized.append(merged)
    return pd.DataFrame(normalized)


def test_build_portfolio_returns_simple_no_cash_flows(tmp_path: Path) -> None:
    """No-cash-flow path should produce 10%, 10%, cumulative 21%."""
    nav_path = tmp_path / "portfolio_nav.csv"
    cf_path = tmp_path / "portfolio_cash_flows.csv"
    output_path = tmp_path / "portfolio_returns.csv"

    nav_df = _make_nav_df(
        [
            {"date": "2026-01-01", "portfolio": "FGI", "total_market_value_usd": "100"},
            {"date": "2026-01-02", "portfolio": "FGI", "total_market_value_usd": "110"},
            {"date": "2026-01-03", "portfolio": "FGI", "total_market_value_usd": "121"},
        ]
    )
    nav_df.to_csv(nav_path, index=False)
    _make_cf_df([]).to_csv(cf_path, index=False)

    out_df = build_portfolio_returns(
        data_dir=tmp_path,
        nav_path=nav_path,
        cash_flows_path=cf_path,
        output_path=output_path,
    )

    assert output_path.exists()
    assert len(out_df) == 3
    assert pd.isna(out_df.loc[0, "daily_return_twr"])
    assert float(out_df.loc[1, "daily_return_twr"]) == 0.1
    assert float(out_df.loc[2, "daily_return_twr"]) == 0.1
    assert float(out_df.loc[2, "cumulative_twr"]) == 0.21
    assert float(out_df.loc[2, "itd_return"]) == 0.21


def test_build_portfolio_returns_adjusts_for_inflow_cash_flow(tmp_path: Path) -> None:
    """BUY cash flow (negative) should apply inflow-adjusted denominator."""
    nav_path = tmp_path / "portfolio_nav.csv"
    cf_path = tmp_path / "portfolio_cash_flows.csv"
    output_path = tmp_path / "portfolio_returns.csv"

    nav_df = _make_nav_df(
        [
            {"date": "2026-01-01", "portfolio": "FGI", "total_market_value_usd": "100"},
            {"date": "2026-01-02", "portfolio": "FGI", "total_market_value_usd": "160"},
            {"date": "2026-01-03", "portfolio": "FGI", "total_market_value_usd": "168"},
        ]
    )
    nav_df.to_csv(nav_path, index=False)

    cf_df = _make_cf_df(
        [
            {
                "date": "2026-01-02",
                "portfolio": "FGI",
                "cf_type": "BUY",
                "amount_local": "-50",
                "amount_usd": "-50",
            }
        ]
    )
    cf_df.to_csv(cf_path, index=False)

    out_df = build_portfolio_returns(
        data_dir=tmp_path,
        nav_path=nav_path,
        cash_flows_path=cf_path,
        output_path=output_path,
    )

    assert float(out_df.loc[1, "daily_net_cf_usd"]) == -50.0
    assert abs(float(out_df.loc[1, "daily_return_twr"]) - (10.0 / 150.0)) < 1e-6
    assert float(out_df.loc[2, "daily_return_twr"]) == 0.05
    cumulative = (1.0 + 10.0 / 150.0) * 1.05 - 1.0
    assert abs(float(out_df.loc[2, "cumulative_twr"]) - cumulative) < 1e-6


def test_build_portfolio_returns_irr_known_answer(tmp_path: Path) -> None:
    """Full-period annualized IRR should be ~10% for 1000 to 1100 in 365 days."""
    nav_path = tmp_path / "portfolio_nav.csv"
    cf_path = tmp_path / "portfolio_cash_flows.csv"
    output_path = tmp_path / "portfolio_returns.csv"

    nav_df = _make_nav_df(
        [
            {"date": "2026-01-01", "portfolio": "FGI", "total_market_value_usd": "1000"},
            {"date": "2027-01-01", "portfolio": "FGI", "total_market_value_usd": "1100"},
        ]
    )
    nav_df.to_csv(nav_path, index=False)
    _make_cf_df([]).to_csv(cf_path, index=False)

    out_df = build_portfolio_returns(
        data_dir=tmp_path,
        nav_path=nav_path,
        cash_flows_path=cf_path,
        output_path=output_path,
    )

    irr_value = float(out_df.loc[1, "irr_annualized"])
    assert abs(irr_value - 0.10) < 1e-6
    assert out_df["irr_annualized"].notna().all()
