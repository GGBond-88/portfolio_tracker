"""Tests for Tool 6 portfolio return calculator."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

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
    """BUY cash flow (negative): simple TWR uses prior NAV as denominator."""
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
    assert abs(float(out_df.loc[1, "daily_return_twr"]) - (10.0 / 100.0)) < 1e-6
    assert float(out_df.loc[2, "daily_return_twr"]) == 0.05
    cumulative = (1.0 + 10.0 / 100.0) * 1.05 - 1.0
    assert abs(float(out_df.loc[2, "cumulative_twr"]) - cumulative) < 1e-6


def test_build_portfolio_returns_sell_outflow_uses_nav_prev_denominator(tmp_path: Path) -> None:
    """SELL cash flow (positive): same denominator nav_prev as BUY path."""
    nav_path = tmp_path / "portfolio_nav.csv"
    cf_path = tmp_path / "portfolio_cash_flows.csv"
    output_path = tmp_path / "portfolio_returns.csv"

    nav_df = _make_nav_df(
        [
            {"date": "2026-01-01", "portfolio": "FGI", "total_market_value_usd": "100"},
            {"date": "2026-01-02", "portfolio": "FGI", "total_market_value_usd": "90"},
            {"date": "2026-01-03", "portfolio": "FGI", "total_market_value_usd": "94.5"},
        ]
    )
    nav_df.to_csv(nav_path, index=False)

    cf_df = _make_cf_df(
        [
            {
                "date": "2026-01-02",
                "portfolio": "FGI",
                "cf_type": "SELL",
                "amount_local": "20",
                "amount_usd": "20",
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

    assert float(out_df.loc[1, "daily_net_cf_usd"]) == 20.0
    # (90 - 100 + 20) / 100 = 0.10
    assert abs(float(out_df.loc[1, "daily_return_twr"]) - 0.10) < 1e-6
    assert float(out_df.loc[2, "daily_return_twr"]) == 0.05
    cumulative = 1.10 * 1.05 - 1.0
    assert abs(float(out_df.loc[2, "cumulative_twr"]) - cumulative) < 1e-6


def test_build_portfolio_returns_feb23_real_numbers_hand_check(tmp_path: Path) -> None:
    """FGI Feb 23 row: (nav_t - nav_prev + cf) / nav_prev matches hand calculation."""
    nav_path = tmp_path / "portfolio_nav.csv"
    cf_path = tmp_path / "portfolio_cash_flows.csv"
    output_path = tmp_path / "portfolio_returns.csv"

    nav_prev = 2219708.36
    nav_t = 2423530.69
    buy_usd = 164566.78
    expected_daily = (nav_t - nav_prev - buy_usd) / nav_prev

    nav_df = _make_nav_df(
        [
            {"date": "2026-02-22", "portfolio": "FGI", "total_market_value_usd": str(nav_prev)},
            {"date": "2026-02-23", "portfolio": "FGI", "total_market_value_usd": str(nav_t)},
        ]
    )
    nav_df.to_csv(nav_path, index=False)

    cf_df = _make_cf_df(
        [
            {
                "date": "2026-02-23",
                "portfolio": "FGI",
                "cf_type": "BUY",
                "amount_local": f"-{buy_usd}",
                "amount_usd": f"-{buy_usd}",
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

    assert abs(float(out_df.loc[1, "daily_return_twr"]) - expected_daily) < 1e-5
    assert abs(expected_daily - 0.01769) < 2e-4


def test_build_portfolio_returns_irr_known_answer(tmp_path: Path) -> None:
    """Full-period annualized IRR ~10% for 1000→1100 in 365 days.

    Cash flows file is empty → ``daily_net_cf_usd.iloc[0] == 0`` → initial IRR
    outflow falls back to ``-nav_usd[0]`` (-1000).
    """
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

    irr_full = float(out_df.loc[1, "irr_annualized_full"])
    assert abs(irr_full - 0.10) < 1e-6
    assert out_df["irr_annualized_full"].notna().all()
    assert pd.isna(out_df.loc[0, "irr_annualized_itd"])
    irr_itd_end = float(out_df.loc[1, "irr_annualized_itd"])
    assert abs(irr_itd_end - 0.10) < 1e-6


def test_irr_uses_initial_cf_not_nav(tmp_path: Path) -> None:
    """When day-0 has net CF, IRR uses it as t=0 outflow, not -NAV."""
    nav_path = tmp_path / "portfolio_nav.csv"
    cf_path = tmp_path / "portfolio_cash_flows.csv"
    output_path = tmp_path / "portfolio_returns.csv"

    nav_df = _make_nav_df(
        [
            {"date": "2026-01-01", "portfolio": "FGI", "total_market_value_usd": "1050"},
            {"date": "2026-01-26", "portfolio": "FGI", "total_market_value_usd": "1100"},
        ]
    )
    nav_df.to_csv(nav_path, index=False)
    cf_df = _make_cf_df(
        [
            {
                "date": "2026-01-01",
                "portfolio": "FGI",
                "cf_type": "BUY",
                "amount_local": "-1000",
                "amount_usd": "-1000",
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

    span_days = 25  # 2026-01-01 → 2026-01-26 in IRR day-count
    irr_cf_based = (1100.0 / 1000.0) ** (365.0 / span_days) - 1.0
    irr_nav_based = (1100.0 / 1050.0) ** (365.0 / span_days) - 1.0
    irr_full = float(out_df.loc[1, "irr_annualized_full"])
    assert irr_full == pytest.approx(irr_cf_based, rel=1e-6)
    assert irr_full != pytest.approx(irr_nav_based, rel=1e-2)


def test_irr_fallback_no_day0_cf_uses_nav_for_itd(tmp_path: Path) -> None:
    """No day-0 CF → initial outflow is -NAV; short-window ITD matches CAGR on NAV."""
    nav_path = tmp_path / "portfolio_nav.csv"
    cf_path = tmp_path / "portfolio_cash_flows.csv"
    output_path = tmp_path / "portfolio_returns.csv"

    nav_df = _make_nav_df(
        [
            {"date": "2026-01-01", "portfolio": "FGI", "total_market_value_usd": "1000"},
            {"date": "2026-01-26", "portfolio": "FGI", "total_market_value_usd": "1004"},
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

    span_days = 25  # 2026-01-01 → 2026-01-26
    expected = (1004.0 / 1000.0) ** (365.0 / span_days) - 1.0
    assert expected == pytest.approx(0.0596, rel=0.02)
    irr_itd = float(out_df.loc[1, "irr_annualized_itd"])
    assert irr_itd == pytest.approx(expected, rel=1e-6)
    irr_full = float(out_df.loc[1, "irr_annualized_full"])
    assert irr_full == pytest.approx(expected, rel=1e-6)


def test_irr_day0_small_cf_vs_nav_uses_full_nav(tmp_path: Path) -> None:
    """Day-0 |CF| < 50% of NAV → treat as top-up; initial outflow is -NAV, not CF."""
    nav_path = tmp_path / "portfolio_nav.csv"
    cf_path = tmp_path / "portfolio_cash_flows.csv"
    output_path = tmp_path / "portfolio_returns.csv"

    nav_df = _make_nav_df(
        [
            {"date": "2026-01-01", "portfolio": "FGI", "total_market_value_usd": "51000"},
            {"date": "2026-01-26", "portfolio": "FGI", "total_market_value_usd": "52000"},
        ]
    )
    nav_df.to_csv(nav_path, index=False)
    cf_df = _make_cf_df(
        [
            {
                "date": "2026-01-01",
                "portfolio": "FGI",
                "cf_type": "BUY",
                "amount_local": "-1000",
                "amount_usd": "-1000",
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

    span_days = 25
    expected_nav_basis = (52000.0 / 51000.0) ** (365.0 / span_days) - 1.0
    irr_if_only_cf = (52000.0 / 1000.0) ** (365.0 / span_days) - 1.0
    irr_full = float(out_df.loc[1, "irr_annualized_full"])
    assert irr_full == pytest.approx(expected_nav_basis, rel=1e-6)
    assert irr_full != pytest.approx(irr_if_only_cf, rel=1e-2)
