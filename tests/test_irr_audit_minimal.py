"""Audit: full-period vs rolling ITD IRR behavior."""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import pytest

from tools.t6_return_calculator import _compute_annualized_irr, build_portfolio_returns


def _linear_nav_series(*, last_day: int, end_nav: float) -> tuple[pd.Series, pd.Series]:
    """Day 0 invest 10k; linear path to end_nav on calendar day `last_day` (inclusive rows 0..last_day)."""
    start = date(2026, 1, 1)
    dates = [start + timedelta(days=d) for d in range(last_day + 1)]
    nav0 = 10_000.0
    nav = [nav0 + (end_nav - nav0) * (d / last_day) for d in range(last_day + 1)]
    daily_cf = [0.0] * len(dates)
    return pd.Series(dates), pd.Series(nav), pd.Series(daily_cf)


def test_compute_annualized_irr_differs_for_day5_vs_day10_window() -> None:
    """IRR through day 5 (MV=10250) vs through day 10 (MV=10500) must differ."""
    d5, n5, cf5 = _linear_nav_series(last_day=5, end_nav=10_250.0)
    d10, n10, cf10 = _linear_nav_series(last_day=10, end_nav=10_500.0)

    irr5 = _compute_annualized_irr(dates=d5, nav_usd=n5, daily_net_cf_usd=cf5)
    irr10 = _compute_annualized_irr(dates=d10, nav_usd=n10, daily_net_cf_usd=cf10)

    expected_10 = (10_500.0 / 10_000.0) ** (365.0 / 10.0) - 1.0
    expected_5 = (10_250.0 / 10_000.0) ** (365.0 / 5.0) - 1.0

    assert irr10 == pytest.approx(expected_10, rel=1e-9)
    assert irr5 == pytest.approx(expected_5, rel=1e-9)
    assert irr5 != pytest.approx(irr10, rel=1e-6)


def test_build_portfolio_returns_full_irr_constant_itd_varies(tmp_path) -> None:
    """Full-period IRR is constant; rolling ITD (sampled + ffill) takes more than one value."""
    nav_rows = []
    start = date(2026, 1, 1)
    for d in range(11):
        day = start + timedelta(days=d)
        nav = 10_000.0 + (10_500.0 - 10_000.0) * (d / 10.0)
        nav_rows.append(
            {
                "date": day.isoformat(),
                "portfolio": "FGI",
                "total_market_value_usd": str(nav),
            }
        )
    nav_df = pd.DataFrame(nav_rows)
    cf_df = pd.DataFrame(
        columns=["date", "portfolio", "amount_usd"],
    )

    nav_path = tmp_path / "nav.csv"
    cf_path = tmp_path / "cf.csv"
    out_path = tmp_path / "returns.csv"
    nav_df.to_csv(nav_path, index=False)
    cf_df.to_csv(cf_path, index=False)

    result = build_portfolio_returns(
        data_dir=tmp_path,
        nav_path=nav_path,
        cash_flows_path=cf_path,
        output_path=out_path,
        portfolio_filter="FGI",
        scope="equity_sub",
    )

    full_col = pd.to_numeric(result["irr_annualized_full"], errors="coerce")
    assert full_col.nunique(dropna=True) == 1
    itd_col = pd.to_numeric(result["irr_annualized_itd"], errors="coerce")
    assert itd_col.nunique(dropna=True) > 1
    assert len(result) == 11


def test_build_portfolio_returns_itd_day5_differs_from_day10_linear_nav(tmp_path) -> None:
    """Linear NAV from audit helper: ITD IRR at row 5 (ffill from first Sunday) != row 10."""
    nav_rows = []
    start = date(2026, 1, 1)
    for d in range(11):
        day = start + timedelta(days=d)
        nav = 10_000.0 + (10_500.0 - 10_000.0) * (d / 10.0)
        nav_rows.append(
            {
                "date": day.isoformat(),
                "portfolio": "FGI",
                "total_market_value_usd": str(nav),
            }
        )
    nav_df = pd.DataFrame(nav_rows)
    cf_df = pd.DataFrame(columns=["date", "portfolio", "amount_usd"])
    nav_path = tmp_path / "nav.csv"
    cf_path = tmp_path / "cf.csv"
    out_path = tmp_path / "returns.csv"
    nav_df.to_csv(nav_path, index=False)
    cf_df.to_csv(cf_path, index=False)

    result = build_portfolio_returns(
        data_dir=tmp_path,
        nav_path=nav_path,
        cash_flows_path=cf_path,
        output_path=out_path,
        portfolio_filter="FGI",
        scope="equity_sub",
    )

    irr5 = float(pd.to_numeric(result.loc[5, "irr_annualized_itd"], errors="coerce"))
    irr10 = float(pd.to_numeric(result.loc[10, "irr_annualized_itd"], errors="coerce"))
    assert irr5 != pytest.approx(irr10, rel=1e-6)
