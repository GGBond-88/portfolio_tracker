"""Tests for Tool 4 portfolio NAV aggregation."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from tools.t4_portfolio_nav import build_portfolio_nav


def _d(days_offset: int) -> str:
    return (date.today() + timedelta(days=days_offset)).isoformat()


def test_build_portfolio_nav_default_filters_fgi_equities(tmp_path: Path) -> None:
    """Tool 4 should aggregate only FGI Equities by default."""
    priced_holdings_path = tmp_path / "priced_holdings_usd.csv"
    output_path = tmp_path / "portfolio_nav.csv"

    priced_df = pd.DataFrame(
        [
            {
                "date": _d(-2),
                "Portfolio": "FGI",
                "Asset class": "Equities",
                "market_value_usd": "1000",
                "cost_basis_usd": "900",
                "unrealized_pnl_usd": "100",
                "realized_pnl_usd": "10",
                "total_pnl_usd": "110",
            },
            {
                "date": _d(-2),
                "Portfolio": "FGI",
                "Asset class": "Equities",
                "market_value_usd": "2000",
                "cost_basis_usd": "1800",
                "unrealized_pnl_usd": "200",
                "realized_pnl_usd": "30",
                "total_pnl_usd": "230",
            },
            {
                "date": _d(-2),
                "Portfolio": "FGI",
                "Asset class": "Cash",
                "market_value_usd": "9999",
                "cost_basis_usd": "9999",
                "unrealized_pnl_usd": "0",
                "realized_pnl_usd": "0",
                "total_pnl_usd": "0",
            },
            {
                "date": _d(-2),
                "Portfolio": "SINOWELL",
                "Asset class": "Equities",
                "market_value_usd": "8888",
                "cost_basis_usd": "8000",
                "unrealized_pnl_usd": "888",
                "realized_pnl_usd": "88",
                "total_pnl_usd": "976",
            },
            {
                "date": _d(-1),
                "Portfolio": "FGI",
                "Asset class": "Equities",
                "market_value_usd": "3000",
                "cost_basis_usd": "2500",
                "unrealized_pnl_usd": "500",
                "realized_pnl_usd": "40",
                "total_pnl_usd": "540",
            },
        ]
    )
    priced_df.to_csv(priced_holdings_path, index=False)

    nav_df = build_portfolio_nav(
        data_dir=tmp_path,
        priced_holdings_usd_path=priced_holdings_path,
        output_path=output_path,
    )

    assert output_path.exists()
    assert len(nav_df) == 2
    assert set(nav_df["portfolio"].tolist()) == {"FGI"}
    assert set(nav_df["scope"].tolist()) == {"fgi_equities"}

    day_1 = nav_df[nav_df["date"] == _d(-2)].iloc[0]
    assert int(day_1["position_count"]) == 2
    assert float(day_1["total_market_value_usd"]) == 3000.0
    assert float(day_1["total_cost_basis_usd"]) == 2700.0
    assert float(day_1["total_unrealized_pnl_usd"]) == 300.0
    assert float(day_1["total_realized_pnl_usd"]) == 40.0
    assert float(day_1["total_pnl_usd"]) == 340.0
    assert pd.isna(day_1["daily_return_pct"])

    day_2 = nav_df[nav_df["date"] == _d(-1)].iloc[0]
    assert float(day_2["total_market_value_usd"]) == 3000.0
    assert float(day_2["daily_return_pct"]) == 0.0


def test_build_portfolio_nav_with_custom_filters(tmp_path: Path) -> None:
    """Tool 4 should respect configurable portfolio/asset class filters."""
    priced_holdings_path = tmp_path / "priced_holdings_usd.csv"
    output_path = tmp_path / "portfolio_nav.csv"

    priced_df = pd.DataFrame(
        [
            {
                "date": _d(-1),
                "Portfolio": "FGI",
                "Asset class": "Cash",
                "market_value_usd": "500",
                "cost_basis_usd": "500",
                "unrealized_pnl_usd": "0",
                "realized_pnl_usd": "0",
                "total_pnl_usd": "0",
            },
            {
                "date": _d(-1),
                "Portfolio": "FGI",
                "Asset class": "Equities",
                "market_value_usd": "1000",
                "cost_basis_usd": "900",
                "unrealized_pnl_usd": "100",
                "realized_pnl_usd": "5",
                "total_pnl_usd": "105",
            },
        ]
    )
    priced_df.to_csv(priced_holdings_path, index=False)

    nav_df = build_portfolio_nav(
        data_dir=tmp_path,
        priced_holdings_usd_path=priced_holdings_path,
        output_path=output_path,
        portfolio_filter="FGI",
        asset_class_filter="Cash",
        scope="equity_sub",
    )

    assert len(nav_df) == 1
    row = nav_df.iloc[0]
    assert row["portfolio"] == "FGI"
    assert row["scope"] == "equity_sub"
    assert int(row["position_count"]) == 1
    assert float(row["total_market_value_usd"]) == 500.0
    assert float(row["total_pnl_usd"]) == 0.0
    assert pd.isna(row["daily_return_pct"])
