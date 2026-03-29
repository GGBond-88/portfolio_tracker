"""Pipeline entry point to run Tool 0 through Tool 7 in sequence."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from tools.t0_tradelist_standardizer import standardize_tradelist
from tools.t1_holdings_builder import build_holdings
from tools.t2_price_fetcher import build_priced_holdings
from tools.t3_fx_converter import build_fx_converted_holdings
from tools.t4_portfolio_nav import build_portfolio_nav
from tools.t5_cash_flow_builder import build_cash_flows
from tools.t6_return_calculator import build_portfolio_returns
from tools.t7_fundamentals_snapshot import build_fundamentals_snapshot

LOGGER = logging.getLogger(__name__)


def run_pipeline(data_dir: Path) -> dict[str, pd.DataFrame]:
    """Run t0, t1, t2, t3, t4, t5, t6, and t7 sequentially and return output DataFrames."""
    data_dir = data_dir.resolve()
    LOGGER.info("Running portfolio tracker pipeline in %s", data_dir)

    standardized_tradelist = standardize_tradelist(
        data_dir=data_dir,
        output_path=data_dir / "t0_standardized_tradelist.csv",
    )
    daily_holdings = build_holdings(
        data_dir=data_dir,
        output_path=data_dir / "daily_holdings.csv",
    )
    priced_holdings = build_priced_holdings(
        data_dir=data_dir,
        holdings_path=data_dir / "daily_holdings.csv",
        output_path=data_dir / "priced_holdings.csv",
        cache_path=data_dir / "prices_cache.csv",
    )
    priced_holdings_usd = build_fx_converted_holdings(
        data_dir=data_dir,
        priced_holdings_path=data_dir / "priced_holdings.csv",
        output_path=data_dir / "priced_holdings_usd.csv",
        fx_cache_path=data_dir / "fx_cache.csv",
    )
    portfolio_nav = build_portfolio_nav(
        data_dir=data_dir,
        priced_holdings_usd_path=data_dir / "priced_holdings_usd.csv",
        output_path=data_dir / "portfolio_nav.csv",
        portfolio_filter="FGI",
        asset_class_filter="Equities",
        scope="equity_sub",
    )
    portfolio_cash_flows = build_cash_flows(
        data_dir=data_dir,
        tradelist_path=data_dir / "t0_standardized_tradelist.csv",
        fx_cache_path=data_dir / "fx_cache.csv",
        output_path=data_dir / "portfolio_cash_flows.csv",
        portfolio_filter="FGI",
        asset_class_filter="Equities",
        scope="equity_sub",
    )
    portfolio_returns = build_portfolio_returns(
        data_dir=data_dir,
        nav_path=data_dir / "portfolio_nav.csv",
        cash_flows_path=data_dir / "portfolio_cash_flows.csv",
        output_path=data_dir / "portfolio_returns.csv",
        portfolio_filter="FGI",
        scope="equity_sub",
    )
    fundamentals_snapshot = build_fundamentals_snapshot(
        data_dir=data_dir,
        priced_holdings_usd_path=data_dir / "priced_holdings_usd.csv",
        output_path=data_dir / "fundamentals_snapshot.csv",
        portfolio_filter="FGI",
    )

    LOGGER.info("Pipeline finished successfully.")
    LOGGER.info("t0 rows: %s", len(standardized_tradelist))
    LOGGER.info("t1 rows: %s", len(daily_holdings))
    LOGGER.info("t2 rows: %s", len(priced_holdings))
    LOGGER.info("t3 rows: %s", len(priced_holdings_usd))
    LOGGER.info("t4 rows: %s", len(portfolio_nav))
    LOGGER.info("t5 rows: %s", len(portfolio_cash_flows))
    LOGGER.info("t6 rows: %s", len(portfolio_returns))
    LOGGER.info("t7 rows: %s", len(fundamentals_snapshot))

    return {
        "t0_standardized_tradelist": standardized_tradelist,
        "t1_daily_holdings": daily_holdings,
        "t2_priced_holdings": priced_holdings,
        "t3_priced_holdings_usd": priced_holdings_usd,
        "t4_portfolio_nav": portfolio_nav,
        "t5_portfolio_cash_flows": portfolio_cash_flows,
        "t6_portfolio_returns": portfolio_returns,
        "t7_fundamentals_snapshot": fundamentals_snapshot,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run_pipeline(data_dir=Path("data"))
