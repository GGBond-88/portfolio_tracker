"""Pipeline entry point to run Tool 0, Tool 1, and Tool 2 in sequence."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from tools.t0_tradelist_standardizer import standardize_tradelist
from tools.t1_holdings_builder import build_holdings
from tools.t2_price_fetcher import build_priced_holdings

LOGGER = logging.getLogger(__name__)


def run_pipeline(data_dir: Path) -> dict[str, pd.DataFrame]:
    """Run t0, t1, and t2 sequentially and return all output DataFrames."""
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

    LOGGER.info("Pipeline finished successfully.")
    LOGGER.info("t0 rows: %s", len(standardized_tradelist))
    LOGGER.info("t1 rows: %s", len(daily_holdings))
    LOGGER.info("t2 rows: %s", len(priced_holdings))

    return {
        "t0_standardized_tradelist": standardized_tradelist,
        "t1_daily_holdings": daily_holdings,
        "t2_priced_holdings": priced_holdings,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run_pipeline(data_dir=Path("data"))
