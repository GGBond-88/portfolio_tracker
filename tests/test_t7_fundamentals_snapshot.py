"""Tests for Tool 7 fundamentals snapshot."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from tools.t7_fundamentals_snapshot import build_fundamentals_snapshot


def _make_holdings_df() -> pd.DataFrame:
    """Create priced holdings fixture with two dates and two tickers."""
    return pd.DataFrame(
        [
            {
                "date": "2026-01-01",
                "Portfolio": "FGI",
                "Security name": "Old Position",
                "Currency": "USD",
                "Yahoo Ticker": "OLD",
                "quantity": "10",
                "market_value_usd": "1000",
            },
            {
                "date": "2026-01-02",
                "Portfolio": "FGI",
                "Security name": "Apple Inc",
                "Currency": "USD",
                "Yahoo Ticker": "AAPL",
                "quantity": "5",
                "market_value_usd": "600",
            },
            {
                "date": "2026-01-02",
                "Portfolio": "FGI",
                "Security name": "Microsoft",
                "Currency": "USD",
                "Yahoo Ticker": "MSFT",
                "quantity": "4",
                "market_value_usd": "400",
            },
        ]
    )


def test_build_fundamentals_snapshot_has_expected_columns(monkeypatch, tmp_path: Path) -> None:
    """Tool 7 should output expected schema and weights from latest-date holdings."""
    holdings_path = tmp_path / "priced_holdings_usd.csv"
    output_path = tmp_path / "fundamentals_snapshot.csv"
    _make_holdings_df().to_csv(holdings_path, index=False)

    price_history = pd.DataFrame({"Close": list(range(1, 17))})

    ticker_payload = {
        "AAPL": {
            "info": {
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "trailingPE": 22.0,
                "priceToBook": 35.0,
                "trailingEps": 6.0,
                "bookValue": 4.0,
                "dividendYield": 0.005,
                "trailingAnnualDividendRate": 0.96,
                "marketCap": 1000000000,
                "fiftyTwoWeekHigh": 220.0,
                "fiftyTwoWeekLow": 160.0,
            },
            "history": price_history,
        },
        "MSFT": {
            "info": {
                "sector": "Technology",
                "industry": "Software",
                "trailingPE": 30.0,
                "priceToBook": 10.0,
                "trailingEps": 8.0,
                "bookValue": 40.0,
                "dividendYield": 0.01,
                "trailingAnnualDividendRate": 2.0,
                "marketCap": 2000000000,
                "fiftyTwoWeekHigh": 500.0,
                "fiftyTwoWeekLow": 300.0,
            },
            "history": price_history,
        },
    }

    class FakeTicker:
        def __init__(self, ticker: str) -> None:
            self._ticker = ticker

        @property
        def info(self) -> dict[str, object]:
            return ticker_payload[self._ticker]["info"]  # type: ignore[return-value]

        def history(self, period: str = "1mo") -> pd.DataFrame:
            assert period == "1mo"
            return ticker_payload[self._ticker]["history"]  # type: ignore[return-value]

    monkeypatch.setattr("tools.t7_fundamentals_snapshot.yf.Ticker", FakeTicker)

    out_df = build_fundamentals_snapshot(
        data_dir=tmp_path,
        priced_holdings_usd_path=holdings_path,
        output_path=output_path,
        portfolio_filter="FGI",
    )

    assert output_path.exists()
    assert len(out_df) == 2
    expected_columns = {
        "snapshot_date",
        "portfolio",
        "yahoo_ticker",
        "security_name",
        "currency",
        "quantity",
        "market_value_usd",
        "weight_pct",
        "sector",
        "industry",
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
    }
    assert expected_columns.issubset(out_df.columns)
    assert out_df.iloc[0]["yahoo_ticker"] == "AAPL"
    assert float(out_df.iloc[0]["weight_pct"]) == 0.6
    assert float(out_df.iloc[1]["weight_pct"]) == 0.4


def test_build_fundamentals_snapshot_handles_empty_info(monkeypatch, tmp_path: Path) -> None:
    """Empty info dict should produce NaN fundamentals without crashing."""
    holdings_path = tmp_path / "priced_holdings_usd.csv"
    output_path = tmp_path / "fundamentals_snapshot.csv"
    holdings_df = pd.DataFrame(
        [
            {
                "date": "2026-01-02",
                "Portfolio": "FGI",
                "Security name": "Unknown",
                "Currency": "USD",
                "Yahoo Ticker": "UNKNOWN",
                "quantity": "1",
                "market_value_usd": "100",
            }
        ]
    )
    holdings_df.to_csv(holdings_path, index=False)

    class FakeTicker:
        def __init__(self, ticker: str) -> None:
            self._ticker = ticker

        @property
        def info(self) -> dict[str, object]:
            _ = self._ticker
            return {}

        def history(self, period: str = "1mo") -> pd.DataFrame:
            _ = period
            return pd.DataFrame({"Close": list(range(1, 17))})

    monkeypatch.setattr("tools.t7_fundamentals_snapshot.yf.Ticker", FakeTicker)

    out_df = build_fundamentals_snapshot(
        data_dir=tmp_path,
        priced_holdings_usd_path=holdings_path,
        output_path=output_path,
        portfolio_filter="FGI",
    )

    assert len(out_df) == 1
    row = out_df.iloc[0]
    assert pd.isna(row["sector"])
    assert pd.isna(row["industry"])
    assert pd.isna(row["pe_ttm"])


def test_build_fundamentals_snapshot_computes_rsi_14(monkeypatch, tmp_path: Path) -> None:
    """RSI should be 100 for a strictly increasing close series."""
    holdings_path = tmp_path / "priced_holdings_usd.csv"
    output_path = tmp_path / "fundamentals_snapshot.csv"
    holdings_df = pd.DataFrame(
        [
            {
                "date": "2026-01-02",
                "Portfolio": "FGI",
                "Security name": "RSI Test",
                "Currency": "USD",
                "Yahoo Ticker": "RSI",
                "quantity": "2",
                "market_value_usd": "200",
            }
        ]
    )
    holdings_df.to_csv(holdings_path, index=False)

    class FakeTicker:
        def __init__(self, ticker: str) -> None:
            self._ticker = ticker

        @property
        def info(self) -> dict[str, object]:
            _ = self._ticker
            return {}

        def history(self, period: str = "1mo") -> pd.DataFrame:
            _ = period
            return pd.DataFrame({"Close": list(range(1, 17))})

    monkeypatch.setattr("tools.t7_fundamentals_snapshot.yf.Ticker", FakeTicker)

    out_df = build_fundamentals_snapshot(
        data_dir=tmp_path,
        priced_holdings_usd_path=holdings_path,
        output_path=output_path,
        portfolio_filter="FGI",
    )

    assert len(out_df) == 1
    assert float(out_df.loc[0, "rsi_14"]) == 100.0
