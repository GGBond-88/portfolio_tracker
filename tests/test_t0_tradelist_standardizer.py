"""Tests for Tool 0 tradelist standardization."""

from pathlib import Path

import pandas as pd

from tools.t0_tradelist_standardizer import standardize_tradelist


def test_standardize_tradelist_adds_yahoo_ticker(tmp_path: Path) -> None:
    """It converts Bloomberg-style tickers into Yahoo tickers."""
    tradelist = pd.DataFrame(
        [
            {
                "Action": "Buy",
                "Portfolio": "FGI",
                "Broker & acct": "",
                "Asset class": "",
                "Ticker / ISIN / Reference": "700 HK EQUITY",
                "Security name": "Tencent Holdings Ltd",
                "Currency": "HKD",
                "Trade date": "28/8/2023",
                " Executed unit price ": "362.38",
                " Executed quantity ": "19,900",
                "Transaction cost": "0",
            },
            {
                "Action": "Buy",
                "Portfolio": "FGI",
                "Broker & acct": "",
                "Asset class": "",
                "Ticker / ISIN / Reference": "GOOGL US EQUITY",
                "Security name": "Alphabet Inc",
                "Currency": "USD",
                "Trade date": "28/8/2023",
                " Executed unit price ": "100.00",
                " Executed quantity ": "10",
                "Transaction cost": "0",
            },
        ]
    )
    bookings = pd.DataFrame(
        [
            {
                "Position": "Tencent Holdings Ltd\nRegistered-shares",
                "ISIN": "KYG875721634",
            }
        ]
    )

    tradelist_path = tmp_path / "20260328 Tradelist.csv"
    bookings_path = tmp_path / "20260306143204_Securities bookings.csv"
    output_path = tmp_path / "t0_standardized_tradelist.csv"
    tradelist.to_csv(tradelist_path, index=False)
    bookings.to_csv(bookings_path, index=False)

    result = standardize_tradelist(
        data_dir=tmp_path,
        output_path=output_path,
        reference_bookings_path=bookings_path,
    )

    assert output_path.exists()
    assert result.loc[0, "Yahoo Ticker"] == "0700.HK"
    assert result.loc[1, "Yahoo Ticker"] == "GOOGL"
    assert result.loc[0, "ISIN"] == "KYG875721634"
    assert result.loc[0, "Order type"] == "BUY"
    assert result.loc[0, "Asset Type"] == "Equities"


def test_standardize_tradelist_applies_manual_ticker_overrides(tmp_path: Path) -> None:
    """Manual overrides should take precedence over automatic mapping."""
    tradelist = pd.DataFrame(
        [
            {
                "Action": "Buy",
                "Portfolio": "FGI",
                "Broker & acct": "",
                "Asset class": "",
                "Ticker / ISIN / Reference": "CLAR SP EQUITY",
                "Security name": "CapLand Ascendas REIT",
                "Currency": "SGD",
                "Trade date": "28/8/2023",
                " Executed unit price ": "2.35",
                " Executed quantity ": "1000",
                "Transaction cost": "0",
            },
            {
                "Action": "Buy",
                "Portfolio": "FGI",
                "Broker & acct": "",
                "Asset class": "",
                "Ticker / ISIN / Reference": "11 HK EQUITY",
                "Security name": "Hang Seng Bank",
                "Currency": "HKD",
                "Trade date": "28/8/2023",
                " Executed unit price ": "80.0",
                " Executed quantity ": "100",
                "Transaction cost": "0",
            },
        ]
    )
    bookings = pd.DataFrame(columns=["Position", "ISIN"])
    overrides = pd.DataFrame(
        [
            {"bloomberg_reference": "CLAR SP EQUITY", "yahoo_ticker": "A17U.SI"},
            {"bloomberg_reference": "11 HK EQUITY", "yahoo_ticker": "0011_OL.HK"},
        ]
    )

    tradelist_path = tmp_path / "20260328 Tradelist.csv"
    bookings_path = tmp_path / "20260306143204_Securities bookings.csv"
    overrides_path = tmp_path / "ticker_overrides.csv"
    output_path = tmp_path / "t0_standardized_tradelist.csv"
    tradelist.to_csv(tradelist_path, index=False)
    bookings.to_csv(bookings_path, index=False)
    overrides.to_csv(overrides_path, index=False)

    result = standardize_tradelist(
        data_dir=tmp_path,
        output_path=output_path,
        reference_bookings_path=bookings_path,
    )

    assert result.loc[0, "Yahoo Ticker"] == "A17U.SI"
    assert result.loc[1, "Yahoo Ticker"] == "0011_OL.HK"
    assert (result["Asset Type"] == "Equities").all()
