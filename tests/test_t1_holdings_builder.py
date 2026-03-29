"""Tests for Tool 1 holdings replay, including short-selling edge cases."""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from tools.t1_holdings_builder import replay_transactions_with_exits


def _date_str(days_offset: int) -> str:
    """Return YYYY-MM-DD date string relative to today."""
    return (date.today() + timedelta(days=days_offset)).isoformat()


def _tx_rows(rows: list[dict[str, object]]) -> pd.DataFrame:
    """Build normalized transaction DataFrame expected by Tool 1 replay."""
    return pd.DataFrame(rows)


def _latest_row(daily: pd.DataFrame, symbol: str) -> pd.Series:
    """Fetch latest daily snapshot row for one symbol."""
    filtered = daily[daily["symbol"] == symbol].reset_index(drop=True)
    assert not filtered.empty
    return filtered.iloc[-1]


def test_replay_long_round_trip_exits_with_realized_gain() -> None:
    """Normal long buy then sell should exit and realize gain."""
    tx = _tx_rows(
        [
            {
                "date": _date_str(-2),
                "symbol": "TEST",
                "isin": "TESTISIN",
                "name": "Test Co",
                "currency": "USD",
                "order_type": "BUY",
                "quantity": 10.0,
                "price": 100.0,
            },
            {
                "date": _date_str(-1),
                "symbol": "TEST",
                "isin": "TESTISIN",
                "name": "Test Co",
                "currency": "USD",
                "order_type": "SELL",
                "quantity": -10.0,
                "price": 110.0,
            },
        ]
    )

    daily, exited = replay_transactions_with_exits(transactions=tx)
    assert len(exited) == 1
    assert exited.loc[0, "symbol"] == "TEST"
    assert exited.loc[0, "total_realized_pnl"] == 100.0
    exit_date = str(exited.loc[0, "exit_date"])
    assert daily[(daily["symbol"] == "TEST") & (daily["date"] >= exit_date)].empty
    assert (daily["symbol"] == "TEST").any()


def test_replay_open_short_from_flat_has_no_realized_pnl() -> None:
    """Sell from flat opens short with avg cost, without realized PnL."""
    tx = _tx_rows(
        [
            {
                "date": _date_str(-1),
                "symbol": "SHORT0",
                "isin": "SHORT0ISIN",
                "name": "Short Zero Co",
                "currency": "USD",
                "order_type": "SELL",
                "quantity": -5.0,
                "price": 50.0,
            }
        ]
    )

    daily, exited = replay_transactions_with_exits(transactions=tx)
    latest = _latest_row(daily=daily, symbol="SHORT0")
    assert latest["quantity"] == -5.0
    assert latest["avg_cost"] == 50.0
    assert latest["realized_pnl"] == 0.0
    assert exited.empty


def test_replay_sell_crosses_zero_realizes_only_closed_long_part() -> None:
    """Crossing from long to short realizes only the closed long quantity."""
    tx = _tx_rows(
        [
            {
                "date": _date_str(-2),
                "symbol": "CROSS",
                "isin": "CROSSISIN",
                "name": "Cross Co",
                "currency": "USD",
                "order_type": "BUY",
                "quantity": 10.0,
                "price": 100.0,
            },
            {
                "date": _date_str(-1),
                "symbol": "CROSS",
                "isin": "CROSSISIN",
                "name": "Cross Co",
                "currency": "USD",
                "order_type": "SELL",
                "quantity": -15.0,
                "price": 120.0,
            },
        ]
    )

    daily, exited = replay_transactions_with_exits(transactions=tx)
    latest = _latest_row(daily=daily, symbol="CROSS")
    assert latest["quantity"] == -5.0
    assert latest["avg_cost"] == 120.0
    assert latest["realized_pnl"] == 200.0
    assert exited.empty


def test_replay_buy_to_cover_short_realizes_on_covered_portion() -> None:
    """Buy-to-cover should realize (short_avg - buy_price) on covered quantity."""
    tx = _tx_rows(
        [
            {
                "date": _date_str(-3),
                "symbol": "COVER",
                "isin": "COVERISIN",
                "name": "Cover Co",
                "currency": "USD",
                "order_type": "SELL",
                "quantity": -10.0,
                "price": 100.0,
            },
            {
                "date": _date_str(-2),
                "symbol": "COVER",
                "isin": "COVERISIN",
                "name": "Cover Co",
                "currency": "USD",
                "order_type": "BUY",
                "quantity": 4.0,
                "price": 90.0,
            },
            {
                "date": _date_str(-1),
                "symbol": "COVER",
                "isin": "COVERISIN",
                "name": "Cover Co",
                "currency": "USD",
                "order_type": "BUY",
                "quantity": 6.0,
                "price": 95.0,
            },
        ]
    )

    daily, exited = replay_transactions_with_exits(transactions=tx)
    assert len(exited) == 1
    assert exited.loc[0, "symbol"] == "COVER"
    assert exited.loc[0, "total_realized_pnl"] == 70.0
    exit_date = str(exited.loc[0, "exit_date"])
    assert daily[(daily["symbol"] == "COVER") & (daily["date"] >= exit_date)].empty


def test_replay_tracks_same_symbol_separately_by_portfolio() -> None:
    """Same symbol held by different portfolios should not be merged."""
    tx = _tx_rows(
        [
            {
                "date": _date_str(-1),
                "Portfolio": "FGI",
                "symbol": "SAME",
                "isin": "SAMEISIN",
                "name": "Same Co",
                "currency": "USD",
                "order_type": "BUY",
                "quantity": 10.0,
                "price": 100.0,
            },
            {
                "date": _date_str(-1),
                "Portfolio": "SINOWELL",
                "symbol": "SAME",
                "isin": "SAMEISIN",
                "name": "Same Co",
                "currency": "USD",
                "order_type": "BUY",
                "quantity": 20.0,
                "price": 120.0,
            },
        ]
    )

    daily, exited = replay_transactions_with_exits(transactions=tx)
    latest_date = daily["date"].max()
    latest = daily[(daily["date"] == latest_date) & (daily["symbol"] == "SAME")]

    assert exited.empty
    assert len(latest) == 2
    assert set(latest["Portfolio"].tolist()) == {"FGI", "SINOWELL"}
    assert sorted(latest["quantity"].tolist()) == [10.0, 20.0]
