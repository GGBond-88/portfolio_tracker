"""One-off audit script for t0 — run: python audit/t0_stage_audit.py from repo root."""

from __future__ import annotations

import json
import random
import re
from collections import Counter
from datetime import date, datetime
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
DATA = REPO / "data"

_TRADELIST_EXCLUDE = {
    "daily_holdings.csv",
    "prices_cache.csv",
    "normalized_transactions.csv",
    "t0_standardized_tradelist.csv",
}

RAW_COLS_COMPARE = [
    "Action",
    "Ticker / ISIN / Reference",
    "Currency",
    "Trade date",
    " Executed unit price ",
    " Executed quantity ",
    "Transaction cost",
]


def find_latest_tradelist_csv(data_dir: Path) -> Path:
    csv_paths = [p for p in data_dir.glob("*.csv") if p.name not in _TRADELIST_EXCLUDE]
    tradelist_candidates = [p for p in csv_paths if "tradelist" in p.name.lower()]
    candidates = tradelist_candidates or csv_paths
    if not candidates:
        raise FileNotFoundError(data_dir)
    return max(candidates, key=lambda p: p.stat().st_mtime)


def parse_qty(s: str) -> float:
    t = str(s).strip().replace(",", "").replace(" ", "")
    t = t.replace("(", "-").replace(")", "")
    try:
        return float(t)
    except ValueError:
        return float("nan")


def parse_price(s: str) -> float:
    t = str(s).strip().replace(",", "").replace(" ", "")
    try:
        return float(t)
    except ValueError:
        return float("nan")


def parse_trade_date(s: str) -> pd.Timestamp | None:
    text = str(s).strip()
    if not text:
        return None
    for fmt in ("%d/%m/%Y", "%d.%m.%Y", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return pd.Timestamp(datetime.strptime(text, fmt))
        except ValueError:
            continue
    try:
        return pd.to_datetime(text)
    except Exception:
        return None


def main() -> None:
    raw_path = find_latest_tradelist_csv(DATA)
    t0_path = DATA / "t0_standardized_tradelist.csv"
    raw = pd.read_csv(raw_path, dtype=str)
    t0 = pd.read_csv(t0_path, dtype=str)
    broker_col = "Broker & acct"

    print("=== SOURCE FILES ===")
    print(f"Latest tradelist (t0 rule): {raw_path.name}")
    print(f"t0 output: {t0_path.name}")

    # --- 1. Completeness ---
    print("\n=== 1. COMPLETENESS ===")
    if broker_col not in raw.columns:
        print("No broker column; cannot split LGT/HSBC/Yuanta.")
        by_broker = {}
    else:
        by_broker = raw[broker_col].fillna("").str.strip().value_counts().to_dict()
        for b in sorted(by_broker.keys()):
            if b:
                print(f"  Raw rows, broker '{b}': {by_broker[b]}")

    raw_n = len(raw)
    t0_n = len(t0)
    print(f"\nRaw total (all rows): {raw_n}")
    print(f"t0 output total: {t0_n}")
    print(f"Difference: {raw_n - t0_n}")
    if raw_n == t0_n:
        print("Row counts match (1:1 pipeline design).")
    else:
        print("MISMATCH — t0 should emit one row per input row.")

    # --- 2. Accuracy spot check (aligned by row index) ---
    print("\n=== 2. ACCURACY (spot check, same row index raw vs t0) ===")
    rng = random.Random(42)
    if raw_n != t0_n:
        print("Skipping detailed row alignment due to length mismatch.")
    else:
        for broker in ["LGT", "HSBC", "Yuanta"]:
            idxs = raw.index[raw[broker_col].fillna("").str.strip() == broker].tolist()
            if not idxs:
                print(f"\n--- {broker}: no rows ---")
                continue
            pick = [idxs[0], idxs[-1]] + rng.sample(idxs[1:-1], min(3, max(0, len(idxs) - 2)))
            pick = sorted(set(pick))
            print(f"\n--- {broker}: rows {pick} ---")
            for i in pick:
                r = raw.iloc[i]
                o = t0.iloc[i]
                raw_date = r.get("Trade date", "")
                t0_book = o.get("Booking date", "")
                raw_ref = str(r.get("Ticker / ISIN / Reference", "")).strip()
                yahoo = str(o.get("Yahoo Ticker", "")).strip()
                rq = parse_qty(r.get(" Executed quantity ", ""))
                oq = float(o.get("Quantity", "nan"))
                rp = parse_price(r.get(" Executed unit price ", ""))
                op = float(o.get("Execution price", "nan"))
                mismatches = []
                if abs(rq) != oq and not (pd.isna(rq) and pd.isna(oq)):
                    mismatches.append(f"qty abs(raw)={abs(rq)} vs t0 Quantity={oq}")
                if rp != op and not (pd.isna(rp) and pd.isna(op)):
                    mismatches.append(f"price raw={rp} vs t0={op}")
                raw_act = str(r.get("Action", "")).strip().upper()
                ot = str(o.get("Order type", "")).strip().upper()
                if raw_act != ot and not (raw_act.startswith("B") and ot == "BUY"):
                    if raw_act == "BUY" and ot == "BUY":
                        pass
                    elif raw_act == "SELL" and ot == "SELL":
                        pass
                    else:
                        mismatches.append(f"action raw={raw_act} vs order_type={ot}")
                # Direction vs quantity sign
                sign_note = ""
                if rq < 0 and ot == "BUY":
                    sign_note = "WARN: negative raw qty on BUY (t0 stores abs)"
                elif rq > 0 and ot == "SELL":
                    sign_note = "WARN: positive raw qty on SELL (t0 stores abs)"
                fee_r = str(r.get("Transaction cost", "")).strip()
                fee_t0 = str(o.get("Transaction cost", "")).strip()
                print(f"  Row {i}: date raw={raw_date!r} -> t0 Booking date={t0_book!r}")
                print(f"           ref={raw_ref[:50]}... -> Yahoo={yahoo}")
                print(f"           qty raw={rq} -> t0 Quantity={oq} ({sign_note or 'abs convention'})")
                print(f"           price raw={rp} -> t0 Execution price={op}")
                print(f"           currency raw={r.get('Currency','')!r} t0={o.get('Currency','')!r}")
                print(f"           fees raw Transaction cost={fee_r!r} t0={fee_t0!r}")
                if mismatches:
                    print(f"           FLAG: {mismatches}")

    # --- 3. Logic ---
    print("\n=== 3. LOGIC ===")
    bloomberg = re.compile(r"^.+\s+[A-Z]{1,4}\s+EQUITY$", re.I)
    mappings: dict[str, str] = {}
    suspicious: list[str] = []
    for _, row in t0.iterrows():
        ref = str(row.get("Ticker / ISIN / Reference", "")).strip()
        y = str(row.get("Yahoo Ticker", "")).strip()
        if ref and y and bloomberg.match(ref):
            key = ref.upper()
            if key not in mappings:
                mappings[key] = y
    # detect inconsistent mappings
    ref_to_y: dict[str, set[str]] = {}
    for _, row in t0.iterrows():
        ref = str(row.get("Ticker / ISIN / Reference", "")).strip().upper()
        y = str(row.get("Yahoo Ticker", "")).strip()
        if ref and bloomberg.match(ref):
            ref_to_y.setdefault(ref, set()).add(y)
    for ref, ys in sorted(ref_to_y.items()):
        if len(ys) > 1:
            suspicious.append(f"Inconsistent Yahoo for same ref {ref}: {ys}")
    # heuristic: HK ref should map to .HK
    for ref, y in sorted(mappings.items()):
        if " HK " in ref.upper() or ref.upper().endswith(" HK EQUITY"):
            if y and not y.endswith(".HK"):
                suspicious.append(f"HK Bloomberg ref but Yahoo not .HK: {ref} -> {y}")
        if " TB " in ref.upper() and y and not y.endswith(".BK"):
            suspicious.append(f"Thailand TB equity: {ref} -> {y} (expect .BK)")

    print(f"Unique Bloomberg equity ref -> Yahoo mappings: {len(mappings)}")
    print("(sample)", json.dumps(dict(list(mappings.items())[:15]), indent=2))
    if suspicious:
        print("Suspicious / review:")
        for s in suspicious[:30]:
            print(f"  - {s}")
        if len(suspicious) > 30:
            print(f"  ... +{len(suspicious) - 30} more")

    # BUY/SELL vs quantity sign in RAW
    issues_sign = []
    for i, r in raw.iterrows():
        act = str(r.get("Action", "")).strip().upper()
        q = parse_qty(r.get(" Executed quantity ", ""))
        if act == "BUY" and q < 0:
            issues_sign.append(i)
        if act == "SELL" and q > 0:
            issues_sign.append(i)
    print(f"\nRaw file: BUY with negative qty rows: {sum(1 for i in issues_sign if str(raw.iloc[i].get('Action','')).upper()=='BUY')}")
    print(f"Raw file: SELL with positive qty rows: {sum(1 for i in issues_sign if str(raw.iloc[i].get('Action','')).upper()=='SELL')}")
    print("t0 always stores Quantity = abs(raw executed quantity); Order type comes from Action (BUY/SELL only normalized).")

    # Weekends
    weekend_rows = []
    for i, r in raw.iterrows():
        ts = parse_trade_date(r.get("Trade date", ""))
        if ts is not None and ts.dayofweek >= 5:
            weekend_rows.append((i, r.get("Trade date"), str(r.get(broker_col, ""))))
    print(f"\nWeekend trade dates in raw: {len(weekend_rows)}")
    if weekend_rows[:5]:
        print("  examples:", weekend_rows[:5])

    curr_blank = raw["Currency"].fillna("").astype(str).str.strip().eq("").sum()
    print(f"\nRaw rows with blank Currency: {curr_blank}")

    # --- 4. Duplicates ---
    print("\n=== 4. DUPLICATES ===")
    dup_exact = raw.duplicated(keep=False).sum()
    print(f"Raw exact duplicate rows (all columns): {dup_exact}")
    if dup_exact:
        print(raw[raw.duplicated(keep=False)].head(10).to_string())

    key_cols = [
        "Trade date",
        "Ticker / ISIN / Reference",
        " Executed quantity ",
        " Executed unit price ",
    ]
    sub = raw[key_cols].copy()
    dup_like = sub.duplicated(keep=False).sum()
    print(f"Likely duplicates (same date+ticker+raw qty str+price str): {dup_like}")

    # --- 5. Data types ---
    print("\n=== 5. DATA TYPES (t0 output) ===")
    bad_booking = t0["Booking date"].isna() | (t0["Booking date"].astype(str).str.strip() == "") | (
        t0["Booking date"].astype(str) == "NaT"
    )
    print(f"t0 Booking date null/empty/NaT: {bad_booking.sum()}")
    if bad_booking.any():
        print(t0.loc[bad_booking, ["Action", "Trade date", "Booking date", "Ticker / ISIN / Reference"]].head(5).to_string())

    for col in ["Quantity", "Execution price"]:
        num = pd.to_numeric(t0[col], errors="coerce")
        bad = num.isna()
        print(f"t0 {col} non-numeric or NaN: {bad.sum()}")

    crit = ["Booking date", "Yahoo Ticker", "Quantity", "Execution price", "Currency"]
    # Note: Yahoo can be blank for non-equity
    for c in ["Booking date", "Quantity", "Execution price", "Currency"]:
        empty = t0[c].isna() | (t0[c].astype(str).str.strip().isin(["", "nan", "NaT"]))
        print(f"t0 critical '{c}' empty-like: {empty.sum()}")


if __name__ == "__main__":
    main()
