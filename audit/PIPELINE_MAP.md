# Portfolio pipeline audit — data flow map

Generated for confirmation against your mental model. **No fixes applied** (documentation only).

Orchestrator: `pipeline.py` runs tools **t0 → t1 → t2 → t3 → t4 → t5 → t6 → t7** in that order (see `run_pipeline`).

---

## 1. Python modules in execution order

| Step | Module path |
|------|-------------|
| (entry) | `pipeline.py` |
| t0 | `tools/t0_tradelist_standardizer.py` |
| t1 | `tools/t1_holdings_builder.py` |
| t2 | `tools/t2_price_fetcher.py` |
| t3 | `tools/t3_fx_converter.py` |
| t4 | `tools/t4_portfolio_nav.py` |
| t5 | `tools/t5_cash_flow_builder.py` |
| t6 | `tools/t6_return_calculator.py` |
| t7 | `tools/t7_fundamentals_snapshot.py` |

Other Python in the repo: tests under `tests/test_t*.py` (not part of runtime pipeline order).

---

## 2. Per-tool I/O (as implemented + as `pipeline.py` wires paths)

Default `data_dir` is `data/` when you run `python pipeline.py`.

### `pipeline.py`

- **Input:** None (no CSV reads; calls tool functions with explicit paths).
- **Output:** In-memory dict of DataFrames; tools write CSVs to disk.
- **Consumed by:** N/A (CLI entry).

---

### `tools/t0_tradelist_standardizer.py` — `standardize_tradelist`

- **Input:**
  - **Primary:** “Latest” tradelist CSV under `data_dir`: files matching `*.csv` whose name is not in the exclude set **and** (if any) whose name contains `tradelist` (case-insensitive); if none match `tradelist`, falls back to newest-by-`st_mtime` among eligible `*.csv`.
  - **Optional:** `data/ticker_overrides.csv` (manual Bloomberg → Yahoo mapping).
  - **Optional:** Latest `*Securities bookings*.csv` in `data_dir` for Position→ISIN enrichment (or `reference_bookings_path` if passed; pipeline does not pass it).
- **Output:**
  - `t0_standardized_tradelist.csv` (path from caller; pipeline uses `data/t0_standardized_tradelist.csv`).
- **Consumed by (downstream tools that read this artifact):**
  - **Explicit in pipeline:** **t5** (`build_cash_flows` ← `t0_standardized_tradelist.csv`).
  - **Not explicit:** **t1** does *not* take this path from `pipeline.py`; see §4.

---

### `tools/t1_holdings_builder.py` — `build_holdings`

- **Input:**
  - **Single “latest CSV”** under `data_dir`: newest-by-`st_mtime` among `*.csv` **excluding** only: `daily_holdings.csv`, `exited_positions.csv`, `prices_cache.csv`, `normalized_transactions.csv`.
  - (So `t0_standardized_tradelist.csv`, `fx_cache.csv`, `priced_holdings*.csv`, etc. *can* be selected if they are the newest file.)
- **Output:**
  - `daily_holdings.csv` (path from caller; pipeline uses `data/daily_holdings.csv`).
  - `exited_positions.csv` (always sibling of `daily_holdings.csv`: `output_path.parent / "exited_positions.csv"`).
- **Consumed by:**
  - **t2** reads `daily_holdings.csv`.
  - **`exited_positions.csv`:** no other tool in `tools/` reads it → **orphaned** for pipeline purposes (see §4).

---

### `tools/t2_price_fetcher.py` — `build_priced_holdings`

- **Input:**
  - `daily_holdings.csv` (pipeline: `data/daily_holdings.csv`).
  - `prices_cache.csv` (read/update; pipeline: `data/prices_cache.csv`).
- **Output:**
  - `priced_holdings.csv` (`data/priced_holdings.csv`).
  - `prices_cache.csv` (updated/created).
- **Consumed by:**
  - **t3** reads `priced_holdings.csv`.

---

### `tools/t3_fx_converter.py` — `build_fx_converted_holdings`

- **Input:**
  - `priced_holdings.csv`.
  - `fx_cache.csv` (read/update; pipeline: `data/fx_cache.csv`).
- **Output:**
  - `priced_holdings_usd.csv` (`data/priced_holdings_usd.csv`).
  - `fx_cache.csv` (updated/created).
- **Consumed by:**
  - **t4** and **t7** read `priced_holdings_usd.csv`.
  - **t5** reads `fx_cache.csv` (after t3 has populated it for the holdings date range).

---

### `tools/t4_portfolio_nav.py` — `build_portfolio_nav`

- **Input:**
  - `priced_holdings_usd.csv`.
- **Output:**
  - `portfolio_nav.csv` (`data/portfolio_nav.csv`).
- **Consumed by:**
  - **t6** reads `portfolio_nav.csv`.

---

### `tools/t5_cash_flow_builder.py` — `build_cash_flows`

- **Input:**
  - `t0_standardized_tradelist.csv`.
  - `fx_cache.csv` (must exist; t3 creates/updates it).
- **Output:**
  - `portfolio_cash_flows.csv` (`data/portfolio_cash_flows.csv`).
- **Consumed by:**
  - **t6** reads `portfolio_cash_flows.csv`.

---

### `tools/t6_return_calculator.py` — `build_portfolio_returns`

- **Input:**
  - `portfolio_nav.csv`.
  - `portfolio_cash_flows.csv`.
- **Output:**
  - `portfolio_returns.csv` (`data/portfolio_returns.csv`).
- **Consumed by:**
  - No subsequent tool in the repo reads this file → **terminal / BI / manual** (see §4).

---

### `tools/t7_fundamentals_snapshot.py` — `build_fundamentals_snapshot`

- **Input:**
  - `priced_holdings_usd.csv`.
- **Output:**
  - `fundamentals_snapshot.csv` (`data/fundamentals_snapshot.csv`).
- **Consumed by:**
  - No subsequent tool in the repo reads this file → **terminal / BI / manual** (see §4).

---

## 3. CSV feed graph (who writes → who reads)

```
[Raw *Tradelist*.csv or fallback newest eligible CSV]  →  t0  →  t0_standardized_tradelist.csv
                                                              ↘
[ticker_overrides.csv] ───────────────────────────────────── t0
[*Securities bookings*.csv] ─────────────────────────────── t0

[“Latest” *.csv per t1 rules — see §4]  →  t1  →  daily_holdings.csv  →  t2  →  priced_holdings.csv
                ↘                                      ↓
            exited_positions.csv                    prices_cache.csv ←→ t2
                                                         ↓
                                              t3  →  priced_holdings_usd.csv  ┬→  t4  →  portfolio_nav.csv  ─┐
                                                    ↑                          │                              │
                                              fx_cache.csv ←→ t3               └→  t7  →  fundamentals_snapshot.csv
                                                                                                             
t0_standardized_tradelist.csv  ──────────────→  t5  ┐
                                                    ├→  portfolio_cash_flows.csv  →  t6  →  portfolio_returns.csv
fx_cache.csv  ──────────────────────────────→  t5  ┘
```

**Parallel branches after t3:** t4→t6 and t7 both start from `priced_holdings_usd.csv`. t5 depends on t0 output + `fx_cache` (typically filled by t3 before t5 runs).

---

## 4. Orphaned outputs, missing links, and behavioral gaps

### Orphaned (produced by pipeline tools, not read by any other tool in `tools/`)

| File | Produced by | Notes |
|------|----------------|-------|
| `exited_positions.csv` | t1 | Useful for analysis; not wired into t2–t7. |
| `portfolio_returns.csv` | t6 | End state unless you consume in Power BI / other scripts. |
| `fundamentals_snapshot.csv` | t7 | End state unless external consumer. |

### Raw / auxiliary inputs (not outputs of t0–t7)

| File | Used by |
|------|---------|
| `*Tradelist*.csv` (dated exports) | t0 (as primary tradelist source when naming/mtime rules pick them) |
| `ticker_overrides.csv` | t0 |
| `*Securities bookings*.csv` | t0 (ISIN enrichment) |

### Missing / fragile links (worth confirming)

1. **t1 is not wired to `t0_standardized_tradelist.csv` in `pipeline.py`.**  
   t1 picks the **newest modification time** among almost all `data/*.csv` (only four names excluded). After a full run, **cache or output CSVs touched by t2/t3 can become “newer” than the standardized tradelist**, so t1 may read the **wrong** file on the next run unless file mtimes happen to favor the intended input.  
   **Contrast:** t5 **does** use `t0_standardized_tradelist.csv` explicitly.

2. **t0 and t1 use different “pick latest file” rules** (t0 prefers filenames containing `tradelist`; t1 uses global newest CSV with a short exclude list). They can disagree on which file is “source of truth” for the same folder.

3. **t5 requires `fx_cache.csv` to exist** before cash-flow FX lookup; in the ordered pipeline, t3 normally creates it first. A standalone call to t5 without running t3 would fail unless the cache file is already present.

---

## 5. Requested compact format (copy-friendly)

```
pipeline.py
  Input:  (none — orchestrates tools)
  Output: (none to disk — returns dict of DataFrames)
  Consumed by: N/A

tools/t0_tradelist_standardizer.py
  Input:  [latest eligible *Tradelist* or newest *.csv per t0 rules; optional ticker_overrides.csv; optional *Securities bookings*.csv]
  Output: [t0_standardized_tradelist.csv]
  Consumed by: t5 (explicit); t1 only if that file wins t1’s “newest csv” rule — not guaranteed

tools/t1_holdings_builder.py
  Input:  [newest *.csv in data_dir excluding daily_holdings.csv, exited_positions.csv, prices_cache.csv, normalized_transactions.csv]
  Output: [daily_holdings.csv, exited_positions.csv]
  Consumed by: t2 reads daily_holdings.csv; exited_positions.csv — nothing in pipeline

tools/t2_price_fetcher.py
  Input:  [daily_holdings.csv, prices_cache.csv]
  Output: [priced_holdings.csv, prices_cache.csv]
  Consumed by: t3

tools/t3_fx_converter.py
  Input:  [priced_holdings.csv, fx_cache.csv]
  Output: [priced_holdings_usd.csv, fx_cache.csv]
  Consumed by: t4, t5, t7

tools/t4_portfolio_nav.py
  Input:  [priced_holdings_usd.csv]
  Output: [portfolio_nav.csv]
  Consumed by: t6

tools/t5_cash_flow_builder.py
  Input:  [t0_standardized_tradelist.csv, fx_cache.csv]
  Output: [portfolio_cash_flows.csv]
  Consumed by: t6

tools/t6_return_calculator.py
  Input:  [portfolio_nav.csv, portfolio_cash_flows.csv]
  Output: [portfolio_returns.csv]
  Consumed by: (none in repo tools)

tools/t7_fundamentals_snapshot.py
  Input:  [priced_holdings_usd.csv]
  Output: [fundamentals_snapshot.csv]
  Consumed by: (none in repo tools)
```

If this matches your understanding except for the **t0/t1 coupling**, that gap is the main thing to reconcile before treating the pipeline as strictly linear on disk.
