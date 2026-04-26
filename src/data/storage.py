"""Storage layout and path helpers.

Layout (all under ``data/`` at repo root):

    data/
    ├── parquet/
    │   ├── futures_daily/        # 期货日线
    │   ├── bond_yield_curve/     # 中债收益率曲线
    │   ├── bond_valuation/       # 中债估值
    │   ├── repo_rate/            # DR007 / R007 / GC007 / FR007
    │   └── shibor/
    └── sqlite/
        └── meta.db               # 合约元数据、CF 表、信号、回测结果

SQLite is used for structured / transactional data where row-level updates
matter. Parquet is used for append-friendly time-series.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = REPO_ROOT / "data"
PARQUET_ROOT = DATA_ROOT / "parquet"
SQLITE_PATH = DATA_ROOT / "sqlite" / "meta.db"

# Parquet datasets — one subdirectory per logical table.
PARQUET_DATASETS = {
    "futures_daily": PARQUET_ROOT / "futures_daily",
    "bond_yield_curve": PARQUET_ROOT / "bond_yield_curve",
    "bond_valuation": PARQUET_ROOT / "bond_valuation",
    "repo_rate": PARQUET_ROOT / "repo_rate",
    "shibor": PARQUET_ROOT / "shibor",
    "futures_oi_rank": PARQUET_ROOT / "futures_oi_rank",
}


def ensure_layout() -> None:
    """Create the standard data directory layout if missing."""
    for path in PARQUET_DATASETS.values():
        path.mkdir(parents=True, exist_ok=True)
    SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)


def parquet_dir(dataset: str) -> Path:
    """Return the parquet directory for ``dataset``; create on demand."""
    if dataset not in PARQUET_DATASETS:
        raise KeyError(
            f"Unknown parquet dataset {dataset!r}. "
            f"Known: {sorted(PARQUET_DATASETS)}"
        )
    path = PARQUET_DATASETS[dataset]
    path.mkdir(parents=True, exist_ok=True)
    return path


@contextmanager
def sqlite_conn() -> Iterator[sqlite3.Connection]:
    """Context-managed SQLite connection. Commits on clean exit, rollbacks on error."""
    SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(SQLITE_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# --- Schema ---------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS contracts (
    contract_id      TEXT PRIMARY KEY,        -- e.g. T2509
    product          TEXT NOT NULL,           -- TS / TF / T / TL
    listing_date     TEXT,                    -- YYYY-MM-DD
    last_trade_date  TEXT,
    delivery_date    TEXT,
    created_at       TEXT DEFAULT CURRENT_TIMESTAMP
);

-- CF table is append-only. (contract_id, bond_code) is permanent once written.
-- No FK to contracts(contract_id) on purpose: CF announcements often arrive
-- before contract metadata is registered. Use a separate audit if needed.
CREATE TABLE IF NOT EXISTS conversion_factors (
    contract_id   TEXT NOT NULL,
    bond_code     TEXT NOT NULL,
    bond_name     TEXT,
    coupon_rate   REAL,
    maturity_date TEXT,
    cf            REAL NOT NULL,
    announce_date TEXT,
    source_url    TEXT,
    created_at    TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (contract_id, bond_code)
);

CREATE TABLE IF NOT EXISTS signals (
    signal_date  TEXT NOT NULL,
    strategy     TEXT NOT NULL,         -- basis / calendar / butterfly / ...
    contract_id  TEXT NOT NULL,
    metric       TEXT NOT NULL,         -- irr / net_basis / spread_z / ...
    value        REAL,
    extra_json   TEXT,                  -- optional JSON payload
    PRIMARY KEY (signal_date, strategy, contract_id, metric)
);

CREATE TABLE IF NOT EXISTS backtest_runs (
    run_id      TEXT PRIMARY KEY,
    strategy    TEXT NOT NULL,
    start_date  TEXT NOT NULL,
    end_date    TEXT NOT NULL,
    params_json TEXT,
    metrics_json TEXT,
    created_at  TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_runs (
    job         TEXT NOT NULL,
    run_date    TEXT NOT NULL,
    status      TEXT NOT NULL,         -- ok / failed / partial
    rows        INTEGER,
    note        TEXT,
    created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (job, run_date)
);
"""


def init_schema() -> None:
    """Create all tables if missing. Idempotent."""
    ensure_layout()
    with sqlite_conn() as conn:
        conn.executescript(SCHEMA_SQL)
