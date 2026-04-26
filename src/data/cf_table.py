"""Conversion factor (CF) table — append-only operations.

Contract: once written, ``(contract_id, bond_code) -> cf`` is permanent.
Any subsequent write attempt with a different CF value raises
:class:`CFConflictError`. This is the data-integrity protection that
prevents silently corrupting historical CFs.

CFs are sourced from CFFEX announcements; announcements never modify
already-published CFs, only add new (contract, bond) pairs as new bonds
enter the deliverable pool.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd
from loguru import logger

from .storage import sqlite_conn

# CF values are quoted to 4 decimals by CFFEX; allow a small float tolerance.
CF_EQ_TOLERANCE = 1e-4

# Sanity bounds: CFs realistically fall well within [0.7, 1.3].
CF_MIN, CF_MAX = 0.5, 1.5


class CFConflictError(ValueError):
    """Raised when an insert would change an existing CF value."""


@dataclass(frozen=True)
class CFRow:
    contract_id: str
    bond_code: str
    cf: float
    bond_name: str | None = None
    coupon_rate: float | None = None
    maturity_date: str | None = None
    announce_date: str | None = None
    source_url: str | None = None

    def validate(self) -> None:
        if not self.contract_id or not self.bond_code:
            raise ValueError(f"contract_id and bond_code required: {self}")
        if self.cf is None or math.isnan(self.cf):
            raise ValueError(f"cf must be set: {self}")
        if not (CF_MIN <= self.cf <= CF_MAX):
            raise ValueError(
                f"cf={self.cf} out of sanity range [{CF_MIN}, {CF_MAX}]: {self}"
            )


# ---------------------------------------------------------------------
# Read helpers
# ---------------------------------------------------------------------


def get_cf(contract_id: str, bond_code: str) -> float | None:
    with sqlite_conn() as conn:
        row = conn.execute(
            "SELECT cf FROM conversion_factors WHERE contract_id=? AND bond_code=?",
            (contract_id, bond_code),
        ).fetchone()
    return row[0] if row else None


def list_cfs(contract_id: str | None = None) -> pd.DataFrame:
    """Return CFs as a DataFrame, optionally filtered to one contract."""
    with sqlite_conn() as conn:
        if contract_id:
            df = pd.read_sql_query(
                "SELECT * FROM conversion_factors WHERE contract_id=? "
                "ORDER BY bond_code",
                conn,
                params=(contract_id,),
            )
        else:
            df = pd.read_sql_query(
                "SELECT * FROM conversion_factors ORDER BY contract_id, bond_code",
                conn,
            )
    return df


# ---------------------------------------------------------------------
# Write helpers (append-only, conflict-detecting)
# ---------------------------------------------------------------------


def insert_cf(row: CFRow, *, allow_idempotent: bool = True) -> str:
    """Insert one CF row.

    Returns one of:
        ``"inserted"``  - new row written
        ``"unchanged"`` - identical row already existed (idempotent re-run)

    Raises :class:`CFConflictError` if a row exists with a *different* cf.
    """
    row.validate()
    existing = get_cf(row.contract_id, row.bond_code)

    if existing is not None:
        if abs(existing - row.cf) <= CF_EQ_TOLERANCE:
            if allow_idempotent:
                return "unchanged"
            raise CFConflictError(
                f"CF for ({row.contract_id}, {row.bond_code}) already exists "
                f"with the same value {existing}"
            )
        raise CFConflictError(
            f"CF conflict for ({row.contract_id}, {row.bond_code}): "
            f"existing={existing}, new={row.cf}. "
            "CF table is append-only; existing CFs may never change."
        )

    with sqlite_conn() as conn:
        conn.execute(
            """
            INSERT INTO conversion_factors(
                contract_id, bond_code, bond_name, coupon_rate,
                maturity_date, cf, announce_date, source_url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row.contract_id,
                row.bond_code,
                row.bond_name,
                row.coupon_rate,
                row.maturity_date,
                row.cf,
                row.announce_date,
                row.source_url,
            ),
        )
    logger.info(f"CF inserted: {row.contract_id} / {row.bond_code} = {row.cf}")
    return "inserted"


def insert_cfs(rows: Iterable[CFRow]) -> dict[str, int]:
    """Bulk insert. Stops and rolls back on first conflict.

    Returns a counter dict like ``{"inserted": 12, "unchanged": 3}``.
    """
    counter = {"inserted": 0, "unchanged": 0}
    rows_list = list(rows)
    # Dry-run pass: validate all rows and check for conflicts before any write.
    for r in rows_list:
        r.validate()
        existing = get_cf(r.contract_id, r.bond_code)
        if existing is not None and abs(existing - r.cf) > CF_EQ_TOLERANCE:
            raise CFConflictError(
                f"CF conflict for ({r.contract_id}, {r.bond_code}): "
                f"existing={existing}, new={r.cf}"
            )
    # Commit pass: all rows are safe to insert.
    for r in rows_list:
        outcome = insert_cf(r)
        counter[outcome] += 1
    return counter


# ---------------------------------------------------------------------
# CSV bridge
# ---------------------------------------------------------------------


CSV_COLUMNS = [
    "contract_id",
    "bond_code",
    "bond_name",
    "coupon_rate",
    "maturity_date",
    "cf",
    "announce_date",
    "source_url",
]


def import_csv(path: str | Path) -> dict[str, int]:
    """Read a CF CSV (commented lines starting with '#' allowed) and insert."""
    path = Path(path)
    df = pd.read_csv(path, comment="#")
    missing = set(CSV_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"CSV {path} missing columns: {sorted(missing)}")

    rows = [
        CFRow(
            contract_id=str(r.contract_id).strip(),
            bond_code=str(r.bond_code).strip(),
            bond_name=_clean(r.bond_name),
            coupon_rate=_to_float(r.coupon_rate),
            maturity_date=_clean(r.maturity_date),
            cf=float(r.cf),
            announce_date=_clean(r.announce_date),
            source_url=_clean(r.source_url),
        )
        for r in df.itertuples(index=False)
    ]
    return insert_cfs(rows)


def export_csv(path: str | Path, contract_id: str | None = None) -> int:
    """Export CFs to CSV. Returns row count written."""
    df = list_cfs(contract_id)
    df = df[CSV_COLUMNS]  # consistent column order
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return len(df)


def _clean(v) -> str | None:
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    s = str(v).strip()
    return s or None


def _to_float(v) -> float | None:
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def row_to_dict(row: CFRow) -> dict:
    return asdict(row)
