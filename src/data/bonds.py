"""Bond master table operations.

Unlike conversion_factors, bonds metadata can change (rarely) when issuers
publish corrections. We use upsert semantics here, but log all changes so
operators can audit.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd
from loguru import logger

from .storage import sqlite_conn


@dataclass(frozen=True)
class Bond:
    bond_code: str
    bond_name: str
    sh_code: str | None = None
    sz_code: str | None = None
    coupon_rate: float | None = None
    coupon_frequency: int = 1
    maturity_date: str | None = None  # YYYY-MM-DD


def get_bond(bond_code: str) -> Bond | None:
    with sqlite_conn() as conn:
        row = conn.execute(
            """SELECT bond_code, bond_name, sh_code, sz_code,
                      coupon_rate, coupon_frequency, maturity_date
               FROM bonds WHERE bond_code = ?""",
            (bond_code,),
        ).fetchone()
    if not row:
        return None
    return Bond(*row)


def upsert_bond(bond: Bond) -> str:
    """Insert a new bond or update an existing one.

    Returns:
        ``"inserted"`` / ``"updated"`` / ``"unchanged"``.
    Logs at WARNING when overwriting non-null fields with different values.
    """
    existing = get_bond(bond.bond_code)
    if existing is None:
        with sqlite_conn() as conn:
            conn.execute(
                """INSERT INTO bonds(bond_code, bond_name, sh_code, sz_code,
                                     coupon_rate, coupon_frequency, maturity_date)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    bond.bond_code,
                    bond.bond_name,
                    bond.sh_code,
                    bond.sz_code,
                    bond.coupon_rate,
                    bond.coupon_frequency,
                    bond.maturity_date,
                ),
            )
        return "inserted"

    if existing == bond:
        return "unchanged"

    # Detect non-trivial mutations (coupon or maturity) which would be alarming.
    for field in ("coupon_rate", "coupon_frequency", "maturity_date"):
        old, new = getattr(existing, field), getattr(bond, field)
        if old is not None and new is not None and old != new:
            logger.warning(
                f"Bond {bond.bond_code} {field} changed: {old!r} -> {new!r}"
            )

    with sqlite_conn() as conn:
        conn.execute(
            """UPDATE bonds
               SET bond_name=?, sh_code=?, sz_code=?,
                   coupon_rate=?, coupon_frequency=?, maturity_date=?,
                   updated_at=CURRENT_TIMESTAMP
               WHERE bond_code=?""",
            (
                bond.bond_name,
                bond.sh_code,
                bond.sz_code,
                bond.coupon_rate,
                bond.coupon_frequency,
                bond.maturity_date,
                bond.bond_code,
            ),
        )
    return "updated"


def upsert_bonds(bonds: Iterable[Bond]) -> dict[str, int]:
    counter = {"inserted": 0, "updated": 0, "unchanged": 0}
    for b in bonds:
        counter[upsert_bond(b)] += 1
    return counter


def list_bonds() -> pd.DataFrame:
    with sqlite_conn() as conn:
        return pd.read_sql_query(
            "SELECT * FROM bonds ORDER BY bond_code", conn
        )
