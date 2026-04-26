"""Authoritative CFFEX data fetchers.

The deliverable-bond CSV at ``/sj/jgsj/jgqsj/index_6882.csv`` is the cleanest
machine-readable source on the CFFEX site. It carries, in 9 unlabelled
columns:

    bond_name | interbank_code | sh_code | sz_code |
    maturity_date (YYYYMMDD) | coupon_rate (%) | cf | contract_id | product

It is updated on every trading day and contains the full deliverable-bond
universe across all currently-listed contracts. This single endpoint
replaces the bond-master + scraper combination we were originally building.
"""

from __future__ import annotations

import io
from dataclasses import dataclass

import pandas as pd
import requests
from loguru import logger

from .bonds import Bond
from .cf_table import CFRow
from .utils import retry

CFFEX_DELIVERABLE_BOND_CSV = (
    "http://www.cffex.com.cn/sj/jgsj/jgqsj/index_6882.csv"
)
HTTP_TIMEOUT = 20
DEFAULT_HEADERS = {"User-Agent": "Mozilla/5.0 (cffex-tbf-arb research)"}

CFFEX_CSV_COLUMNS = [
    "bond_name",
    "interbank_code",
    "sh_code",
    "sz_code",
    "maturity_yyyymmdd",
    "coupon_pct",
    "cf",
    "contract_id",
    "product",
]


@dataclass(frozen=True)
class DeliverablePoolSnapshot:
    """One row of the CFFEX CSV, normalised to project conventions."""

    bond: Bond
    cf_row: CFRow
    contract_id: str
    product: str  # TS / TF / T / TL


@retry(max_attempts=3, initial_wait=2.0)
def _download_deliverable_csv() -> bytes:
    resp = requests.get(
        CFFEX_DELIVERABLE_BOND_CSV,
        headers=DEFAULT_HEADERS,
        timeout=HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.content


def parse_deliverable_csv(raw: bytes, source_url: str = CFFEX_DELIVERABLE_BOND_CSV
                          ) -> list[DeliverablePoolSnapshot]:
    """Parse the raw CFFEX CSV bytes into normalised snapshots."""
    text = raw.decode("utf-8")
    df = pd.read_csv(
        io.StringIO(text),
        header=None,
        names=CFFEX_CSV_COLUMNS,
        dtype=str,
        keep_default_na=False,
    )

    snapshots: list[DeliverablePoolSnapshot] = []
    for r in df.itertuples(index=False):
        bond_code = r.interbank_code.strip()
        if not bond_code:
            logger.warning(f"Skipping row with empty interbank_code: {r}")
            continue

        bond = Bond(
            bond_code=bond_code,
            bond_name=r.bond_name.strip(),
            sh_code=_clean(r.sh_code),
            sz_code=_clean(r.sz_code),
            coupon_rate=_pct_to_decimal(r.coupon_pct),
            maturity_date=_yyyymmdd_to_date(r.maturity_yyyymmdd),
        )
        cf_row = CFRow(
            contract_id=r.contract_id.strip(),
            bond_code=bond_code,
            bond_name=bond.bond_name,
            coupon_rate=bond.coupon_rate,
            maturity_date=bond.maturity_date,
            cf=float(r.cf),
            announce_date=None,
            source_url=source_url,
        )
        snapshots.append(
            DeliverablePoolSnapshot(
                bond=bond,
                cf_row=cf_row,
                contract_id=r.contract_id.strip(),
                product=r.product.strip(),
            )
        )
    return snapshots


def fetch_deliverable_pool() -> list[DeliverablePoolSnapshot]:
    """Pull and parse the CFFEX deliverable-bond CSV.

    On a typical day this returns ~120 snapshots covering all currently
    listed contracts.
    """
    raw = _download_deliverable_csv()
    snaps = parse_deliverable_csv(raw)
    logger.info(
        f"CFFEX CSV: {len(snaps)} (contract, bond) snapshots "
        f"covering {len({s.contract_id for s in snaps})} contracts"
    )
    return snaps


# ---- helpers -------------------------------------------------------------


def _clean(v: str | None) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _pct_to_decimal(s: str) -> float | None:
    """CFFEX writes coupon as percent: '2.35' -> 0.0235."""
    s = (s or "").strip()
    if not s:
        return None
    try:
        return float(s) / 100
    except ValueError:
        return None


def _yyyymmdd_to_date(s: str) -> str | None:
    s = (s or "").strip()
    if len(s) != 8 or not s.isdigit():
        return None
    return f"{s[:4]}-{s[4:6]}-{s[6:]}"
