"""Backfill historical CFs from Wayback Machine snapshots of the CFFEX
deliverable-bond CSV.

The current CFFEX endpoint at ``/sj/jgsj/jgqsj/index_6882.csv`` only carries
contracts that are currently listed. Once a contract reaches its delivery
date, its CFs disappear from the endpoint forever.

By happy accident, the Internet Archive captured a snapshot on 2024-08-16
that contains the *full historical* deliverable-bond table from T1803
onward — 821 (contract, bond) rows covering ~5.5 years.

This script pulls Wayback snapshots, runs them through the same
:func:`parse_deliverable_csv` parser, and inserts via the same
append-only path used for live data. Rows that already exist with a
matching CF are reported as ``unchanged``; any conflict aborts.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from loguru import logger  # noqa: E402

from src.data.bonds import upsert_bonds  # noqa: E402
from src.data.cf_table import CFConflictError, insert_cfs  # noqa: E402
from src.data.fetchers import (  # noqa: E402
    DeliverablePoolSnapshot,
    fetch_deliverable_pool,
)
from src.data.storage import init_schema, sqlite_conn  # noqa: E402
from src.data.utils import configure_logger  # noqa: E402


# Known good Wayback snapshot — comprehensive (T1803..T2503).
WAYBACK_SNAPSHOTS = {
    "2024-08-16": (
        "https://web.archive.org/web/20240816084719/"
        "http://www.cffex.com.cn/sj/jgsj/jgqsj/index_6882.csv"
    ),
}


def _summarise(snaps: list[DeliverablePoolSnapshot]) -> None:
    by_product = Counter(s.product for s in snaps)
    by_contract = Counter(s.contract_id for s in snaps)
    contracts = sorted(by_contract)
    logger.info(f"  total rows  : {len(snaps)}")
    logger.info(f"  unique bonds: {len({s.bond.bond_code for s in snaps})}")
    logger.info(f"  by product  : {dict(by_product)}")
    logger.info(f"  contract span: {contracts[0]} .. {contracts[-1]}")
    logger.info(f"  contracts    : {len(by_contract)}")


def _upsert_contracts(snaps: list[DeliverablePoolSnapshot]) -> int:
    contracts = {(s.contract_id, s.product) for s in snaps}
    with sqlite_conn() as conn:
        for cid, product in contracts:
            conn.execute(
                """INSERT INTO contracts(contract_id, product)
                   VALUES (?, ?)
                   ON CONFLICT(contract_id) DO UPDATE
                   SET product=excluded.product""",
                (cid, product),
            )
    return len(contracts)


def _ingest(snaps: list[DeliverablePoolSnapshot], dry_run: bool) -> int:
    if not snaps:
        logger.warning("No snapshots to ingest")
        return 1

    _summarise(snaps)
    if dry_run:
        logger.info("Dry run — not writing")
        return 0

    n_contracts = _upsert_contracts(snaps)
    logger.info(f"Upserted {n_contracts} contracts")

    bond_counter = upsert_bonds({s.bond for s in snaps})
    logger.info(f"Bonds: {bond_counter}")

    try:
        cf_counter = insert_cfs([s.cf_row for s in snaps])
    except CFConflictError as exc:
        logger.error(f"CF conflict — refusing to write: {exc}")
        return 2
    logger.success(f"CFs: {cf_counter}")
    return 0


def cmd_wayback(label: str, dry_run: bool) -> int:
    if label not in WAYBACK_SNAPSHOTS:
        logger.error(
            f"Unknown snapshot {label!r}. Known: {sorted(WAYBACK_SNAPSHOTS)}"
        )
        return 1
    url = WAYBACK_SNAPSHOTS[label]
    logger.info(f"Fetching Wayback snapshot {label}: {url}")
    snaps = fetch_deliverable_pool(url=url)
    return _ingest(snaps, dry_run)


def cmd_url(url: str, dry_run: bool) -> int:
    logger.info(f"Fetching from custom URL: {url}")
    snaps = fetch_deliverable_pool(url=url)
    return _ingest(snaps, dry_run)


def cmd_file(path: Path, dry_run: bool) -> int:
    from src.data.fetchers import parse_deliverable_csv

    raw = path.read_bytes()
    snaps = parse_deliverable_csv(raw, source_url=f"file://{path.resolve()}")
    return _ingest(snaps, dry_run)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Backfill historical CFs from a Wayback / mirror snapshot"
    )
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument(
        "--snapshot",
        default="2024-08-16",
        help=f"Known Wayback snapshot label (default: 2024-08-16). "
             f"Known: {sorted(WAYBACK_SNAPSHOTS)}",
    )
    grp.add_argument("--url", help="Custom URL to fetch")
    grp.add_argument("--file", type=Path, help="Local CSV file")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    configure_logger()
    init_schema()

    if args.url is not None:
        return cmd_url(args.url, args.dry_run)
    if args.file is not None:
        return cmd_file(args.file, args.dry_run)
    return cmd_wayback(args.snapshot, args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
