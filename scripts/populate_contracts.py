"""Populate contracts / bonds / conversion_factors from the authoritative
CFFEX deliverable-bond CSV (``/sj/jgsj/jgqsj/index_6882.csv``).

This is the canonical day-zero / quarterly refresh entry point.

Usage::

    python3 scripts/populate_contracts.py                # live fetch + write
    python3 scripts/populate_contracts.py --dry-run      # fetch + report only
    python3 scripts/populate_contracts.py --export-csv FILE
                                                         # also dump configs
    python3 scripts/populate_contracts.py --from-csv FILE
                                                         # offline import
                                                         # (uses configs/cf_table.csv as seed when missing)
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from loguru import logger  # noqa: E402

from src.data.bonds import Bond, upsert_bonds  # noqa: E402
from src.data.cf_table import (  # noqa: E402
    CFConflictError,
    export_csv,
    insert_cfs,
)
from src.data.fetchers import (  # noqa: E402
    DeliverablePoolSnapshot,
    fetch_deliverable_pool,
    parse_deliverable_csv,
)
from src.data.storage import init_schema, sqlite_conn  # noqa: E402
from src.data.utils import configure_logger  # noqa: E402


def _upsert_contracts(snapshots: list[DeliverablePoolSnapshot]) -> int:
    """Register one row per unique contract in the contracts table."""
    contracts = {(s.contract_id, s.product) for s in snapshots}
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


def _apply(snapshots: list[DeliverablePoolSnapshot], dry_run: bool) -> int:
    if not snapshots:
        logger.warning("No snapshots to apply")
        return 1

    by_product = Counter(s.product for s in snapshots)
    by_contract = Counter(s.contract_id for s in snapshots)
    logger.info(f"Snapshots: {len(snapshots)} rows")
    logger.info(f"  by product : {dict(by_product)}")
    logger.info(f"  by contract: {dict(sorted(by_contract.items()))}")

    if dry_run:
        for s in snapshots[:5]:
            logger.info(
                f"  sample: {s.contract_id} {s.bond.bond_code} "
                f"{s.bond.bond_name} cf={s.cf_row.cf}"
            )
        logger.info("Dry run — not writing")
        return 0

    n_contracts = _upsert_contracts(snapshots)
    logger.info(f"Upserted {n_contracts} contracts")

    bond_counter = upsert_bonds({s.bond for s in snapshots})
    logger.info(f"Bonds: {bond_counter}")

    try:
        cf_counter = insert_cfs([s.cf_row for s in snapshots])
    except CFConflictError as exc:
        logger.error(f"CF conflict — refusing to write: {exc}")
        return 2
    logger.success(f"CFs: {cf_counter}")
    return 0


def cmd_live(dry_run: bool, export_path: Path | None) -> int:
    snapshots = fetch_deliverable_pool()
    rc = _apply(snapshots, dry_run)
    if rc == 0 and export_path is not None:
        n = export_csv(export_path)
        logger.success(f"Exported {n} CFs to {export_path}")
    return rc


def cmd_from_csv(path: Path, dry_run: bool) -> int:
    """Load the CFFEX raw CSV from a local file (testing / offline)."""
    raw = path.read_bytes()
    snapshots = parse_deliverable_csv(raw, source_url=str(path))
    return _apply(snapshots, dry_run)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Refresh contracts / bonds / CFs from CFFEX CSV"
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--export-csv",
        type=Path,
        help="After successful write, also export configs/cf_table.csv",
    )
    parser.add_argument(
        "--from-csv",
        type=Path,
        help="Read raw CFFEX CSV from local file instead of HTTP",
    )
    args = parser.parse_args(argv)

    configure_logger()
    init_schema()

    if args.from_csv is not None:
        return cmd_from_csv(args.from_csv, args.dry_run)
    return cmd_live(args.dry_run, args.export_csv)


if __name__ == "__main__":
    sys.exit(main())
