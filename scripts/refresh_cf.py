"""Refresh the conversion factor (CF) table.

Usage:

    python3 scripts/refresh_cf.py                  # scrape CFFEX, append new
    python3 scripts/refresh_cf.py --csv FILE       # import a manually-curated CSV
    python3 scripts/refresh_cf.py --export FILE    # dump current CFs to CSV
    python3 scripts/refresh_cf.py --dry-run        # scrape and print, don't write

The CF table is *append-only*: existing ``(contract_id, bond_code)`` rows
are immutable. Any conflict aborts the run.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Make `src` importable when run as a script.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.data.cf_table import (  # noqa: E402
    CFConflictError,
    export_csv,
    import_csv,
    insert_cfs,
)
from src.data.cffex_scraper import discover_recent_cf_rows  # noqa: E402
from src.data.storage import init_schema  # noqa: E402
from src.data.utils import configure_logger  # noqa: E402
from loguru import logger  # noqa: E402


def cmd_scrape(dry_run: bool) -> int:
    rows = list(discover_recent_cf_rows())
    if not rows:
        logger.warning("No CF rows discovered from CFFEX announcements")
        return 0

    logger.info(f"Discovered {len(rows)} CF rows")
    for r in rows[:20]:
        logger.info(f"  {r.contract_id:8s} {r.bond_code:24s} cf={r.cf}  ({r.bond_name})")
    if len(rows) > 20:
        logger.info(f"  ... and {len(rows) - 20} more")

    if dry_run:
        logger.info("Dry run — not writing")
        return 0

    try:
        result = insert_cfs(rows)
    except CFConflictError as exc:
        logger.error(f"CF conflict aborted run: {exc}")
        return 2

    logger.success(
        f"Inserted {result['inserted']}, unchanged {result['unchanged']}"
    )
    return 0


def cmd_import(path: Path) -> int:
    logger.info(f"Importing CFs from {path}")
    try:
        result = import_csv(path)
    except CFConflictError as exc:
        logger.error(f"CF conflict aborted import: {exc}")
        return 2
    logger.success(
        f"Inserted {result['inserted']}, unchanged {result['unchanged']}"
    )
    return 0


def cmd_export(path: Path) -> int:
    n = export_csv(path)
    logger.success(f"Exported {n} rows to {path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Refresh CFFEX CF table")
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--csv", type=Path, help="Import from CSV file")
    grp.add_argument("--export", type=Path, help="Export current CFs to CSV")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scrape and report only; do not write",
    )
    args = parser.parse_args(argv)

    configure_logger()
    init_schema()

    if args.export is not None:
        return cmd_export(args.export)
    if args.csv is not None:
        return cmd_import(args.csv)
    return cmd_scrape(args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
