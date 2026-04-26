"""Populate the contracts and conversion_factors tables.

Usage:
    python3 scripts/populate_contracts.py              # full sync
    python3 scripts/populate_contracts.py --dry-run    # preview only
    python3 scripts/populate_contracts.py --export-csv configs/cf_table.csv  # also export

This script:
1. Fetches ALL current deliverable bonds + CFs from CFFEX's public CSV API
2. Derives contract metadata from that data
3. Upserts contracts table
4. Inserts CFs into conversion_factors (append-only, conflict-detecting)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from loguru import logger  # noqa: E402

from src.data.cf_table import CFConflictError, CFRow, export_csv, insert_cfs  # noqa: E402
from src.data.fetchers import CFFEXDeliverableBondFetcher  # noqa: E402
from src.data.storage import init_schema, sqlite_conn  # noqa: E402
from src.data.utils import configure_logger  # noqa: E402


def upsert_contracts(df) -> int:
    """Insert/update contracts from the deliverable bonds DataFrame."""
    contracts = (
        df[["contract_id", "product"]]
        .drop_duplicates()
        .sort_values("contract_id")
    )
    count = 0
    with sqlite_conn() as conn:
        for _, row in contracts.iterrows():
            conn.execute(
                """
                INSERT INTO contracts(contract_id, product)
                VALUES (?, ?)
                ON CONFLICT(contract_id) DO UPDATE SET product=excluded.product
                """,
                (row["contract_id"], row["product"]),
            )
            count += 1
    logger.info(f"Upserted {count} contracts")
    return count


def build_cf_rows(df) -> list[CFRow]:
    """Convert the CFFEX deliverable bond DataFrame into CFRow objects."""
    rows = []
    for _, r in df.iterrows():
        # Use bank inter-bank code as primary bond_code
        bond_code = str(r["bond_code_ib"]).strip()
        rows.append(
            CFRow(
                contract_id=r["contract_id"],
                bond_code=bond_code,
                bond_name=r["bond_name"],
                coupon_rate=r["coupon_rate"] / 100 if r["coupon_rate"] > 1 else r["coupon_rate"],
                maturity_date=r["maturity_date"],
                cf=r["cf"],
                announce_date=None,
                source_url="http://www.cffex.com.cn/sj/jgsj/jgqsj/index_6882.csv",
            )
        )
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Populate contracts & CF tables from CFFEX")
    parser.add_argument("--dry-run", action="store_true", help="Preview only, don't write")
    parser.add_argument("--export-csv", type=Path, help="Also export CFs to CSV after insert")
    args = parser.parse_args(argv)

    configure_logger()
    init_schema()

    # Fetch all deliverable bonds
    fetcher = CFFEXDeliverableBondFetcher()
    df = fetcher.fetch()

    logger.info(
        f"Fetched {len(df)} CF entries across "
        f"{df['contract_id'].nunique()} contracts, "
        f"{df['product'].nunique()} products"
    )

    # Show summary
    for product in sorted(df["product"].unique()):
        mask = df["product"] == product
        contracts = sorted(df.loc[mask, "contract_id"].unique())
        bonds = df.loc[mask, "bond_code_ib"].nunique()
        logger.info(f"  {product}: contracts={contracts}, unique bonds={bonds}")

    if args.dry_run:
        logger.info("Dry run — not writing to database")
        for _, r in df.iterrows():
            print(
                f"  {r['contract_id']:8s} {str(r['bond_code_ib']):>10s} "
                f"cf={r['cf']:.4f}  {r['bond_name']}"
            )
        return 0

    # Upsert contracts
    upsert_contracts(df)

    # Insert CFs (append-only)
    cf_rows = build_cf_rows(df)
    try:
        result = insert_cfs(cf_rows)
    except CFConflictError as exc:
        logger.error(f"CF conflict: {exc}")
        return 2

    logger.success(
        f"CF table: inserted={result['inserted']}, unchanged={result['unchanged']}"
    )

    # Verify
    with sqlite_conn() as conn:
        n_contracts = conn.execute("SELECT COUNT(*) FROM contracts").fetchone()[0]
        n_cfs = conn.execute("SELECT COUNT(*) FROM conversion_factors").fetchone()[0]
    logger.info(f"Database now has {n_contracts} contracts, {n_cfs} CF entries")

    # Optional CSV export
    if args.export_csv:
        n = export_csv(args.export_csv)
        logger.success(f"Exported {n} CF rows to {args.export_csv}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
