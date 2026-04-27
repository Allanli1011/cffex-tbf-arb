"""Verify our CF formula against the 944 CFs published by CFFEX.

This is the primary integrity check on the pricing engine. It joins
every (contract_id, bond_code) row in ``conversion_factors`` with the
matching ``bonds`` master row, recomputes the CF from the official
formula, and reports the distribution of differences.

Outputs:
- stdout: summary statistics
- ``docs/cf_verification.md``: per-contract and per-product detail

A conforming run sees the bulk of diffs within ±5 basis points of the
published CFs. Larger outliers usually point to:
- bond master metadata bugs (coupon, maturity)
- non-standard coupon conventions (e.g. stub first coupon)
- contracts whose delivery convention differs from our default
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402

from src.data.storage import init_schema, sqlite_conn  # noqa: E402
from src.data.utils import configure_logger  # noqa: E402
from src.pricing.cf_calculator import (  # noqa: E402
    CFInputs,
    compute_cf,
    parse_contract_id,
)
from loguru import logger  # noqa: E402


def load_cf_with_bond() -> pd.DataFrame:
    """Join CFs with bond master metadata."""
    with sqlite_conn() as conn:
        df = pd.read_sql_query(
            """
            SELECT cf.contract_id, cf.bond_code, cf.cf AS cf_published,
                   b.bond_name, b.coupon_rate, b.maturity_date
            FROM conversion_factors cf
            LEFT JOIN bonds b ON cf.bond_code = b.bond_code
            ORDER BY cf.contract_id, cf.bond_code
            """,
            conn,
        )
    return df


def recompute(row: pd.Series) -> tuple[float | None, str | None]:
    """Recompute one row. Returns (cf_computed, error_or_none)."""
    if (
        pd.isna(row.coupon_rate)
        or pd.isna(row.maturity_date)
        or row.maturity_date == ""
    ):
        return None, "missing bond metadata"
    try:
        _, delivery = parse_contract_id(row.contract_id)
        maturity = dt.date.fromisoformat(row.maturity_date)
        out = compute_cf(CFInputs(
            coupon_rate=float(row.coupon_rate),
            maturity=maturity,
            delivery_month_start=delivery,
        ))
        return out.cf, None
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verify CF formula")
    parser.add_argument(
        "-o", "--output", type=Path,
        default=Path("docs/cf_verification.md"),
        help="Markdown report output",
    )
    parser.add_argument(
        "--threshold-bp", type=float, default=5.0,
        help="Threshold for 'major outlier' classification (basis points)",
    )
    args = parser.parse_args(argv)

    configure_logger()
    init_schema()

    df = load_cf_with_bond()
    logger.info(f"Loaded {len(df)} CF rows")

    df["cf_computed"], df["error"] = zip(*df.apply(recompute, axis=1))
    valid = df[df["cf_computed"].notna()].copy()
    invalid = df[df["cf_computed"].isna()]
    valid["diff"] = valid["cf_computed"] - valid["cf_published"]
    valid["diff_bp"] = valid["diff"] * 10_000

    abs_bp = valid["diff_bp"].abs()
    summary = {
        "total_rows": len(df),
        "computed": len(valid),
        "skipped": len(invalid),
        "median_abs_bp": float(abs_bp.median()),
        "mean_abs_bp": float(abs_bp.mean()),
        "p95_abs_bp": float(abs_bp.quantile(0.95)),
        "max_abs_bp": float(abs_bp.max()),
        "exact_match": int((abs_bp == 0).sum()),
        "within_1bp": int((abs_bp <= 1).sum()),
        "within_5bp": int((abs_bp <= 5).sum()),
        "within_10bp": int((abs_bp <= 10).sum()),
        "outliers": int((abs_bp > args.threshold_bp).sum()),
    }

    logger.info(
        f"Match: exact={summary['exact_match']}, "
        f"≤1bp={summary['within_1bp']}, "
        f"≤5bp={summary['within_5bp']}, "
        f"max={summary['max_abs_bp']:.1f}bp"
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(_render_markdown(df, valid, summary, args.threshold_bp),
                           encoding="utf-8")
    logger.success(f"Report written to {args.output}")
    return 0


def _render_markdown(df, valid, summary, threshold_bp) -> str:
    lines = ["# CF Formula Verification", ""]
    lines.append(
        f"_Generated: {dt.datetime.now().isoformat(timespec='seconds')}_"
    )
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- Total CFs in DB: **{summary['total_rows']}**")
    lines.append(f"- Recomputed: **{summary['computed']}**, skipped: {summary['skipped']}")
    lines.append("")
    lines.append("### Diff distribution (computed - published, bp)")
    lines.append("")
    lines.append("| metric | value |")
    lines.append("|--|--|")
    lines.append(f"| Exact match | {summary['exact_match']} ({summary['exact_match']/summary['computed']:.1%}) |")
    lines.append(f"| ≤ 1 bp | {summary['within_1bp']} ({summary['within_1bp']/summary['computed']:.1%}) |")
    lines.append(f"| ≤ 5 bp | {summary['within_5bp']} ({summary['within_5bp']/summary['computed']:.1%}) |")
    lines.append(f"| ≤ 10 bp | {summary['within_10bp']} ({summary['within_10bp']/summary['computed']:.1%}) |")
    lines.append(f"| Median \\|diff\\| | {summary['median_abs_bp']:.2f} bp |")
    lines.append(f"| Mean \\|diff\\| | {summary['mean_abs_bp']:.2f} bp |")
    lines.append(f"| P95 \\|diff\\| | {summary['p95_abs_bp']:.2f} bp |")
    lines.append(f"| Max \\|diff\\| | {summary['max_abs_bp']:.2f} bp |")
    lines.append("")

    # By product
    valid["product"] = valid["contract_id"].str.extract(r"^(TS|TF|TL|T)")
    by_product = valid.groupby("product").agg(
        rows=("cf_published", "size"),
        median_abs_bp=("diff_bp", lambda s: s.abs().median()),
        max_abs_bp=("diff_bp", lambda s: s.abs().max()),
    )
    lines.append("### By product")
    lines.append("")
    lines.append("| product | rows | median \\|bp\\| | max \\|bp\\| |")
    lines.append("|--|--|--|--|")
    for p, row in by_product.iterrows():
        lines.append(f"| {p} | {int(row.rows)} | {row.median_abs_bp:.2f} | {row.max_abs_bp:.2f} |")
    lines.append("")

    # Top 20 outliers
    outliers = valid.reindex(valid["diff_bp"].abs().sort_values(ascending=False).index).head(20)
    if len(outliers):
        lines.append(f"### Top 20 outliers (|diff| > {threshold_bp} bp)")
        lines.append("")
        lines.append("| contract | bond | name | coupon | maturity | published | computed | bp |")
        lines.append("|--|--|--|--|--|--|--|--|")
        for _, r in outliers.iterrows():
            lines.append(
                f"| {r.contract_id} | {r.bond_code} | {r.bond_name or ''} | "
                f"{r.coupon_rate:.4f} | {r.maturity_date} | "
                f"{r.cf_published:.4f} | {r.cf_computed:.4f} | "
                f"{r.diff_bp:+.1f} |"
            )
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    sys.exit(main())
