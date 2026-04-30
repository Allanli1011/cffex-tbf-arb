"""Generate the daily ETL + signal digest.

Reads the latest day across all signal datasets, filters to "actionable"
signals via configurable thresholds, and emits two artefacts:

- ``data/logs/daily-digest-YYYY-MM-DD.json`` — machine-readable form for
  downstream consumers (e.g. the Streamlit ETL health card).
- ``data/logs/daily-digest-YYYY-MM-DD.md`` — operator-readable digest
  the LaunchAgent wrapper tees to stdout.

Default thresholds (override via CLI flags):
- Basis carry trade entry      : ``|IRR − FDR007| > 30 bp``
- Calendar mean reversion      : ``|z60| > 2.0``
- Curve fly / steepener        : ``|z60| > 2.0``
- CTD switch alert             : ``switch_probability > 30%``
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402

from src.data.storage import PARQUET_DATASETS  # noqa: E402


@dataclass
class Thresholds:
    basis_bp: float = 30.0
    calendar_z: float = 2.0
    curve_z: float = 2.0
    ctd_prob: float = 0.30


@dataclass
class DigestSection:
    name: str
    asof: str | None
    threshold_descr: str
    rows: list[dict] = field(default_factory=list)


@dataclass
class DailyDigest:
    generated_at: str
    asof_overall: str | None
    sections: list[DigestSection]
    counts: dict[str, int]


def _latest_file(dataset: str) -> Path | None:
    files = sorted(PARQUET_DATASETS[dataset].glob("*.parquet"))
    return files[-1] if files else None


def _load_latest(dataset: str) -> tuple[pd.DataFrame, str | None]:
    p = _latest_file(dataset)
    if p is None:
        return pd.DataFrame(), None
    return pd.read_parquet(p), p.stem


def _basis_section(t: Thresholds) -> DigestSection:
    df, asof = _load_latest("basis_signals")
    sec = DigestSection(
        name="basis",
        asof=asof,
        threshold_descr=f"|IRR − FDR007| > {t.basis_bp:.0f}bp on CTD",
    )
    if df.empty:
        return sec
    ctd = df[df["is_ctd"]].copy()
    ctd["abs_carry"] = ctd["irr_minus_fdr007_bp"].astype(float).abs()
    hits = ctd[ctd["abs_carry"] > t.basis_bp].sort_values(
        "abs_carry", ascending=False
    )
    for _, r in hits.iterrows():
        sec.rows.append({
            "contract_id": str(r["contract_id"]),
            "product": str(r["product"]),
            "bond_code": str(r["bond_code"]),
            "irr_pct": round(float(r["irr"]) * 100, 3),
            "irr_minus_fdr007_bp": round(float(r["irr_minus_fdr007_bp"]), 1),
            "net_basis": round(float(r["net_basis"]), 4),
            "ytm_source": (
                str(r["ytm_source"]) if "ytm_source" in r else "n/a"
            ),
            "side": ("long_basis"
                     if float(r["irr_minus_fdr007_bp"]) > 0
                     else "short_basis"),
        })
    return sec


def _calendar_section(t: Thresholds) -> DigestSection:
    df, asof = _load_latest("calendar_spreads")
    sec = DigestSection(
        name="calendar",
        asof=asof,
        threshold_descr=f"|z60| > {t.calendar_z:.1f} on near_far / mid_far / near_mid",
    )
    if df.empty:
        return sec
    df["abs_z"] = df["z60"].astype(float).abs()
    hits = df[df["abs_z"] > t.calendar_z].sort_values(
        "abs_z", ascending=False
    )
    for _, r in hits.iterrows():
        sec.rows.append({
            "product": str(r["product"]),
            "leg": str(r["leg"]),
            "near_contract": str(r["near_contract"]),
            "far_contract": str(r["far_contract"]),
            "spread": round(float(r["spread"]), 4),
            "z60": round(float(r["z60"]), 2),
            "percentile60": round(float(r["percentile60"]), 3)
                if pd.notna(r.get("percentile60")) else None,
            "side": ("short_spread" if float(r["z60"]) > 0
                     else "long_spread"),
        })
    return sec


def _curve_section(t: Thresholds) -> DigestSection:
    df, asof = _load_latest("curve_signals")
    sec = DigestSection(
        name="curve",
        asof=asof,
        threshold_descr=f"|z60| > {t.curve_z:.1f} on fly / steepener",
    )
    if df.empty:
        return sec
    df["abs_z"] = df["z60"].astype(float).abs()
    hits = df[df["abs_z"] > t.curve_z].sort_values(
        "abs_z", ascending=False
    )
    for _, r in hits.iterrows():
        sec.rows.append({
            "structure": str(r["structure"]),
            "spread_bp": round(float(r["spread_bp"]), 2),
            "z60": round(float(r["z60"]), 2),
            "n_short_wing": round(float(r["n_short_wing"]), 3)
                if pd.notna(r.get("n_short_wing")) else None,
            "n_belly": round(float(r["n_belly"]), 3)
                if pd.notna(r.get("n_belly")) else None,
            "n_long_wing": round(float(r["n_long_wing"]), 3)
                if pd.notna(r.get("n_long_wing")) else None,
            "side": ("short_fly" if float(r["z60"]) > 0
                     else "long_fly"),
        })
    return sec


def _ctd_section(t: Thresholds) -> DigestSection:
    df, asof = _load_latest("ctd_switch")
    sec = DigestSection(
        name="ctd_switch",
        asof=asof,
        threshold_descr=f"switch_probability > {t.ctd_prob*100:.0f}%",
    )
    if df.empty:
        return sec
    hits = df[df["switch_probability"].astype(float) > t.ctd_prob].sort_values(
        "switch_probability", ascending=False
    )
    for _, r in hits.iterrows():
        sec.rows.append({
            "contract_id": str(r["contract_id"]),
            "product": str(r["product"]),
            "current_ctd": str(r["current_ctd_bond"]),
            "irr_ctd": str(r["irr_ctd_bond"]),
            "anchor_disagrees": bool(r["ctd_anchor_disagrees"]),
            "days_to_delivery": int(r["days_to_delivery"]),
            "switch_prob_pct": round(
                float(r["switch_probability"]) * 100, 1
            ),
            "top_alt_bond": (
                str(r["top_alt_bond"])
                if pd.notna(r.get("top_alt_bond")) else None
            ),
            "top_alt_prob_pct": round(
                float(r["top_alt_prob"]) * 100, 1
            ) if pd.notna(r.get("top_alt_prob")) else None,
        })
    return sec


def build_digest(t: Thresholds) -> DailyDigest:
    sections = [
        _basis_section(t),
        _calendar_section(t),
        _curve_section(t),
        _ctd_section(t),
    ]
    asofs = [s.asof for s in sections if s.asof]
    asof_overall = max(asofs) if asofs else None
    return DailyDigest(
        generated_at=pd.Timestamp.now().isoformat(timespec="seconds"),
        asof_overall=asof_overall,
        sections=sections,
        counts={s.name: len(s.rows) for s in sections},
    )


def render_markdown(digest: DailyDigest) -> str:
    out: list[str] = []
    out.append(f"# Daily signal digest — {digest.asof_overall or 'no data'}")
    out.append("")
    out.append(f"_Generated: {digest.generated_at}_")
    out.append("")
    total = sum(digest.counts.values())
    out.append(f"**{total} actionable signals** "
               f"({', '.join(f'{k}={v}' for k, v in digest.counts.items())})")
    out.append("")

    for section in digest.sections:
        out.append(f"## {section.name}  (n={len(section.rows)})")
        out.append(f"Filter: {section.threshold_descr}")
        out.append(f"As-of: {section.asof or 'no data'}")
        out.append("")
        if not section.rows:
            out.append("_(no signals over threshold)_")
            out.append("")
            continue
        keys = list(section.rows[0])
        out.append("| " + " | ".join(keys) + " |")
        out.append("|" + "|".join(["---"] * len(keys)) + "|")
        for r in section.rows[:25]:  # cap per section so the digest stays terse
            out.append("| " + " | ".join(str(r[k]) for k in keys) + " |")
        if len(section.rows) > 25:
            out.append(f"_(+{len(section.rows) - 25} more rows truncated)_")
        out.append("")
    return "\n".join(out)


def render_summary_lines(digest: DailyDigest) -> list[str]:
    """Tight one-liner per section for the wrapper's stdout summary."""
    lines = [f"=== Daily signal digest — {digest.asof_overall or 'no data'} ==="]
    icon = lambda n: "🚦" if n == 0 else "🔔"  # noqa: E731
    for section in digest.sections:
        n = len(section.rows)
        if n == 0:
            sample = "—"
        else:
            top = section.rows[0]
            if section.name == "basis":
                sample = (
                    f"{top['contract_id']}/{top['bond_code']} "
                    f"IRR-FDR={top['irr_minus_fdr007_bp']:+.0f}bp"
                )
            elif section.name == "calendar":
                sample = (
                    f"{top['product']} {top['leg']} z={top['z60']:+.2f}"
                )
            elif section.name == "curve":
                sample = (
                    f"{top['structure']} z={top['z60']:+.2f}"
                )
            else:  # ctd_switch
                sample = (
                    f"{top['contract_id']} {top['switch_prob_pct']:.0f}% "
                    f"→ {top['top_alt_bond']}"
                )
        lines.append(f"  {icon(n)} {section.name:>12s}: {n} hits — top: {sample}")
    return lines


def write_outputs(digest: DailyDigest, out_dir: Path) -> tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = digest.asof_overall or pd.Timestamp.now().date().isoformat()
    json_path = out_dir / f"daily-digest-{stamp}.json"
    md_path = out_dir / f"daily-digest-{stamp}.md"

    payload = {
        "generated_at": digest.generated_at,
        "asof_overall": digest.asof_overall,
        "counts": digest.counts,
        "sections": [asdict(s) for s in digest.sections],
    }
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    md_path.write_text(render_markdown(digest))

    # Also write a stable "latest" pointer for the panel to read
    (out_dir / "daily-digest-latest.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False)
    )
    return json_path, md_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Daily signal digest")
    parser.add_argument("--basis-bp", type=float, default=30.0)
    parser.add_argument("--calendar-z", type=float, default=2.0)
    parser.add_argument("--curve-z", type=float, default=2.0)
    parser.add_argument("--ctd-prob", type=float, default=0.30)
    parser.add_argument(
        "--out-dir", type=Path, default=ROOT / "data" / "logs",
        help="Where to write digest files (default: data/logs/)",
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help="Suppress stdout summary (still writes files)",
    )
    args = parser.parse_args(argv)

    t = Thresholds(
        basis_bp=args.basis_bp,
        calendar_z=args.calendar_z,
        curve_z=args.curve_z,
        ctd_prob=args.ctd_prob,
    )
    digest = build_digest(t)
    json_path, md_path = write_outputs(digest, args.out_dir)
    if not args.quiet:
        for line in render_summary_lines(digest):
            print(line)
        print(f"  → {md_path.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
