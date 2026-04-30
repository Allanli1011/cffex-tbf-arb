"""Tests for the daily signal digest builder.

We don't go through the parquet layer here — instead we patch the
loaders to feed synthetic frames so threshold logic + output shape
can be exercised deterministically.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts import daily_digest as dd


def _basis_frame(rows):
    return pd.DataFrame(rows, columns=[
        "date", "contract_id", "product", "bond_code",
        "irr", "irr_minus_fdr007_bp", "net_basis", "ytm_source",
        "is_ctd",
    ])


def _calendar_frame(rows):
    return pd.DataFrame(rows, columns=[
        "date", "product", "leg", "near_contract", "far_contract",
        "spread", "z60", "percentile60",
    ])


def _curve_frame(rows):
    return pd.DataFrame(rows, columns=[
        "date", "structure", "spread_bp", "z60",
        "n_short_wing", "n_belly", "n_long_wing",
    ])


def _ctd_frame(rows):
    return pd.DataFrame(rows, columns=[
        "date", "contract_id", "product", "current_ctd_bond",
        "irr_ctd_bond", "ctd_anchor_disagrees", "days_to_delivery",
        "switch_probability", "top_alt_bond", "top_alt_prob",
    ])


@pytest.fixture
def patched(monkeypatch):
    """Replace ``_load_latest`` with a function that serves synthetic
    frames per dataset name."""
    payloads: dict[str, tuple[pd.DataFrame, str]] = {}

    def fake_load(dataset: str):
        return payloads.get(dataset, (pd.DataFrame(), None))

    monkeypatch.setattr(dd, "_load_latest", fake_load)
    return payloads


def test_basis_threshold_filters_to_extremes(patched):
    patched["basis_signals"] = (_basis_frame([
        # Above threshold — long_basis
        ("2026-04-29", "T2606", "T", "230004",
         0.025, 50.0, -0.10, "par_curve", True),
        # Below threshold — should be dropped
        ("2026-04-29", "T2609", "T", "260007",
         0.012, 5.0, -0.05, "par_curve", True),
        # Above threshold negative — short_basis
        ("2026-04-29", "TL2606", "TL", "2400001",
         -0.05, -800.0, 1.5, "bond_valuation", True),
        # Not CTD — even at extreme bp it should be excluded
        ("2026-04-29", "T2606", "T", "240017",
         0.04, 200.0, 0.0, "par_curve", False),
    ]), "2026-04-29")

    digest = dd.build_digest(dd.Thresholds())
    basis = next(s for s in digest.sections if s.name == "basis")
    assert basis.asof == "2026-04-29"
    assert len(basis.rows) == 2
    sides = {r["side"] for r in basis.rows}
    assert sides == {"long_basis", "short_basis"}


def test_calendar_threshold_strict_inequality(patched):
    patched["calendar_spreads"] = (_calendar_frame([
        ("2026-04-29", "T", "near_far", "T2606", "T2612", 0.5, 2.5, 0.99),
        ("2026-04-29", "T", "near_mid", "T2606", "T2609", 0.2, 2.0, 0.97),  # ==2 → not strict
        ("2026-04-29", "TF", "near_far", "TF2606", "TF2612", 0.3, -2.4, 0.02),
        ("2026-04-29", "TS", "near_far", "TS2606", "TS2612", 0.05, 0.1, 0.5),
    ]), "2026-04-29")

    digest = dd.build_digest(dd.Thresholds())
    cal = next(s for s in digest.sections if s.name == "calendar")
    assert len(cal.rows) == 2
    z_values = sorted(r["z60"] for r in cal.rows)
    assert z_values == [-2.4, 2.5]


def test_ctd_section_uses_probability_threshold(patched):
    patched["ctd_switch"] = (_ctd_frame([
        ("2026-04-29", "TL2606", "TL", "2400001", "2400001",
         False, 49, 0.45, "230018", 0.30),
        ("2026-04-29", "T2612", "T", "230018", "230018",
         False, 200, 0.25, None, 0.0),
        ("2026-04-29", "TL2612", "TL", "2400001", "2500005",
         True, 226, 0.55, "2500005", 0.42),
    ]), "2026-04-29")

    digest = dd.build_digest(dd.Thresholds(ctd_prob=0.3))
    sec = next(s for s in digest.sections if s.name == "ctd_switch")
    assert len(sec.rows) == 2
    assert sec.rows[0]["switch_prob_pct"] == 55.0  # sorted desc
    assert sec.rows[1]["switch_prob_pct"] == 45.0


def test_empty_inputs_produce_empty_sections(patched):
    digest = dd.build_digest(dd.Thresholds())
    assert digest.asof_overall is None
    for section in digest.sections:
        assert section.rows == []


def test_render_markdown_and_summary_lines(patched):
    patched["basis_signals"] = (_basis_frame([
        ("2026-04-29", "T2606", "T", "230004",
         0.025, 50.0, -0.10, "par_curve", True),
    ]), "2026-04-29")
    patched["calendar_spreads"] = (_calendar_frame([]), None)
    patched["curve_signals"] = (_curve_frame([]), None)
    patched["ctd_switch"] = (_ctd_frame([]), None)

    digest = dd.build_digest(dd.Thresholds())
    md = dd.render_markdown(digest)
    summary_lines = dd.render_summary_lines(digest)

    assert "Daily signal digest" in md
    assert "T2606" in md
    assert "230004" in md
    assert any("basis: 1 hits" in line for line in summary_lines)
    assert any("calendar: 0 hits" in line for line in summary_lines)


def test_write_outputs_creates_json_md_and_latest(tmp_path, patched):
    patched["basis_signals"] = (_basis_frame([
        ("2026-04-29", "T2606", "T", "230004",
         0.025, 50.0, -0.10, "par_curve", True),
    ]), "2026-04-29")

    digest = dd.build_digest(dd.Thresholds())
    json_path, md_path = dd.write_outputs(digest, tmp_path)

    assert json_path.exists()
    assert md_path.exists()
    assert (tmp_path / "daily-digest-latest.json").exists()
    assert json_path.name == "daily-digest-2026-04-29.json"
    assert md_path.name == "daily-digest-2026-04-29.md"

    payload = json.loads(json_path.read_text())
    assert payload["asof_overall"] == "2026-04-29"
    assert payload["counts"]["basis"] == 1
    assert payload["sections"][0]["name"] == "basis"
