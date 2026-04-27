"""Tests for the data audit module."""

from __future__ import annotations

import pandas as pd
import pytest

from src.data import audit, storage
from src.data.bonds import Bond, upsert_bond
from src.data.cf_table import CFRow, insert_cf


@pytest.fixture(autouse=True)
def fresh_schema(tmp_path, monkeypatch):
    db = tmp_path / "test.db"
    monkeypatch.setattr(storage, "SQLITE_PATH", db)
    storage.init_schema()
    # Redirect parquet datasets so checks don't see the real repo data
    new_paths = {k: tmp_path / "parquet" / k for k in storage.PARQUET_DATASETS}
    for p in new_paths.values():
        p.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(storage, "PARQUET_DATASETS", new_paths)
    monkeypatch.setattr(audit, "PARQUET_DATASETS", new_paths)
    yield


def _seed_minimal():
    """Seed one contract + one bond + one CF for happy-path tests."""
    with storage.sqlite_conn() as conn:
        conn.execute(
            "INSERT INTO contracts(contract_id, product) VALUES (?, ?)",
            ("T2606", "T"),
        )
    upsert_bond(Bond(
        bond_code="240017",
        bond_name="24国债17",
        coupon_rate=0.0211,
        maturity_date="2034-08-25",
    ))
    insert_cf(CFRow(
        contract_id="T2606",
        bond_code="240017",
        cf=0.9359,
        bond_name="24国债17",
        coupon_rate=0.0211,
        maturity_date="2034-08-25",
    ))


# ---- Inventory ----------------------------------------------------------


def test_sqlite_inventory_includes_all_tables():
    _seed_minimal()
    results = list(audit.check_sqlite_inventory())
    names = {r.name for r in results}
    assert {f"sqlite.{t}" for t in
            ("contracts", "bonds", "conversion_factors", "signals", "etl_runs")
            }.issubset(names)


def test_parquet_inventory_warns_on_empty():
    results = list(audit.check_parquet_inventory())
    # All directories created but empty -> warning per dataset
    for r in results:
        assert r.severity == "warning"
        assert r.message == "no files"


# ---- Consistency --------------------------------------------------------


def test_consistency_passes_when_seeded():
    _seed_minimal()
    results = list(audit.check_cf_bond_consistency())
    assert all(r.severity == "ok" for r in results)


def test_consistency_detects_orphan_cf():
    # Insert a CF without bond / contract — direct DB insert bypassing checks
    with storage.sqlite_conn() as conn:
        conn.execute(
            """INSERT INTO conversion_factors(contract_id, bond_code, cf)
               VALUES (?, ?, ?)""",
            ("T9999", "999999", 0.95),
        )
    results = list(audit.check_cf_bond_consistency())
    sev = {r.name: r.severity for r in results}
    assert sev["consistency.cf_bond_fk"] == "error"
    assert sev["consistency.cf_contract_fk"] == "error"


# ---- Bond completeness --------------------------------------------------


def test_bond_completeness_warns_on_null_coupon():
    upsert_bond(Bond("123456", "测试国债"))  # no coupon, no maturity
    results = list(audit.check_bonds_completeness())
    sev = {r.name: r.severity for r in results}
    assert sev["completeness.bonds_coupon"] == "warning"
    assert sev["completeness.bonds_maturity"] == "warning"


# ---- CF range -----------------------------------------------------------


def test_cf_range_ok_when_in_bounds():
    _seed_minimal()
    [r] = list(audit.check_cf_range())
    assert r.severity == "ok"


def test_cf_range_detects_out_of_bounds():
    # Bypass validation to insert an extreme value
    with storage.sqlite_conn() as conn:
        conn.execute(
            "INSERT INTO conversion_factors(contract_id, bond_code, cf) "
            "VALUES (?, ?, ?)",
            ("T2606", "BADCF", 5.0),
        )
    [r] = list(audit.check_cf_range())
    assert r.severity == "error"
    assert "1 CFs outside" in r.message


# ---- Futures price sanity -----------------------------------------------


def test_futures_price_ok_when_in_range():
    df = pd.DataFrame({
        "date": ["2026-04-24"],
        "contract_id": ["T2606"],
        "close": [108.7],
    })
    out = storage.PARQUET_DATASETS["futures_daily"] / "2026-04-24.parquet"
    df.to_parquet(out, index=False)
    [r] = list(audit.check_futures_price_sanity())
    assert r.severity == "ok"


def test_futures_price_warns_on_zero():
    df = pd.DataFrame({
        "date": ["2026-04-24"],
        "contract_id": ["T2606"],
        "close": [0.0],
    })
    out = storage.PARQUET_DATASETS["futures_daily"] / "2026-04-24.parquet"
    df.to_parquet(out, index=False)
    [r] = list(audit.check_futures_price_sanity())
    assert r.severity == "warning"


# ---- Aggregation --------------------------------------------------------


def test_run_all_checks_yields_results():
    _seed_minimal()
    results = audit.run_all_checks()
    assert len(results) > 5
    summary = audit.summarise(results)
    assert summary["error"] == 0


def test_render_markdown_basic():
    _seed_minimal()
    results = audit.run_all_checks()
    md = audit.render_markdown(results)
    assert md.startswith("# Data Audit Report")
    assert "Summary" in md
    assert "| | Check | Message |" in md


def test_render_json_parseable():
    _seed_minimal()
    results = audit.run_all_checks()
    js = audit.render_json(results)
    import json
    parsed = json.loads(js)
    assert "summary" in parsed
    assert "checks" in parsed
    assert all("severity" in c for c in parsed["checks"])
