"""Smoke tests for Phase 1.1 infrastructure.

Run: pytest tests/test_infra.py -v
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.data import storage
from src.data.base import (
    ETLJob,
    Fetcher,
    NotEmptyValidator,
    ParquetSaver,
    RequiredColumnsValidator,
)
from src.data.utils import retry


# ---- Storage ------------------------------------------------------------


def test_init_schema_creates_tables():
    storage.init_schema()
    with storage.sqlite_conn() as conn:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    names = {r[0] for r in rows}
    assert {"contracts", "conversion_factors", "signals", "etl_runs"}.issubset(names)


def test_parquet_dir_known():
    p = storage.parquet_dir("futures_daily")
    assert p.exists() and p.is_dir()


def test_parquet_dir_unknown_raises():
    with pytest.raises(KeyError):
        storage.parquet_dir("not_a_dataset")


# ---- Retry --------------------------------------------------------------


def test_retry_succeeds_after_transient_failures():
    state = {"calls": 0}

    @retry(max_attempts=3, initial_wait=0.01, backoff=1.0)
    def flaky():
        state["calls"] += 1
        if state["calls"] < 3:
            raise RuntimeError("transient")
        return "ok"

    assert flaky() == "ok"
    assert state["calls"] == 3


def test_retry_raises_after_max_attempts():
    @retry(max_attempts=2, initial_wait=0.01)
    def always_fail():
        raise RuntimeError("nope")

    with pytest.raises(RuntimeError):
        always_fail()


# ---- Validators ---------------------------------------------------------


def test_not_empty_validator():
    NotEmptyValidator().check(pd.DataFrame({"x": [1]}))
    with pytest.raises(ValueError):
        NotEmptyValidator().check(pd.DataFrame())


def test_required_columns_validator():
    df = pd.DataFrame({"a": [1], "b": [2]})
    RequiredColumnsValidator(["a", "b"]).check(df)
    with pytest.raises(ValueError):
        RequiredColumnsValidator(["a", "c"]).check(df)


# ---- ETL end-to-end (no network) ----------------------------------------


class _StubFetcher(Fetcher):
    name = "stub"

    def fetch(self) -> pd.DataFrame:
        return pd.DataFrame({"date": ["2026-01-02"], "value": [1.23]})


def test_etl_job_runs_and_records(tmp_path, monkeypatch):
    # Isolate from the real data directory: use a tmp parquet datasets map
    # and a fresh SQLite file.
    monkeypatch.setattr(storage, "SQLITE_PATH", tmp_path / "test.db")
    new_paths = {k: tmp_path / "parquet" / k for k in storage.PARQUET_DATASETS}
    for p in new_paths.values():
        p.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(storage, "PARQUET_DATASETS", new_paths)

    storage.init_schema()
    job = ETLJob(
        name="stub_test",
        fetcher=_StubFetcher(),
        saver=ParquetSaver("futures_daily", run_date="2026-01-02"),
        validator=RequiredColumnsValidator(["date", "value"]),
    )
    result = job.run(run_date="2026-01-02")
    assert result.status == "ok"
    assert result.rows == 1

    with storage.sqlite_conn() as conn:
        row = conn.execute(
            "SELECT status, rows FROM etl_runs WHERE job=? AND run_date=?",
            ("stub_test", "2026-01-02"),
        ).fetchone()
    assert row == ("ok", 1)


# ---- Calendar (live AKShare; mark slow) ---------------------------------


@pytest.mark.network
def test_calendar_live():
    from src.data import calendar as cal

    df = cal.load_calendar(force_refresh=True)
    assert not df.empty
    assert "date" in df.columns
    # 2024-01-02 was a trading day; 2024-01-01 was not.
    import datetime as dt
    assert cal.is_trading_day(dt.date(2024, 1, 2)) is True
    assert cal.is_trading_day(dt.date(2024, 1, 1)) is False
