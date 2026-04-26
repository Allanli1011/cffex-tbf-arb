"""ETL base framework.

Each ETL job follows the pipeline::

    Fetcher.fetch()  ->  DataFrame
    Validator.check(df)  ->  DataFrame  (raise on hard failure)
    Saver.save(df)  ->  rows written

The :class:`ETLJob` orchestrator runs them in order, logs progress,
and records outcomes to the ``etl_runs`` SQLite table.
"""

from __future__ import annotations

import datetime as dt
from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd
from loguru import logger

from .storage import sqlite_conn
from .utils import configure_logger


class Fetcher(ABC):
    """Pulls a DataFrame from an upstream source."""

    name: str = "fetcher"

    @abstractmethod
    def fetch(self) -> pd.DataFrame: ...


class Validator(ABC):
    """Verifies a DataFrame. Raise on hard failure; log warnings on soft issues."""

    @abstractmethod
    def check(self, df: pd.DataFrame) -> pd.DataFrame: ...


class Saver(ABC):
    """Persists a DataFrame. Returns the number of rows written."""

    @abstractmethod
    def save(self, df: pd.DataFrame) -> int: ...


# --- Default validators ---------------------------------------------------


class NotEmptyValidator(Validator):
    """Hard fail if the frame is empty."""

    def check(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            raise ValueError("Fetched DataFrame is empty")
        return df


class RequiredColumnsValidator(Validator):
    """Hard fail if any required column is missing."""

    def __init__(self, required: list[str]) -> None:
        self.required = required

    def check(self, df: pd.DataFrame) -> pd.DataFrame:
        missing = set(self.required) - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {sorted(missing)}")
        return df


class CompositeValidator(Validator):
    """Run a sequence of validators, all must pass."""

    def __init__(self, validators: list[Validator]) -> None:
        self.validators = validators

    def check(self, df: pd.DataFrame) -> pd.DataFrame:
        for v in self.validators:
            df = v.check(df)
        return df


# --- Default savers -------------------------------------------------------


class ParquetSaver(Saver):
    """Append-style parquet writer; one file per run, partitioned by run_date.

    The dataset is read with pandas/pyarrow which tolerates many small files.
    Use a compaction job later if file count gets unwieldy.
    """

    def __init__(self, dataset: str, run_date: str | None = None) -> None:
        from .storage import parquet_dir

        self.dir = parquet_dir(dataset)
        self.run_date = run_date or dt.date.today().isoformat()

    def save(self, df: pd.DataFrame) -> int:
        path = self.dir / f"{self.run_date}.parquet"
        df.to_parquet(path, index=False, engine="pyarrow", compression="snappy")
        logger.info(f"Wrote {len(df)} rows -> {path}")
        return len(df)


# --- Orchestrator ---------------------------------------------------------


@dataclass
class ETLResult:
    job: str
    run_date: str
    status: str
    rows: int
    note: str = ""


class ETLJob:
    """Wires together a fetcher, validator and saver."""

    def __init__(
        self,
        name: str,
        fetcher: Fetcher,
        saver: Saver,
        validator: Validator | None = None,
    ) -> None:
        self.name = name
        self.fetcher = fetcher
        self.validator = validator or NotEmptyValidator()
        self.saver = saver
        configure_logger()

    def run(self, run_date: str | None = None) -> ETLResult:
        run_date = run_date or dt.date.today().isoformat()
        logger.info(f"[{self.name}] start, run_date={run_date}")
        try:
            df = self.fetcher.fetch()
            df = self.validator.check(df)
            rows = self.saver.save(df)
            result = ETLResult(self.name, run_date, "ok", rows)
            logger.success(f"[{self.name}] done, rows={rows}")
        except Exception as exc:  # noqa: BLE001
            logger.exception(f"[{self.name}] failed: {exc}")
            result = ETLResult(self.name, run_date, "failed", 0, note=str(exc)[:500])
        self._record(result)
        return result

    @staticmethod
    def _record(result: ETLResult) -> None:
        with sqlite_conn() as conn:
            conn.execute(
                """
                INSERT INTO etl_runs(job, run_date, status, rows, note)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(job, run_date) DO UPDATE SET
                    status=excluded.status, rows=excluded.rows, note=excluded.note
                """,
                (result.job, result.run_date, result.status, result.rows, result.note),
            )
