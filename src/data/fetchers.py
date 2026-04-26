"""Concrete data fetchers for Phase 1.

Each fetcher pulls data from a free/open source and returns a DataFrame.
Fetchers are paired with savers in the ETL orchestration layer.
"""

from __future__ import annotations

import datetime as dt
from io import BytesIO

import pandas as pd
import requests
from loguru import logger

from .base import Fetcher
from .utils import retry

# ── Shared HTTP ──────────────────────────────────────────────────────

DEFAULT_HEADERS = {"User-Agent": "Mozilla/5.0 (cffex-tbf-arb research)"}
HTTP_TIMEOUT = 15


@retry(max_attempts=3, initial_wait=2.0)
def _http_get(url: str, *, encoding: str | None = None) -> bytes:
    resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    if encoding:
        resp.encoding = encoding
    return resp.content


# ══════════════════════════════════════════════════════════════════════
# Phase 1.2 — 合约与基础信息
# ══════════════════════════════════════════════════════════════════════


class CFFEXDeliverableBondFetcher(Fetcher):
    """Fetch all deliverable bonds + CF from CFFEX's public CSV API.

    Source: http://www.cffex.com.cn/sj/jgsj/jgqsj/index_6882.csv
    This CSV has NO header row; columns are:
        bond_name, bond_code_ib, bond_code_sse, bond_code_szse,
        maturity_date, coupon_rate, cf, contract_id, product
    """

    name = "cffex_deliverable_bonds"
    URL = "http://www.cffex.com.cn/sj/jgsj/jgqsj/index_6882.csv"
    COLUMNS = [
        "bond_name",
        "bond_code_ib",
        "bond_code_sse",
        "bond_code_szse",
        "maturity_date",
        "coupon_rate",
        "cf",
        "contract_id",
        "product",
    ]

    def fetch(self) -> pd.DataFrame:
        content = _http_get(self.URL)
        df = pd.read_csv(
            BytesIO(content), encoding="utf-8", header=None, names=self.COLUMNS
        )
        # Strip whitespace from string columns
        for col in ["bond_name", "bond_code_ib", "contract_id", "product"]:
            df[col] = df[col].astype(str).str.strip()
        # Normalise bond_code_sse/szse to strings (may have leading zeros)
        for col in ["bond_code_sse", "bond_code_szse"]:
            df[col] = df[col].astype(str).str.strip()
        # Ensure numeric types
        df["coupon_rate"] = pd.to_numeric(df["coupon_rate"], errors="coerce")
        df["cf"] = pd.to_numeric(df["cf"], errors="coerce")
        # Parse maturity_date from YYYYMMDD to YYYY-MM-DD
        df["maturity_date"] = pd.to_datetime(
            df["maturity_date"].astype(str), format="%Y%m%d", errors="coerce"
        ).dt.strftime("%Y-%m-%d")
        logger.info(
            f"Fetched {len(df)} deliverable bond rows covering "
            f"{df['contract_id'].nunique()} contracts"
        )
        return df


class CFFEXContractInfoFetcher(Fetcher):
    """Derive contract metadata from the deliverable bond CSV + AKShare.

    The contract list comes from the deliverable bond data (which lists
    all currently-live contracts). Listing/last-trade dates come from
    ``akshare.futures_comm_info`` when available.
    """

    name = "cffex_contract_info"

    def fetch(self) -> pd.DataFrame:
        # Step 1: Get contract list from deliverable bonds
        bond_fetcher = CFFEXDeliverableBondFetcher()
        bonds_df = bond_fetcher.fetch()
        contracts = (
            bonds_df[["contract_id", "product"]]
            .drop_duplicates()
            .sort_values("contract_id")
            .reset_index(drop=True)
        )

        # Step 2: Try to enrich with listing/last-trade dates from AKShare
        try:
            import akshare as ak

            comm_df = ak.futures_comm_info(symbol="中金所")
            # Filter to treasury futures
            mask = comm_df["合约代码"].str.strip().isin(contracts["contract_id"])
            if mask.any():
                extra = comm_df.loc[mask, ["合约代码"]].copy()
                extra = extra.rename(columns={"合约代码": "contract_id"})
                extra["contract_id"] = extra["contract_id"].str.strip()
                contracts = contracts.merge(extra, on="contract_id", how="left")
        except Exception as exc:
            logger.warning(f"Could not enrich from futures_comm_info: {exc}")

        # Step 3: Try to get listing/last-trade from futures_contract_info_cffex
        try:
            import akshare as ak

            info_df = ak.futures_contract_info_cffex()
            # Filter to our contracts
            info_df["合约代码"] = info_df["合约代码"].str.strip()
            mask = info_df["合约代码"].isin(contracts["contract_id"])
            if mask.any():
                extra = info_df.loc[mask, ["合约代码", "上市日", "最后交易日"]].copy()
                extra = extra.rename(
                    columns={
                        "合约代码": "contract_id",
                        "上市日": "listing_date",
                        "最后交易日": "last_trade_date",
                    }
                )
                contracts = contracts.merge(extra, on="contract_id", how="left")
        except Exception as exc:
            logger.warning(f"Could not enrich from futures_contract_info_cffex: {exc}")

        # Ensure all columns exist
        for col in ["listing_date", "last_trade_date"]:
            if col not in contracts.columns:
                contracts[col] = None

        logger.info(f"Built contract info for {len(contracts)} contracts")
        return contracts[["contract_id", "product", "listing_date", "last_trade_date"]]


# ══════════════════════════════════════════════════════════════════════
# Phase 1.3 — 行情数据 (stubs, to be filled in later)
# ══════════════════════════════════════════════════════════════════════


class FuturesDailyFetcher(Fetcher):
    """Fetch daily OHLCV for treasury futures from CFFEX via AKShare."""

    name = "futures_daily"

    def __init__(self, date: str | None = None) -> None:
        """date: YYYYMMDD string. If None, uses latest trading day."""
        self.date = date

    def fetch(self) -> pd.DataFrame:
        import akshare as ak

        date_str = self.date
        if date_str is None:
            from .calendar import latest_trading_day

            date_str = latest_trading_day().strftime("%Y%m%d")

        df = ak.futures_hist_daily_cffex(date=date_str)
        # Filter to treasury futures only
        mask = df["variety"].isin(["TS", "TF", "T", "TL"])
        df = df.loc[mask].copy()
        # Standardise column names
        df = df.rename(
            columns={
                "symbol": "contract_id",
                "date": "trade_date",
                "variety": "product",
            }
        )
        logger.info(f"Fetched {len(df)} futures daily rows for {date_str}")
        return df
