"""Tests for CFFEX daily / OI rank fetchers."""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

from src.data.fetchers import (
    CFFEX_DAILY_COLUMNS,
    OI_RANK_COLUMNS,
    fetch_cffex_daily,
    fetch_cffex_oi_rank,
)


# ---- Daily fetcher with mocked AKShare ----------------------------------


@pytest.fixture
def fake_cffex_raw():
    return pd.DataFrame({
        "symbol": ["T2606", "TF2606", "TS2606", "TL2606", "IF2606", "IO2606-C-3700"],
        "date": ["20260424"] * 6,
        "open": [108.78, 106.27, 102.59, 113.90, 4000.0, 25.0],
        "high": [108.86, 106.32, 102.63, 114.07, 4010.0, 26.0],
        "low": [108.71, 106.24, 102.59, 113.27, 3990.0, 24.5],
        "close": [108.72, 106.26, 102.59, 113.28, 4005.0, 25.5],
        "volume": [84358, 73216, 50856, 129258, 50000, 100],
        "open_interest": [348481, 211209, 90844, 155506, 200000, 50],
        "turnover": [1e7, 7e6, 1e7, 1.4e7, 1e8, 250.0],
        "settle": [108.735, 106.250, 102.598, 113.390, 4002.0, 25.4],
        "pre_settle": [108.765, 106.245, 102.588, 113.850, 4001.0, 25.0],
        "variety": ["T", "TF", "TS", "TL", "IF", "IO"],
    })


def test_fetch_cffex_daily_filters_to_tbf(fake_cffex_raw):
    with patch("akshare.get_cffex_daily", return_value=fake_cffex_raw):
        df = fetch_cffex_daily("20260424")
    # Only TS/TF/T/TL kept; option (IO2606-C-3700) and stock index (IF) dropped
    assert set(df["contract_id"]) == {"T2606", "TF2606", "TS2606", "TL2606"}
    assert list(df.columns) == CFFEX_DAILY_COLUMNS
    assert df["date"].iloc[0] == "2026-04-24"
    assert df["settle"].iloc[0] == pytest.approx(108.735)


def test_fetch_cffex_daily_handles_empty():
    with patch("akshare.get_cffex_daily", return_value=pd.DataFrame()):
        df = fetch_cffex_daily("20260424")
    assert df.empty
    assert list(df.columns) == CFFEX_DAILY_COLUMNS


def test_fetch_cffex_daily_accepts_date_object():
    import datetime as dt
    with patch("akshare.get_cffex_daily", return_value=pd.DataFrame()):
        df = fetch_cffex_daily(dt.date(2026, 4, 24))
    assert df.empty


# ---- OI rank fetcher with mocked AKShare --------------------------------


@pytest.fixture
def fake_oi_raw():
    cols = [
        "long_open_interest", "long_open_interest_chg", "long_party_name",
        "rank", "short_open_interest", "short_open_interest_chg",
        "short_party_name", "symbol", "vol", "vol_chg", "vol_party_name",
        "variety",
    ]
    t2606 = pd.DataFrame([
        [67791, -895, "中信(代客)", 1, 48825, 414, "东证(代客)",
         "T2606", 40447, 4527, "中信(代客)", "T"],
        [49517, 234, "国泰君安(代客)", 2, 46771, 166, "中信(代客)",
         "T2606", 22920, 3017, "国泰君安(代客)", "T"],
    ], columns=cols)
    return {"T2606": t2606, "T2609": t2606.head(0)}  # T2609 empty


def test_fetch_cffex_oi_rank_flattens(fake_oi_raw):
    with patch("akshare.get_cffex_rank_table", return_value=fake_oi_raw):
        df = fetch_cffex_oi_rank("20260424")
    assert len(df) == 2  # T2609 was empty
    assert all(c in df.columns for c in
               ["date", "contract_id", "product", "rank"])
    assert df["date"].iloc[0] == "2026-04-24"
    assert df["product"].iloc[0] == "T"


def test_fetch_cffex_oi_rank_handles_empty():
    with patch("akshare.get_cffex_rank_table", return_value={}):
        df = fetch_cffex_oi_rank("20260424")
    assert df.empty
    assert list(df.columns) == OI_RANK_COLUMNS


# ---- Live integration ---------------------------------------------------


@pytest.mark.network
def test_fetch_cffex_daily_live():
    df = fetch_cffex_daily("20260424")
    assert len(df) == 12  # all 12 currently-listed TBF contracts
    assert {"TS", "TF", "T", "TL"} == set(df["product"])
    assert df["settle"].notna().all()


@pytest.mark.network
def test_fetch_cffex_oi_rank_live():
    df = fetch_cffex_oi_rank("20260424")
    # Each TBF contract has up to 21 rows (top-20 + total). Some less-active
    # contracts may have 0; just assert at least the active ones populate.
    assert len(df) > 0
    assert "T2606" in set(df["contract_id"])


# ---- Yield curve fetcher ------------------------------------------------


def test_fetch_treasury_yield_curve_long_format():
    from src.data.fetchers import (
        TREASURY_CURVE_NAME,
        YIELD_CURVE_TENORS_CN_TO_YEARS,
        fetch_treasury_yield_curve,
    )

    fake = pd.DataFrame({
        "曲线名称": [TREASURY_CURVE_NAME, "中债AAA", TREASURY_CURVE_NAME],
        "日期": ["2026-04-23", "2026-04-23", "2026-04-24"],
        "3月": [1.10, 1.40, 1.11],
        "6月": [1.12, 1.42, 1.12],
        "1年": [1.14, 1.48, 1.14],
        "3年": [1.27, 1.58, 1.28],
        "5年": [1.47, 1.67, 1.48],
        "7年": [1.62, 1.89, 1.63],
        "10年": [1.75, 2.03, 1.76],
        "30年": [2.24, 2.46, 2.25],
    })
    with patch("akshare.bond_china_yield", return_value=fake):
        df = fetch_treasury_yield_curve("20260423", "20260424")

    # Two sovereign rows × 8 tenors = 16 long-format rows
    assert len(df) == 16
    assert set(df["date"]) == {"2026-04-23", "2026-04-24"}
    assert set(df["tenor_years"]) == set(YIELD_CURVE_TENORS_CN_TO_YEARS.values())
    assert df["curve"].unique().tolist() == ["treasury"]


@pytest.mark.network
def test_fetch_treasury_yield_curve_live():
    from src.data.fetchers import fetch_treasury_yield_curve
    df = fetch_treasury_yield_curve("20260420", "20260424")
    assert len(df) > 0
    assert {1.0, 5.0, 10.0}.issubset(df["tenor_years"])


# ---- Funding rates ------------------------------------------------------


def test_fetch_cfets_repo_fixings_long_format():
    from src.data.fetchers import fetch_cfets_repo_fixings, REPO_RATE_COLUMNS

    fake = pd.DataFrame({
        "date": ["2026-04-23", "2026-04-24"],
        "FR001": [1.30, 1.30],
        "FR007": [1.40, 1.38],
        "FR014": [1.42, 1.40],
        "FDR001": [1.22, 1.22],
        "FDR007": [1.32, 1.31],
        "FDR014": [1.35, 1.35],
    })
    with patch("akshare.repo_rate_hist", return_value=fake):
        df = fetch_cfets_repo_fixings("20260423", "20260424")

    # 2 dates × 6 fixings = 12 long-format rows
    assert len(df) == 12
    assert list(df.columns) == REPO_RATE_COLUMNS
    assert set(df["rate_name"]) == {
        "FR001", "FR007", "FR014", "FDR001", "FDR007", "FDR014"
    }


def test_fetch_exchange_repo_filters_columns():
    from src.data.fetchers import fetch_exchange_repo

    fake = pd.DataFrame({
        "日期": ["2026-04-23", "2026-04-24"],
        "开盘": [1.40, 1.40],
        "收盘": [1.39, 1.38],
        "最高": [1.43, 1.40],
        "最低": [1.21, 1.36],
        "成交量": [1000, 2000],
        "成交额": [1e8, 2e8],
    })
    with patch("akshare.bond_buy_back_hist_em", return_value=fake):
        df = fetch_exchange_repo(symbol="GC007")

    assert len(df) == 2
    assert (df["rate_name"] == "GC007").all()
    assert df["value_pct"].iloc[0] == pytest.approx(1.39)


def test_fetch_exchange_repo_unknown_symbol():
    from src.data.fetchers import fetch_exchange_repo
    with pytest.raises(ValueError):
        fetch_exchange_repo(symbol="GC0099")


def test_fetch_shibor_long_format():
    from src.data.fetchers import fetch_shibor

    fake = pd.DataFrame({
        "日期": ["2026-04-23", "2026-04-24"],
        "O/N-定价": [1.219, 1.219],
        "O/N-涨跌幅": [0.0, 0.0],
        "1W-定价": [1.329, 1.324],
        "1W-涨跌幅": [0.2, -0.5],
        "1Y-定价": [1.4825, 1.478],
        "1Y-涨跌幅": [-0.4, -0.45],
    })
    with patch("akshare.macro_china_shibor_all", return_value=fake):
        df = fetch_shibor()

    assert {"Shibor_ON", "Shibor_1W", "Shibor_1Y"} == set(df["rate_name"])
    assert df.dropna().shape[0] == 6


@pytest.mark.network
def test_fetch_funding_rates_live():
    from src.data.fetchers import (
        fetch_cfets_repo_fixings,
        fetch_exchange_repo,
        fetch_shibor,
    )
    cfets = fetch_cfets_repo_fixings("20260420", "20260424")
    assert {"FR007", "FDR007"}.issubset(set(cfets["rate_name"]))

    gc = fetch_exchange_repo("GC007")
    assert len(gc) > 1000

    shibor = fetch_shibor()
    assert {"Shibor_ON", "Shibor_1W"}.issubset(set(shibor["rate_name"]))
