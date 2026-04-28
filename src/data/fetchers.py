"""Open-source data fetchers.

Two families:

1. **CFFEX deliverable-bond CSV** (``/sj/jgsj/jgqsj/index_6882.csv``) —
   the source of truth for bond master + CF + deliverable pool. One pull
   covers everything for currently-listed contracts.

2. **AKShare CFFEX daily / OI rank** — daily OHLCV+settle+OI per contract,
   plus top-20 member long/short rankings. Backfillable from 2010-04-16.
"""

from __future__ import annotations

import datetime as dt
import io
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import requests
from loguru import logger

from .bonds import Bond
from .cf_table import CFRow
from .utils import retry

CFFEX_TBF_PRODUCTS = ("TS", "TF", "T", "TL")
CFFEX_CONTRACT_RE = re.compile(r"^(?:TS|TF|TL|T)\d{4}$")

CFFEX_DELIVERABLE_BOND_CSV = (
    "http://www.cffex.com.cn/sj/jgsj/jgqsj/index_6882.csv"
)
HTTP_TIMEOUT = 20
DEFAULT_HEADERS = {"User-Agent": "Mozilla/5.0 (cffex-tbf-arb research)"}

CFFEX_CSV_COLUMNS = [
    "bond_name",
    "interbank_code",
    "sh_code",
    "sz_code",
    "maturity_yyyymmdd",
    "coupon_pct",
    "cf",
    "contract_id",
    "product",
]


@dataclass(frozen=True)
class DeliverablePoolSnapshot:
    """One row of the CFFEX CSV, normalised to project conventions."""

    bond: Bond
    cf_row: CFRow
    contract_id: str
    product: str  # TS / TF / T / TL


@retry(max_attempts=3, initial_wait=2.0)
def _download_deliverable_csv(url: str = CFFEX_DELIVERABLE_BOND_CSV,
                              timeout: int = HTTP_TIMEOUT) -> bytes:
    """Download the deliverable-bond CSV from CFFEX or any mirror.

    Wayback Machine snapshots use the same URL prefixed with
    ``https://web.archive.org/web/<timestamp>/``; the response body is the
    original CSV so this function is mirror-agnostic.
    """
    resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp.content


def parse_deliverable_csv(raw: bytes, source_url: str = CFFEX_DELIVERABLE_BOND_CSV
                          ) -> list[DeliverablePoolSnapshot]:
    """Parse the CFFEX deliverable-bond CSV bytes into normalised snapshots.

    Robust to Wayback Machine and similar mirrors: any line that does not
    parse to a valid TBF ``contract_id`` (after stripping) is silently
    skipped, so HTML wrappers / toolbars cause no harm.
    """
    text = raw.decode("utf-8", errors="replace")
    # Pre-filter: keep only lines that have at least 8 commas (CSV body rows)
    csv_lines = [ln for ln in text.splitlines() if ln.count(",") >= 8]
    if not csv_lines:
        return []
    df = pd.read_csv(
        io.StringIO("\n".join(csv_lines)),
        header=None,
        names=CFFEX_CSV_COLUMNS,
        dtype=str,
        keep_default_na=False,
        on_bad_lines="skip",
    )

    snapshots: list[DeliverablePoolSnapshot] = []
    skipped = 0
    for r in df.itertuples(index=False):
        contract_id = r.contract_id.strip()
        if not CFFEX_CONTRACT_RE.match(contract_id):
            skipped += 1
            continue
        bond_code = r.interbank_code.strip()
        if not bond_code:
            logger.warning(f"Skipping row with empty interbank_code: {r}")
            continue

        bond = Bond(
            bond_code=bond_code,
            bond_name=r.bond_name.strip(),
            sh_code=_clean(r.sh_code),
            sz_code=_clean(r.sz_code),
            coupon_rate=_pct_to_decimal(r.coupon_pct),
            maturity_date=_yyyymmdd_to_date(r.maturity_yyyymmdd),
        )
        cf_row = CFRow(
            contract_id=contract_id,
            bond_code=bond_code,
            bond_name=bond.bond_name,
            coupon_rate=bond.coupon_rate,
            maturity_date=bond.maturity_date,
            cf=float(r.cf),
            announce_date=None,
            source_url=source_url,
        )
        snapshots.append(
            DeliverablePoolSnapshot(
                bond=bond,
                cf_row=cf_row,
                contract_id=contract_id,
                product=r.product.strip(),
            )
        )
    if skipped:
        logger.debug(f"Parser skipped {skipped} non-TBF rows from {source_url}")
    return snapshots


def fetch_deliverable_pool(url: str = CFFEX_DELIVERABLE_BOND_CSV,
                           snapshot_path: Path | None = None,
                           ) -> list[DeliverablePoolSnapshot]:
    """Pull and parse a deliverable-bond CSV.

    Defaults to CFFEX live; pass an alternative URL (e.g. a Wayback Machine
    archive) to load historical snapshots.

    Parameters
    ----------
    url:
        Source URL.
    snapshot_path:
        If given, the raw CSV bytes are written to this path before parsing.
        The file is never overwritten — if it already exists the snapshot
        step is skipped (existing archive wins).
    """
    raw = _download_deliverable_csv(url)
    if snapshot_path is not None:
        snapshot_path = Path(snapshot_path)
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        if snapshot_path.exists():
            logger.info(f"Snapshot already exists at {snapshot_path}; not overwriting")
        else:
            snapshot_path.write_bytes(raw)
            logger.info(f"Snapshot archived to {snapshot_path} ({len(raw)} bytes)")
    snaps = parse_deliverable_csv(raw, source_url=url)
    logger.info(
        f"{url}: {len(snaps)} (contract, bond) snapshots "
        f"covering {len({s.contract_id for s in snaps})} contracts"
    )
    return snaps


# ---- helpers -------------------------------------------------------------


def _clean(v: str | None) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _pct_to_decimal(s: str) -> float | None:
    """CFFEX writes coupon as percent: '2.35' -> 0.0235."""
    s = (s or "").strip()
    if not s:
        return None
    try:
        return float(s) / 100
    except ValueError:
        return None


def _yyyymmdd_to_date(s: str) -> str | None:
    s = (s or "").strip()
    if len(s) != 8 or not s.isdigit():
        return None
    return f"{s[:4]}-{s[4:6]}-{s[6:]}"


# ============================================================================
# CFFEX daily futures (OHLCV + settle + OI) via AKShare
# ============================================================================


CFFEX_DAILY_COLUMNS = [
    "date",
    "contract_id",
    "product",
    "open",
    "high",
    "low",
    "close",
    "settle",
    "pre_settle",
    "volume",
    "open_interest",
    "turnover",
]


@retry(max_attempts=3, initial_wait=2.0)
def fetch_cffex_daily(date: str | dt.date) -> pd.DataFrame:
    """Fetch CFFEX daily trading data for a single trading day.

    Returns a tidy DataFrame restricted to TBF contracts only.

    Parameters
    ----------
    date:
        Trading day. Either ``YYYYMMDD`` string or ``datetime.date``.
    """
    import akshare as ak

    if isinstance(date, dt.date):
        date_str = date.strftime("%Y%m%d")
    else:
        date_str = str(date).replace("-", "")

    raw = ak.get_cffex_daily(date=date_str)
    if raw is None or raw.empty:
        return pd.DataFrame(columns=CFFEX_DAILY_COLUMNS)

    df = raw[raw["symbol"].astype(str).str.match(CFFEX_CONTRACT_RE)].copy()
    if df.empty:
        return pd.DataFrame(columns=CFFEX_DAILY_COLUMNS)

    df = df.rename(columns={"symbol": "contract_id", "variety": "product"})
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df = df[CFFEX_DAILY_COLUMNS].reset_index(drop=True)

    # Numeric coercion
    for col in ("open", "high", "low", "close", "settle", "pre_settle",
                "volume", "open_interest", "turnover"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# ============================================================================
# CFFEX top-20 OI rank via AKShare
# ============================================================================


OI_RANK_COLUMNS = [
    "date",
    "contract_id",
    "product",
    "rank",
    "vol_party_name",
    "vol",
    "vol_chg",
    "long_party_name",
    "long_open_interest",
    "long_open_interest_chg",
    "short_party_name",
    "short_open_interest",
    "short_open_interest_chg",
]


@retry(max_attempts=3, initial_wait=2.0)
def fetch_cffex_oi_rank(date: str | dt.date) -> pd.DataFrame:
    """Top-20 member volume / long-OI / short-OI rankings for TBF contracts.

    AKShare returns ``dict[contract_id, DataFrame]``; we flatten to one tidy
    long-format table.
    """
    import akshare as ak

    if isinstance(date, dt.date):
        date_str = date.strftime("%Y%m%d")
    else:
        date_str = str(date).replace("-", "")

    data = ak.get_cffex_rank_table(
        date=date_str, vars_list=list(CFFEX_TBF_PRODUCTS)
    )
    if not isinstance(data, dict) or not data:
        return pd.DataFrame(columns=OI_RANK_COLUMNS)

    parts = []
    iso_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    for contract_id, df in data.items():
        if df is None or df.empty:
            continue
        if not CFFEX_CONTRACT_RE.match(str(contract_id)):
            continue
        d = df.copy()
        d["date"] = iso_date
        d["contract_id"] = contract_id
        if "variety" in d.columns:
            d = d.rename(columns={"variety": "product"})
        else:
            d["product"] = re.match(r"^(TS|TF|TL|T)", contract_id).group(1)
        parts.append(d)

    if not parts:
        return pd.DataFrame(columns=OI_RANK_COLUMNS)

    out = pd.concat(parts, ignore_index=True)
    keep = [c for c in OI_RANK_COLUMNS if c in out.columns]
    out = out[keep]
    return out


# ============================================================================
# CCDC China Treasury yield curve via AKShare
# ============================================================================


# CCDC publishes many curve names; we filter to the sovereign curve.
TREASURY_CURVE_NAME = "中债国债收益率曲线"

YIELD_CURVE_TENORS_CN_TO_YEARS = {
    "3月": 0.25,
    "6月": 0.5,
    "1年": 1.0,
    "3年": 3.0,
    "5年": 5.0,
    "7年": 7.0,
    "10年": 10.0,
    "30年": 30.0,
}

YIELD_CURVE_COLUMNS = ["date", "curve", "tenor_years", "yield_pct"]


@retry(max_attempts=3, initial_wait=2.0)
def fetch_treasury_yield_curve(start: str, end: str) -> pd.DataFrame:
    """Fetch CCDC China Treasury yield curve over a date range.

    Returns a long-format frame with one row per (date, tenor).
    Yields are quoted in percent (e.g. ``2.35`` means 2.35%).

    AKShare requires ``end - start < 1 year``; we don't enforce that here
    so callers can split larger ranges themselves.
    """
    import akshare as ak

    start_str = str(start).replace("-", "")
    end_str = str(end).replace("-", "")
    raw = ak.bond_china_yield(start_date=start_str, end_date=end_str)
    if raw is None or raw.empty:
        return pd.DataFrame(columns=YIELD_CURVE_COLUMNS)

    # Keep only the sovereign curve, drop other AAA bank/note curves
    sov = raw[raw["曲线名称"] == TREASURY_CURVE_NAME].copy()
    if sov.empty:
        logger.warning(
            f"No rows for curve {TREASURY_CURVE_NAME!r} in {start_str}..{end_str}"
        )
        return pd.DataFrame(columns=YIELD_CURVE_COLUMNS)

    # Wide -> long
    rows = []
    for _, r in sov.iterrows():
        date = pd.to_datetime(r["日期"]).strftime("%Y-%m-%d")
        for cn_tenor, years in YIELD_CURVE_TENORS_CN_TO_YEARS.items():
            if cn_tenor not in r:
                continue
            v = r[cn_tenor]
            if pd.isna(v):
                continue
            rows.append((date, "treasury", years, float(v)))
    return pd.DataFrame(rows, columns=YIELD_CURVE_COLUMNS)


# ============================================================================
# Funding rates
# ============================================================================
#
# Three sources cover the rates we care about for IRR / financing-cost
# calculations:
#
#   * CFETS daily fixings (R-series and DR-series) — published 11:30 each
#     trading day. FR007 ≈ R007 weighted average; FDR007 ≈ DR007 weighted
#     average for depository institutions only. We use these as the
#     authoritative daily reference. AKShare: ``repo_rate_hist``.
#   * Exchange pledged repo (GC001 / GC007 / GC014) — Shanghai exchange
#     codes 204001 / 204007 / 204014. AKShare:
#     ``bond_buy_back_hist_em(symbol=...)``.
#   * Shibor — published by 全国银行间同业拆借中心. Full term structure
#     O/N..1Y. AKShare: ``macro_china_shibor_all``.

REPO_RATE_COLUMNS = ["date", "rate_name", "value_pct"]

CFETS_FIXING_NAMES = ("FR001", "FR007", "FR014", "FDR001", "FDR007", "FDR014")


@retry(max_attempts=3, initial_wait=2.0)
def fetch_cfets_repo_fixings(start: str, end: str) -> pd.DataFrame:
    """CFETS daily repo fixings (FR/FDR series) over a date range.

    Returns long format: ``[date, rate_name, value_pct]``.
    """
    import akshare as ak

    s = str(start).replace("-", "")
    e = str(end).replace("-", "")
    raw = ak.repo_rate_hist(start_date=s, end_date=e)
    if raw is None or raw.empty:
        return pd.DataFrame(columns=REPO_RATE_COLUMNS)

    df = raw.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    rate_cols = [c for c in CFETS_FIXING_NAMES if c in df.columns]
    long = df.melt(
        id_vars=["date"],
        value_vars=rate_cols,
        var_name="rate_name",
        value_name="value_pct",
    )
    long = long.dropna(subset=["value_pct"]).reset_index(drop=True)
    return long[REPO_RATE_COLUMNS]


GC_SYMBOLS = {"GC001": "204001", "GC007": "204007", "GC014": "204014"}


@retry(max_attempts=3, initial_wait=2.0)
def fetch_exchange_repo(symbol: str = "GC007") -> pd.DataFrame:
    """SSE pledged repo daily history for ``GC001`` / ``GC007`` / ``GC014``.

    Returns long format ``[date, rate_name, value_pct]`` using the
    closing rate as ``value_pct`` (consistent with CFETS fixings).
    The full history (~5000 trading days back to 2006) is returned by
    AKShare in a single call; callers can slice client-side.
    """
    import akshare as ak

    if symbol not in GC_SYMBOLS:
        raise ValueError(
            f"Unknown SSE repo symbol {symbol!r}. Known: {sorted(GC_SYMBOLS)}"
        )
    raw = ak.bond_buy_back_hist_em(symbol=GC_SYMBOLS[symbol])
    if raw is None or raw.empty:
        return pd.DataFrame(columns=REPO_RATE_COLUMNS)

    df = raw.copy()
    df["date"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
    df["rate_name"] = symbol
    df["value_pct"] = pd.to_numeric(df["收盘"], errors="coerce")
    return df[REPO_RATE_COLUMNS].dropna().reset_index(drop=True)


SHIBOR_TENOR_COLUMN_MAP = {
    "O/N-定价": "Shibor_ON",
    "1W-定价": "Shibor_1W",
    "2W-定价": "Shibor_2W",
    "1M-定价": "Shibor_1M",
    "3M-定价": "Shibor_3M",
    "6M-定价": "Shibor_6M",
    "9M-定价": "Shibor_9M",
    "1Y-定价": "Shibor_1Y",
}


@retry(max_attempts=3, initial_wait=2.0)
def fetch_shibor() -> pd.DataFrame:
    """Shibor full term structure (O/N..1Y), full history in one call.

    Returns long format ``[date, rate_name, value_pct]``.
    """
    import akshare as ak

    raw = ak.macro_china_shibor_all()
    if raw is None or raw.empty:
        return pd.DataFrame(columns=REPO_RATE_COLUMNS)

    df = raw.copy()
    df["date"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
    keep_cols = [c for c in SHIBOR_TENOR_COLUMN_MAP if c in df.columns]
    df = df[["date"] + keep_cols].rename(columns=SHIBOR_TENOR_COLUMN_MAP)

    long = df.melt(
        id_vars=["date"], var_name="rate_name", value_name="value_pct"
    )
    long["value_pct"] = pd.to_numeric(long["value_pct"], errors="coerce")
    return long.dropna(subset=["value_pct"]).reset_index(drop=True)[
        REPO_RATE_COLUMNS
    ]


# ---- Exchange-listed bond clean-price history ---------------------------

EXCHANGE_BOND_COLUMNS = ["date", "sh_code", "open", "high", "low", "close", "volume"]


@retry(max_attempts=3, initial_wait=2.0)
def fetch_sina_bond_history(sh_code: str) -> pd.DataFrame:
    """Sina daily OHLCV for an exchange-listed Chinese government bond.

    ``sh_code`` is the SSE code, e.g. ``019697`` for 230004. Sina expects
    the prefix ``sh``; we add it if missing. Coverage is sparse for old
    off-the-run bonds but ~100% for new benchmark issues.

    Returns columns ``[date, sh_code, open, high, low, close, volume]``;
    empty DataFrame when the bond has no exchange trading history.
    """
    import akshare as ak

    if not sh_code:
        return pd.DataFrame(columns=EXCHANGE_BOND_COLUMNS)
    symbol = sh_code if sh_code.startswith("sh") else f"sh{sh_code}"
    try:
        raw = ak.bond_zh_hs_daily(symbol=symbol)
    except KeyError:
        # Sina returns empty payload for invalid/no-history codes,
        # which manifests as a KeyError inside akshare.
        return pd.DataFrame(columns=EXCHANGE_BOND_COLUMNS)
    if raw is None or raw.empty:
        return pd.DataFrame(columns=EXCHANGE_BOND_COLUMNS)

    df = raw.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["sh_code"] = sh_code.lstrip("sh")
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df[EXCHANGE_BOND_COLUMNS].dropna(
        subset=["close"]
    ).reset_index(drop=True)
