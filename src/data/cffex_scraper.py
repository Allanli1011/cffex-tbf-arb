"""CFFEX announcement scraper for conversion factors.

Two announcement formats are recognised:

1. **Incremental** — title contains "增加X年期国债期货合约可交割国债的通知".
   Body text follows a stable pattern::

       "...符合 TF2606、TF2609 和 TF2612 合约的可交割国债条件，
        转换因子分别为 0.9334、0.9366 和 0.9398。"

   These are parsed deterministically with regex. The bond name appears
   earlier in the prose ("2026年记账式附息（八期）国债") but the bond ISIN
   code is rarely present — operators may need to fill it in later.

2. **Bulk listing** — title contains "发布国债期货合约可交割国债" (announced
   alongside new contract listings each quarter). Body usually carries an
   HTML table or a linked PDF. Best-effort table parsing is attempted; if
   it fails the announcement URL is returned for manual handling.

Listing page paginates via JavaScript; only the front page (~10 latest
announcements) is reliably scrapeable without a browser. Operators should
seed historical CFs from CSV (configs/cf_table.csv) and use this scraper
for incremental updates going forward.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterator

import requests
from bs4 import BeautifulSoup
from loguru import logger

from .cf_table import CFRow
from .utils import retry

CFFEX_BASE = "http://www.cffex.com.cn"
ANNOUNCEMENT_INDEX = f"{CFFEX_BASE}/jystz/"
DEFAULT_HEADERS = {"User-Agent": "Mozilla/5.0 (cffex-tbf-arb research)"}
HTTP_TIMEOUT = 15

# Title heuristics
TITLE_INCR_RE = re.compile(r"关于增加.*年期国债期货.*可交割国债")
TITLE_BULK_RE = re.compile(r"发布.*国债期货.*可交割国债")
TITLE_NEW_LISTING_RE = re.compile(r"国债期货.*新合约上市")

# Body parsing — incremental announcement
CONTRACT_CODE_RE = re.compile(r"(?:TS|TF|T|TL)\d{4}")
# Match "转换因子分别为 0.9334、0.9366 和 0.9398" or "转换因子为 0.9334".
# Stop greedily at the first non-CF character (Chinese period 。, full stop,
# or a Chinese word character).
CF_LIST_RE = re.compile(
    r"转换因子(?:分别)?为\s*([0-9.、和\s,，]+)"
)
BOND_NAME_RE = re.compile(r"(\d{4}年记账式附息（[一二三四五六七八九十百零〇0-9]+期）国债)")
# Issue title shorthand
ISSUE_DOC_RE = re.compile(r"中金所发[〔\[][\d]{4}[〕\]]\d+号")


@dataclass
class AnnouncementRef:
    url: str
    title: str
    publish_date: str  # YYYY-MM-DD


@dataclass
class ParsedIncremental:
    bond_name: str
    contracts: list[str]
    cfs: list[float]
    source_url: str
    announce_date: str
    raw_text: str

    def to_cf_rows(self, bond_code: str | None = None) -> list[CFRow]:
        """Convert to CFRow list. ``bond_code`` should be supplied by caller
        (e.g. interbank ISIN) since announcements rarely include it."""
        if len(self.contracts) != len(self.cfs):
            raise ValueError(
                f"Contract/CF count mismatch: "
                f"{len(self.contracts)} contracts vs {len(self.cfs)} CFs"
            )
        # Fall back to bond_name as a soft key if no real code is supplied;
        # downstream consumers can rewrite this once mapped.
        code = bond_code or f"NAME::{self.bond_name}"
        return [
            CFRow(
                contract_id=c,
                bond_code=code,
                bond_name=self.bond_name,
                cf=cf,
                announce_date=self.announce_date,
                source_url=self.source_url,
            )
            for c, cf in zip(self.contracts, self.cfs)
        ]


# ---------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------


@retry(max_attempts=3, initial_wait=2.0)
def _get(url: str) -> str:
    resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    resp.encoding = "utf-8"
    return resp.text


def _absolute(href: str) -> str:
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return f"{CFFEX_BASE}{href}"
    return f"{CFFEX_BASE}/{href}"


def _extract_date_from_url(url: str) -> str | None:
    m = re.search(r"/(\d{8})/", url)
    if not m:
        return None
    s = m.group(1)
    return f"{s[:4]}-{s[4:6]}-{s[6:]}"


# ---------------------------------------------------------------------
# Listing page
# ---------------------------------------------------------------------


def list_announcements(html: str | None = None) -> list[AnnouncementRef]:
    """Return announcements visible on the front index page."""
    html = html if html is not None else _get(ANNOUNCEMENT_INDEX)
    soup = BeautifulSoup(html, "lxml")
    refs: list[AnnouncementRef] = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        title = a.get_text(strip=True)
        if "/jystz/" not in href or href.rstrip("/") == "/jystz":
            continue
        if not title:
            continue
        url = _absolute(href)
        if url in seen:
            continue
        seen.add(url)
        refs.append(
            AnnouncementRef(
                url=url,
                title=title,
                publish_date=_extract_date_from_url(url) or "",
            )
        )
    return refs


def filter_cf_announcements(refs: list[AnnouncementRef]) -> list[AnnouncementRef]:
    return [
        r
        for r in refs
        if TITLE_INCR_RE.search(r.title) or TITLE_BULK_RE.search(r.title)
    ]


# ---------------------------------------------------------------------
# Detail page parsing
# ---------------------------------------------------------------------


def fetch_detail_text(url: str) -> str:
    """Return the cleaned plain-text body of an announcement."""
    html = _get(url)
    soup = BeautifulSoup(html, "lxml")
    # Strip nav / script
    for tag in soup(["script", "style", "nav", "header", "footer"]):
        tag.decompose()
    body = soup.find("body")
    text = body.get_text("\n", strip=True) if body else ""
    text = re.sub(r"\n\s*\n", "\n", text)
    return text


def parse_incremental(ref: AnnouncementRef, text: str) -> ParsedIncremental | None:
    """Parse an "增加可交割国债" announcement.

    Returns None if the text doesn't match the expected pattern.
    """
    if not TITLE_INCR_RE.search(ref.title):
        return None

    # Restrict to the body paragraph that mentions 转换因子, since the
    # announcement text otherwise contains nav / template noise.
    cf_match = CF_LIST_RE.search(text)
    if not cf_match:
        logger.warning(f"No 转换因子 phrase found in {ref.url}")
        return None
    cf_str = cf_match.group(1)
    cfs = [float(x) for x in re.findall(r"\d+\.\d+", cf_str)]
    if not cfs:
        return None

    # Take the *immediately preceding* sentence to find contracts and bond name.
    # That sentence carries phrases like "符合TF2606、TF2609和TF2612合约的可交割..."
    # We search a window of text preceding the CF phrase.
    pre_window = text[: cf_match.start()][-400:]
    contracts = list(dict.fromkeys(CONTRACT_CODE_RE.findall(pre_window)))
    if len(contracts) != len(cfs):
        # Look across the whole body as fallback
        contracts = list(dict.fromkeys(CONTRACT_CODE_RE.findall(text)))

    bond_match = BOND_NAME_RE.search(text)
    bond_name = bond_match.group(1) if bond_match else "未识别"

    if len(contracts) != len(cfs):
        logger.warning(
            f"Contract/CF count mismatch in {ref.url}: "
            f"{len(contracts)} contracts vs {len(cfs)} cfs"
        )
        return None

    return ParsedIncremental(
        bond_name=bond_name,
        contracts=contracts,
        cfs=cfs,
        source_url=ref.url,
        announce_date=ref.publish_date,
        raw_text=text,
    )


def parse_bulk_table(ref: AnnouncementRef, html: str) -> list[CFRow]:
    """Best-effort parser for "发布可交割国债" bulk announcements.

    These announcements may include HTML tables. We look for tables whose
    header mentions 转换因子 and extract bond rows. If the announcement
    instead links to a PDF, returns an empty list and logs a warning so
    the operator can handle it manually.
    """
    soup = BeautifulSoup(html, "lxml")
    rows: list[CFRow] = []
    for table in soup.find_all("table"):
        header_text = table.get_text()
        if "转换因子" not in header_text:
            continue
        rows.extend(_extract_rows_from_html_table(table, ref))
    if not rows:
        logger.warning(
            f"Bulk CF announcement {ref.url} produced no rows; "
            "manual PDF parsing may be required"
        )
    return rows


def _extract_rows_from_html_table(table, ref: AnnouncementRef) -> list[CFRow]:
    """Extract CFRow list from an HTML table.

    Heuristically maps columns based on header text. Robust to column
    re-ordering since CFFEX is not perfectly consistent across years.
    """
    rows = table.find_all("tr")
    if len(rows) < 2:
        return []
    headers = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])]

    col_map: dict[str, int] = {}
    for i, h in enumerate(headers):
        if "代码" in h:
            col_map["bond_code"] = i
        elif "简称" in h or "名称" in h:
            col_map["bond_name"] = i
        elif "票面" in h or "息票" in h:
            col_map["coupon_rate"] = i
        elif "到期" in h:
            col_map["maturity_date"] = i
        elif "转换因子" in h:
            col_map["cf"] = i
        elif "合约" in h:
            col_map["contract_id"] = i

    if "cf" not in col_map or "bond_code" not in col_map:
        return []

    out: list[CFRow] = []
    for tr in rows[1:]:
        cells = [c.get_text(strip=True) for c in tr.find_all(["th", "td"])]
        if not cells:
            continue
        try:
            cf_val = float(cells[col_map["cf"]])
        except (ValueError, IndexError):
            continue
        out.append(
            CFRow(
                contract_id=cells[col_map["contract_id"]] if "contract_id" in col_map else "",
                bond_code=cells[col_map["bond_code"]],
                bond_name=cells[col_map["bond_name"]] if "bond_name" in col_map else None,
                coupon_rate=_pct_to_float(cells[col_map["coupon_rate"]]) if "coupon_rate" in col_map else None,
                maturity_date=cells[col_map["maturity_date"]] if "maturity_date" in col_map else None,
                cf=cf_val,
                announce_date=ref.publish_date,
                source_url=ref.url,
            )
        )
    return out


def _pct_to_float(s: str) -> float | None:
    s = s.strip().replace("%", "")
    try:
        v = float(s)
    except ValueError:
        return None
    return v / 100 if v > 1 else v


# ---------------------------------------------------------------------
# High-level helper
# ---------------------------------------------------------------------


def discover_recent_cf_rows() -> Iterator[CFRow]:
    """Yield CFRows from all CF-related announcements on the front index page.

    Caller is responsible for inserting them via :func:`insert_cfs` (which
    enforces append-only). Bond codes for incremental announcements are
    placeholders unless overridden.
    """
    refs = list_announcements()
    cf_refs = filter_cf_announcements(refs)
    logger.info(f"Found {len(cf_refs)} CF-related announcements on front page")

    for ref in cf_refs:
        try:
            text = fetch_detail_text(ref.url)
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"Skip {ref.url}: fetch failed ({exc})")
            continue

        if TITLE_INCR_RE.search(ref.title):
            parsed = parse_incremental(ref, text)
            if parsed:
                yield from parsed.to_cf_rows()
        elif TITLE_BULK_RE.search(ref.title):
            html = _get(ref.url)
            yield from parse_bulk_table(ref, html)
