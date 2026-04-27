# CFFEX Treasury Bond Futures (TBF) Arbitrage - Codebase Analysis Report

## 1. Project Overview & Purpose

Based on a deep architectural and structural analysis, this project is a specialized **Quantitative Research & Data Infrastructure Platform for Chinese Treasury Bond Futures (TBF) Arbitrage**. 

Its primary objective is to systematically and autonomously identify pricing inefficiencies in the CFFEX (China Financial Futures Exchange) market, specifically targeting two core strategies:
*   **Basis Arbitrage (期现套利):** Exploiting mispricing between the futures contract and the deliverable cash bonds (Cash and Carry).
*   **Calendar Spread Arbitrage (跨期套利):** Capturing abnormal price spreads between different delivery months (e.g., M1 vs M2) of the same futures product.

The system aims to be a production-ready, zero-maintenance data pipeline (ETL) and pricing engine that operates independently of expensive commercial data terminals (like Wind), relying entirely on robust open-source data (AKShare) and direct exchange scraping (CFFEX).

## 2. Financial Modeling & Theoretical Alignment

The internal logic and metrics calculated within this codebase are **highly accurate and strictly adhere to classical Fixed Income, Currencies, and Commodities (FICC) theory**, as well as the specific clearing rules of the Chinese interbank bond market and CFFEX.

*   **Conversion Factor (CF) Calculation (`src/pricing/cf_calculator.py`):** Flawlessly implements the official CFFEX discounting formula (3% notional rate, annual coupon assumption, 30/360 day-count convention).
*   **Accrued Interest & Bond Pricing (`src/pricing/accrued.py`):** Correctly utilizes the **ACT/ACT** day-count convention for accrued interest, matching CCDC (China Central Depository & Clearing) valuation standards.
*   **Core Arbitrage Metrics (`src/pricing/irr.py`):** Calculates Gross Basis, Carry, Net Basis, and Implied Repo Rate (IRR) with textbook precision. It handles "dirty price" to "clean price" conversions and annualizes the IRR accurately to compare against money market repo rates (like DR007).
*   **Risk Metrics (`src/pricing/bond_pricing.py`):** Accurately computes Modified Duration and calculates per-contract DV01 (Dollar Value of 1 Basis Point) based on the specific face values of TS, TF, T, and TL contracts.

*Note on advanced modeling:* The only significant FICC component missing is an Option-Adjusted Spread (OAS) model (e.g., binomial trees) to price the short position's delivery options (Quality/Timing options). The current model relies on static IRR vs. Repo Rate comparisons and historical Z-scores for trading signals.

## 3. Codebase Quality Assessment: 85 / 100 (Excellent / Industrial Prototype)

This project exhibits the engineering maturity of a senior quantitative developer. It features excellent domain-driven design, immutable data structures for financial math, and a robust ETL pipeline.

**Strengths (+35 Points):**
*   **Immutability & Data Integrity:** Uses `dataclass(frozen=True)` extensively in pricing to prevent state mutation. The `conversion_factors` table is strictly Append-Only with conflict detection, preventing silent historical data corruption.
*   **Production-Grade ETL Architecture:** Implements a clean Fetcher -> Validator -> Saver pattern (`src/data/base.py`) with centralized logging and orchestrator state tracking (`etl_runs` table).
*   **Smart Storage Engine:** Brilliantly mixes SQLite for relational metadata/transactions and Parquet for high-performance, append-friendly time-series data without the overhead of a heavy database like PostgreSQL.
*   **Defensive Programming & Testing:** Features robust `@retry` decorators for network calls, a comprehensive data quality auditing suite (`src/data/audit.py`), and a solid test suite (107 passing tests) using modern Python typing.

**Weaknesses & Deductions (-15 Points):**
*   **Critical Parsing Logic Flaws (-6):** (See Bug #1 and #4 below). Severe precision errors in coupon parsing and fragile table extraction that can crash the ETL pipeline.
*   **State Management & Cache Leaks (-4):** (See Bug #2 below). Improper use of `lru_cache` on trading calendars which will break long-running daemon processes or dashboards.
*   **Missing Advanced Pricing Models (-3):** Lack of OAS modeling for delivery options.
*   **Broad Exception Handling (-2):** Instances of catching generic `Exception` (e.g., in SQLite operations), which can mask specific database locks or deadlocks.

## 4. Critical Bugs & Vulnerabilities Identified

During the deep dive, 4 critical issues were identified that require immediate remediation before deploying to production.

### Bug 1: Catastrophic Precision Loss for Low-Coupon Bonds
*   **Location:** `src/data/cffex_scraper.py` -> `_pct_to_float()`
*   **Description:** The function incorrectly uses `v > 1` to determine if a string represents a percentage. If a bond's coupon rate is exactly `1.00%` or less (e.g., `0.50`), the function returns `0.5` instead of `0.005`. This massive theoretical interest rate will completely corrupt downstream CF calculations and pricing curves.

### Bug 2: Cache Leak in Trading Calendar for Daemon Processes
*   **Location:** `src/data/calendar.py` -> `_trading_dates_set()`
*   **Description:** The function is decorated with `@lru_cache(maxsize=1)`. In a long-running process (like a Streamlit dashboard or background scraper), this cache is never explicitly cleared when the underlying Parquet file updates. As a result, the system will fail to recognize new trading days as time progresses, leading to missing data and calculation errors.

### Bug 3: Logical Error in Data Audit Lookback Window
*   **Location:** `src/data/audit.py` -> `check_trading_day_gaps()`
*   **Description:** The audit is intended to check for missing data over the last N *trading days*. However, the logic `(today - d).days <= lookback_days` calculates the difference in *calendar days*. A 60-day lookback will only cover ~42 trading days, significantly shrinking the intended audit coverage window.

### Bug 4: Fragile Bulk Table Extraction Leads to Unhandled Crashes
*   **Location:** `src/data/cffex_scraper.py` -> `_extract_rows_from_html_table()`
*   **Description:** When parsing CFFEX bulk announcements, if the HTML table is unexpectedly missing the "contract" column, the code blindly falls back to assigning an empty string `""` to `contract_id`. When this flawed `CFRow` is yielded, `CFRow.validate()` immediately throws an unhandled `ValueError`, crashing the entire bulk scraping job instead of logging a warning and continuing or skipping the malformed row.
