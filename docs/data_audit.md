# Data Audit Report

_Generated: 2026-04-27T10:47:47_

**Summary**: 16 ok, 3 warning, 0 error

| | Check | Message |
|--|--|--|
| ✅ | `sqlite.contracts` | 104 rows, span T1803..TS2612 |
| ✅ | `sqlite.bonds` | 198 rows, span 050004..260008 |
| ✅ | `sqlite.conversion_factors` | 944 rows, span T1803..TS2612 |
| ✅ | `sqlite.signals` | empty |
| ✅ | `sqlite.etl_runs` | empty |
| ✅ | `parquet.futures_daily` | 250 files, 2025-04-14..2026-04-24 |
| ✅ | `parquet.bond_yield_curve` | 257 files, 2025-04-14..2026-04-24 |
| ⚠️ | `parquet.bond_valuation` | no files |
| ✅ | `parquet.repo_rate` | 258 files, 2025-04-14..2026-04-27 |
| ✅ | `parquet.futures_oi_rank` | 250 files, 2025-04-14..2026-04-24 |
| ✅ | `consistency.cf_bond_fk` | all CF rows have a matching bond |
| ✅ | `consistency.cf_contract_fk` | all CF rows have a matching contract |
| ✅ | `completeness.bonds_coupon` | all bonds have coupon_rate |
| ✅ | `completeness.bonds_maturity` | all bonds have maturity_date |
| ✅ | `range.cf_bounds` | all CFs in [0.5, 1.5] |
| ✅ | `range.futures_price` | all futures closes in plausible range |
| ✅ | `range.yield_curve` | all curve points in plausible range |
| ⚠️ | `gaps.futures_daily` | 1 missing days in last 60 trading days |
| ⚠️ | `gaps.bond_yield_curve` | 1 missing days in last 60 trading days |

## Details
### `parquet.bond_valuation` — warning
- Message: no files
### `gaps.futures_daily` — warning
- Message: 1 missing days in last 60 trading days
- Detail: `{'missing': ['2026-04-27'], 'expected_count': 42, 'actual_count': 41}`
### `gaps.bond_yield_curve` — warning
- Message: 1 missing days in last 60 trading days
- Detail: `{'missing': ['2026-04-27'], 'expected_count': 42, 'actual_count': 41}`
