# Data Audit Report

_Generated: 2026-04-27T10:32:09_

**Summary**: 16 ok, 3 warning, 0 error

| | Check | Message |
|--|--|--|
| ✅ | `sqlite.contracts` | 104 rows, span T1803..TS2612 |
| ✅ | `sqlite.bonds` | 198 rows, span 050004..260008 |
| ✅ | `sqlite.conversion_factors` | 944 rows, span T1803..TS2612 |
| ✅ | `sqlite.signals` | empty |
| ✅ | `sqlite.etl_runs` | 1 rows, span 2026-01-02..2026-01-02 |
| ✅ | `parquet.futures_daily` | 4 files, 2026-04-21..2026-04-24 |
| ✅ | `parquet.bond_yield_curve` | 4 files, 2026-04-21..2026-04-24 |
| ⚠️ | `parquet.bond_valuation` | no files |
| ✅ | `parquet.repo_rate` | 5 files, 2026-04-21..2026-04-27 |
| ✅ | `parquet.futures_oi_rank` | 4 files, 2026-04-21..2026-04-24 |
| ✅ | `consistency.cf_bond_fk` | all CF rows have a matching bond |
| ✅ | `consistency.cf_contract_fk` | all CF rows have a matching contract |
| ✅ | `completeness.bonds_coupon` | all bonds have coupon_rate |
| ✅ | `completeness.bonds_maturity` | all bonds have maturity_date |
| ✅ | `range.cf_bounds` | all CFs in [0.5, 1.5] |
| ✅ | `range.futures_price` | all futures closes in plausible range |
| ✅ | `range.yield_curve` | all curve points in plausible range |
| ⚠️ | `gaps.futures_daily` | 38 missing days in last 60 trading days |
| ⚠️ | `gaps.bond_yield_curve` | 38 missing days in last 60 trading days |

## Details
### `parquet.bond_valuation` — warning
- Message: no files
### `gaps.futures_daily` — warning
- Message: 38 missing days in last 60 trading days
- Detail: `{'missing': ['2026-02-26', '2026-02-27', '2026-03-02', '2026-03-03', '2026-03-04', '2026-03-05', '2026-03-06', '2026-03-09', '2026-03-10', '2026-03-11', '2026-03-12', '2026-03-13', '2026-03-16', '2026-03-17', '2026-03-18', '2026-03-19', '2026-03-20', '2026-03-23', '2026-03-24', '2026-03-25', '2026-03-26', '2026-03-27', '2026-03-30', '2026-03-31', '2026-04-01', '2026-04-02', '2026-04-03', '2026-04-07', '2026-04-08', '2026-04-09', '2026-04-10', '2026-04-13', '2026-04-14', '2026-04-15', '2026-04-16', '2026-04-17', '2026-04-20', '2026-04-27'], 'expected_count': 42, 'actual_count': 4}`
### `gaps.bond_yield_curve` — warning
- Message: 38 missing days in last 60 trading days
- Detail: `{'missing': ['2026-02-26', '2026-02-27', '2026-03-02', '2026-03-03', '2026-03-04', '2026-03-05', '2026-03-06', '2026-03-09', '2026-03-10', '2026-03-11', '2026-03-12', '2026-03-13', '2026-03-16', '2026-03-17', '2026-03-18', '2026-03-19', '2026-03-20', '2026-03-23', '2026-03-24', '2026-03-25', '2026-03-26', '2026-03-27', '2026-03-30', '2026-03-31', '2026-04-01', '2026-04-02', '2026-04-03', '2026-04-07', '2026-04-08', '2026-04-09', '2026-04-10', '2026-04-13', '2026-04-14', '2026-04-15', '2026-04-16', '2026-04-17', '2026-04-20', '2026-04-27'], 'expected_count': 42, 'actual_count': 4}`
