# Project Status

> Last updated: 2026-04-27. Read this first when resuming work in a new
> session — it captures everything needed to pick up without re-reading
> the conversation history.

## Constraint

Open-source data only (AKShare + CFFEX direct scrape + Wayback). Daily
frequency. Use system Python 3.9.6 directly, no venv.

## Phase progress

| Phase | Status |
|------|------|
| 0 — 脚手架 / 文档 | ✅ done |
| 1.1 — 基础设施 (storage / utils / ETL base / calendar) | ✅ done |
| 1.2 — 合约 / CF / bonds master | ✅ done; 944 historical CFs (T1803..TS2612) |
| 1.3 — 行情 (futures daily / OI rank / yield curve) | ✅ done; **现券估值推迟** |
| 1.4 — 资金面 (CFETS / GC / Shibor, 15 系列) | ✅ done |
| 1.5 — 数据校验 (audit + report) | ✅ done; baseline 16 ok / 3 warning / 0 error |
| 2.1 — CF 公式 + 应计利息 | ✅ done; max diff vs official 47bp (1 outlier) |
| 2.2 — IRR / 基差 / 净基差 | ✅ done; 8988 signals × 144 days |
| 2.3 — CTD 切换概率 | ⛔ todo |
| 2.4 — 跨期价差 + Z-score | ✅ done; 3000 spread rows × 250 days |
| 2.5 — 期货隐含 yield + DV01 | ✅ done; matches industry typical |
| 2.6 — 蝶式 / 陡平 (DV01 中性) | ⛔ **next up** |
| 3 — 回测框架 | ⛔ todo |
| 4 — Streamlit MVP 面板 | ⛔ todo |
| 5 — 完整面板 (8 模块) | ⛔ todo |
| 6 — ML / regime / 流动性评分 / 压测 | ⛔ todo |

## Code map

```
src/data/
  storage.py        — SQLite schema + Parquet datasets registry
  base.py           — Fetcher / Validator / Saver ETL framework
  calendar.py       — AKShare 交易日历，本地 parquet 缓存
  bonds.py          — bonds master upsert
  cf_table.py       — append-only CF table (CFConflictError)
  cffex_scraper.py  — 公告页爬虫（增量 CF 通知）
  fetchers.py       — CFFEX CSV / 期货日线 / OI rank / 收益率曲线 / 3 套资金面
  audit.py          — 9 类数据质量检查
  utils.py          — loguru + retry decorator

src/pricing/
  cf_calculator.py  — 官方 CFFEX CF 公式（年付息 + 30/360 月差）
  accrued.py        — ACT/ACT 应计利息
  bond_pricing.py   — DCF 定价 / YTM 反求 / 久期 / 凸性 / **futures DV01**
  irr.py            — BasisQuote: gross/net basis, carry, IRR, vs repo bp
  spreads.py        — 跨期价差（near_mid / mid_far / near_far）+ rolling Z

scripts/
  populate_contracts.py   — CFFEX 全量 CF（--snapshot 归档原始 CSV）
  refresh_cf.py           — CFFEX 公告增量
  fetch_historical_cf.py  — Wayback 历史快照
  backfill_market_data.py — futures + OI + curve + 资金面
  data_audit.py           — Markdown / JSON 审计报告
  verify_cf_formula.py    — CF 公式 vs 944 行官方对比
  compute_basis_signals.py — 日终 IRR + DV01 + CTD 信号
  compute_calendar_spreads.py — 跨期价差 + Z-score

tests/
  test_infra.py / test_cf_table.py / test_fetchers.py /
  test_market_fetchers.py / test_audit.py / test_pricing.py
  共 100/100 通过（offline）+ 6 联网用例
```

## 数据库现状（2026-04-27）

| 表 / 数据集 | 行数 | 时间跨度 |
|---|---|---|
| `contracts` (SQLite) | 104 | T1803..TS2612 |
| `bonds` (SQLite) | 198 | 2015..2026 |
| `conversion_factors` (SQLite) | 944 | 8 年 |
| `futures_daily` parquet | 3000 / 250 天 | 2025-04-14..2026-04-24 |
| `futures_oi_rank` parquet | 41730 | 同上 |
| `bond_yield_curve` parquet | 2056 / 257 天 | 同上 |
| `repo_rate` parquet | 3850 / 258 天 | 15 利率序列 |
| `basis_signals` parquet | 11079 / 250 天 | IRR + DV01 |
| `calendar_spreads` parquet | 3000 / 250 天 | Z-score 含 |

## 关键设计决策（已确定，不要再讨论）

1. **CF 表 append-only** — `(contract, bond)` 一旦写入永不修改；冲突即报错而非覆盖。
2. **Wayback 2024-08-16 快照已锁住 5+ 年历史** — 缺口仅 T2506-T2603 等 16 合约，留待用公式补。
3. **现券估值用 par 曲线插值** — 已知偏差：TL 系列 IRR 系统性偏负 ~400bp（CTD 高息老券真实 YTM 高于 par）。Phase 2.6 / Phase 3 不修，等接入 CCDC per-bond 估值再修。
4. **CF 公式精度** — 92.9% 在 5bp 以内，公式实现正确；个别 outlier 是中途加入交割池的特殊券（T1809/180020 47bp），不调公式。
5. **GC001/GC014 历史回填留缺** — 本机 eastmoney 代理拦截，GC007 已完整。
6. **CFETS 接口按月切片** — `repo_rate_hist` 跨月偶发返回单行，已分月拉取。
7. **CCDC 收益率曲线 < 1 年限制** — `_process_yield_curve` 自动 330 天分段。
8. **eastmoney 节流** — 多 GC 代码连续拉触发限流，已加 3s inter-symbol 延迟。

## 已知信号样本（2026-04-24）

- T2606 CTD = 230004，IRR=1.71%，vs FDR007=1.31% → +40bp 正向基差信号
- T2609-T2612 价差 z60=-2.33（4% 分位） → 跨期信号
- T 系列 DV01 ≈ 697 RMB/bp/合约；TL ≈ 1999；TF ≈ 466；TS ≈ 402

## 下一步：Phase 2.6（蝶式 / 陡平价差）

**输入**：已有 DV01 / 隐含 yield / futures_daily

**新增模块**：
- `src/pricing/curve_trades.py`
  - `dv01_neutral_weights(dv01_a, dv01_b)` → 比例 N_b/N_a
  - `butterfly_weights(dv01_short_wing, dv01_belly, dv01_long_wing)` → 三腿权重
  - 支持 chain：`compute_butterfly(date, product_a, product_b, product_c)`
- `scripts/compute_curve_signals.py`
  - 蝶式：2-5-10 (TS+T-2×TF), 5-10-30 (TF+TL-2×T)
  - 陡峭化 / 平坦化：2s10s, 5s30s
  - 输出 spread + DV01 中性比例 + 60d Z

**验收标准**：
- 单元测试 ≥ 5 个
- 全部 250 天历史数据跑完无错误
- 结果落盘 `parquet/curve_signals/`
- 给出最近一日的样例信号（z + 比例）

**预计工时**：1 个 prompt 周期内可完成。

## 后续 Phase 优先顺序（建议）

1. **Phase 2.6** — 蝶式 / 陡平（45 分钟）
2. **Phase 2.3** — CTD 切换概率（蒙特卡洛或情景，1.5 小时）
3. **Phase 4** 跳过完整面板，先做 **Streamlit MVP** 把现有 4 类信号可视化（半天）
4. **Phase 3** 简单事件驱动回测（1 天）
5. CCDC 现券估值接入（修 TL 偏差）

## 常用命令

```bash
# 日终 ETL（cron 推荐 16:30+ 跑）
python3 scripts/populate_contracts.py --snapshot
python3 scripts/backfill_market_data.py
python3 scripts/compute_basis_signals.py
python3 scripts/compute_calendar_spreads.py

# 健康检查
python3 scripts/data_audit.py -o docs/data_audit.md
python3 scripts/verify_cf_formula.py

# 测试
python3 -m pytest tests/ -m "not network"
```
