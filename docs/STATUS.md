# Project Status

> Last updated: 2026-04-30 (Phase 1.3 v1 + 2.3 + 3 + 4 done; coupon
> frequency fix applied across pricing engine). Read this first when
> resuming work in a new session — it captures everything needed to
> pick up without re-reading the conversation history.

## Constraint

Open-source data only (AKShare + CFFEX direct scrape + Wayback). Daily
frequency. Use system Python 3.9.6 directly, no venv.

## Phase progress

| Phase | Status |
|------|------|
| 0 — 脚手架 / 文档 | ✅ done |
| 1.1 — 基础设施 (storage / utils / ETL base / calendar) | ✅ done |
| 1.2 — 合约 / CF / bonds master | ✅ done; 944 historical CFs (T1803..TS2612) |
| 1.3 — 行情 (futures / OI rank / yield curve / 单券估值 v1) | ✅ done; Sina 交易所收盘 → 解 YTM；TL 偏差 -490→-129bp |
| 1.4 — 资金面 (CFETS / GC / Shibor, 15 系列) | ✅ done |
| 1.5 — 数据校验 (audit + report) | ✅ done; baseline 16 ok / 3 warning / 0 error |
| 2.1 — CF 公式 + 应计利息 | ✅ done; max diff vs official 47bp (1 outlier) |
| 2.2 — IRR / 基差 / 净基差 | ✅ done; 8988 signals × 144 days |
| 2.3 — CTD 切换概率 | ✅ done; 蒙特卡洛 + 6 档情景表 |
| 2.4 — 跨期价差 + Z-score | ✅ done; 3000 spread rows × 250 days |
| 2.5 — 期货隐含 yield + DV01 | ✅ done; matches industry typical |
| 2.6 — 蝶式 / 陡平 (DV01 中性) | ✅ done; 576 curve_signals × 144 days |
| 3 — 回测框架 | ✅ done; 6 策略（basis / calendar / 4 curve）|
| 4 — Streamlit MVP 面板 | ✅ done; 5 tabs（Overview/Basis/Calendar/Curve/Backtest）|
| 5 — 完整面板 (8 模块) | ⛔ **next up** |
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
  fetchers.py       — CFFEX CSV / 期货日线 / OI rank / 收益率曲线 / 3 套资金面 / Sina 单券收盘
  audit.py          — 9 类数据质量检查
  utils.py          — loguru + retry decorator

src/pricing/
  cf_calculator.py  — 官方 CFFEX CF 公式（年付息 + 30/360 月差）
  accrued.py        — ACT/ACT 应计利息
  bond_pricing.py   — DCF 定价 / YTM 反求 / 久期 / 凸性 / **futures DV01**
  irr.py            — BasisQuote: gross/net basis, carry, IRR, vs repo bp
  spreads.py        — 跨期价差（near_mid / mid_far / near_far）+ rolling Z
  curve_trades.py   — DV01 中性权重 / 50/50 蝶式 / fly_yield_bp + steepener_bp
  ctd_probability.py — 平行移位线性 MC + ±25/50/100bp 情景表

src/backtest/
  engine.py         — 单策略事件循环（mean-reversion + directional carry）
  strategies.py     — 6 策略：calendar / basis / 4 curve mean-reversion
  metrics.py        — Sharpe / max DD / hit rate / hit count

app/
  data_loaders.py   — Streamlit cached parquet/sqlite readers
  streamlit_app.py  — 5 tab MVP 面板（plotly + dataframe）

scripts/
  populate_contracts.py   — CFFEX 全量 CF（--snapshot 归档原始 CSV）
  refresh_cf.py           — CFFEX 公告增量
  fetch_historical_cf.py  — Wayback 历史快照
  backfill_market_data.py — futures + OI + curve + 资金面
  data_audit.py           — Markdown / JSON 审计报告
  verify_cf_formula.py    — CF 公式 vs 944 行官方对比
  compute_basis_signals.py — 日终 IRR + DV01 + CTD 信号
  compute_calendar_spreads.py — 跨期价差 + Z-score
  compute_curve_signals.py    — 蝶式 / 陡平 + 60d Z（DV01 中性比例）
  compute_ctd_switch.py       — CTD 切换概率（MC，per-(date, contract)）
  backfill_bond_valuation.py — Sina 交易所收盘 → 单券 YTM 求解，写 bond_valuation
  run_backtest.py            — CLI 跑策略，写 trades + nav parquet + SQLite 指标

tests/
  test_infra.py / test_cf_table.py / test_fetchers.py /
  test_market_fetchers.py / test_audit.py / test_pricing.py /
  test_backtest.py
  共 126/126 通过（offline）+ 7 联网用例
```

## 数据库现状（2026-04-28）

| 表 / 数据集 | 行数 | 时间跨度 |
|---|---|---|
| `contracts` (SQLite) | 104 | T1803..TS2612 |
| `bonds` (SQLite) | 198 | 2015..2026 |
| `conversion_factors` (SQLite) | 944 | 8 年 |
| `futures_daily` parquet | 3000 / 250 天 | 2025-04-14..2026-04-24 |
| `futures_oi_rank` parquet | 41730 | 同上 |
| `bond_yield_curve` parquet | 2056 / 257 天 | 同上 |
| `bond_valuation` parquet | 4359 / 147 天 | Sina 交易所收盘 → 单券 YTM |
| `repo_rate` parquet | 3850 / 258 天 | 15 利率序列 |
| `basis_signals` parquet | 11079 / 144 天 | IRR + DV01 |
| `calendar_spreads` parquet | 3000 / 250 天 | Z-score 含 |
| `curve_signals` parquet | 576 / 144 天 | 4 结构 × 144 天，含 60d Z |
| `backtest_runs` parquet | 6 runs (26 trades + 720 nav rows) | calendar / basis / 4 curve |
| `backtest_runs` (SQLite) | 6 行 | params + metrics JSON |
| `ctd_switch` parquet | 1036 / 144 天 | MC switch prob + 6 档情景 |

## 关键设计决策（已确定，不要再讨论）

1. **CF 表 append-only** — `(contract, bond)` 一旦写入永不修改；冲突即报错而非覆盖。
2. **Wayback 2024-08-16 快照已锁住 5+ 年历史** — 缺口仅 T2506-T2603 等 16 合约，留待用公式补。
3. **单券估值（v1）已接入** — Phase 1.3 完成。Sina 交易所日收盘 → ``yield_from_price`` 解 YTM，写 ``bond_valuation`` parquet；``compute_basis_signals`` 优先用单券 YTM，缺数据则 fallback par 曲线。覆盖：50% 行 (5587/11079)，TL 68% / TS 48% / T 46% / TF 30%。**TL bias 从 -490bp → -129bp（74% 修复）**；剩余 -129bp 来自老券交易所稀疏 + 交易所/银行间价差。CCDC 官方付息估值仍需付费接入。
4. **CF 公式精度** — **94.4% 在 5bp 以内**（2026-04-30 加入半年付息后从 92.9% 提升）；公式实现正确；个别 outlier 是中途加入交割池的特殊券（T1809/180020 47bp），不调公式。
9. **半年付息（coupon_frequency）已建模** — Chinese gov bonds: 1y/3y/5y/7y → annual；10y/30y/50y → semi-annual。`bonds.coupon_frequency` 列 + `compute_cf` / `price_from_yield` / `compute_basis` 全链路支持 f=2。198 个债按"原始期限 ≥10y → f=2"自动派生（122 annual + 76 semi-annual）。
5. **GC001/GC014 历史回填留缺** — 本机 eastmoney 代理拦截，GC007 已完整。
6. **CFETS 接口按月切片** — `repo_rate_hist` 跨月偶发返回单行，已分月拉取。
7. **CCDC 收益率曲线 < 1 年限制** — `_process_yield_curve` 自动 330 天分段。
8. **eastmoney 节流** — 多 GC 代码连续拉触发限流，已加 3s inter-symbol 延迟。

## 已知信号样本（2026-04-24，Phase 1.3 修正后）

- T2606 CTD = 230004，IRR=1.71%，vs FDR007=1.31% → +40bp 正向基差信号
- TL2606 CTD = 220008（旧 210014），IRR=3.31% → **+200bp 正向基差**（修前为 -300bp）
- T2609-T2612 价差 z60=-2.33（4% 分位） → 跨期信号
- T 系列 DV01 ≈ 697 RMB/bp/合约；TL ≈ 1999；TF ≈ 466；TS ≈ 402
- 2-5-10 fly = -0.34bp（z60=+1.56），belly 略贵
- 5-10-30 fly = -44.6bp（z60=-0.54），belly (T) 偏贵但仍在区间
- 2s10s 陡度 = 37.8bp（z60=-0.23），近中性
- 5s30s 陡度 = 82.7bp（z60=-0.38），略平于均值

## 已知回测结果（v1，144 天样本）

| run_id | trades | hit | total P&L (RMB) | Sharpe | max DD |
|---|---|---|---|---|---|
| `calendar_T_v1` (T near_far z>2 反转) | 8 | 50% | +2,050 | +0.59 | -3,450 |
| `basis_T_v1` (irr-fdr007>30bp) | 8 | 87.5% | +10,352 | **+2.77** | -1,756 |
| `curve_mr_fly_2_5_10_v1` | 3 | 67% | +178 | +0.07 | -3,143 |
| `curve_mr_fly_5_10_30_v1` | 2 | 50% | -3,396 | -0.76 | -5,379 |
| `curve_mr_steepener_2s10s_v1` | 3 | 67% | +6,591 | **+2.11** | -1,383 |
| `curve_mr_steepener_5s30s_v1` | 2 | 0% | -24,394 | -1.66 | -35,535 |

注：单合约名义 P&L（curve 策略 P&L 单位 ≈ 1 个 belly/long-tenor 合约的 DV01 × 价差变化）；样本仅 144 天，5s30s 跌惨实属过窄样本，需更长历史。

## CTD 切换概率（2026-04-24，5bp/d 假设）

| 合约 | 锚 CTD | 距交割 | 横轴 vol bp | 切换概率 | top alt |
|---|---|---|---|---|---|
| T2606 | 250025 | 49 | 35 | 2.0% | 260007 |
| T2612 | 230018 | 231 | 76 | 19.8% | 260005 |
| TF2612 | 260003 | 231 | 76 | 9.6% | 260008 |
| TL2606 | 210014 | 49 | 35 | 31.9% | 260002 |
| TL2612 | 260002 | 231 | 76 | 66.7% | 220008 |
| TS2606 | 250024 | 49 | 35 | 0.0% | — |

均值 / 中位（全 144 天）：
- T 11.6% / 11.5%
- TF 8.4% / 8.3%
- TL **54.1% / 57.7%**（长端最不稳定）
- TS 3.1% / 2.1%

**注**：MC 锚点为 *min-gross-basis*；basis_signals 的 ``is_ctd`` 是 max-IRR。
**40.8%** 的 (date, contract) 行两套定义不一致（``ctd_anchor_disagrees``），说明 carry 差异对 CTD 排序影响实质性。

## 下一步：见 `docs/BACKLOG.md`

`BACKLOG.md` 是单一信源，按 P0/P1/P2 优先级排序所有未完成任务。最高优先级（P0）：

1. **B1** Phase 5 模块 E — CTD 与交割分析面板（数据已有，1–2 h）
2. **B2** Phase 5 模块 G — 风险与持仓分析（2–3 h）
3. **B3** Phase 3 — 参数扫描与敏感度分析（2 h）

完整阶段视图（含历史）：`docs/roadmap.md`。

## 常用命令

```bash
# 日终 ETL（cron 推荐 16:30+ 跑）
python3 scripts/populate_contracts.py --snapshot
python3 scripts/backfill_market_data.py
python3 scripts/backfill_bond_valuation.py        # Phase 1.3 单券估值
python3 scripts/compute_basis_signals.py
python3 scripts/compute_calendar_spreads.py
python3 scripts/compute_curve_signals.py
python3 scripts/compute_ctd_switch.py            # MC 1000 paths / vol 5bp/d

# 回测（共 6 个 registered strategy；--strategy ? 可枚举）
python3 scripts/run_backtest.py --strategy calendar_mr_T_near_far
python3 scripts/run_backtest.py --strategy basis_long_carry_T
python3 scripts/run_backtest.py --strategy curve_mr_fly_5_10_30
python3 scripts/run_backtest.py --strategy curve_mr_steepener_2s10s

# 启动 Streamlit MVP 面板（5 个 tab）
python3 -m streamlit run app/streamlit_app.py

# 健康检查
python3 scripts/data_audit.py -o docs/data_audit.md
python3 scripts/verify_cf_formula.py

# 测试
python3 -m pytest tests/ -m "not network"
```
