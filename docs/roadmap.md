# 分阶段开发计划

> 版本：v0.3（2026-04-28）
> 关键约束：**仅使用开源免费数据，日频分析**
> 每个 Phase 结束后做一次回顾与文档更新。

> **快速读法**：
> - 想看每日开发指引 → `docs/STATUS.md`
> - 想看产品愿景与策略矩阵 → `docs/design.md`
> - 本文档：阶段进度与待办清单（checkbox 实时维护）

## Phase 0 — 文档与脚手架 ✅ done

- [x] 创建项目目录与 git 仓库（GitHub: Allanli1011/cffex-tbf-arb，public）
- [x] 撰写 design.md / strategies.md / roadmap.md
- [x] 初始化 configs（合约元数据、阈值参数）
- [x] .gitignore、首个 commit
- [x] **Python 环境策略**：直接使用系统 Python（`/usr/bin/python3`，3.9.6），不创建 venv
- [ ] 仅 Phase 2 启动前补装 QuantLib（暂未启用，纯 Python DCF 已足够）

> **环境约定**：本项目不使用 venv / conda。`requirements.txt` 仅作清单参考，不强制运行。

## Phase 1 — 数据层（开源免费）✅ done（v1）

### 1.1 基础设施 ✅
- [x] 数据存储：SQLite + Parquet 双层布局
- [x] 通用 ETL 框架（`src/data/base.py`）
- [x] loguru + retry 装饰器
- [x] 交易日历缓存

### 1.2 合约与基础信息 ✅（cron 待设）
- [x] CFFEX 公开 CSV API（`/sj/jgsj/jgqsj/index_6882.csv`）发现并接入
- [x] `populate_contracts.py` 全量同步
- [x] `refresh_cf.py` 公告页增量
- [x] CF append-only + 冲突检测，幂等性验证通过
- [x] Wayback 历史回填，覆盖 T1803–TS2612 共 944 条
- [x] 可交割券池维护（与 CF 同源）
- [ ] **季度自动运行（cron / launchd）配置**（代码已就绪）

### 1.3 行情数据 ✅（v1 单券估值）
- [x] 期货日线（OHLCV + settle + OI + turnover）
- [x] 期货持仓排名（flatten 为 long）
- [x] 中债收益率曲线（关键期限）
- [x] **国债现券估值 v1**：Sina 交易所收盘价 → `yield_from_price` 解 YTM，写
      `parquet/bond_valuation/`，4359 行 / 147 天，覆盖 50% basis_signals 行（TL 68%）
- [x] `compute_basis_signals` 已优先用单券 YTM，缺则 fallback par 曲线（`ytm_source` 列记录）
- **TL 偏差从 -490bp → -129bp（74% 修复）**；残余 -129bp 受限于交易所稀疏 + 交易所/银行间价差
- [ ] **现券估值 v2（可选）**：接 CCDC 付费估值或 chinabond.com.cn 公开扫描，闭合最后 -129bp

### 1.4 资金面数据 ✅
- [x] CFETS FR/FDR 全 6 序列（`repo_rate_hist`）
- [x] **GC001 / GC007 / GC014** 全部 254 天完整 — 切到 sina 路径（``bond_zh_hs_daily(symbol='sh204XXX')``），sina 历史可回溯到 2016-11
- [x] Shibor 全期限 O/N..1Y
- [x] 17 利率序列覆盖（vs 之前 15）

### 1.5 数据校验 ✅
- [x] `audit.py` 9 类检查
- [x] `data_audit.py` Markdown / JSON 报告
- [x] 当前基线：20 ok / 3 warning / 0 error（warning 全为预期）

## Phase 2 — 定价与信号引擎 ✅ done

- [x] 应计利息计算（ACT/ACT；ACT/365 用于 par 曲线对齐）
- [x] CF 自验证脚本：944 行 vs 中金所公告，92.9% 在 5bp 以内（个别交割池中途加入券除外）
- [x] **Gross / Net Basis / IRR**（`src/pricing/irr.py`，`compute_basis_signals.py` 全量回填）
- [x] CTD 识别（max IRR per (date, contract)）
- [x] **CTD 切换概率**（`src/pricing/ctd_probability.py`，蒙特卡洛 + ±25/50/100bp 情景表，
      1036 行 / 144 天）
- [x] 跨期价差 + 60d/120d Z-score（`src/pricing/spreads.py`，3000 行 / 250 天）
- [x] 期货隐含 yield + 修正久期 DV01（`src/pricing/bond_pricing.py`）
- [x] 蝶式 / 陡平价差 + DV01 中性权重（`src/pricing/curve_trades.py`）

**信号样本（2026-04-24，Phase 1.3 v1 修正后）**：
- T2606 IRR 1.71% vs FDR007 1.31% → +40bp 正向基差
- TL2606 IRR 3.31% → +200bp 正向基差（修前为负）
- 2-5-10 fly = -0.34bp（z60=+1.56，belly 略贵）

## Phase 3 — 回测框架 ✅ done

- [x] 单标的事件驱动回测引擎（mean-reversion + directional carry，
      `src/backtest/engine.py`）
- [x] 基差套利回测（`basis_long_carry_T`：87.5% hit, Sharpe +2.77）
- [x] 跨期套利回测（`calendar_mr_T_near_far`：50% hit, Sharpe +0.59）
- [x] **蝶式 / 陡平回测**（4 个 curve mean-reversion 策略，最佳 `steepener_2s10s` Sharpe +2.11）
- [x] 绩效指标：Sharpe（年化×√252）/ MaxDD / 胜率 / 平均持仓天数（`src/backtest/metrics.py`）
- [x] CLI：`scripts/run_backtest.py --strategy <name>`，写 trades + nav parquet + SQLite
      `backtest_runs` 表
- [x] **参数扫描与敏感度分析**：`scripts/backtest_grid.py` 跑 entry × exit × hold 网格，710 cells × 6 策略；面板 Backtest tab 加 Sharpe 热图

**已跑 6 个 baseline run**（144 天样本，单合约名义 P&L，RMB）：

| run_id | trades | hit | total | Sharpe | max DD |
|---|---|---|---|---|---|
| basis_T_v1 | 8 | 87.5% | +10,352 | **+2.77** | -1,756 |
| calendar_T_v1 | 8 | 50% | +2,050 | +0.59 | -3,450 |
| curve_mr_steepener_2s10s_v1 | 3 | 67% | +6,591 | +2.11 | -1,383 |
| curve_mr_fly_2_5_10_v1 | 3 | 67% | +178 | +0.07 | -3,143 |
| curve_mr_fly_5_10_30_v1 | 2 | 50% | -3,396 | -0.76 | -5,379 |
| curve_mr_steepener_5s30s_v1 | 2 | 0% | -24,394 | -1.66 | -35,535 |

样本仅 144 天，结果示意性。

## Phase 4 — 监控面板 MVP（Streamlit）✅ done

- [x] Streamlit 应用骨架，**5 个 tab**（Overview / Basis / Calendar / Curve / Backtest）
- [x] `app/data_loaders.py`：缓存 parquet/SQLite 读取（TTL 300s）
- [x] 模块 A 总览驾驶舱（当日 4 类信号汇总卡片）
- [x] 模块 B 基差监控（明细表 + IRR 时序 + 净基差时序）
- [x] 模块 C 跨期监控（spread + z60 双轴时序）
- [x] 模块 D 曲线套利（fly/steepener live + 历史 z）
- [x] 模块 F 回测与统计（NAV 曲线 + 交易表）
- [x] 启动命令：`python3 -m streamlit run app/streamlit_app.py`

**为什么用 Streamlit**：日频场景下，写交互式数据应用只需 React + FastAPI 方案 1/3 的工时。

## Phase 5 — 监控面板完整版 2/3 done

- [x] 模块 E **CTD 与交割分析**：`parquet/ctd_switch/` 接入，含切换概率 / 6 档情景表 / 历史时序 / product×contract 热图
- [x] 模块 G **风险与持仓分析**：market $-DV01 热图（OI × 单合约 DV01）+ 各品种 总暴露 metric + top-5 长短头集中度表 + 选合约 top-20 多空机构柱图（Δ 着色）+ 当日最大变化机构
- [ ] 模块 H **信号告警**（邮件 / 钉钉 / 企微 webhook，触发条件可配置）
- [ ] sidebar 全局日期 picker / 产品 picker
- [ ] Backtest tab 升级：参数扫描热图（与 Phase 3 参数扫描功能配合）
- [ ] P&L 拆解视图（gross_basis vs carry，按 leg）

## Phase 6 — 增强（按需）⛔ todo

- [ ] **机器学习信号层**（LightGBM 预测基差均值回归概率）
- [ ] **Regime Detection**（牛陡 / 熊陡 / 牛平 / 熊平四态分类，HMM 或滚动相关）
- [ ] **流动性评分**（成交量 + 收盘价滑点 + bid-ask 估算）
- [ ] **压力测试场景库**（2016 钱荒 / 2020 永煤 / 2022 理财赎回回放）

## Phase 7 — 实盘联调（远期，超出当前免费数据范围）⛔ out of scope

> 当前开源免费方案不支持实盘交易；如需推进实盘，需开通期货账户并接入 CTP。

- [ ] CTP / 柜台 API 接入（仅查询）
- [ ] 模拟单与算法执行器
- [ ] 风控前置（DV01 限额、品种集中度）

## 完成情况速览

```
Phase 0  ████████████████████ done
Phase 1  ███████████████████░ done v1（CCDC v2、GC001/014、cron 待）
Phase 2  ████████████████████ done
Phase 3  ███████████████████░ done（参数扫描待）
Phase 4  ████████████████████ done MVP（5 tabs）
Phase 5  ░░░░░░░░░░░░░░░░░░░░ todo（next up）
Phase 6  ░░░░░░░░░░░░░░░░░░░░ todo（按需）
Phase 7  ░░░░░░░░░░░░░░░░░░░░ out of scope
```

## 当前未完成清单（按优先级）

### 高优先级（建议下次开 prompt 直接做）

1. **Phase 5 模块 E — CTD 与交割分析面板**
   - 数据已有（`parquet/ctd_switch/`）
   - 工时：1–2 小时
   - 价值：直接 surface MC 切换概率 + 情景表
2. **Phase 5 模块 G — 风险与持仓分析**
   - 把 DV01 / 跨品种暴露可视化
   - 工时：2–3 小时
3. **Phase 3 参数扫描**
   - 把现有 6 策略的 z 阈值 + 持仓期做网格回测
   - 写到面板做热图
   - 工时：2 小时

### 中优先级

4. **Phase 5 模块 H — 信号告警 webhook**
   - 邮件最简单，钉钉/企微其次
   - 工时：2–3 小时
5. **Phase 1.4 — GC001/GC014 完整回填**
   - eastmoney 节流问题，可改用 sina 货币市场接口或 cnki
   - 工时：1 小时
6. **Phase 1.2 — CF 季度 cron / launchd**
   - macOS LaunchAgent plist
   - 工时：30 分钟

### 低优先级（非阻塞）

7. **Phase 1.3 v2 — CCDC 付费估值或 chinabond.com.cn 扫描**
   - 闭合最后 -129bp TL 残差
   - 工时：1 天（含调研）；或 ¥XXk/年付费 CCDC 接入
8. **Phase 6 — ML / regime / 流动性 / 压力测试**
   - 持续迭代
   - 工时：每项 1–3 天

## 阶段工时（已花费 vs 预估对照）

| Phase | 预估（人日） | 实际花费 | 备注 |
|-------|----|----|----|
| 0 | 1 | ~0.5 | 跳过 venv 节省时间 |
| 1 | 6–10 | ~5 | CFFEX CSV 直接出 944 行省了大量爬虫工时 |
| 2 | 8–12 | ~3 | QuantLib 没装，纯 Python DCF 够用 |
| 3 | 5–8 | ~1 | 日频回测确实简单 |
| 4 | 4–6 | ~0.5 | Streamlit + plotly 极快 |
| 5 | 6–10 | — | 待做 |
| 6 | — | — | 按需 |

实际工时远低于预估，主要是因为：(1) CFFEX CSV API 直接给出 CF + 票息 + 到期，省去 isin/akshare 来回；(2) 不用 QuantLib；(3) Streamlit 极简；(4) 单券估值用 sina + akshare 已有接口包装。
