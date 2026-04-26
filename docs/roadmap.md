# 分阶段开发计划

> 版本：v0.2（2026-04-26）
> 关键约束：**仅使用开源免费数据，日频分析**
> 每个 Phase 结束后做一次回顾与文档更新。

## Phase 0 — 文档与脚手架（当前）

**目标**：完成项目结构、设计文档、依赖管理。

- [x] 创建项目目录与 git 仓库
- [x] 撰写 design.md / strategies.md / roadmap.md
- [x] 初始化 configs（合约元数据、阈值参数）
- [x] 加 .gitignore、首个 commit
- [x] **Python 环境策略**：直接使用系统 Python（`/usr/bin/python3`，3.9.6），不创建 venv。本地已装好 akshare / pandas / numpy / pyarrow / streamlit / plotly / loguru / pyyaml / scipy / pytest 等核心依赖
- [ ] 仅 Phase 2 启动前补装 QuantLib（`pip3 install QuantLib`）

**交付物**：可被新人 clone 后理解整体目标的代码库骨架。

> **环境约定**：本项目不使用 venv / conda。所有 `pip install` 操作直接装在系统 Python 上。`requirements.txt` 仅作清单参考，不强制运行。

## Phase 1 — 数据层（开源免费）

**目标**：基于 AKShare + CFFEX 直爬，建立稳定的日频数据 ETL。

### 1.1 基础设施
- [ ] 数据存储设计：SQLite（结构化）+ Parquet（时序）目录布局
- [ ] 通用 ETL 框架：`src/data/base.py`（抽象 fetcher / saver / 校验器）
- [ ] 日志与错误处理（loguru + 统一 retry 装饰器）
- [ ] 交易日历模块（`akshare.tool_trade_date_hist_sina`）

### 1.2 合约与基础信息
- [ ] 合约元数据表（contract_id, product, listing_date, last_trade_date）
- [ ] **CF 表 append-only 维护**
  - [ ] `scripts/refresh_cf.py`：爬中金所公告页，解析挂牌通知
  - [ ] 冲突检测：已存在的 (合约, 券) 对若新值不同则报错而非覆盖
  - [ ] 季度自动运行（cron）+ 公告增量手动触发模式
  - [ ] 首次全量回填当前在挂合约的 CF 表
- [ ] 可交割券池维护（与 CF 同步更新）

### 1.3 行情数据
- [ ] 期货日线接入（`akshare.futures_zh_daily_sina`，覆盖 TS/TF/T/TL 全部活跃合约）
- [ ] 期货持仓排名（`akshare.get_cffex_rank_table`）
- [ ] 中债收益率曲线（`akshare.bond_china_yield`，关键期限 1Y/3Y/5Y/7Y/10Y/30Y）
- [ ] 国债现券估值（中债估值，按可交割券代码批量拉）

### 1.4 资金面数据
- [ ] DR007 / R007（`akshare.repo_rate_hist`）
- [ ] GC007（交易所回购日频）
- [ ] Shibor 全期限（`akshare.macro_china_shibor_all`）
- [ ] FR007（中国货币网）

### 1.5 数据校验
- [ ] 每个 ETL 输出 sanity check 报告（缺失日、异常值、价格 0、CF 越界）
- [ ] 失败重试 + 告警（邮件或日志）

**交付物**：
- 一键运行 `scripts/run_daily_etl.py` 完成所有日频数据更新
- 历史回填脚本可拉取过去 3 年的数据（约几十万行）
- 数据完整性 dashboard（简单的 markdown 报告即可）

## Phase 2 — 定价与信号引擎

**目标**：基差、IRR、CTD、跨期价差、曲线指标的离线计算。

- [ ] 应计利息计算（ACT/ACT，对齐中债估值口径）
- [ ] CF 自验证脚本（用公式重算并与中金所公告值对比，误差 < 1e-4）
- [ ] Gross Basis / Net Basis / IRR 计算器
- [ ] CTD 识别（每日基于 IRR 最大）+ 切换概率（蒙特卡洛或情景法）
- [ ] 跨期价差计算 + 历史分位数（rolling 窗口）
- [ ] 期货隐含 yield 与 DV01（修正久期近似 → QuantLib 完整定价）
- [ ] 蝶式 / 陡平价差计算（DV01 中性权重）

**交付物**：
- 能对历史数据批量生成全量信号时序的引擎
- 单元测试覆盖关键公式（输入/输出与已知样例对齐）
- 一份样例报告（notebook）展示某主力合约的 IRR 时序

## Phase 3 — 回测框架

**目标**：策略历史表现评估，验证信号是否真有 alpha。

- [ ] 简单事件驱动回测引擎（日频，处理保证金、滑点、手续费、移仓）
- [ ] 基差套利回测（注意国内反向套利限制）
- [ ] 跨期套利回测
- [ ] 蝶式 / 陡平回测
- [ ] 绩效指标：PnL / Sharpe / MaxDD / 胜率 / 单次收益分布
- [ ] 参数扫描与敏感度分析（阈值、持仓周期）

**交付物**：每个策略的回测报告（notebook + 图表 + Markdown 总结）。

## Phase 4 — 监控面板 MVP（Streamlit）

**目标**：日终批处理后展示当日信号与关键监控图。

- [ ] Streamlit 应用骨架 + 多页路由
- [ ] 模块 A 总览驾驶舱（4 品种主力 + 信号 Top10）
- [ ] 模块 B 基差监控（明细表 + IRR 时序图）
- [ ] 日终批处理脚本：拉数据 → 计算信号 → 生成 dashboard 用的中间数据
- [ ] 本机部署 + 浏览器访问验证

**为什么用 Streamlit**：日频场景下，写一个交互式数据应用只需 React + FastAPI 方案 1/3 的工时，且无需前后端联调。后续如需更复杂交互再升级。

**交付物**：本机运行 `streamlit run src/dashboard/app.py` 可访问的可视化面板，覆盖最关键的两个模块。

## Phase 5 — 监控面板完整版

- [ ] 模块 C 跨期监控
- [ ] 模块 D 曲线套利监控
- [ ] 模块 E CTD 与交割分析
- [ ] 模块 F 回测与统计（嵌入 Phase 3 结果）
- [ ] 模块 G 风险与持仓分析
- [ ] 模块 H 信号告警（邮件 / 钉钉 / 企微 webhook）

## Phase 6 — 增强（按需）

- [ ] 机器学习信号层（LightGBM 预测基差均值回归概率）
- [ ] Regime Detection（牛陡 / 熊陡 / 牛平 / 熊平四态分类）
- [ ] 流动性评分（基于成交量 + 收盘价滑点）
- [ ] 压力测试场景库（2016 钱荒 / 2020 永煤 / 2022 理财赎回回放）

## Phase 7 — 实盘联调（远期，超出当前免费数据范围）

> 当前开源免费方案不支持实盘交易；如需推进实盘，需开通期货账户并接入 CTP。

- [ ] CTP / 柜台 API 接入（仅查询）
- [ ] 模拟单与算法执行器
- [ ] 风控前置（DV01 限额、品种集中度）

## 优先级建议

资源有限时按以下优先级推进：

1. **Phase 0 → Phase 1 → Phase 2** 是地基，必须完成
2. **CF 表 append-only 机制** 是 Phase 1 的关键护栏，先做对再做多
3. **基差监控**（Phase 4 模块 B）价值密度最高，应作为 Phase 4 的第一个面板
4. **回测框架（Phase 3）** 与定价引擎（Phase 2）并行开发，互相验证

## 阶段工时预估（基于开源免费方案）

| Phase | 预估工时（人日） | 说明 |
|-------|------------------|------|
| 0 | 1 | 已基本完成 |
| 1 | 6–10 | CF 爬虫与首次回填占大头 |
| 2 | 8–12 | QuantLib 集成需要时间调试 |
| 3 | 5–8 | 日频回测较简单 |
| 4 | 4–6 | Streamlit 比 React 快很多 |
| 5 | 6–10 | 模块化扩展 |
| 6 | 持续迭代 | — |
| 7 | 视实盘需求 | 不在免费范围 |

> 相比商业数据方案，Phase 4 工时大幅缩减（8–10 → 4–6 人日），因为不再做 React + WebSocket。
