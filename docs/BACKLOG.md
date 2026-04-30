# Backlog

> 单一信源：本文件维护"待办任务"。完成后划掉并迁移到 `docs/roadmap.md` 历史。
> 更新时间：2026-04-28

阅读顺序：
1. **本文件** — 想知道下一步做什么
2. **docs/STATUS.md** — 想恢复一个旧 session 的上下文
3. **docs/roadmap.md** — 想看完整阶段清单与历史

---

## P0（高优先级，可立即推进）

> **2026-04-30 截止：B1 / B2 / B3 / B5 / B6 / B7 全部清空。**
> 剩余 P1 仅 B4（模块 H 信号告警 webhook，daily 监控紧迫度低）。
> 下一阶段建议重心转向 P2：CCDC v2 估值 / ML 信号 / regime / 流动性 / 压测。


### ~~B1. Phase 5 — 模块 E：CTD 与交割分析面板~~ ✅ 2026-04-30
完成。`render_ctd_delivery()` tab 提供当日切换概率排序表 + 选中合约 6 档情景表
+ 历史时序 + product × contract 热图，并显示 IRR-CTD vs min-basis-CTD 不一致提示。

### ~~B2. Phase 5 — 模块 G：风险与持仓分析~~ ✅ 2026-04-30
完成。`render_risk_positions()` 7th tab：
- 市场 $-DV01 热图（4 product × 12 contract，单位 kCNY/bp，深红越大）
- 各品种总 $-DV01 metric（实测 TL 401 / T 266 / TF 108 / TS 33 mCNY/bp）
- 每合约 top-5 长 / 短头集中度百分比表
- 选合约 → top-20 多空机构柱图，颜色按 Δ OI（红蓝双向）
- 当日 |ΔOI| 最大的 8 个长 / 短头变化

### ~~B3. Phase 3 — 参数扫描与敏感度分析~~ ✅ 2026-04-30
完成。`scripts/backtest_grid.py` 对每个策略跑 5×5×5 entry/exit/hold 网格；710 cells / 6 策略
落盘 `parquet/backtest_grid/` + SQLite `backtest_grid` 表。Backtest tab 加 entry × exit Sharpe
热图 + trade-count 配套图 + 全 cells 排序表。最高 Sharpe（样本内）：fly_2_5_10 +4.55、
steepener_5s30s +4.66；规律性发现：更紧入场 (~1.0σ) + 更短持仓 (10-15d) 显著优于 2σ/20d 基线。

## P1（中优先级）

### B4. Phase 5 — 模块 H：信号告警 webhook
- **场景触发**：
  - basis IRR-FDR007 突破 ±50bp
  - calendar |z60| 突破 ±2.5
  - CTD 切换概率 >50%
- **通道**：邮件（smtplib）/ 钉钉（webhook）/ 企微（webhook）
- **配置**：`configs/alerts.yaml`
- **新增**：`scripts/send_alerts.py`，按日跑
- **预估**：2–3 小时

### ~~B5. Phase 1.4 — GC001 / GC014 完整回填~~ ✅ 2026-04-30
完成。原 eastmoney 路径（``bond_buy_back_hist_em``）通过本机代理彻底失败；
切到 sina 路径（``bond_zh_hs_daily(symbol='sh204XXX')``），3 个 GC 系列均 254 天完整，
sina 历史可回溯到 2016-11。``repo_rate`` 从 15 序列升至 17 序列。

### ~~B6. Phase 1.2 — CF 季度自动运行~~ ✅ 2026-04-30
完成。`scripts/install_launchd.sh` 生成 macOS LaunchAgent plist；
`--install` / `--uninstall` / `--status` / `--print`（默认）四种动作，
默认 dry-run 防误装。触发时间：Mar/Jun/Sep/Dec 1 @ 17:00 local，
跑 `populate_contracts.py --snapshot`，stdout/stderr 写
`data/logs/cf-refresh.{out,err}.log`。`plutil -lint` 通过。

### ~~B7. 现有面板小改进~~ ✅ 2026-04-30
完成。
- Sidebar **全局 As-of 日期 picker**：每 tab 通过 `_resolve_asof()` + `_apply_asof()`
  尊重 picker 选择，支持回放过去任一交易日；"Reset to latest" 按钮即时复位
- Overview 加 **ETL 健康表**：每 parquet dataset 显示 file_count / latest_date /
  days_lag，按 lag 颜色编码（绿 ≤1d / 黄 ≤5d / 红 >5d 或缺失）
- Basis tab 加 **ytm_source 列** + 覆盖率 caption（"X/Y bonds priced via Sina"）

## P2（低优先级 / 长期）

### B8. Phase 1.3 v2 — 闭合 TL 残余 -129bp
- **原因**：交易所收盘对老券稀疏；交易所 vs 银行间存在系统性价差
- **方案**：
  - A. 接 chinabond.com.cn 公开 yield 页面（无需付费但需 JS rendering）
  - B. 付费 CCDC 估值（¥XXk/年）
  - C. 用 230004 (10Y) 收盘 + 老券 spread 模型估算（需要更多观测）
- **预估**：1 天调研 + 1 天实施

### B9. Phase 6 — 机器学习信号层
- LightGBM 预测基差均值回归概率
- 特征：z-score、carry、CTD 距离、OI 变化
- 标签：N 日后 z-score 是否回到 |z|<0.5
- **预估**：3–5 天

### B10. Phase 6 — Regime Detection
- 牛陡 / 熊陡 / 牛平 / 熊平 四态
- HMM 或滚动收益率相关性
- **预估**：2–3 天

### B11. Phase 6 — 流动性评分
- 成交量 + 收盘价滑点 + bid-ask 代理
- 用作策略权重 / 仓位管理输入
- **预估**：2 天

### B12. Phase 6 — 压力测试场景库
- 2016 钱荒、2020 永煤、2022 理财赎回历史回放
- **预估**：3–4 天

### B13. Phase 7 — 实盘联调（远期，超出免费范围）
- CTP / 柜台 API 仅查询
- 模拟单 + 算法执行器
- 风控前置（DV01 限额）
- **依赖**：开通期货账户

## 已完成（历史，搬到这里防止刷屏）

> 完成的任务每周从此处迁移到 `docs/roadmap.md` 的对应 Phase。

- ✅ 2026-04-26 — Phase 0/1/2 主体落地
- ✅ 2026-04-27 — Phase 2.5 期货 DV01
- ✅ 2026-04-28 — Phase 2.6 蝶式 / 陡平
- ✅ 2026-04-28 — Phase 3 回测框架（6 策略）
- ✅ 2026-04-28 — Phase 4 Streamlit MVP（5 tabs）
- ✅ 2026-04-28 — Phase 2.3 CTD 切换概率（MC + 情景）
- ✅ 2026-04-28 — Phase 1.3 v1 单券估值（TL 偏差 -490→-129bp）
- ✅ 2026-04-30 — coupon_frequency 半年付息建模（CF ≤5bp 92.9→94.4%）
- ✅ 2026-04-30 — B1 Phase 5 模块 E CTD & 交割分析面板
- ✅ 2026-04-30 — B3 Phase 3 参数扫描（5×5×5 网格 / 710 cells / 6 策略）
- ✅ 2026-04-30 — B2 Phase 5 模块 G 风险与持仓分析面板
- ✅ 2026-04-30 — B5 Phase 1.4 GC001/014 完整回填（sina 路径替代 eastmoney）
- ✅ 2026-04-30 — B7 面板小改进（sidebar as-of / ETL 健康卡 / ytm_source 列）
- ✅ 2026-04-30 — B6 Phase 1.2 CF 季度 LaunchAgent 安装脚本
