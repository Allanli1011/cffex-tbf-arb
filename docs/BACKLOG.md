# Backlog

> 单一信源：本文件维护"待办任务"。完成后划掉并迁移到 `docs/roadmap.md` 历史。
> 更新时间：2026-04-28

阅读顺序：
1. **本文件** — 想知道下一步做什么
2. **docs/STATUS.md** — 想恢复一个旧 session 的上下文
3. **docs/roadmap.md** — 想看完整阶段清单与历史

---

## P0（高优先级，可立即推进）

### B1. Phase 5 — 模块 E：CTD 与交割分析面板
- **依赖**：`parquet/ctd_switch/`（已有 1036 行 / 144 天）
- **新增**：`app/streamlit_app.py` 加 `render_ctd_delivery()` tab
- **内容**：
  - 当日所有合约的切换概率排序表
  - 选中合约 → 蒙特卡洛分布图 + 6 档情景表
  - IRR-CTD vs min-basis-CTD 不一致提示（`ctd_anchor_disagrees`）
- **验收**：Streamlit 新 tab 渲染，无空数据
- **预估**：1–2 小时

### B2. Phase 5 — 模块 G：风险与持仓分析
- **依赖**：`futures_oi_rank` parquet（已有 41730 行）+ `basis_signals` DV01 列
- **内容**：
  - DV01 暴露热图（产品 × 合约月）
  - 多空比 / OI 集中度分析
  - 头寸时序：top-5 多头与 top-5 空头机构
- **验收**：模块 G tab 可用
- **预估**：2–3 小时

### B3. Phase 3 — 参数扫描与敏感度分析
- **依赖**：现有 6 个策略 + `src/backtest/`
- **新增**：`scripts/backtest_grid.py`，对每个策略在 (entry_z × exit_z × max_hold_days)
  网格上扫描，每组合一次回测，写 `parquet/backtest_grid/` + SQLite
- **面板配套**：Backtest tab 加热图（grid_id × Sharpe）
- **预估**：2 小时

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

### B5. Phase 1.4 — GC001 / GC014 完整回填
- **现状**：本机 eastmoney 代理拦截 GC001/014，仅 GC007 完整 252 天
- **方案候选**：
  - sina 货币市场接口（需调研）
  - 改用浏览器 user-agent header 重试
  - 同源页面手动下载 CSV
- **预估**：1 小时

### B6. Phase 1.2 — CF 季度自动运行
- **新增**：`scripts/install_launchd.sh` + `configs/com.cffex.cf-refresh.plist`
- **内容**：每季度首日 17:00 跑 `populate_contracts.py --snapshot` + 通知
- **预估**：30 分钟

### B7. 现有面板小改进
- sidebar 全局日期 picker（覆盖所有 tab）
- Overview tab 加 ETL 健康卡片（最近一次 ETL 运行 / 数据延迟）
- Basis tab 加 ytm_source 列（区分单券 vs par 曲线）
- **预估**：1–2 小时

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
