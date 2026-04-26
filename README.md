# CFFEX Treasury Bond Futures Arbitrage Monitor

中金所国债期货套利策略监控面板。

覆盖 TS / TF / T / TL 四个品种，支持基差套利、跨期套利、收益率曲线套利及拓展套利场景的监控、回测与告警。

## 项目结构

```
cffex_tbf_arb/
├── docs/          # 设计文档、策略说明、开发计划
├── src/           # 核心代码（数据接入、定价、信号、面板）
├── data/          # 本地数据（gitignored）
├── notebooks/     # 研究与原型
├── tests/         # 单元/集成测试
├── configs/       # 合约元数据、CF 表、阈值参数
└── scripts/       # 一次性脚本与运维工具
```

## 文档索引

- [docs/design.md](docs/design.md) — 总体设计方案
- [docs/strategies.md](docs/strategies.md) — 套利策略详解与公式
- [docs/roadmap.md](docs/roadmap.md) — 分阶段开发计划

## 开发阶段

见 `docs/roadmap.md`。当前处于 **Phase 0 — 文档与脚手架**。
