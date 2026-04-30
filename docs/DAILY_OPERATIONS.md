# Daily Update 操作手册

> 给操作者的日常使用指南：怎么让数据每天自动更新，怎么读输出，遇到问题怎么办。
> 开发者文档请看 `STATUS.md` / `BACKLOG.md` / `roadmap.md`。

## TL;DR

```bash
# 一次性激活（macOS）
scripts/install_launchd.sh daily-etl --install

# 之后什么都不用管。Mon–Fri 17:30 自动跑全 pipeline。
# 想看今天的信号：
cat data/logs/daily-digest-latest.json | jq .counts
# 或在面板里看：
python3 -m streamlit run app/streamlit_app.py
```

---

## 一、Daily update 做了什么

每个工作日 17:30，wrapper `scripts/run_daily_etl.sh` 顺序执行 7 个脚本：

| # | 脚本 | 作用 | 典型耗时 |
|---|---|---|---|
| 1 | `backfill_market_data.py --days 5` | CFFEX 期货 / OI 排名 / 中债收益率曲线 / 资金面 | ~10s |
| 2 | `backfill_bond_valuation.py` | Sina 交易所收盘 → 单券 YTM（198 个债，1.5s/债） | **~6 min** |
| 3 | `compute_basis_signals.py` | per-(date, contract, bond) IRR + DV01 + CTD 标记 | ~20s |
| 4 | `compute_calendar_spreads.py --force` | 跨期价差 + 60d/120d Z-score | ~1s |
| 5 | `compute_curve_signals.py` | 蝶式 / 陡平 + 60d Z（DV01 中性比例） | <1s |
| 6 | `compute_ctd_switch.py --n-sims 1000` | CTD 切换概率（蒙特卡洛 + 6 档情景表） | ~2s |
| 7 | `daily_digest.py --quiet` | 阈值过滤 → 写 JSON + Markdown digest | <1s |

总时长 **5–7 分钟**，瓶颈是步骤 2 对 sina 的节流（每个债 1.5s × 198 ≈ 5 min）。

---

## 二、两种启动方式

### A. 自动（推荐）— macOS LaunchAgent

```bash
# 查看可用的 cron-style jobs
scripts/install_launchd.sh

# 安装日终 ETL（工作日 17:30 触发）
scripts/install_launchd.sh daily-etl --install

# 安装季度 CF 刷新（每季度 1 号 17:00 触发）— 可选但建议一起装
scripts/install_launchd.sh cf-refresh --install

# 检查状态
scripts/install_launchd.sh daily-etl --status

# 卸载
scripts/install_launchd.sh daily-etl --uninstall
```

`--install` 之前默认是 dry-run，会把 plist 打到 stdout 让你看一眼再决定是不是装。

**注意**：
- 需要电脑在 17:30 处于唤醒状态。睡眠中 launchd 会在下次唤醒时补跑（最多迟延几小时）
- macOS 可能首次跑时弹隐私授权（终端访问磁盘）— 允许就行
- 不需要 root / sudo，安装到 `~/Library/LaunchAgents/` 即可

### B. 手动 — 一行命令

```bash
scripts/run_daily_etl.sh
```

适合：调试、修了 bug 想立即重跑、补昨天落下的数据。

---

## 三、输出在哪里看

所有产物在 `data/logs/`：

```
data/logs/
├── daily-etl-2026-04-30.log          # 当天的详细日志（每步 stdout/stderr）
├── daily-etl-summary.log             # 最近一次的紧凑摘要（每天覆盖）
├── daily-etl.out.log                 # launchd stdout（追加，所有运行）
├── daily-etl.err.log                 # launchd stderr
├── daily-digest-2026-04-30.json      # 当天 actionable 信号（机器可读）
├── daily-digest-2026-04-30.md        # 当天 actionable 信号（人读）
└── daily-digest-latest.json          # 始终指向最新一次（面板用这个）
```

### 摘要长这样

```
=== Daily ETL summary  2026-04-30 21:40:57 ===
Total runtime: 421s, failures=0
  ✅ backfill_market_data         12s
  ✅ backfill_bond_valuation     387s
  ✅ compute_basis_signals        20s
  ✅ compute_calendar_spreads      1s
  ✅ compute_curve_signals         0s
  ✅ compute_ctd_switch            1s
  ✅ daily_digest                  0s

=== Daily signal digest — 2026-04-30 ===
  🔔        basis: 9 hits — top: TL2606/2400001 IRR-FDR=-1414bp
  🚦     calendar: 0 hits — top: —
  🚦        curve: 0 hits — top: —
  🔔   ctd_switch: 1 hits — top: TL2612 32% → 250002
```

📌 `🚦 = 0 hits`（市场平静）；`🔔 = ≥1 actionable 信号`。

### 在面板里看

```bash
python3 -m streamlit run app/streamlit_app.py
```

Overview 的 ETL 健康卡会用颜色编码每个 dataset 的滞后天数（绿 ≤1d / 黄 ≤5d / 红 >5d）。

---

## 四、怎么读 Digest

### Basis（基差套利信号）

| 字段 | 含义 |
|---|---|
| `irr_minus_fdr007_bp` | IRR 与 FDR007（7 天回购定盘）之差，bp |
| `side = long_basis` | IRR > FDR007，**正向基差**：长债 + 短期货能赚 carry |
| `side = short_basis` | IRR < FDR007，**反向基差**：理论上短债 + 长期货赚（但国内反向受限）|
| `ytm_source = bond_valuation` | 用了 sina 单券估值（更准）|
| `ytm_source = par_curve` | 没单券估值，落到 par 曲线插值（粗略）|

**触发阈值**：`|IRR − FDR007| > 30bp`（仅 CTD 行）。可改：`daily_digest.py --basis-bp 50`

### Calendar（跨期价差）

| 字段 | 含义 |
|---|---|
| `leg = near_far / mid_far / near_mid` | 哪两个合约的价差 |
| `z60 > 0` | spread 高于 60 日均值 → **short_spread**（卖远买近） |
| `z60 < 0` | spread 低于均值 → **long_spread**（买远卖近） |
| `percentile60` | 当前 spread 在过去 60 天的百分位 |

**触发**：`|z60| > 2.0`。可改：`--calendar-z 1.5`

### Curve（蝶式 / 陡平）

| 字段 | 含义 |
|---|---|
| `structure` | `fly_2_5_10` / `fly_5_10_30` / `steepener_2s10s` / `steepener_5s30s` |
| `n_short_wing / n_belly / n_long_wing` | DV01 中性合约比例（belly = 1）|
| `side = short_fly` | belly 偏便宜 → 卖蝶式（卖 belly 买 wings）|

### CTD switch（交割切换）

| 字段 | 含义 |
|---|---|
| `current_ctd` | 当前最便宜可交割券（按 min gross basis）|
| `irr_ctd` | 按 max IRR 选出的 CTD（若与 current 不同会警告）|
| `switch_prob_pct` | 估值日到交割日间，因收益率波动 5bp/d × √days 而切换 CTD 的蒙特卡洛概率 |
| `top_alt_bond` | 最可能成为新 CTD 的备选券 |

**触发**：`switch_prob > 30%`。可改：`--ctd-prob 0.4`

---

## 五、故障处理

### "ETL 健康卡显示某 dataset 红色（>5 天滞后）"

LaunchAgent 跑失败了。看：
```bash
tail -50 data/logs/daily-etl.err.log
tail -50 data/logs/daily-etl-$(date +%Y-%m-%d).log
```

常见原因：
1. **Sina / akshare 暂时挂了** — 重新手动跑：`scripts/run_daily_etl.sh`
2. **网络代理问题** — 换网络 / VPN
3. **CFFEX 交易日历对不上**（节假日）— 通常自愈

### "数字看着不对（IRR -1300bp 这种）"

可能是真实的极端信号（比如 50 年特别国债 2400001 的交易所价远高于期货蕴含价）。先验证：

```bash
# 看一只异常债的 sina 收盘和 YTM 是否合理
python3 -c "
import pandas as pd
v = pd.read_parquet('data/parquet/bond_valuation/2026-04-30.parquet')
print(v[v['bond_code']=='2400001'].to_string())
"
```

如果 sina 收盘价正常，那异常 IRR 反映的是市场割裂（交易所 vs 银行间），不是 bug。

### "今天忘了开机，Daily ETL 没跑"

```bash
scripts/run_daily_etl.sh   # 立刻跑
```

### "我想只跑信号刷新（不重新拉数据）"

```bash
python3 scripts/compute_basis_signals.py --force
python3 scripts/compute_calendar_spreads.py --force
python3 scripts/compute_curve_signals.py --force
python3 scripts/compute_ctd_switch.py --force
python3 scripts/daily_digest.py
```

### "我想加一只新债 / 新合约"

不用手动加。CFFEX CSV 一天爬一次（步骤 1 里就拉了）。如果是季度初新挂合约，也会被 `cf-refresh` LaunchAgent 季度自动同步。

### "我想改阈值"

编辑 `scripts/run_daily_etl.sh` 里的最后一行：

```bash
# 改前：
"$PYTHON_BIN $REPO_ROOT/scripts/daily_digest.py --quiet"

# 改后（更宽松）：
"$PYTHON_BIN $REPO_ROOT/scripts/daily_digest.py --quiet --basis-bp 20 --calendar-z 1.5"
```

---

## 六、定期维护（可选）

| 项 | 频率 | 命令 |
|---|---|---|
| 数据完整性 audit | 每周 | `python3 scripts/data_audit.py` |
| 验证 CF 公式精度 | 每月 | `python3 scripts/verify_cf_formula.py` |
| 重跑参数扫描（信号迭代后） | 每月或参数有变 | `python3 scripts/backtest_grid.py --strategy <name>` |
| 清理旧日志 | 每季度 | `find data/logs/daily-etl-*.log -mtime +90 -delete` |

---

## 七、想关掉自动化

```bash
scripts/install_launchd.sh daily-etl --uninstall
scripts/install_launchd.sh cf-refresh --uninstall
```

不影响数据，只是不再定时跑。

---

## 附录：Pipeline 数据依赖图

```
backfill_market_data ──┬─→ futures_daily ──┬─→ basis_signals ──┬─→ ctd_switch
                       │                   │                   ├─→ daily_digest
                       ├─→ futures_oi_rank │                   │
                       ├─→ bond_yield_curve┘                   │
                       └─→ repo_rate ─────────────────→────────┤
backfill_bond_valuation ─→ bond_valuation ────────────→────────┤
                                                                ├─→ calendar_spreads (uses futures_daily)
                                                                └─→ curve_signals    (uses basis_signals)
```

任何一步失败，下游会用最近一次成功的数据继续。所以单步偶尔失败不会让面板马上崩。
