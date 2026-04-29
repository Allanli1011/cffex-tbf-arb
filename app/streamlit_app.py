"""Streamlit MVP panel for CFFEX TBF arb signals.

Run with::

    streamlit run app/streamlit_app.py

Six tabs:
    1. Overview  — latest-day cards across all 4 signal families
    2. Basis     — IRR / net-basis tables + IRR vs FDR007 timeseries
    3. Calendar  — cross-quarter spreads + Z-score timeseries
    4. Curve     — fly + steepener live levels + history
    5. CTD       — Monte Carlo switch probabilities + scenario table
    6. Backtest  — pick a run, NAV curve, trades, summary metrics
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402
import plotly.express as px  # noqa: E402
import plotly.graph_objects as go  # noqa: E402
import streamlit as st  # noqa: E402

from app.data_loaders import (  # noqa: E402
    latest_date,
    load_backtest_grid_cells,
    load_backtest_grid_summary,
    load_backtest_run_artifacts,
    load_backtest_runs,
    load_basis_signals,
    load_calendar_spreads,
    load_ctd_switch,
    load_curve_signals,
    load_futures_daily,
    load_repo_rate,
)

st.set_page_config(
    page_title="CFFEX TBF Arb Panel",
    page_icon="📈",
    layout="wide",
)


# ---- Tab 1: Overview ----------------------------------------------------


def render_overview():
    st.header("Overview")

    basis = load_basis_signals()
    cal = load_calendar_spreads()
    curve = load_curve_signals()
    futures = load_futures_daily()

    asof = latest_date(basis, cal, curve, futures)
    st.caption(f"As of: **{asof}**" if asof else "No data loaded yet.")
    if not asof:
        st.warning("No signal parquet files found under data/parquet/. "
                   "Run the ETL + signal scripts first.")
        return

    cols = st.columns(4)

    # Card 1 — top basis carry signal of the day
    with cols[0]:
        st.subheader("Basis (CTDs)")
        sub = basis[(basis["date"] == asof) & (basis["is_ctd"])]
        if sub.empty:
            st.info("No CTD rows.")
        else:
            top = sub.sort_values("irr_minus_fdr007_bp",
                                  ascending=False).head(4)
            st.dataframe(
                top[[
                    "contract_id", "bond_code", "irr",
                    "irr_minus_fdr007_bp", "net_basis",
                ]].rename(columns={
                    "irr_minus_fdr007_bp": "irr-fdr007 bp",
                    "net_basis": "net basis",
                }),
                hide_index=True,
                use_container_width=True,
            )

    # Card 2 — calendar mean-reversion candidates (|z60| sorted)
    with cols[1]:
        st.subheader("Calendar |z60|")
        sub = cal[cal["date"] == asof].copy()
        if sub.empty:
            st.info("No calendar rows.")
        else:
            sub["abs_z60"] = sub["z60"].astype(float).abs()
            top = sub.sort_values("abs_z60", ascending=False).head(4)
            st.dataframe(
                top[["product", "leg", "spread", "z60", "percentile60"]],
                hide_index=True,
                use_container_width=True,
            )

    # Card 3 — curve trades
    with cols[2]:
        st.subheader("Curve trades")
        sub = curve[curve["date"] == asof]
        if sub.empty:
            st.info("No curve rows.")
        else:
            st.dataframe(
                sub[["structure", "spread_bp", "z60"]],
                hide_index=True,
                use_container_width=True,
            )

    # Card 4 — repo rates today
    with cols[3]:
        st.subheader("Funding (FDR007 etc.)")
        repo = load_repo_rate()
        sub = repo[repo["date"] == asof] if not repo.empty else pd.DataFrame()
        if sub.empty:
            st.info("No repo rows.")
        else:
            sub = sub.sort_values("rate_name")
            st.dataframe(
                sub[["rate_name", "value_pct"]].rename(
                    columns={"value_pct": "rate %"}
                ),
                hide_index=True,
                use_container_width=True,
            )


# ---- Tab 2: Basis -------------------------------------------------------


def render_basis():
    st.header("Basis (IRR / Net basis)")
    basis = load_basis_signals()
    if basis.empty:
        st.warning("No basis_signals data.")
        return

    products = sorted(basis["product"].unique().tolist())
    product = st.selectbox("Product", products, index=products.index("T")
                           if "T" in products else 0)

    sub = basis[basis["product"] == product]
    contracts = sorted(sub["contract_id"].unique().tolist())
    if not contracts:
        st.info("No contracts.")
        return
    default_idx = max(0, len(contracts) - 1)
    contract = st.selectbox("Contract", contracts, index=default_idx)

    sub = sub[sub["contract_id"] == contract]
    asof = sub["date"].max()
    today = sub[sub["date"] == asof].sort_values("irr",
                                                 ascending=False)
    st.subheader(f"Deliverable pool — {contract} on {asof}")
    st.dataframe(
        today[[
            "bond_code", "bond_name", "coupon_rate", "maturity_date",
            "cf", "futures_settle", "ytm_used", "implied_ytm",
            "bond_clean", "gross_basis", "net_basis", "irr",
            "irr_minus_fdr007_bp", "is_ctd",
        ]],
        hide_index=True,
        use_container_width=True,
    )

    # CTD timeseries
    ctd = sub[sub["is_ctd"]].sort_values("date")
    if not ctd.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=ctd["date"], y=ctd["irr_minus_fdr007_bp"],
                                 mode="lines", name="IRR − FDR007 (bp)"))
        fig.add_hline(y=0, line_dash="dash", line_color="grey")
        fig.update_layout(
            title=f"{contract} CTD: IRR − FDR007 (bp)",
            yaxis_title="bp",
            height=350,
        )
        st.plotly_chart(fig, use_container_width=True)

        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(x=ctd["date"], y=ctd["net_basis"],
                                  mode="lines", name="Net basis"))
        fig2.add_hline(y=0, line_dash="dash", line_color="grey")
        fig2.update_layout(
            title=f"{contract} CTD: Net basis (RMB / 100 face)",
            yaxis_title="RMB",
            height=350,
        )
        st.plotly_chart(fig2, use_container_width=True)


# ---- Tab 3: Calendar ----------------------------------------------------


def render_calendar():
    st.header("Calendar spreads")
    cal = load_calendar_spreads()
    if cal.empty:
        st.warning("No calendar_spreads data.")
        return

    products = sorted(cal["product"].unique().tolist())
    legs = sorted(cal["leg"].unique().tolist())
    c1, c2 = st.columns(2)
    with c1:
        product = st.selectbox("Product", products,
                               index=products.index("T")
                               if "T" in products else 0)
    with c2:
        leg = st.selectbox("Leg", legs,
                           index=legs.index("near_far")
                           if "near_far" in legs else 0)

    sub = cal[(cal["product"] == product) & (cal["leg"] == leg)].sort_values("date")
    if sub.empty:
        st.info("Empty selection.")
        return

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=sub["date"], y=sub["spread"],
                             mode="lines", name="spread"))
    fig.update_layout(title=f"{product} {leg} spread (points)",
                      yaxis_title="points", height=320)
    st.plotly_chart(fig, use_container_width=True)

    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=sub["date"], y=sub["z60"].astype(float),
                              mode="lines", name="z60"))
    fig2.add_hline(y=2, line_dash="dash", line_color="red")
    fig2.add_hline(y=-2, line_dash="dash", line_color="red")
    fig2.add_hline(y=0, line_dash="dot", line_color="grey")
    fig2.update_layout(title="60-day rolling Z-score",
                       yaxis_title="z", height=320)
    st.plotly_chart(fig2, use_container_width=True)

    asof = sub["date"].max()
    st.caption(f"Latest: {asof} — spread "
               f"{sub['spread'].iloc[-1]:.4f}, z60 "
               f"{float(sub['z60'].iloc[-1]):.2f}")


# ---- Tab 4: Curve -------------------------------------------------------


def render_curve():
    st.header("Curve trades — butterfly + steepener")
    curve = load_curve_signals()
    if curve.empty:
        st.warning("No curve_signals data.")
        return

    asof = curve["date"].max()
    today = curve[curve["date"] == asof].sort_values("structure")
    st.subheader(f"Live levels — {asof}")
    st.dataframe(
        today[[
            "structure", "contract_short_wing", "contract_belly",
            "contract_long_wing", "spread_bp", "z60",
            "n_short_wing", "n_belly", "n_long_wing",
        ]],
        hide_index=True,
        use_container_width=True,
    )

    structures = sorted(curve["structure"].unique().tolist())
    sel = st.selectbox("History for structure:", structures, index=0)
    sub = curve[curve["structure"] == sel].sort_values("date")

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=sub["date"], y=sub["spread_bp"],
                             mode="lines", name="spread (bp)"))
    fig.update_layout(title=f"{sel} spread (bp)",
                      yaxis_title="bp", height=320)
    st.plotly_chart(fig, use_container_width=True)

    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=sub["date"], y=sub["z60"].astype(float),
                              mode="lines", name="z60"))
    fig2.add_hline(y=2, line_dash="dash", line_color="red")
    fig2.add_hline(y=-2, line_dash="dash", line_color="red")
    fig2.add_hline(y=0, line_dash="dot", line_color="grey")
    fig2.update_layout(title="60-day rolling Z-score",
                       yaxis_title="z", height=320)
    st.plotly_chart(fig2, use_container_width=True)


# ---- Tab 5: CTD & Delivery ----------------------------------------------


SCENARIO_COLS = [
    "scenario_minus_100", "scenario_minus_50", "scenario_minus_25",
    "scenario_plus_25", "scenario_plus_50", "scenario_plus_100",
]
SCENARIO_LABELS = {
    "scenario_minus_100": "−100bp",
    "scenario_minus_50": "−50bp",
    "scenario_minus_25": "−25bp",
    "scenario_plus_25": "+25bp",
    "scenario_plus_50": "+50bp",
    "scenario_plus_100": "+100bp",
}


def render_ctd_delivery():
    st.header("CTD switch probabilities")
    st.caption(
        "Monte Carlo with parallel yield-shift; horizon vol = "
        "5 bp/day × √days_to_delivery. Anchor = min-gross-basis CTD."
    )

    df = load_ctd_switch()
    if df.empty:
        st.warning(
            "No ``ctd_switch`` parquet found. Run "
            "``python3 scripts/compute_ctd_switch.py`` first."
        )
        return

    asof = df["date"].max()
    today = df[df["date"] == asof].sort_values(
        ["product", "contract_id"]
    ).reset_index(drop=True)

    # Live table
    st.subheader(f"Live — {asof}")
    display_cols = [
        "contract_id", "product", "current_ctd_bond", "irr_ctd_bond",
        "ctd_anchor_disagrees", "days_to_delivery", "horizon_vol_bp",
        "switch_probability", "top_alt_bond", "top_alt_prob",
    ]
    table = today[display_cols].copy()
    table["switch_probability"] = (
        table["switch_probability"].astype(float) * 100
    ).round(1)
    table["top_alt_prob"] = (
        table["top_alt_prob"].astype(float) * 100
    ).round(1)
    table["horizon_vol_bp"] = table["horizon_vol_bp"].astype(float).round(1)
    table = table.rename(columns={
        "current_ctd_bond": "MC anchor (min basis)",
        "irr_ctd_bond": "IRR-CTD",
        "ctd_anchor_disagrees": "anchor ≠ IRR-CTD",
        "days_to_delivery": "days→delivery",
        "horizon_vol_bp": "horizon vol bp",
        "switch_probability": "switch prob %",
        "top_alt_bond": "top alt bond",
        "top_alt_prob": "top alt prob %",
    })
    st.dataframe(table, hide_index=True, use_container_width=True)

    if today["ctd_anchor_disagrees"].any():
        n = int(today["ctd_anchor_disagrees"].sum())
        st.info(
            f"⚠️ {n}/{len(today)} contracts have different CTDs under "
            "min-basis vs max-IRR ranking — carry differences across "
            "deliverables are material."
        )

    # Per-contract drill-down
    contracts = today["contract_id"].tolist()
    if not contracts:
        return
    sel = st.selectbox("Drill-down contract:", contracts, index=0)
    row = today[today["contract_id"] == sel].iloc[0]

    c1, c2, c3 = st.columns(3)
    c1.metric("Switch probability",
              f"{float(row['switch_probability']) * 100:.1f}%")
    c2.metric("Days to delivery", f"{int(row['days_to_delivery'])}")
    c3.metric("Horizon vol", f"{float(row['horizon_vol_bp']):.1f} bp")

    # Scenario table for the selected contract
    st.subheader(f"{sel} — deterministic scenario table")
    scen_rows = [
        {"shift": SCENARIO_LABELS[c],
         "ctd_bond": str(row[c]),
         "switched":
            str(row[c]) != str(row["current_ctd_bond"])}
        for c in SCENARIO_COLS
    ]
    scen_df = pd.DataFrame(scen_rows)
    scen_df.insert(0, "row",
                   ["−100", "−50", "−25", "+25", "+50", "+100"])
    st.dataframe(
        scen_df.drop(columns=["row"]),
        hide_index=True,
        use_container_width=True,
    )

    # History timeseries — switch probability over time for this contract
    hist = df[df["contract_id"] == sel].sort_values("date")
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=hist["date"],
        y=(hist["switch_probability"].astype(float) * 100).round(1),
        mode="lines",
        name="switch prob %",
    ))
    fig.update_layout(
        title=f"{sel} — switch probability history",
        yaxis_title="%", height=320,
    )
    st.plotly_chart(fig, use_container_width=True)

    # Cross-section heatmap: product × contract switch probability
    st.subheader("Today's switch probability matrix")
    pivot = today.pivot(index="product", columns="contract_id",
                        values="switch_probability").fillna(0.0) * 100
    fig2 = px.imshow(
        pivot,
        text_auto=".1f",
        color_continuous_scale="Reds",
        aspect="auto",
        labels=dict(x="contract", y="product", color="switch %"),
    )
    fig2.update_layout(height=300)
    st.plotly_chart(fig2, use_container_width=True)


# ---- Tab 6: Backtest ----------------------------------------------------


def render_backtest():
    st.header("Backtest runs")
    runs = load_backtest_runs()
    if runs.empty:
        st.warning(
            "No backtest_runs in SQLite. Run "
            "``python3 scripts/run_backtest.py --strategy ...`` first."
        )
        return

    label_to_id = {
        f"{r['run_id']} ({r['strategy']}, {r['start_date']}→{r['end_date']})":
            r["run_id"]
        for _, r in runs.iterrows()
    }
    label = st.selectbox("Run", list(label_to_id))
    run_id = label_to_id[label]
    info = runs[runs["run_id"] == run_id].iloc[0]

    # Metrics summary
    metrics = info["metrics"] or {}
    if metrics:
        m_cols = st.columns(5)
        m_cols[0].metric("Trades", f"{metrics.get('n_trades', 0)}")
        m_cols[1].metric("Hit rate",
                         f"{metrics.get('hit_rate', 0)*100:.1f}%")
        m_cols[2].metric("Total P&L (RMB)",
                         f"{metrics.get('total_pnl', 0):,.0f}")
        m_cols[3].metric("Sharpe",
                         f"{metrics.get('sharpe_annualised', 0):.2f}")
        m_cols[4].metric("Max DD (RMB)",
                         f"{metrics.get('max_drawdown', 0):,.0f}")

    trades, nav = load_backtest_run_artifacts(run_id)
    if not nav.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=nav["date"],
                                 y=nav["cum_pnl"].astype(float),
                                 mode="lines", name="Cumulative P&L"))
        fig.update_layout(title="NAV (cumulative P&L, RMB)",
                          yaxis_title="RMB", height=380)
        st.plotly_chart(fig, use_container_width=True)

    if not trades.empty:
        st.subheader("Trades")
        st.dataframe(
            trades.sort_values("entry_date"),
            hide_index=True,
            use_container_width=True,
        )

    with st.expander("Params"):
        st.json(info["params"])

    # ---- Grid sweep section ---------------------------------------------
    st.divider()
    st.subheader("Parameter sweeps")
    st.caption(
        "Grids written by ``scripts/backtest_grid.py``. Sharpes are "
        "in-sample on a ~144-day window — interpret as "
        "*directional* signals, not promises of out-of-sample edge."
    )
    grid_df = load_backtest_grid_summary()
    if grid_df.empty:
        st.info(
            "No grids in SQLite yet. Run "
            "``python3 scripts/backtest_grid.py --strategy <name>`` to "
            "populate."
        )
        return

    # Pick the most recent grid per strategy
    by_strategy = grid_df.sort_values("created_at", ascending=False)\
        .drop_duplicates("strategy")
    options = {
        f"{r['strategy']}  ({r['grid_id']})": (r["strategy"], r["grid_id"])
        for _, r in by_strategy.iterrows()
    }
    sel_label = st.selectbox(
        "Strategy / grid", list(options),
        index=0, key="grid_select",
    )
    sel_strategy, sel_grid_id = options[sel_label]
    cells = load_backtest_grid_cells(sel_grid_id)
    if cells.empty:
        st.info("Selected grid has no cells.")
        return

    # Hold-day picker (default = best Sharpe's hold)
    holds = sorted(cells["max_hold_days"].unique().tolist())
    best_row = cells.sort_values("sharpe", ascending=False).iloc[0]
    default_hold_idx = holds.index(int(best_row["max_hold_days"]))
    hold_pick = st.select_slider(
        "max_hold_days slice",
        options=holds, value=int(best_row["max_hold_days"]),
        key="grid_hold_slice",
    )
    sub = cells[cells["max_hold_days"] == hold_pick]

    g1, g2, g3 = st.columns(3)
    g1.metric("Best Sharpe (this grid)", f"{best_row['sharpe']:+.2f}")
    g2.metric("Best entry / exit",
              f"{best_row['entry_param']} / {best_row['exit_param']}")
    g3.metric("Best hold", f"{int(best_row['max_hold_days'])}d")

    # Sharpe heatmap (entry × exit) at the selected hold
    pivot_sharpe = sub.pivot(
        index="entry_param", columns="exit_param", values="sharpe"
    )
    fig = px.imshow(
        pivot_sharpe,
        text_auto=".2f",
        color_continuous_scale="RdBu",
        color_continuous_midpoint=0.0,
        aspect="auto",
        labels=dict(x="exit_param", y="entry_param", color="Sharpe"),
    )
    fig.update_layout(
        title=f"Sharpe heatmap @ hold={hold_pick}d  "
              f"(NaN = exit ≥ entry, skipped)",
        height=380,
    )
    st.plotly_chart(fig, use_container_width=True)

    # Companion heatmap: number of trades (sanity check on sample size)
    pivot_n = sub.pivot(
        index="entry_param", columns="exit_param", values="n_trades"
    )
    fig2 = px.imshow(
        pivot_n,
        text_auto=".0f",
        color_continuous_scale="Greys",
        aspect="auto",
        labels=dict(x="exit_param", y="entry_param", color="trades"),
    )
    fig2.update_layout(
        title=f"Trade count @ hold={hold_pick}d", height=320,
    )
    st.plotly_chart(fig2, use_container_width=True)

    with st.expander("All cells (sorted by Sharpe)"):
        cells_show = cells.sort_values("sharpe", ascending=False).copy()
        for col in ("hit_rate",):
            cells_show[col] = (
                cells_show[col].astype(float) * 100
            ).round(1)
        for col in ("sharpe", "total_pnl", "max_drawdown",
                    "avg_holding_days"):
            cells_show[col] = cells_show[col].astype(float).round(2)
        st.dataframe(cells_show, hide_index=True, use_container_width=True)


# ---- App entry ----------------------------------------------------------


def main():
    st.title("CFFEX TBF Arb Panel")
    st.caption(
        "MVP — basis / calendar / curve signals + backtest runs."
        " Data: open-source AKShare + CFFEX scrape."
    )

    (tab_overview, tab_basis, tab_cal, tab_curve, tab_ctd,
     tab_bt) = st.tabs([
        "Overview", "Basis", "Calendar", "Curve", "CTD", "Backtest",
    ])
    with tab_overview:
        render_overview()
    with tab_basis:
        render_basis()
    with tab_cal:
        render_calendar()
    with tab_curve:
        render_curve()
    with tab_ctd:
        render_ctd_delivery()
    with tab_bt:
        render_backtest()


if __name__ == "__main__":
    main()
