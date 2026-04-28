"""Streamlit MVP panel for CFFEX TBF arb signals.

Run with::

    streamlit run app/streamlit_app.py

Five tabs:
    1. Overview  — latest-day cards across all 4 signal families
    2. Basis     — IRR / net-basis tables + IRR vs FDR007 timeseries
    3. Calendar  — cross-quarter spreads + Z-score timeseries
    4. Curve     — fly + steepener live levels + history
    5. Backtest  — pick a run, NAV curve, trades, summary metrics
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
    load_backtest_run_artifacts,
    load_backtest_runs,
    load_basis_signals,
    load_calendar_spreads,
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


# ---- Tab 5: Backtest ----------------------------------------------------


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


# ---- App entry ----------------------------------------------------------


def main():
    st.title("CFFEX TBF Arb Panel")
    st.caption(
        "MVP — basis / calendar / curve signals + backtest runs."
        " Data: open-source AKShare + CFFEX scrape."
    )

    tab_overview, tab_basis, tab_cal, tab_curve, tab_bt = st.tabs([
        "Overview", "Basis", "Calendar", "Curve", "Backtest",
    ])
    with tab_overview:
        render_overview()
    with tab_basis:
        render_basis()
    with tab_cal:
        render_calendar()
    with tab_curve:
        render_curve()
    with tab_bt:
        render_backtest()


if __name__ == "__main__":
    main()
