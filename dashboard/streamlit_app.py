"""
dashboard/streamlit_app.py

Payments Risk Controls Monitoring Dashboard (synthetic/mock data).

Reads outputs from:
- data/combined_transactions.csv
- data/control_decisions.csv
- data/control_hits.csv
- data/control_metrics.csv

Goal:
A clean internal-tool style UI to monitor:
- decision volume (ALLOW/REVIEW/BLOCK)
- control noise (top firing controls)
- proxy effectiveness (precision_proxy vs synthetic label)
- drill-down into flagged transactions
"""

from __future__ import annotations

import os
from typing import Tuple, List

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt


# -----------------------------
# Page config
# -----------------------------
st.set_page_config(
    page_title="Payments Risk Controls Monitor",
    page_icon="🛡️",
    layout="wide",
)


# -----------------------------
# Data loading
# -----------------------------
@st.cache_data
def load_data() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Load required CSVs. If missing, raise a friendly error with next steps.
    """
    required = [
        "data/combined_transactions.csv",
        "data/control_decisions.csv",
        "data/control_hits.csv",
        "data/control_metrics.csv",
    ]
    missing = [p for p in required if not os.path.exists(p)]
    if missing:
        raise FileNotFoundError(
            "Missing required file(s):\n"
            + "\n".join(missing)
            + "\n\nRun:\n"
            "  python src/generate_synthetic_data.py\n"
            "  python src/run_controls.py\n"
        )

    tx = pd.read_csv("data/combined_transactions.csv")
    decisions = pd.read_csv("data/control_decisions.csv")
    hits = pd.read_csv("data/control_hits.csv")
    metrics = pd.read_csv("data/control_metrics.csv")

    return tx, decisions, hits, metrics


def safe_value_counts(series: pd.Series) -> pd.Series:
    """Return value counts even if series is missing or empty."""
    if series is None or series.empty:
        return pd.Series(dtype=int)
    return series.value_counts()


# -----------------------------
# Small chart helpers
# -----------------------------
def bar_chart(series: pd.Series, title: str, xlabel: str = "", ylabel: str = "") -> None:
    """
    Render a basic bar chart using matplotlib.
    Keeps charts consistent and lightweight for a GitHub portfolio project.
    """
    if series.empty:
        st.info("No data to chart for current filters.")
        return

    fig, ax = plt.subplots()
    series.plot(kind="bar", ax=ax)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    plt.xticks(rotation=0)
    st.pyplot(fig)


def percent(numer: int, denom: int) -> str:
    """Format a percentage safely."""
    if denom <= 0:
        return "0.0%"
    return f"{(numer / denom) * 100:.1f}%"


# -----------------------------
# App
# -----------------------------
def main() -> None:
    st.title("🛡️ Payments Risk Controls Monitor")
    st.caption("Synthetic/mock data only. Config-driven controls across ACH, Card, and Crypto.")

    # Load raw data
    tx, decisions, hits, metrics = load_data()

    # Join decisions to the transaction rows to create one rich table for drill-down.
    # We join on a few stable keys (tx_id + rail) and keep fields consistent.
    df = decisions.merge(
        tx,
        on=["tx_id", "rail"],
        how="left",
        suffixes=("", "_tx"),
    )

    # -----------------------------
    # Sidebar filters
    # -----------------------------
    st.sidebar.header("Filters")

    rails = ["ALL"] + sorted(df["rail"].dropna().unique().tolist())
    rail_choice = st.sidebar.selectbox("Rail", rails, index=0)

    actions = ["ALL", "ALLOW", "REVIEW", "BLOCK"]
    action_choice = st.sidebar.selectbox("Final action", actions, index=0)

    control_options = ["ALL"]
    if not hits.empty and "control_id" in hits.columns:
        control_options += sorted(hits["control_id"].dropna().unique().tolist())
    control_choice = st.sidebar.selectbox("Control", control_options, index=0)

    # Amount slider
    min_amt = float(df["amount"].min()) if "amount" in df.columns else 0.0
    max_amt = float(df["amount"].max()) if "amount" in df.columns else 0.0
    amt_low, amt_high = st.sidebar.slider(
        "Amount range",
        min_value=float(min_amt),
        max_value=float(max_amt),
        value=(float(min_amt), float(max_amt)),
    )

    # Apply filters to the joined dataframe
    filtered = df.copy()

    if rail_choice != "ALL":
        filtered = filtered[filtered["rail"] == rail_choice]

    if action_choice != "ALL":
        filtered = filtered[filtered["final_action"] == action_choice]

    if "amount" in filtered.columns:
        filtered = filtered[(filtered["amount"] >= amt_low) & (filtered["amount"] <= amt_high)]

    # Control filter: keep only tx_ids that fired that control
    if control_choice != "ALL" and not hits.empty:
        tx_ids = set(hits[hits["control_id"] == control_choice]["tx_id"].tolist())
        filtered = filtered[filtered["tx_id"].isin(tx_ids)]

    # -----------------------------
    # KPI row
    # -----------------------------
    st.subheader("Portfolio KPIs")

    total = len(filtered)
    allow_ct = int((filtered["final_action"] == "ALLOW").sum()) if total else 0
    review_ct = int((filtered["final_action"] == "REVIEW").sum()) if total else 0
    block_ct = int((filtered["final_action"] == "BLOCK").sum()) if total else 0

    hit_ct = total - allow_ct  # anything not allow is "actioned"

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Transactions", f"{total:,}")
    c2.metric("ALLOW", f"{allow_ct:,}")
    c3.metric("REVIEW", f"{review_ct:,}")
    c4.metric("BLOCK", f"{block_ct:,}")
    c5.metric("Action rate", percent(hit_ct, total))

    st.divider()

    # -----------------------------
    # Top charts / tables
    # -----------------------------
    left, right = st.columns(2)

    with left:
        st.markdown("### Final action distribution")
        action_counts = safe_value_counts(filtered["final_action"])
        bar_chart(action_counts, "Final action counts", xlabel="Action", ylabel="Count")

    with right:
        st.markdown("### Decision breakdown by rail")
        if filtered.empty:
            st.info("No rows for current filters.")
        else:
            pivot = (
                filtered.groupby(["rail", "final_action"])
                .size()
                .unstack(fill_value=0)
            )
            st.dataframe(pivot, use_container_width=True)

    st.divider()

    # -----------------------------
    # Controls monitoring
    # -----------------------------
    st.subheader("Control monitoring")

    colA, colB = st.columns(2)

    with colA:
        st.markdown("**Noisiest controls (most hits)**")
        if hits.empty:
            st.info("No control hits found. Run scoring again or adjust controls.")
        else:
            noisy = hits["control_id"].value_counts().head(10)
            noisy_df = noisy.rename("hits").reset_index().rename(columns={"index": "control_id"})
            st.dataframe(noisy_df, use_container_width=True)

    with colB:
        st.markdown("**Control effectiveness (precision proxy)**")
        st.caption("Precision proxy uses the synthetic is_fraud_pattern label — it’s a learning metric, not a real fraud KPI.")
        if metrics.empty:
            st.info("No metrics available.")
        else:
            top = metrics.sort_values(by="precision_proxy", ascending=False).head(10)
            st.dataframe(top, use_container_width=True)

    st.divider()

    # -----------------------------
    # Drill-down table
    # -----------------------------
    st.subheader("Flagged transactions (drill-down)")

    # Sort so REVIEW/BLOCK appear first, then largest amounts
    rank = {"ALLOW": 0, "REVIEW": 1, "BLOCK": 2}
    filtered = filtered.copy()
    filtered["action_rank"] = filtered["final_action"].map(rank).fillna(0)
    filtered = filtered.sort_values(by=["action_rank", "amount"], ascending=[False, False])

    # Choose a readable set of columns for the table
    preferred_cols: List[str] = [
        "tx_id",
        "rail",
        "timestamp",
        "final_action",
        "triggered_controls",
        "triggered_actions",
        "user_id",
        "country",
        "device_id",
        "account_age_days",
        "amount",
        "currency",
        # ACH
        "funding_speed",
        "return_code",
        # Card
        "card_present",
        "mcc",
        "bin",
        "is_new_device",
        # Crypto
        "from_wallet_id",
        "to_wallet_id",
        "wallet_age_days",
        "to_is_high_risk",
        # Label
        "is_fraud_pattern",
    ]
    cols = [c for c in preferred_cols if c in filtered.columns]

    # Show top 250 rows to keep it fast
    st.dataframe(filtered[cols].head(250), use_container_width=True)
    st.caption("Showing up to 250 rows. Use the left filters to narrow results.")

    # Optional: raw hits table for transparency
    with st.expander("Show raw control hits (long format)"):
        if hits.empty:
            st.info("No hits to display.")
        else:
            st.dataframe(hits.head(500), use_container_width=True)
            st.caption("Showing up to 500 hit rows.")

    st.divider()
    st.caption("Tip: tweak thresholds in controls/controls.yaml and re-run to see how control noise changes.")


if __name__ == "__main__":
    main()
