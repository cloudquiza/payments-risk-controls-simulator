"""
src/controls_engine.py

A small config-driven controls engine.

Reads controls from controls/controls.yaml and evaluates them against a unified
transactions dataset (data/combined_transactions.csv).

Outputs:
- control_hits.csv      (long format: one row per tx-control hit)
- control_decisions.csv (one row per transaction: final action + reasons)
- control_metrics.csv   (monitoring summary per control)

Why this exists:
Controls and monitoring are the layer above detection. Real risk teams need to know:
- Which controls are firing?
- Which are noisy?
- Which correlate with risky outcomes?
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import yaml


# -----------------------------
# Data model
# -----------------------------

@dataclass
class Control:
    """A single risk control definition loaded from YAML."""
    control_id: str
    rail: str
    severity: str
    action: str  # ALLOW / REVIEW / BLOCK (we use REVIEW/BLOCK from YAML; ALLOW is default)
    description: str
    conditions: Dict[str, Any]


# -----------------------------
# Loading utilities
# -----------------------------

def load_controls(path: str = "controls/controls.yaml") -> List[Control]:
    """Load controls from YAML and return a list of Control objects."""
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    controls: List[Control] = []
    for item in raw:
        controls.append(
            Control(
                control_id=item["control_id"],
                rail=item["rail"],
                severity=item.get("severity", "MEDIUM"),
                action=item.get("action", "REVIEW"),
                description=item.get("description", ""),
                conditions=item.get("conditions", {}),
            )
        )
    return controls


def ensure_data_dir() -> None:
    """Ensure the data/ directory exists."""
    os.makedirs("data", exist_ok=True)


# -----------------------------
# Condition evaluation helpers
# -----------------------------

def _safe_series(df: pd.DataFrame, col: str) -> pd.Series:
    """
    Return df[col] if it exists; otherwise return a Series of NA values.

    This prevents KeyErrors and lets controls gracefully evaluate to False when
    a required field isn't present for a given rail.
    """
    if col in df.columns:
        return df[col]
    return pd.Series([pd.NA] * len(df), index=df.index)


def build_mask_for_conditions(df: pd.DataFrame, conditions: Dict[str, Any]) -> pd.Series:
    """
    Build a boolean mask for all conditions in a control.

    Supported condition patterns:
    - exact match: field: value
      ex: funding_speed: instant

    - membership list: field_in: [a, b, c]
      ex: return_code_in: [R01, R10]

    - comparisons:
      field_gt, field_gte, field_lt, field_lte
      ALSO supports:
      field_gt_days, field_gte_days, field_lt_days, field_lte_days
      ex: amount_gt: 5000
          account_age_lt_days: 30
          wallet_age_lt_days: 7

    - booleans: is_new_device: true
      Handles boolean values stored as strings in CSV ("True"/"False").
    """
    mask = pd.Series([True] * len(df), index=df.index)

    def coerce_bool_series(s: pd.Series) -> pd.Series:
        """Convert True/False strings to booleans when needed."""
        if s.dtype == bool:
            return s
        # Convert common string forms to boolean
        mapped = (
            s.astype(str)
            .str.strip()
            .str.lower()
            .map({"true": True, "false": False})
        )
        # If mapping failed (NaN), keep original
        return mapped.where(mapped.notna(), s)

    for key, expected in conditions.items():
        # 1) Handle membership list keys like return_code_in
        if key.endswith("_in"):
            field = key.replace("_in", "")
            series = _safe_series(df, field)
            mask &= series.isin(expected)
            continue

        # 2) Handle *_lt_days / *_gt_days style keys (your YAML uses these)
        day_suffix_ops = {
            "_gt_days": "_gt",
            "_gte_days": "_gte",
            "_lt_days": "_lt",
            "_lte_days": "_lte",
        }
        if any(key.endswith(suf) for suf in day_suffix_ops):
            for suf, op in day_suffix_ops.items():
                if key.endswith(suf):
                    field = key[: -len(suf)]  # strip suffix
                    break

            # Normalize field names used in YAML
            if field == "account_age":
                field = "account_age_days"
            if field == "wallet_age":
                field = "wallet_age_days"

            series = pd.to_numeric(_safe_series(df, field), errors="coerce")

            if op == "_gt":
                mask &= series > float(expected)
            elif op == "_gte":
                mask &= series >= float(expected)
            elif op == "_lt":
                mask &= series < float(expected)
            elif op == "_lte":
                mask &= series <= float(expected)

            continue

        # 3) Handle regular comparison keys like amount_gt
        for op in ["_gt", "_gte", "_lt", "_lte"]:
            if key.endswith(op):
                field = key.replace(op, "")

                # Normalize field names
                if field == "account_age":
                    field = "account_age_days"
                if field == "wallet_age":
                    field = "wallet_age_days"

                series = pd.to_numeric(_safe_series(df, field), errors="coerce")

                if op == "_gt":
                    mask &= series > float(expected)
                elif op == "_gte":
                    mask &= series >= float(expected)
                elif op == "_lt":
                    mask &= series < float(expected)
                elif op == "_lte":
                    mask &= series <= float(expected)
                break
        else:
            # 4) Exact match
            field = key
            series = _safe_series(df, field)

            # If YAML expects bool, coerce series to bool where possible
            if isinstance(expected, bool):
                series = coerce_bool_series(series)
                mask &= series == expected
            elif isinstance(expected, str):
                mask &= series.astype(str).str.lower() == expected.lower()
            else:
                mask &= series == expected

    return mask.fillna(False)


# -----------------------------
# Decision logic
# -----------------------------

ACTION_PRIORITY = {
    "ALLOW": 0,
    "REVIEW": 1,
    "BLOCK": 2,
}


def resolve_final_action(actions: List[str]) -> str:
    """
    Given multiple actions triggered for a transaction, return the highest priority action.
    """
    if not actions:
        return "ALLOW"
    return max(actions, key=lambda a: ACTION_PRIORITY.get(a, 0))


def evaluate_controls(
    tx: pd.DataFrame,
    controls: List[Control],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Evaluate controls against the transactions dataframe.

    Returns:
    - decisions_df: one row per transaction with final action + reasons
    - hits_df: long-form table listing each (tx_id, control_id) hit
    - metrics_df: monitoring summary per control
    """
    # Make sure core columns exist
    required_cols = ["tx_id", "rail", "timestamp", "user_id", "amount", "is_fraud_pattern"]
    for c in required_cols:
        if c not in tx.columns:
            raise ValueError(f"Missing required column '{c}' in combined_transactions.csv")

    hit_rows: List[Dict[str, Any]] = []

    # Evaluate each control only on the rows for its rail
    for control in controls:
        rail_df = tx[tx["rail"] == control.rail].copy()
        if rail_df.empty:
            continue

        mask = build_mask_for_conditions(rail_df, control.conditions)
        hits = rail_df[mask]

        # Record each hit as a row (long format)
        for _, row in hits.iterrows():
            hit_rows.append(
                {
                    "tx_id": row["tx_id"],
                    "rail": row["rail"],
                    "control_id": control.control_id,
                    "severity": control.severity,
                    "action": control.action,
                    "description": control.description,
                }
            )

    hits_df = pd.DataFrame(hit_rows)

    # Build decisions: one row per tx_id
    decisions = tx[["tx_id", "rail", "timestamp", "user_id", "amount", "is_fraud_pattern"]].copy()

    if hits_df.empty:
        # No hits at all — everything ALLOW
        decisions["final_action"] = "ALLOW"
        decisions["triggered_controls"] = ""
        decisions["triggered_actions"] = ""
    else:
        # Group hits per transaction
        grouped = hits_df.groupby("tx_id").agg(
            triggered_controls=("control_id", lambda s: ", ".join(sorted(set(s)))),
            triggered_actions=("action", lambda s: ", ".join(sorted(set(s)))),
            final_action=("action", lambda s: resolve_final_action(list(set(s)))),
        )

        decisions = decisions.merge(grouped, on="tx_id", how="left")
        decisions["final_action"] = decisions["final_action"].fillna("ALLOW")
        decisions["triggered_controls"] = decisions["triggered_controls"].fillna("")
        decisions["triggered_actions"] = decisions["triggered_actions"].fillna("")

    # Monitoring metrics per control:
    # We use "is_fraud_pattern" as a synthetic label to approximate effectiveness.
    metrics_rows: List[Dict[str, Any]] = []
    if not hits_df.empty:
        # Join hits to label and amount info
        labeled_hits = hits_df.merge(
            tx[["tx_id", "is_fraud_pattern", "amount", "rail"]],
            on=["tx_id", "rail"],
            how="left",
        )

        total_tx = len(tx)
        for control_id, g in labeled_hits.groupby("control_id"):
            hits_count = len(g)
            hit_rate = hits_count / total_tx

            # "Precision-ish": of the control hits, how many were labeled as fraud pattern?
            # This is a synthetic proxy, not a real-world metric.
            if "is_fraud_pattern" in g.columns:
                precision_proxy = float(g["is_fraud_pattern"].mean())
            else:
                precision_proxy = 0.0

            metrics_rows.append(
                {
                    "control_id": control_id,
                    "hits": hits_count,
                    "hit_rate": round(hit_rate, 4),
                    "precision_proxy": round(precision_proxy, 4),
                }
            )

        # Build metrics_df safely (even when no controls fired)
    if metrics_rows:
        metrics_df = pd.DataFrame(metrics_rows).sort_values(by="hits", ascending=False)
    else:
        # Create an empty metrics table with the expected columns
        metrics_df = pd.DataFrame(
            columns=["control_id", "hits", "hit_rate", "precision_proxy"]
        )

    return decisions, hits_df, metrics_df


def run() -> None:
    """
    Orchestrator: load transactions + controls, evaluate, and write outputs.
    """
    ensure_data_dir()

    tx_path = "data/combined_transactions.csv"
    if not os.path.exists(tx_path):
        raise FileNotFoundError(
            "Missing data/combined_transactions.csv. Run: python src/generate_synthetic_data.py"
        )

    tx = pd.read_csv(tx_path)

    controls = load_controls("controls/controls.yaml")

    decisions_df, hits_df, metrics_df = evaluate_controls(tx, controls)

    # Write outputs (these are gitignored)
    decisions_df.to_csv("data/control_decisions.csv", index=False)
    hits_df.to_csv("data/control_hits.csv", index=False)
    metrics_df.to_csv("data/control_metrics.csv", index=False)

    print("✅ Controls evaluated")
    print(f"- Transactions: {len(tx):,}")
    print(f"- Hits rows: {len(hits_df):,}")
    print(f"- Decisions written: data/control_decisions.csv")
    print(f"- Hits written: data/control_hits.csv")
    print(f"- Metrics written: data/control_metrics.csv")


if __name__ == "__main__":
    run()
