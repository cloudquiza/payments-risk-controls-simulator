"""
src/generate_synthetic_data.py

Creates a single combined dataset of synthetic transactions across 3 rails:
- ACH
- CARD
- CRYPTO

Why we do this:
Risk teams often need a unified view of activity across rails so controls, monitoring,
and investigation workflows can be consistent.

Output:
- data/combined_transactions.csv

Notes:
- This is synthetic/mock data only (safe for public GitHub).
- Fields are normalized so a controls engine can evaluate them consistently.
"""

from __future__ import annotations

import os
import random
from datetime import datetime, timedelta
from typing import Dict, List

import pandas as pd


# -----------------------------
# Helper utilities
# -----------------------------

def ensure_data_dir() -> None:
    """Ensure the data/ directory exists."""
    os.makedirs("data", exist_ok=True)


def rand_date_within_days(days_back: int = 30) -> str:
    """
    Return a random ISO timestamp within the last `days_back` days.
    Example: 2025-12-15T10:22:31
    """
    now = datetime.now()
    delta = timedelta(days=random.randint(0, days_back), minutes=random.randint(0, 1440))
    ts = now - delta
    return ts.replace(microsecond=0).isoformat()


def weighted_choice(options: List[str], weights: List[float]) -> str:
    """Pick one item from options using weights."""
    return random.choices(options, weights=weights, k=1)[0]


def make_id(prefix: str, n: int) -> str:
    """Create deterministic-ish IDs like ach_tx_000123."""
    return f"{prefix}_{n:06d}"


# -----------------------------
# Synthetic "entity" generators
# -----------------------------

def generate_users(n_users: int = 500) -> pd.DataFrame:
    """
    Generate synthetic users. Users can show up across ACH/card/crypto activity.
    """
    rows: List[Dict] = []
    for i in range(n_users):
        user_id = f"user_{i:05d}"

        # Account age: skew older, but keep some newer accounts for risk patterns
        account_age_days = int(max(1, random.gauss(mu=120, sigma=90)))
        account_age_days = min(account_age_days, 1000)

        # Country mix (simple)
        country = weighted_choice(
            ["US", "CA", "GB", "MX", "NG", "BR", "IN"],
            [0.65, 0.08, 0.08, 0.06, 0.04, 0.05, 0.04],
        )

        # Device IDs: users have a "primary" device; we'll inject device-sharing later.
        device_id = f"dev_{random.randint(1, 350):05d}"

        rows.append(
            {
                "user_id": user_id,
                "account_age_days": account_age_days,
                "country": country,
                "device_id": device_id,
            }
        )

    return pd.DataFrame(rows)


def generate_wallets(users: pd.DataFrame) -> pd.DataFrame:
    """
    Generate one wallet per user (simplified). Crypto rails evaluate wallet_age_days, etc.
    """
    rows: List[Dict] = []
    for idx, row in users.iterrows():
        wallet_id = f"w_{idx:06d}"

        # Wallet age loosely follows account age, but can be newer.
        wallet_age_days = max(1, int(row["account_age_days"] * random.uniform(0.2, 1.0)))

        # Some wallets are exchange-linked (common in real crypto compliance contexts)
        is_exchange_linked = random.random() < 0.25

        rows.append(
            {
                "wallet_id": wallet_id,
                "user_id": row["user_id"],
                "wallet_age_days": wallet_age_days,
                "is_exchange_linked": is_exchange_linked,
            }
        )

    return pd.DataFrame(rows)


# -----------------------------
# Transaction generators by rail
# -----------------------------

def generate_ach_transactions(users: pd.DataFrame, n: int = 2000) -> pd.DataFrame:
    """
    Generate ACH-style transactions. We'll include fields used in controls such as:
    - funding_speed (instant/standard)
    - return_code (sometimes)
    - amount
    """
    rows: List[Dict] = []

    return_codes = [None, None, None, "R01", "R02", "R03", "R10", "R29"]  # more None = fewer returns

    for i in range(n):
        u = users.sample(1).iloc[0]

        funding_speed = weighted_choice(["instant", "standard"], [0.25, 0.75])
        amount = round(abs(random.gauss(mu=250, sigma=400)), 2)
        amount = max(5.0, min(amount, 12000.0))

        # Return code occurs sometimes; high-risk returns are rarer but present
        return_code = random.choice(return_codes)

        rows.append(
            {
                "tx_id": make_id("ach_tx", i),
                "rail": "ACH",
                "timestamp": rand_date_within_days(45),
                "user_id": u["user_id"],
                "device_id": u["device_id"],
                "country": u["country"],
                "amount": amount,
                "currency": "USD",
                # ACH-specific but normalized into shared schema
                "funding_speed": funding_speed,
                "return_code": return_code,
                # Card fields (unused for ACH)
                "card_present": None,
                "mcc": None,
                "bin": None,
                "is_new_device": None,
                # Crypto fields (unused for ACH)
                "from_wallet_id": None,
                "to_wallet_id": None,
                "wallet_age_days": None,
                "to_is_high_risk": None,
            }
        )

    return pd.DataFrame(rows)


def generate_card_transactions(users: pd.DataFrame, n: int = 2500) -> pd.DataFrame:
    """
    Generate CARD-style transactions. We'll include:
    - card_present (True/False)
    - MCC and BIN
    - is_new_device (simple proxy)
    """
    rows: List[Dict] = []

    # Example MCCs: add some known riskier categories
    mcc_options = [5411, 5812, 5999, 5732, 5967, 4829, 7995]
    mcc_weights = [0.25, 0.25, 0.12, 0.15, 0.08, 0.08, 0.07]

    # BINs: pretend these represent issuers; some bins appear more
    bin_options = [400001, 400002, 510001, 510002, 378001]
    bin_weights = [0.30, 0.15, 0.25, 0.20, 0.10]

    for i in range(n):
        u = users.sample(1).iloc[0]

        card_present = random.random() < 0.15  # most are online (False)
        amount = round(abs(random.gauss(mu=60, sigma=120)), 2)
        amount = max(1.0, min(amount, 3500.0))

        mcc = random.choices(mcc_options, weights=mcc_weights, k=1)[0]
        bin_num = random.choices(bin_options, weights=bin_weights, k=1)[0]

        # is_new_device: pretend newer accounts more often use "new device" (simple proxy)
        is_new_device = (u["account_age_days"] < 30) and (random.random() < 0.5)

        rows.append(
            {
                "tx_id": make_id("card_tx", i),
                "rail": "CARD",
                "timestamp": rand_date_within_days(45),
                "user_id": u["user_id"],
                "device_id": u["device_id"],
                "country": u["country"],
                "amount": amount,
                "currency": "USD",
                # ACH fields
                "funding_speed": None,
                "return_code": None,
                # Card-specific fields
                "card_present": bool(card_present),
                "mcc": int(mcc),
                "bin": int(bin_num),
                "is_new_device": bool(is_new_device),
                # Crypto fields
                "from_wallet_id": None,
                "to_wallet_id": None,
                "wallet_age_days": None,
                "to_is_high_risk": None,
            }
        )

    return pd.DataFrame(rows)


def generate_crypto_transactions(users: pd.DataFrame, wallets: pd.DataFrame, n: int = 1800) -> pd.DataFrame:
    """
    Generate CRYPTO-style transfers. We'll include:
    - from_wallet_id, to_wallet_id
    - wallet_age_days (age of FROM wallet)
    - to_is_high_risk (flag for known bad counterparty)
    """
    rows: List[Dict] = []

    # Create a set of "high risk" destination wallets (synthetic)
    high_risk_wallets = set(wallets.sample(int(len(wallets) * 0.03))["wallet_id"].tolist())

    for i in range(n):
        # Pick a sender wallet
        sender = wallets.sample(1).iloc[0]
        sender_user = users[users["user_id"] == sender["user_id"]].iloc[0]

        # Pick a recipient wallet (can be high-risk sometimes)
        recipient = wallets.sample(1).iloc[0]

        # Probability of sending to a high-risk wallet (small but present)
        if random.random() < 0.04:
            recipient_wallet_id = random.choice(list(high_risk_wallets))
            to_is_high_risk = True
        else:
            recipient_wallet_id = recipient["wallet_id"]
            to_is_high_risk = recipient_wallet_id in high_risk_wallets

        # Amount in "crypto units" (e.g., BTC). Keep small realistic-ish amounts, but with tails.
        amount = round(abs(random.gauss(mu=0.25, sigma=0.6)), 6)
        amount = max(0.0005, min(amount, 25.0))

        rows.append(
            {
                "tx_id": make_id("crypto_tx", i),
                "rail": "CRYPTO",
                "timestamp": rand_date_within_days(45),
                # Use the user who owns the sender wallet
                "user_id": sender["user_id"],
                "device_id": sender_user["device_id"],
                "country": sender_user["country"],
                "amount": amount,
                "currency": "CRYPTO",
                # ACH fields
                "funding_speed": None,
                "return_code": None,
                # Card fields
                "card_present": None,
                "mcc": None,
                "bin": None,
                "is_new_device": None,
                # Crypto fields
                "from_wallet_id": sender["wallet_id"],
                "to_wallet_id": recipient_wallet_id,
                "wallet_age_days": int(sender["wallet_age_days"]),
                "to_is_high_risk": bool(to_is_high_risk),
            }
        )

    return pd.DataFrame(rows)


# -----------------------------
# Pattern injection (optional but useful)
# -----------------------------

def inject_device_sharing(users: pd.DataFrame, share_rate: float = 0.08) -> pd.DataFrame:
    """
    Introduce a simple "device sharing" effect: some users share device_id values.
    This mimics situations like account farms or synthetic identity clusters.

    We do this by selecting some device_ids and assigning them to multiple users.
    """
    users = users.copy()

    n_share = int(len(users) * share_rate)
    if n_share <= 0:
        return users

    # Pick a few "shared" devices
    shared_devices = [f"dev_{random.randint(1, 30):05d}" for _ in range(max(3, n_share // 10))]

    # Assign shared devices to a subset of users
    idxs = users.sample(n_share).index
    for idx in idxs:
        users.loc[idx, "device_id"] = random.choice(shared_devices)

    return users


# -----------------------------
# Main
# -----------------------------

def main() -> None:
    ensure_data_dir()

    # 1) Create entities
    users = generate_users(n_users=500)

    # Inject some device sharing to create realistic clustering signals
    users = inject_device_sharing(users, share_rate=0.10)

    wallets = generate_wallets(users)

    # 2) Create transactions by rail
    ach = generate_ach_transactions(users, n=2000)
    card = generate_card_transactions(users, n=2500)
    crypto = generate_crypto_transactions(users, wallets, n=1800)

    # 3) Combine into one dataset
    combined = pd.concat([ach, card, crypto], ignore_index=True)

    # 4) Add a simple synthetic "is_fraud_pattern" label
    # This is NOT "real fraud detection" - it's a label we can use later to measure controls.
    combined["is_fraud_pattern"] = False

    # Label some patterns for evaluation:
    # - ACH: instant + high amount + new account
    ach_mask = (
        (combined["rail"] == "ACH")
        & (combined["funding_speed"] == "instant")
        & (combined["amount"] > 5000)
    )

    # - CARD: high amount + new device + not card present
    card_mask = (
        (combined["rail"] == "CARD")
        & (combined["card_present"] == False)  # noqa: E712 (intentional for pandas)
        & (combined["amount"] > 800)
        & (combined["is_new_device"] == True)  # noqa: E712
    )

    # - CRYPTO: to high risk counterparty OR new wallet + large send
    crypto_mask = (
        (combined["rail"] == "CRYPTO")
        & (
            (combined["to_is_high_risk"] == True)  # noqa: E712
            | ((combined["wallet_age_days"] < 7) & (combined["amount"] > 2.0))
        )
    )

    combined.loc[ach_mask | card_mask | crypto_mask, "is_fraud_pattern"] = True

    # 5) Save
    out_path = "data/combined_transactions.csv"
    combined.to_csv(out_path, index=False)

    print(f"✅ Wrote {len(combined):,} rows to {out_path}")
    print("Rails breakdown:")
    print(combined["rail"].value_counts().to_string())


if __name__ == "__main__":
    main()
