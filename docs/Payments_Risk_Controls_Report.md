# Payments Risk Controls – Analysis & Observations

This project simulates a config-driven payments risk controls system operating across ACH, card, and crypto rails using synthetic data only.

The goal is not to optimize fraud outcomes, but to demonstrate how controls behave once real-world volume, edge cases, and noise start showing up.

---

## Controls Overview

The system evaluates transactions against a small set of rules defined in YAML, each with:

- A target rail (ACH, CARD, CRYPTO)
- Threshold-based conditions
- An explicit action (ALLOW, REVIEW, BLOCK)

This mirrors how many risk teams separate policy definition from execution.

---

## Observations from the Dataset

### 1. Most transactions should pass

Roughly ~98% of transactions are allowed, which reflects how real controls systems are tuned. A controls engine that flags too much traffic quickly becomes operationally unusable.

### 2. BLOCK actions cluster around clear risk

BLOCK decisions are concentrated around:

- Large instant ACH pulls on new accounts
- Crypto transfers to known high-risk counterparties

These are intentionally high-confidence patterns where false positives are less acceptable.

### 3. REVIEW volume is small but important

REVIEW decisions are rarer and driven by:

- High-value online card transactions on new devices
- Large crypto sends from very new wallets

This reflects how REVIEW queues are typically used for ambiguous risk rather than obvious fraud.

### 4. Control noise is visible and measurable

The dashboard makes it easy to see:

- Which controls fire most often
- Which controls have weaker “precision” against the synthetic fraud label

This mirrors how risk teams identify noisy rules and decide whether to tune, scope, or retire them.

---

## Why This Matters

Building the system end-to-end made it easier to reason about:

- Where risk signals stop being useful at scale
- How thresholds interact across different rails
- Why explainability and observability matter as much as detection

This project is meant to mirror internal risk tooling rather than a data science model, and to provide concrete scenarios I can walk through during interviews using safe, synthetic data.
