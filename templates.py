from __future__ import annotations

TEMPLATE_LIBRARY: dict[str, list[dict[str, str]]] = {
    # ── Existing families (kept) ──────────────────────────────────────
    "mean_reversion": [
        {"template_id": "mr_01", "expression": "rank(ts_mean(close, {n}) - close)"},
        {"template_id": "mr_02", "expression": "rank(-ts_delta(close, {n}))"},
        {"template_id": "mr_03", "expression": "rank(-(close / ts_mean(close, {n}) - 1))"},
        {"template_id": "mr_04", "expression": "rank(-(returns - ts_mean(returns, {n})))"},
    ],
    "momentum": [
        {"template_id": "mom_01", "expression": "rank(ts_delta(close, {n}))"},
        {"template_id": "mom_02", "expression": "rank(ts_rank(close, {n}))"},
        {"template_id": "mom_03", "expression": "rank(ts_mean(returns, {n}))"},
    ],
    "volume_flow": [
        {"template_id": "vol_01", "expression": "rank(volume / ts_mean(volume, {n}))"},
        {"template_id": "vol_02", "expression": "rank(ts_delta(volume, {n}) * returns)"},
        {"template_id": "vol_03", "expression": "rank((volume / ts_mean(volume, {n})) * -returns)"},
    ],
    "vol_adjusted": [
        {"template_id": "va_01", "expression": "rank(ts_delta(close, {n}) / ts_std_dev(returns, {m}))"},
        {"template_id": "va_02", "expression": "rank((ts_mean(close, {n}) - close) / ts_std_dev(returns, {m}))"},
    ],
    "fundamental": [
        {"template_id": "fund_01", "expression": "rank({field})"},
        {"template_id": "fund_02", "expression": "rank(ts_delta({field}, {n}))"},
        {"template_id": "fund_03", "expression": "rank(({field} - ts_mean({field}, {n})))"},
    ],
    "conditional": [
        {"template_id": "cond_01", "expression": "trade_when(volume > ts_mean(volume, {n}), rank(-returns), -1)"},
        {"template_id": "cond_02", "expression": "trade_when(abs(returns) > ts_std_dev(returns, {n}), rank(-returns), -1)"},
        {"template_id": "cond_03", "expression": "trade_when(volume > ts_mean(volume, {n}), rank(ts_delta(close, {m})), -1)"},
    ],

    # ── NEW: Price-volume correlation ─────────────────────────────────
    # Measures how price and volume co-move — orthogonal to mean reversion
    "price_vol_corr": [
        {"template_id": "pvc_01", "expression": "-rank(ts_corr(rank(close), rank(volume), {n}))"},
        {"template_id": "pvc_02", "expression": "rank(ts_corr(ts_delta(close, {m}), volume, {n}))"},
        {"template_id": "pvc_03", "expression": "-rank(ts_corr(vwap, volume, {n}))"},
        {"template_id": "pvc_04", "expression": "-rank((close - vwap) / (vwap + 0.001) * log(volume + 1))"},
    ],

    # ── NEW: Volatility / distribution shape ──────────────────────────
    # Skewness, kurtosis, inverse vol — zero correlation with level signals
    # NOTE: ts_skewness and ts_kurtosis are LOCKED at your tier
    "volatility": [
        {"template_id": "vlty_01", "expression": "rank(1 / (ts_std_dev(returns, {n}) + 0.001))"},
        {"template_id": "vlty_04", "expression": "rank(ts_mean(returns, {m}) / (ts_std_dev(returns, {n}) + 0.001))"},
    ],

    # ── NEW: Intraday range / high-low-open ───────────────────────────
    # Uses high, low, open, vwap — data we've never explored
    "intraday": [
        {"template_id": "iday_01", "expression": "rank(-(high - low) / (close + 0.001))"},
        {"template_id": "iday_02", "expression": "rank((close - low) / (high - low + 0.001))"},
        {"template_id": "iday_03", "expression": "-rank(ts_mean(high - low, {n}) / (close + 0.001))"},
        {"template_id": "iday_04", "expression": "rank(open / close - 1)"},
        {"template_id": "iday_05", "expression": "-rank(ts_mean(open / close - 1, {n}))"},
    ],

    # ── NEW: Cross-sectional / group-based ────────────────────────────
    # group_rank within industry — different signal dimension
    "cross_sectional": [
        {"template_id": "cs_01", "expression": "-rank(ts_zscore(close, {n}))"},
        {"template_id": "cs_02", "expression": "-rank(ts_zscore(returns, {n}))"},
        {"template_id": "cs_03", "expression": "rank(ts_mean(returns, {m}) - ts_mean(returns, {n}))"},
        {"template_id": "cs_04", "expression": "group_rank(-ts_zscore(close, {n}), subindustry)"},
    ],

    # ── NEW: Fundamental value (deeper) ───────────────────────────────
    # Uses cashflow_op, ebitda, ebit, enterprise_value etc — verified data fields
    "fundamental_value": [
        {"template_id": "fv_01", "expression": "ts_zscore({deep_field} / (cap + 0.001), {n})"},
        {"template_id": "fv_02", "expression": "rank(ts_delta({deep_field}, {n}) / ({deep_field} + 0.001))"},
        {"template_id": "fv_03", "expression": "-rank(close / ({deep_field} + 0.001))"},
        {"template_id": "fv_04", "expression": "group_rank(-ts_zscore({deep_field}, {n}), industry)"},
    ],

    # ── NEW: Analyst / sentiment ──────────────────────────────────────
    # Analyst estimates + social sentiment — entirely new data category
    "analyst_sentiment": [
        {"template_id": "ans_01", "expression": "rank(ts_delta({analyst_field}, {n}))"},
        {"template_id": "ans_02", "expression": "-rank({sentiment_field} / (ts_mean({sentiment_field}, {n}) + 0.001))"},
        {"template_id": "ans_03", "expression": "rank(ts_zscore({analyst_field}, {n}))"},
    ],

    # ── NEW: Options / implied volatility ─────────────────────────────
    # IV spread signals from WQ Silver examples
    "options_vol": [
        {"template_id": "opt_01", "expression": "rank(implied_volatility_call_{opt_window} - implied_volatility_put_{opt_window})"},
        {"template_id": "opt_02", "expression": "rank(implied_volatility_call_{opt_window} / (historical_volatility_{opt_window} + 0.001))"},
    ],
}

# ── Field lists ───────────────────────────────────────────────────────

FUNDAMENTAL_FIELDS = ["cap", "assets", "sales", "income", "cash"]

DEEP_FUNDAMENTAL_FIELDS = [
    "cashflow_op", "ebit", "ebitda", "enterprise_value",
    "bookvalue_ps", "debt", "equity", "current_ratio",
    "eps", "capex", "cashflow", "cogs", "cash_st",
]

ANALYST_FIELDS = [
    "snt1_d1_earningssurprise", "snt1_d1_netearningsrevision",
    "snt1_d1_dynamicfocusrank", "consensus_analyst_rating",
    "snt1_d1_earningsrevision", "snt1_d1_stockrank",
]

SENTIMENT_FIELDS = ["scl12_buzz", "scl12_sentiment", "snt_social_value"]

# Options windows — must exist for BOTH implied_volatility_call AND historical_volatility
# historical_volatility available: 10, 20, 30, 60, 90, 120, 150, 180
# implied_volatility_call available: 10, 20, 30, 60, 90, 120, 150, 180, 270, 360, 720, 1080
OPTIONS_WINDOWS = [30, 60, 90, 120, 180]

SAFE_PARAM_RANGES = {
    "n": [3, 5, 10, 20, 40, 60],
    "m": [5, 10, 20, 60],
}
