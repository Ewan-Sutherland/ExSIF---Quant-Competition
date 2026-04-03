"""
v5.6.1: LLM-guided alpha expression generator.

Uses Google Gemini 2.5 Flash (free, 250 RPD) as primary,
Groq GPT-OSS 120B (free, 1000 RPD) as fallback.

Fixes over v5.6:
- Groq model: openai/gpt-oss-120b (was llama-4-scout-17b — much weaker)
- Temperature: 0.7 (was 0.9 — too creative, too many syntax errors)
- Comprehensive operator + field reference in system prompt
- Field name validation (rejects expressions with invalid field names)
- Failed expression feedback in prompt (LLM learns from errors)
"""
from __future__ import annotations

import os
import re
import time
import logging
from typing import Any

import requests

logger = logging.getLogger(__name__)

# ── Complete verified field set ─────────────────────────────────────
# Every field name the WQ API actually accepts. Used for validation.

VALID_FIELDS = {
    # Price Volume (pv1)
    "adv20", "cap", "close", "dividend", "high", "low", "open", "returns",
    "sharesout", "split", "volume", "vwap",
    # Group fields
    "industry", "subindustry", "sector", "market", "exchange",
    # Fundamental — common
    "assets", "assets_curr", "bookvalue_ps", "capex", "cash", "cash_st",
    "cashflow", "cashflow_dividends", "cashflow_fin", "cashflow_invst",
    "cashflow_op", "cogs", "current_ratio", "debt", "debt_lt", "debt_st",
    "depre_amort", "ebit", "ebitda", "employee", "enterprise_value", "eps",
    "equity", "income", "sales",
    # Fundamental Scores (model16)
    "fscore_bfl_value", "fscore_bfl_momentum", "fscore_bfl_quality",
    "fscore_bfl_growth", "fscore_bfl_profitability", "fscore_bfl_total",
    "fscore_bfl_surface", "fscore_bfl_surface_accel",
    "fscore_value", "fscore_momentum", "fscore_quality", "fscore_growth",
    "fscore_profitability", "fscore_total", "fscore_surface", "fscore_surface_accel",
    "analyst_revision_rank_derivative", "cashflow_efficiency_rank_derivative",
    "composite_factor_score_derivative", "earnings_certainty_rank_derivative",
    "growth_potential_rank_derivative", "multi_factor_acceleration_score_derivative",
    "multi_factor_static_score_derivative", "relative_valuation_rank_derivative",
    # Research Sentiment (sentiment1)
    "snt1_cored1_score", "snt1_d1_analystcoverage", "snt1_d1_buyrecpercent",
    "snt1_d1_downtargetpercent", "snt1_d1_dtstsespe", "snt1_d1_dynamicfocusrank",
    "snt1_d1_earningsrevision", "snt1_d1_earningssurprise", "snt1_d1_earningstorpedo",
    "snt1_d1_fundamentalfocusrank", "snt1_d1_longtermepsgrowthest",
    "snt1_d1_netearningsrevision", "snt1_d1_netrecpercent", "snt1_d1_nettargetpercent",
    "snt1_d1_sellrecpercent", "snt1_d1_stockrank", "snt1_d1_uptargetpercent",
    "consensus_analyst_rating",
    # SM Sentiment (socialmedia12)
    "scl12_buzz", "scl12_buzz_fast_d1", "scl12_sentiment", "scl12_sentiment_fast_d1",
    "snt_buzz", "snt_buzz_bfl", "snt_buzz_bfl_fast_d1", "snt_buzz_fast_d1",
    "snt_buzz_ret", "snt_buzz_ret_fast_d1", "snt_value", "snt_value_fast_d1",
    "snt_social_value",
    # Analyst Estimates — key fields
    "est_ptp", "est_fcf", "est_cashflow_op", "est_capex",
    # Ravenpack News — key event sentiment fields
    "rp_css_earnings", "rp_css_revenue", "rp_css_dividends", "rp_css_mna",
    "rp_css_credit", "rp_css_price", "rp_css_product", "rp_css_technical",
    "rp_ess_earnings", "rp_ess_revenue", "rp_ess_dividends", "rp_ess_mna",
    "rp_ess_credit", "rp_ess_price", "rp_ess_product", "rp_ess_technical",
    "nws18_ber", "nws18_bee", "nws18_bam", "nws18_ssc", "nws18_sse",
    # US News Data — key fields
    "news_pct_1min", "news_pct_5_min", "news_pct_10min", "news_pct_30min",
    "news_pct_60min", "news_pct_90min", "news_pct_120min",
    "news_max_up_ret", "news_max_dn_ret", "news_max_up_amt", "news_max_dn_amt",
    "news_open_gap", "news_ls",
}

# Dynamically add volatility/options fields with all known windows
for _prefix in ["implied_volatility_call_", "implied_volatility_put_",
                "implied_volatility_mean_", "implied_volatility_mean_skew_",
                "historical_volatility_"]:
    for _w in [10, 20, 30, 60, 90, 120, 150, 180, 270, 360, 720, 1080]:
        VALID_FIELDS.add(f"{_prefix}{_w}")

for _prefix in ["call_breakeven_", "forward_price_", "option_breakeven_",
                "pcr_oi_", "pcr_vol_"]:
    for _w in [10, 20, 30, 60, 90, 120, 150, 180, 270, 360, 720, 1080]:
        VALID_FIELDS.add(f"{_prefix}{_w}")

# v5.9: Add Parkinson volatility fields
for _w in [10, 20, 30, 60, 90, 120, 150, 180]:
    VALID_FIELDS.add(f"parkinson_volatility_{_w}")

# v5.9: model77 pre-computed anomaly fields (Analysts Factor Model)
VALID_FIELDS.update({
    "standardized_unexpected_earnings", "standardized_unexpected_earnings_2",
    "earnings_momentum_composite_score", "earnings_momentum_analyst_score",
    "earnings_revision_magnitude", "sales_surprise_score",
    "change_in_eps_surprise", "net_fy1_analyst_revisions",
    "three_month_fy1_eps_revision", "six_month_avg_fy1_eps_revision",
    "forward_median_earnings_yield", "normalized_earnings_yield",
    "forward_cash_flow_to_price", "forward_ebitda_to_enterprise_value_2",
    "tobins_q_ratio", "financial_statement_value_score",
    "equity_value_score", "income_statement_value_score",
    "gross_profit_to_assets_ratio", "gross_profit_margin_ttm_2",
    "cash_flow_return_on_invested_capital", "cash_earnings_return_on_equity",
    "return_on_invested_capital_4", "fcf_yield_times_forward_roe",
    "asset_growth_rate", "one_year_change_total_assets",
    "sustainable_growth_rate", "reinvestment_rate",
    "distress_risk_measure", "credit_risk_premium_indicator",
    "twelve_month_short_interest_change",
    "value_momentum_analyst_score", "momentum_analyst_composite_score",
    "price_momentum_module_score", "fundamental_growth_module_score",
    "trailing_twelve_month_accruals",
    "standardized_unexpected_cash_flow", "standardized_unexpected_cashflow",
    "book_leverage_ratio_3", "interest_coverage_ratio_5",
    "yearly_change_leverage", "twelve_month_total_debt_change_2",
    "five_year_eps_stability", "one_year_eps_growth_rate",
    "forward_two_year_eps_growth", "one_year_ahead_eps_growth",
    "one_quarter_ahead_eps_growth", "long_term_growth_estimate",
    "capex_to_total_assets", "capex_to_depreciation_linkage",
    "ttm_operating_cash_flow_to_price", "ttm_operating_income_to_ev",
    "ttm_sales_to_enterprise_value",
    "implied_minus_realized_volatility_2", "implied_option_volatility",
    "out_of_money_put_call_ratio",
    "industry_relative_return_4w", "industry_relative_return_5d",
    "industry_relative_book_to_market", "industry_relative_fcf_to_price",
    "cash_burn_rate", "inventory_change_avg_assets",
    "rd_expense_to_sales_2", "visibility_ratio", "treynor_ratio",
})

# v5.9: Relationship / supply chain data (pv13)
VALID_FIELDS.update({
    "rel_ret_cust", "rel_ret_comp", "rel_ret_all", "rel_ret_part",
    "rel_num_cust", "rel_num_comp", "rel_num_all", "rel_num_part",
    "pv13_ustomergraphrank_page_rank", "pv13_ustomergraphrank_hub_rank",
    "pv13_ustomergraphrank_auth_rank", "pv13_com_page_rank",
})

# v5.9: Risk Metrics (model51)
VALID_FIELDS.update({
    "beta_last_30_days_spy", "beta_last_60_days_spy",
    "beta_last_90_days_spy", "beta_last_360_days_spy",
    "correlation_last_30_days_spy", "correlation_last_60_days_spy",
    "correlation_last_90_days_spy", "correlation_last_360_days_spy",
    "systematic_risk_last_30_days", "systematic_risk_last_60_days",
    "systematic_risk_last_90_days", "systematic_risk_last_360_days",
    "unsystematic_risk_last_30_days", "unsystematic_risk_last_60_days",
    "unsystematic_risk_last_90_days", "unsystematic_risk_last_360_days",
})

# v5.9: Expanded Fundamental fields
VALID_FIELDS.update({
    "operating_income", "retained_earnings", "working_capital",
    "inventory_turnover", "rd_expense", "goodwill", "revenue",
    "return_assets", "return_equity", "sales_ps", "sales_growth",
    "sga_expense", "ppent", "pretax_income", "invested_capital",
    "operating_expense", "income_beforeextra", "income_tax",
    "interest_expense", "receivable", "liabilities", "liabilities_curr",
    "fn_liab_fair_val_l1_a", "fn_liab_fair_val_l1_q",
    "fn_liab_fair_val_a", "fn_liab_fair_val_q",
})

# v5.9: Additional analyst estimate fields
VALID_FIELDS.update({
    "est_eps", "est_epsr", "est_fcf", "est_fcf_ps", "est_ptp",
    "est_cashflow_op", "est_capex", "est_ebit", "est_ebitda",
    "est_sales", "est_netprofit", "est_dividend_ps",
})

# v6.2.1: Vector dataset fields (multiple values per stock per day — use vec_avg/vec_sum ONLY)
VALID_FIELDS.update({
    # Social media vectors
    "scl12_alltype_buzzvec", "scl12_alltype_sentvec",
    # News vectors
    "nws12_afterhsz_sl", "nws12_prez_result2",
    # v6.2.1: scl15_d1_sentiment, fnd6_epsfx, fnd6_newqeventv110_optrfrq — ALL DEAD, removed
})

# v6.2.1: model_data (mdf_*), mdl175_*, fam_* — ALL DEAD on this account, removed from VALID_FIELDS

# v6.2.1: New untapped fields for 10 new template families
VALID_FIELDS.update({
    # Supply chain (pv13)
    "pv13_custretsig_retsig",
    # RavenPack category sentiment
    "rp_ess_insider", "rp_css_legal", "rp_nip_earnings", "nws18_event_relevance",
    "rp_css_mna", "rp_ess_mna", "rp_css_revenue", "rp_ess_revenue",
    "rp_css_product", "rp_ess_product", "rp_css_credit",
    "rp_css_dividends", "rp_ess_dividends", "rp_nip_mna",
    "rp_css_insider", "rp_nip_revenue", "rp_nip_legal",
    # Options analytics — breakeven & forward price
    "put_breakeven_30", "put_breakeven_60", "put_breakeven_90",
    "put_breakeven_120", "put_breakeven_180",
    "option_breakeven_30", "option_breakeven_60", "option_breakeven_90",
    "option_breakeven_120", "option_breakeven_180",
    # Fundamental scores (model16)
    "fscore_bfl_growth", "fscore_bfl_surface",
    "growth_potential_rank_derivative", "composite_factor_score_derivative",
    "analyst_revision_rank_derivative", "cashflow_efficiency_rank_derivative",
    "earnings_certainty_rank_derivative", "relative_valuation_rank_derivative",
    "multi_factor_acceleration_score_derivative", "multi_factor_static_score_derivative",
    # Risk metrics — full field names
    "beta_last_30_days_spy", "beta_last_90_days_spy", "beta_last_360_days_spy",
    "correlation_last_30_days_spy", "correlation_last_90_days_spy", "correlation_last_360_days_spy",
    "systematic_risk_last_30_days", "systematic_risk_last_90_days", "systematic_risk_last_360_days",
    "unsystematic_risk_last_30_days", "unsystematic_risk_last_90_days", "unsystematic_risk_last_360_days",
    # Historical volatility
    "historical_volatility_10", "historical_volatility_20", "historical_volatility_30",
    "historical_volatility_60", "historical_volatility_90", "historical_volatility_120",
    # Deep analyst sentiment
    "snt1_d1_earningstorpedo", "snt1_d1_uptargetpercent", "snt1_d1_downtargetpercent",
    "snt1_d1_analystcoverage", "snt1_d1_longtermepsgrowthest", "snt1_d1_stockrank",
    "snt1_d1_earningsrevision", "snt1_d1_fundamentalfocusrank",
    "snt1_cored1_score", "snt1_d1_netrecpercent", "snt1_d1_nettargetpercent",
    # Social media scalar
    "scl12_buzz", "scl12_buzz_fast_d1", "scl12_sentiment", "scl12_sentiment_fast_d1",
    "snt_value", "snt_value_fast_d1", "snt_buzz_ret", "snt_buzz_ret_fast_d1",
    "snt_buzz", "snt_buzz_fast_d1", "snt_buzz_bfl", "snt_buzz_bfl_fast_d1",
})


# ── Operators ───────────────────────────────────────────────────────

VALID_OPERATORS = {
    "rank", "group_rank", "ts_mean", "ts_std_dev", "ts_zscore", "ts_rank",
    "ts_delta", "ts_decay_linear", "ts_corr", "ts_sum",
    "ts_arg_min", "ts_arg_max", "ts_covariance", "ts_product", "ts_backfill",
    "ts_count_nans", "ts_regression", "ts_step", "ts_delay", "ts_scale",
    "ts_quantile",
    "trade_when", "if_else",  # v6.2.1: if_else is base-level conditional (no position holding)
    "abs", "log", "sign", "max", "min", "power",
    "is_nan", "bucket", "densify", "winsorize", "normalize",
    "group_neutralize", "group_zscore", "group_scale", "group_backfill", "group_mean",
    "scale", "quantile", "zscore",
    "vec_avg", "vec_sum",  # v6.2.1: ONLY these two vec_ operators work
    "signed_power", "sqrt", "inverse", "reverse", "hump",
    # v6.2.1: removed ts_min, ts_max, ts_argmin, ts_argmax — DON'T EXIST on platform
    # v6.2.1: last_diff_value, days_from_last_change — exist but LLM misuses them, kept banned
    # v6.2.1: pasteurize — inaccessible at base level
    "kth_element",
    "ts_av_diff",  # v6.2.1: ts_mean(x,n) - x — proven in mdf_eg3 alpha (S=1.59)
}

LOCKED_OPERATORS = {"ts_skewness", "ts_kurtosis", "ts_momentum"}

# v6.2: Fields that don't exist in WQ BRAIN but LLMs keep generating
BANNED_FIELDS = {
    "unsystematic_risk",   # correct form is unsystematic_risk_last_30_days etc.
    "systematic_risk",     # correct form is systematic_risk_last_30_days etc.
    "short_interest",      # not a valid field name
    "institutional_ownership",  # not available
    "put_call_ratio",      # use pcr_oi_30 etc. instead
    # v6.2.1: ALL confirmed dead fields from overnight logs
    "mdf_nps", "mdf_oey", "mdf_rds", "mdf_pbk", "mdf_eg3", "mdf_sg3",  # model_data family ALL dead
    "mdl175_grossprofit", "mdl175_revenuettm", "mdl175_volatility",  # mdl175 ALL dead
    "fnd6_epsfx",          # event_driven dead
    "fam_earn_surp_pct", "fam_roe_rank",  # event_driven dead
    "scl15_d1_sentiment",  # doesn't exist — not in any dataset
}

# v6.2.1: Operators that are inaccessible or broken at base level
BANNED_OPERATORS = {
    "vec_count", "vec_ir", "vec_max", "vec_min", "vec_stddev", "vec_range",  # only vec_avg and vec_sum work
    "pasteurize",          # inaccessible at base level
    "last_diff_value",     # needs lookback param we don't provide
    "days_from_last_change",  # needs lookback param we don't provide
}


# ── Comprehensive system prompt ─────────────────────────────────────

SYSTEM_PROMPT = """You are an expert quantitative researcher generating alpha expressions for WorldQuant BRAIN's IQC 2026 competition.

Your task: generate alpha expressions that predict which stocks will outperform tomorrow.
Output ONLY raw FastExpression syntax — no Python, no pseudocode.

═══════════════════════════════════════════════════
THINKING PROCESS (follow this for EVERY expression)
═══════════════════════════════════════════════════

Step 1: Pick a MARKET INEFFICIENCY from the UNEXPLORED list below.
  You MUST draw from these underexplored signal families — do NOT generate
  signals from the SATURATED list at the bottom.

  UNEXPLORED FAMILIES (prioritise in this order):
  1. SUPPLY CHAIN (fewest users on entire platform — academic backing):
     rel_ret_cust, rel_ret_comp, pv13_com_page_rank, pv13_custretsig_retsig
     IDEA: Customer returns predict supplier returns with 5-20 day lag (Cohen & Frazzini)
  2. RAVENPACK CATEGORY SENTIMENT (category-specific news, not generic):
     rp_ess_mna, rp_ess_earnings, rp_css_credit, rp_css_legal, rp_ess_insider, rp_nip_earnings
     IDEA: Novelty-weighted sentiment (rp_nip × rp_ess) outperforms raw sentiment
     Always ts_backfill(field, 60) — sparse data
  3. OPTIONS ANALYTICS (breakeven/forward price — almost nobody uses):
     call_breakeven_30, call_breakeven_60, call_breakeven_120, forward_price_60
     put_breakeven_30, put_breakeven_60, option_breakeven_60
     IDEA: call_breakeven/close - 1 = where options market expects the stock
  4. HISTORICAL VOLATILITY (vol risk premium — most robust anomaly in finance):
     historical_volatility_20, historical_volatility_60, historical_volatility_90
     implied_volatility_mean_60, implied_volatility_mean_skew_60
     IDEA: IV/HV spread — stocks where IV >> HV have highest risk premium
  5. RISK METRICS (low-beta anomaly — strongest cross-sectional factor):
     beta_last_60_days_spy, unsystematic_risk_last_60_days, systematic_risk_last_60_days
     correlation_last_30_days_spy, correlation_last_360_days_spy
     IDEA: -rank(beta) = betting against beta; high idio vol stocks underperform
  6. FUNDAMENTAL SCORES (pre-computed quality composites):
     fscore_bfl_quality, fscore_bfl_total, fscore_bfl_value, fscore_bfl_profitability
     growth_potential_rank_derivative, composite_factor_score_derivative
     IDEA: ts_delta(fscore, 63) captures improving fundamentals. Sparse — use ts_backfill(field, 60)
  7. DEEP ANALYST SENTIMENT (mostly untapped snt1_d1_* fields):
     snt1_d1_earningstorpedo, snt1_d1_uptargetpercent, snt1_d1_downtargetpercent,
     snt1_d1_analystcoverage, snt1_d1_longtermepsgrowthest, snt1_d1_stockrank
     IDEA: -rank(earningstorpedo) shorts stocks at risk of breaking earnings streaks
  8. SOCIAL MEDIA SCALAR (non-vector buzz/sentiment):
     scl12_buzz, scl12_sentiment, snt_value, snt_buzz_ret
     IDEA: High buzz + low sentiment = overreaction → short; Low buzz = opportunity → long
  9. VECTOR DATA (use vec_avg/vec_sum ONLY):
     scl12_alltype_buzzvec, scl12_alltype_sentvec, nws12_afterhsz_sl
     PROVEN: ts_av_diff(ts_backfill(-vec_sum(scl12_alltype_buzzvec), 20), 60) → S=1.94
  10. INTRADAY PATTERNS (open/high/low/close relationships):
     IDEA: -(open - ts_delay(close,1)) = overnight gap reversal
     IDEA: (close-open)/(high-low+0.001) = candle body ratio (buying pressure)
  11. OPTIONS IV SPREAD (proven portfolio-additive):
     implied_volatility_call_120 / parkinson_volatility_120 (PROVEN +48 score points)
     pcr_oi_{30,60,180,270}, IV call-put spread

Step 2: Choose 1-2 data fields that CAPTURE that inefficiency.
Step 3: Apply the MINIMUM operators needed — simpler = better fitness.
Step 4: Verify the expression is structurally different from the submitted list.

ALREADY SATURATED — DO NOT USE AS PRIMARY SIGNAL:
  debt, ts_zscore(debt, *), close/debt
  ts_mean(returns, *), ts_zscore(returns, *), ts_decay_linear(rank(-rank(ts_zscore(returns,*))))
  operating_income/cap, ts_rank(operating_income/cap, 252)
  est_eps/close, group_rank(ts_rank(est_eps/close, 60), industry)
  (close-vwap)/vwap as standalone signal
  one_year_change_total_assets, asset_growth_rate as standalone

═══════════════════════════════════════════════════
OPERATOR REFERENCE
═══════════════════════════════════════════════════

ARITHMETIC: +, -, *, /, abs(x), log(x), sign(x), max(x,y), min(x,y), power(x,n)

RANKING (cross-sectional):
  rank(x)                         — rank stocks 0 to 1
  group_rank(x, group)            — rank within group (industry/subindustry/sector/market)

TIME SERIES (per-stock):
  ts_mean(x, n), ts_std_dev(x, n), ts_zscore(x, n), ts_rank(x, n)
  ts_delta(x, n), ts_decay_linear(x, n), ts_corr(x, y, n)
  ts_sum(x, n), ts_min(x, n), ts_max(x, n)
  ts_argmin(x, n), ts_argmax(x, n), ts_covariance(x, y, n)
  ts_backfill(x, n)               — fill NaN (REQUIRED for options/news/social/model data)
  ts_regression(y, x, n, rettype) — rettype=2 for slope
  ts_step(n)                      — sequence 1..n for trend regression
  ts_scale(x, n)                  — scale to sum to 1
  ts_av_diff(x, n)                — ts_mean(x,n) - x (mean deviation)

VECTOR (for multi-value fields like buzzvec, sentvec, nws12):
  vec_avg(x), vec_sum(x)  — ONLY these two work at base level

CONDITIONAL:
  trade_when(cond, x, default)    — use x when cond is true, else default (holds position)
  if_else(cond, x, y)             — returns x when cond is true, y when false (no position holding)

OTHER: winsorize(x, std=4), densify(x), hump(x, hump=0.01) — hump reduces turnover (improves fitness)

LOCKED — DO NOT USE: ts_skewness, ts_kurtosis, ts_momentum

═══════════════════════════════════════════════════
DATA FIELDS
═══════════════════════════════════════════════════

PRICE & VOLUME: close, open, high, low, vwap, returns, volume, adv20, cap, sharesout

FUNDAMENTAL (quarterly, use 60-252 day lookbacks):
  assets, sales, income, cash, debt, equity, eps, ebitda, ebit, enterprise_value,
  bookvalue_ps, capex, cashflow, cashflow_op, cogs, current_ratio,
  assets_curr, debt_lt, debt_st, depre_amort, employee, retained_earnings,
  operating_income, gross_profit, inventory_turnover, rd_expense

FUNDAMENTAL SCORES (model16 — sparse, use ts_backfill or multiply by rank(cap)):
  fscore_bfl_value, fscore_bfl_momentum, fscore_bfl_quality, fscore_bfl_growth,
  fscore_bfl_profitability, fscore_bfl_total, fscore_bfl_surface, fscore_bfl_surface_accel

PRE-COMPUTED ANOMALIES (model77 — ready to use):
  earnings_momentum_composite_score, earnings_momentum_analyst_score,
  five_year_eps_stability, forward_ebitda_to_enterprise_value_2,
  forward_cash_flow_to_price, cash_burn_rate, fcf_yield_times_forward_roe,
  sustainable_growth_rate, normalized_earnings_yield,
  gross_profit_to_assets_ratio, asset_growth_rate, industry_relative_return_5d,
  gross_profit_margin_ttm_2

ANALYST ESTIMATES: est_eps, est_ptp, est_fcf, est_cashflow_op, est_capex

RESEARCH SENTIMENT (daily):
  snt1_d1_earningssurprise, snt1_d1_netearningsrevision, snt1_d1_dynamicfocusrank,
  snt1_d1_buyrecpercent, snt1_d1_sellrecpercent, consensus_analyst_rating

SOCIAL MEDIA: scl12_buzz, scl12_sentiment, snt_social_value

OPTIONS (sparse — ALWAYS use ts_backfill):
  implied_volatility_call_{30,60,120,180}, implied_volatility_put_{30,60,120,180},
  implied_volatility_mean_skew_{30,60,120}, parkinson_volatility_{60,120}
  pcr_oi_{30,60,180,270}, pcr_vol_{30,60,180,270}

RAVENPACK NEWS (sparse — ALWAYS use ts_backfill):
  rp_ess_earnings, rp_ess_revenue, rp_css_earnings,
  news_pct_1min, news_max_up_ret, news_max_dn_ret

SUPPLY CHAIN: rel_ret_cust, rel_ret_comp, rel_num_cust, rel_num_comp

RISK: beta_last_60_days_spy, unsystematic_risk_last_60_days, systematic_risk_last_60_days
  (NOTE: always use the full field name with window suffix — bare "unsystematic_risk" is INVALID)

VECTOR DATASETS (use vec_avg or vec_sum ONLY):
  scl12_alltype_buzzvec, scl12_alltype_sentvec, nws12_afterhsz_sl

HISTORICAL VOLATILITY (NEW — completely untapped):
  historical_volatility_10, historical_volatility_20, historical_volatility_60, historical_volatility_90

OPTIONS ANALYTICS (NEW — breakeven/forward price, almost nobody uses these):
  call_breakeven_30, call_breakeven_60, call_breakeven_120
  forward_price_30, forward_price_60, forward_price_120
  put_breakeven_30, put_breakeven_60

DEEP ANALYST SENTIMENT (NEW — mostly untapped):
  snt1_d1_earningstorpedo, snt1_d1_uptargetpercent, snt1_d1_downtargetpercent,
  snt1_d1_analystcoverage, snt1_d1_longtermepsgrowthest, snt1_d1_stockrank,
  snt1_d1_earningsrevision, snt1_d1_fundamentalfocusrank

═══════════════════════════════════════════════════
PATTERNS THAT ACTUALLY PASS (Sharpe > 1.25, Fitness > 1.0)
═══════════════════════════════════════════════════

MULTI-FACTOR (combine 2 DIFFERENT data sources — prioritise cross-family):
  rank(signal_A * signal_B)          — multiplicative RAW (best — captures interaction effects)
  rank(signal_A) * rank(signal_B)    — multiplicative ranked (good)
  rank(signal_A) + rank(signal_B)    — additive (baseline, weaker than multiplicative)
  IDEAL PAIRINGS (unexplored × already-working):
    vector buzz × short-term reversion
    model data × earnings revision
    event timing × fundamental value
    options IV ratio × volume anomaly
    supply chain return × analyst sentiment

CONDITIONAL ENTRY:
  trade_when(vol_condition, rank(signal), -1) — only trade during vol regimes

INDUSTRY-RELATIVE:
  group_rank(signal, industry)  — outperforms rank() for fundamentals 78% of the time

CRITICAL RULES:
  1. Every expression MUST contain rank() or group_rank()
  2. Options/news/sentiment/social/model data: ALWAYS wrap in ts_backfill(field, 60) first
  3. Fundamental scores (fscore_*): ALWAYS multiply by rank(cap) or rank(adv20)
  4. Keep it simple: 1-3 nested calls. Complex = overfit = low fitness.
  5. Output ONLY the raw expression — no numbering, no explanation, no markdown
"""


def _build_generation_prompt(
    *,
    submitted_exprs: list[str],
    best_near_passers: list[dict],
    underexplored_categories: list[str],
    recent_failures: list[dict],
    recent_eligible_count: int,
    recently_generated: list[str] | None = None,
    num_expressions: int = 5,
) -> str:
    """Build hypothesis-first user prompt with portfolio context."""

    submitted_section = ""
    if submitted_exprs:
        submitted_section = "ALREADY SUBMITTED — do NOT generate variants of these:\n"
        for expr in submitted_exprs[:20]:
            submitted_section += f"  {expr}\n"

    near_passer_section = ""
    if best_near_passers:
        near_passer_section = "\nNEAR-PASSERS — these almost worked, learn from them:\n"
        for np in best_near_passers[:6]:
            near_passer_section += (
                f"  S={np.get('sharpe', 0):.2f} F={np.get('fitness', 0):.2f} "
                f"Fail={np.get('reason', '?')} → {np.get('expression', '?')}\n"
            )

    failure_section = ""
    if recent_failures:
        failure_section = "\nSYNTAX ERRORS — avoid these mistakes:\n"
        for f in recent_failures[:4]:
            failure_section += (
                f"  {f.get('error', '?')} → {f.get('expression', '?')[:60]}\n"
            )

    underexplored_section = ""
    if underexplored_categories:
        underexplored_section = (
            f"\nUNDEREXPLORED CATEGORIES — prioritize these: {', '.join(underexplored_categories)}\n"
        )

    # v6.2: Tell LLM what was already generated this session to prevent repeats
    recently_generated_section = ""
    if recently_generated:
        recently_generated_section = "\nALREADY GENERATED THIS SESSION — do NOT repeat these:\n"
        for expr in recently_generated[-15:]:
            recently_generated_section += f"  {expr[:100]}\n"

    return f"""Generate {num_expressions} alpha expressions. Each must target a DIFFERENT market anomaly.

{submitted_section}
{near_passer_section}
{failure_section}
{underexplored_section}
{recently_generated_section}

OVERUSED FIELDS — do NOT use these, they've been tried hundreds of times:
  cashflow_efficiency_rank_derivative, growth_potential_rank_derivative, operating_income

⚠️ PORTFOLIO-ADDITIVE PRIORITY:
Our portfolio is SATURATED with price_returns and model77 signals. New alphas using those
HURT our score even if they pass individually. The ONLY way to gain points is through
genuinely different data:
  1. VECTOR DATA: vec_sum(scl12_alltype_buzzvec), vec_avg(nws12_afterhsz_sl) (PROVEN S=1.94)
  2. MODEL DATA: mdf_nps, mdf_eg3, mdf_oey, mdl175_* (PROVEN S=1.59)
  3. EVENT-DRIVEN: fnd6_epsfx, fam_earn_surp_pct, days_from_last_change (PROVEN S=2.03)
  4. OPTIONS: implied_volatility_call_120 / parkinson_volatility_120 ratio (PROVEN +48 pts)
  5. NEWS: rp_ess_earnings, rp_css_earnings, news_max_up_ret, news_pct_1min
  6. SENTIMENT: snt1_d1_earningssurprise, snt1_d1_netearningsrevision, scl12_sentiment

WINNING PATTERN EXAMPLES:
  rank(ts_rank(ts_backfill(fnd6_epsfx, 60) / close, 40))  → S=2.03
  ts_av_diff(ts_backfill(-vec_sum(scl12_alltype_buzzvec), 20), 60)  → S=1.94
  rank(-ts_av_diff(ts_backfill(mdf_eg3, 60), 50) * ts_corr(ts_backfill(mdf_eg3, 60), ts_backfill(mdf_sg3, 60), 50))  → S=1.59

At least 3 of your {num_expressions} expressions MUST use VECTOR, MODEL, EVENT, OPTIONS, or NEWS data.
Combine them with fundamentals or reversion for the best results.

Rules:
- Each expression uses DIFFERENT primary data fields from every other expression
- Each expression uses DIFFERENT data from the already-submitted list above
- Prefer multiplicative combos: rank(A * B) or rank(A) * rank(B)
- Keep it simple: 1-3 function calls max
- Total portfolio: {recent_eligible_count} alphas. We need DIVERSE data sources.

CRITICAL: Output EXACTLY {num_expressions} raw expressions, one per line.
NO explanations, NO numbering, NO bullet points, NO markdown.
Just {num_expressions} lines of pure FastExpression code."""


# ── API clients ──────────────────────────────────────────────────────

class LLMClient:
    """Handles API calls to Gemini and Groq with automatic fallback and key rotation."""

    def __init__(self):
        # v6.2.1: Support multiple Gemini API keys for rate limit rotation
        # Set in .env as: GEMINI_API_KEYS=key1,key2,key3,key4,key5
        # Falls back to single GEMINI_API_KEY if GEMINI_API_KEYS not set
        multi_keys = os.environ.get("GEMINI_API_KEYS", "")
        if multi_keys:
            self.gemini_keys = [k.strip() for k in multi_keys.split(",") if k.strip()]
        else:
            single = os.environ.get("GEMINI_API_KEY", "")
            self.gemini_keys = [single] if single else []

        # Same for Groq — GROQ_API_KEYS=key1,key2 or single GROQ_API_KEY
        multi_groq = os.environ.get("GROQ_API_KEYS", "")
        if multi_groq:
            self.groq_keys = [k.strip() for k in multi_groq.split(",") if k.strip()]
        else:
            single_groq = os.environ.get("GROQ_API_KEY", "")
            self.groq_keys = [single_groq] if single_groq else []

        # Per-key tracking
        self._gemini_calls = [0] * len(self.gemini_keys)
        self._gemini_rate_limited_until = [0.0] * len(self.gemini_keys)  # timestamp
        self._groq_calls = [0] * len(self.groq_keys)
        self._groq_rate_limited_until = [0.0] * len(self.groq_keys)
        self._last_reset_day = 0

        # Log key count
        if self.gemini_keys:
            print(f"[LLM] {len(self.gemini_keys)} Gemini API key(s) loaded — rotating on rate limit")
        else:
            print("[LLM_WARN] No GEMINI_API_KEY(S) set")
        if self.groq_keys:
            print(f"[LLM] {len(self.groq_keys)} Groq API key(s) loaded")
        else:
            print("[LLM_WARN] No GROQ_API_KEY(S) set — no fallback when Gemini rate-limits!")

    @property
    def gemini_key(self):
        """Legacy compat — return first key or empty."""
        return self.gemini_keys[0] if self.gemini_keys else ""

    @property
    def groq_key(self):
        return self.groq_keys[0] if self.groq_keys else ""

    def _reset_daily_counters(self):
        today = int(time.time() // 86400)
        if today != self._last_reset_day:
            self._gemini_calls = [0] * len(self.gemini_keys)
            self._groq_calls = [0] * len(self.groq_keys)
            self._last_reset_day = today

    def generate(self, system_prompt: str, user_prompt: str) -> str | None:
        """Try all Gemini keys, then all Groq keys. Returns raw text or None."""
        self._reset_daily_counters()
        now = time.time()

        # Try each Gemini key — skip rate-limited ones
        for i, key in enumerate(self.gemini_keys):
            if self._gemini_calls[i] >= 240:  # 250 RPD limit with buffer
                continue
            if now < self._gemini_rate_limited_until[i]:
                continue  # still in cooldown for this key

            result = self._call_gemini(key, system_prompt, user_prompt)
            if result is not None:
                self._gemini_calls[i] += 1
                return result
            else:
                # Mark this key as rate-limited for 60s
                self._gemini_rate_limited_until[i] = now + 60

        # Fallback: try each Groq key
        for i, key in enumerate(self.groq_keys):
            if self._groq_calls[i] >= 950:  # 1000 RPD limit with buffer
                continue
            if now < self._groq_rate_limited_until[i]:
                continue

            result = self._call_groq(key, system_prompt, user_prompt)
            if result is not None:
                self._groq_calls[i] += 1
                return result
            else:
                self._groq_rate_limited_until[i] = now + 60

        return None

    def _call_gemini(self, api_key: str, system_prompt: str, user_prompt: str) -> str | None:
        """Call Google Gemini 2.5 Flash via REST API."""
        try:
            url = (
                f"https://generativelanguage.googleapis.com/v1beta/models/"
                f"gemini-2.5-flash:generateContent?key={api_key}"
            )
            payload = {
                "systemInstruction": {"parts": [{"text": system_prompt}]},
                "contents": [{"parts": [{"text": user_prompt}]}],
                "generationConfig": {
                    "temperature": 0.7,
                    "maxOutputTokens": 1500,
                },
            }
            resp = requests.post(url, json=payload, timeout=30)

            if resp.status_code == 429:
                print("[LLM] Gemini rate limited")
                return None
            if resp.status_code != 200:
                print(f"[LLM] Gemini error {resp.status_code}: {resp.text[:200]}")
                return None

            data = resp.json()
            candidates = data.get("candidates", [])
            if not candidates:
                return None

            text = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "")
            return text.strip() if text else None

        except Exception as exc:
            print(f"[LLM] Gemini exception: {exc}")
            return None

    def _call_groq(self, api_key: str, system_prompt: str, user_prompt: str) -> str | None:
        """Call Groq GPT-OSS 120B (OpenAI-compatible). Prompt caching is automatic."""
        try:
            url = "https://api.groq.com/openai/v1/chat/completions"
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            }
            payload = {
                "model": "openai/gpt-oss-120b",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "temperature": 0.7,
                "max_tokens": 1500,
            }
            resp = requests.post(url, json=payload, headers=headers, timeout=30)

            if resp.status_code == 429:
                print("[LLM] Groq rate limited")
                return None
            if resp.status_code != 200:
                print(f"[LLM] Groq error {resp.status_code}: {resp.text[:200]}")
                return None

            data = resp.json()
            choices = data.get("choices", [])
            if not choices:
                return None

            text = choices[0].get("message", {}).get("content", "")
            return text.strip() if text else None

        except Exception as exc:
            print(f"[LLM] Groq exception: {exc}")
            return None

    @property
    def available(self) -> bool:
        return bool(self.gemini_key or self.groq_key)


# ── Expression validation ────────────────────────────────────────────

def validate_expression(expr: str) -> tuple[bool, str]:
    """
    Validate that an LLM-generated expression is syntactically plausible
    AND uses real WQ data fields.
    Returns (is_valid, reason).
    """
    expr = expr.strip()

    if not expr:
        return False, "empty"

    if len(expr) > 500:
        return False, "too_long"

    if len(expr) < 8:
        return False, "too_short"

    # Must not contain Python/code artifacts
    for bad in ["import ", "def ", "print(", "return ", "lambda ", "class ", "#", "//", "```", "\"", "'"]:
        if bad in expr:
            return False, f"contains_code: {bad}"

    # Must not start with a number (likely a list item)
    if re.match(r"^\d+[\.\):]", expr):
        return False, "starts_with_number"

    # Check balanced parentheses
    depth = 0
    for ch in expr:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if depth < 0:
            return False, "unbalanced_parens"
    if depth != 0:
        return False, "unbalanced_parens"

    # Must contain at least one function call
    if "(" not in expr:
        return False, "no_function_calls"

    # Check for locked operators
    expr_lower = expr.lower()
    for locked in LOCKED_OPERATORS:
        if locked in expr_lower:
            return False, f"locked_operator: {locked}"

    # v6.2: Check for banned fields (invalid WQ variables the LLM keeps generating)
    for banned in BANNED_FIELDS:
        if banned in expr_lower:
            return False, f"banned_field: {banned}"

    # v6.2.1: Check for banned operators (inaccessible at base level)
    for banned in BANNED_OPERATORS:
        if banned + "(" in expr_lower:
            return False, f"banned_operator: {banned}"

    # Must contain rank() somewhere
    if "rank" not in expr_lower and "group_rank" not in expr_lower:
        return False, "no_rank"

    # Field validation: extract all potential field names and check at least one is valid
    # Extract words that could be field names (not operators, not numbers, not keywords)
    tokens = re.findall(r'[a-zA-Z_][a-zA-Z0-9_]*', expr)
    non_operator_tokens = [
        t for t in tokens
        if t not in VALID_OPERATORS
        and t not in LOCKED_OPERATORS
        and t not in {"rettype", "range", "true", "false", "True", "False"}
        and not t.startswith("ts_")
        and not t.startswith("vec_")
    ]

    if non_operator_tokens:
        # At least one non-operator token must be a valid field
        valid_count = sum(1 for t in non_operator_tokens if t in VALID_FIELDS)
        if valid_count == 0:
            bad_fields = list(set(non_operator_tokens))[:3]
            return False, f"no_valid_fields: {bad_fields}"

    return True, "ok"


def parse_expressions(raw_text: str) -> list[str]:
    """
    Parse LLM output into individual expressions.
    Handles numbered lists, bullet points, and raw lines.
    """
    valid, _ = parse_expressions_with_errors(raw_text)
    return valid


def parse_expressions_with_errors(raw_text: str) -> tuple[list[str], list[tuple[str, str]]]:
    """
    Parse LLM output, returning (valid_expressions, [(failed_expr, reason), ...]).
    Used for self-correcting retry.
    """
    if not raw_text:
        return [], []

    lines = raw_text.strip().split("\n")
    expressions = []
    failures = []

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Strip common prefixes: "1. ", "- ", "* ", "1) ", etc.
        line = re.sub(r"^[\d]+[\.\):\-]\s*", "", line)
        line = re.sub(r"^[-*•]\s*", "", line)
        line = line.strip()

        # Strip markdown code backticks
        line = line.strip("`")
        line = line.strip()

        if not line:
            continue

        # Skip lines that look like explanations
        lower = line.lower()
        if any(w in lower for w in [
            "this ", "the ", "here ", "note:", "explanation",
            "because", "where ", "uses ", "combines ", "measures ",
            "//", "/*", "output:", "expression", "alpha",
        ]):
            continue

        valid, reason = validate_expression(line)
        if valid:
            expressions.append(line)
        else:
            failures.append((line, reason))

    return expressions, failures


# ── Main generator class ─────────────────────────────────────────────

class LLMAlphaGenerator:
    """
    Generates alpha expressions using LLM APIs.
    Integrated into the bot's candidate generation flow.
    """

    def __init__(self):
        self.client = LLMClient()
        self._cache: list[str] = []
        self._total_generated = 0
        self._total_valid = 0
        self._total_api_calls = 0
        self._total_failed_calls = 0
        self._recent_failures: list[dict] = []  # Track failed expressions for feedback
        self._last_api_call_time: float = 0.0  # v5.9: Rate limit cooldown
        self._session_generated_cores: list[str] = []  # v6.2: Track recently generated for dedup prompt

    @property
    def available(self) -> bool:
        return self.client.available

    def record_failure(self, expression: str, error: str):
        """Record a failed expression so the LLM can learn from it."""
        self._recent_failures.append({
            "expression": expression,
            "error": error,
        })
        # Keep only last 10 failures
        if len(self._recent_failures) > 10:
            self._recent_failures = self._recent_failures[-10:]

    def get_expression(
        self,
        *,
        submitted_exprs: list[str] | None = None,
        best_near_passers: list[dict] | None = None,
        underexplored_categories: list[str] | None = None,
        recent_eligible_count: int = 0,
    ) -> str | None:
        """
        Get a single novel expression. Uses cached buffer when available,
        refills from LLM when empty.
        """
        if self._cache:
            return self._cache.pop(0)

        self._refill_cache(
            submitted_exprs=submitted_exprs or [],
            best_near_passers=best_near_passers or [],
            underexplored_categories=underexplored_categories or [],
            recent_eligible_count=recent_eligible_count,
        )

        if self._cache:
            return self._cache.pop(0)

        return None

    def _refill_cache(
        self,
        submitted_exprs: list[str],
        best_near_passers: list[dict],
        underexplored_categories: list[str],
        recent_eligible_count: int,
    ) -> None:
        """Call LLM to generate a batch of expressions."""
        # v5.9: Rate limit cooldown — prevent Gemini 429 errors
        import config as _cfg
        cooldown = getattr(_cfg, "LLM_COOLDOWN_SECONDS", 30)
        now = time.time()
        elapsed = now - self._last_api_call_time
        if elapsed < cooldown:
            wait = cooldown - elapsed
            print(f"[LLM] Cooldown: waiting {wait:.0f}s before next API call")
            time.sleep(wait)

        self._last_api_call_time = time.time()
        self._total_api_calls += 1

        user_prompt = _build_generation_prompt(
            submitted_exprs=submitted_exprs,
            best_near_passers=best_near_passers,
            underexplored_categories=underexplored_categories,
            recent_failures=self._recent_failures,
            recent_eligible_count=recent_eligible_count,
            recently_generated=self._session_generated_cores,
            num_expressions=6,
        )

        raw = self.client.generate(SYSTEM_PROMPT, user_prompt)
        if raw is None:
            self._total_failed_calls += 1
            print("[LLM_GEN] API call failed — no expressions generated")
            return

        expressions, failures = parse_expressions_with_errors(raw)
        self._total_generated += len(expressions)
        self._total_valid += len(expressions)

        # v6.1: Self-correcting retry — feed errors back to LLM for fixing
        max_retries = getattr(_cfg, "LLM_AST_RETRY_MAX", 1)
        if failures and len(expressions) < 3 and max_retries > 0:
            retry_prompt = "These expressions had syntax errors. Fix each one and output ONLY the corrected expressions, one per line:\n\n"
            for failed_expr, reason in failures[:4]:
                retry_prompt += f"  ERROR: {reason}\n  EXPRESSION: {failed_expr}\n\n"
            retry_prompt += "Output ONLY corrected expressions — no explanation, no numbering."

            # Respect cooldown
            time.sleep(max(2, cooldown - (time.time() - self._last_api_call_time)))
            self._last_api_call_time = time.time()
            self._total_api_calls += 1

            retry_raw = self.client.generate(SYSTEM_PROMPT, retry_prompt)
            if retry_raw:
                fixed = parse_expressions(retry_raw)
                if fixed:
                    expressions.extend(fixed)
                    self._total_generated += len(fixed)
                    self._total_valid += len(fixed)
                    print(f"[LLM_AST_RETRY] Fixed {len(fixed)}/{len(failures)} failed expressions")

        if expressions:
            self._cache.extend(expressions)
            # v6.2: Track generated cores to prevent repeats in next API call
            for expr in expressions:
                core = expr.strip().lower()
                if core not in self._session_generated_cores:
                    self._session_generated_cores.append(core)
            # Keep last 30
            self._session_generated_cores = self._session_generated_cores[-30:]
            print(
                f"[LLM_GEN] Generated {len(expressions)} valid expressions "
                f"(api_calls={self._total_api_calls} total_valid={self._total_valid} "
                f"failed_calls={self._total_failed_calls})"
            )
            for i, expr in enumerate(expressions):
                print(f"  [LLM_EXPR_{i}] {expr}")
        else:
            self._total_failed_calls += 1
            print(f"[LLM_GEN] No valid expressions from LLM output. Raw: {raw[:300]}")

    def stats(self) -> dict[str, int]:
        return {
            "total_generated": self._total_generated,
            "total_valid": self._total_valid,
            "total_api_calls": self._total_api_calls,
            "total_failed_calls": self._total_failed_calls,
            "cache_size": len(self._cache),
            "tracked_failures": len(self._recent_failures),
        }
