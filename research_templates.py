"""
v7.2: Research-backed mega template library.

~112 families, ~1000+ templates across 22 academic factor categories + 5 gap areas.
Every template uses fresh (unsaturated) fields and valid FASTEXPR operators.

Generated from 6 deep research sessions cross-referencing:
  - 5,903 available fields (2,496 team / 3,408 Ewan-only)
  - 60 valid FASTEXPR operators
  - 36 saturated fields AVOIDED
  - Academic factor literature

Usage:
  from research_templates import RESEARCH_TEMPLATES, RESEARCH_NEUTRALIZATIONS
  from research_templates import RESEARCH_WEIGHTS, EWAN_ONLY_FAMILIES
  # Merge into main TEMPLATE_LIBRARY at startup
"""

# ══════════════════════════════════════════════════════════════════
# TEMPLATE LIBRARY
# ══════════════════════════════════════════════════════════════════

RESEARCH_TEMPLATES = {

    # ============================================================
    # CATEGORY 1: VALUE
    # ============================================================

    "value_book": [
        {"template_id": "vb_01", "expression": "rank(equity / (cap + 0.001))"},
        {"template_id": "vb_02", "expression": "group_zscore(equity / (cap + 0.001), subindustry)"},
        {"template_id": "vb_03", "expression": "ts_decay_linear(rank(equity / (cap + 0.001)), 40)"},
        {"template_id": "vb_04", "expression": "signed_power(group_zscore(bookvalue_ps / (close + 0.001), industry), 0.5)"},
        {"template_id": "vb_05", "expression": "ts_quantile(equity / (cap + 0.001), 252)"},
        {"template_id": "vb_06", "expression": "-ts_corr(rank(equity / (cap + 0.001)), ts_delta(close, 20), 60)"},
        {"template_id": "vb_07", "expression": "group_zscore(ts_delta(equity / (cap + 0.001), 60), industry)"},
        {"template_id": "vb_08", "expression": "group_rank(ts_mean(equity, 120) / (ts_mean(cap, 5) + 0.001), sector)"},
        {"template_id": "vb_09", "expression": "ts_rank(bookvalue_ps / (close + 0.001), 252)"},
        {"template_id": "vb_10", "expression": "rank(equity / (cap + 0.001)) * rank(-returns)"},
    ],

    "value_earnings_yield": [
        {"template_id": "vey_01", "expression": "rank(ebit / (cap + 0.001))"},
        {"template_id": "vey_02", "expression": "group_zscore(income / (cap + 0.001), subindustry)"},
        {"template_id": "vey_03", "expression": "ts_decay_linear(rank(ebitda / (cap + 0.001)), 20)"},
        {"template_id": "vey_04", "expression": "signed_power(rank(eps / (close + 0.001)), 0.5)"},
        {"template_id": "vey_05", "expression": "ts_quantile(ebit / (cap + 0.001), 252)"},
        {"template_id": "vey_06", "expression": "-ts_corr(rank(income / (cap + 0.001)), ts_delta(close, 10), 40)"},
        {"template_id": "vey_07", "expression": "ts_delta(group_zscore(ebit / (cap + 0.001), industry), 60)"},
        {"template_id": "vey_08", "expression": "winsorize(group_zscore(eps / (close + 0.001), industry))"},
        {"template_id": "vey_09", "expression": "ts_av_diff(group_rank(ebitda / (cap + 0.001), sector), 120)"},
        {"template_id": "vey_10", "expression": "rank(ebitda / (cap + 0.001)) * rank(-ts_mean(returns, 5))"},
    ],

    "value_cashflow_yield": [
        {"template_id": "vcf_01", "expression": "rank(ts_backfill(cashflow_op, 60) / (cap + 0.001))"},
        {"template_id": "vcf_02", "expression": "group_zscore(ts_backfill(cashflow, 60) / (cap + 0.001), subindustry)"},
        {"template_id": "vcf_03", "expression": "ts_decay_linear(rank(ts_backfill(ebitda, 60) / (cap + 0.001)), 40)"},
        {"template_id": "vcf_04", "expression": "signed_power(rank(ts_backfill(cashflow_op, 60) / (cap + 0.001)), 0.5)"},
        {"template_id": "vcf_05", "expression": "ts_quantile(ts_backfill(cashflow, 60) / (cap + 0.001), 252)"},
        {"template_id": "vcf_06", "expression": "-ts_corr(rank(ts_backfill(cashflow_op, 60) / (cap + 0.001)), ts_delta(close, 20), 60)"},
        {"template_id": "vcf_07", "expression": "ts_delta(rank(ts_backfill(ebitda, 60) / (cap + 0.001)), 60)"},
        {"template_id": "vcf_08", "expression": "rank(ts_delta(ts_backfill(cashflow, 60), 120) / (ts_backfill(cashflow, 60) + 0.001))"},
        {"template_id": "vcf_09", "expression": "rank(ts_backfill(cashflow_op, 60) / (cap + 0.001)) * rank(-returns)"},
        {"template_id": "vcf_10", "expression": "winsorize(group_zscore(ts_backfill(cashflow, 60) / (cap + 0.001), industry))"},
    ],

    "value_dividend": [
        {"template_id": "vdv_01", "expression": "rank(dividend / (close + 0.001))"},
        {"template_id": "vdv_02", "expression": "group_zscore(dividend / (close + 0.001), subindustry)"},
        {"template_id": "vdv_03", "expression": "ts_decay_linear(rank(dividend / (close + 0.001)), 20)"},
        {"template_id": "vdv_04", "expression": "signed_power(rank(est_eps / (close + 0.001)), 0.5)"},
        {"template_id": "vdv_05", "expression": "ts_quantile(dividend / (close + 0.001), 252)"},
        {"template_id": "vdv_06", "expression": "-ts_corr(rank(dividend / (close + 0.001)), ts_delta(close, 20), 60)"},
        {"template_id": "vdv_07", "expression": "ts_rank(est_eps / (close + 0.001), 120)"},
        {"template_id": "vdv_08", "expression": "trade_when(ts_delta(dividend / (close + 0.001), 5) > 0, rank(dividend / (close + 0.001)), -1)"},
        {"template_id": "vdv_09", "expression": "ts_quantile(dividend / (close + 0.001), 252)"},
        {"template_id": "vdv_10", "expression": "signed_power(rank(est_eps / (close + 0.001)), 0.5)"},
    ],

    "m77_value": [
        {"template_id": "m7v_01", "expression": "rank(forward_median_earnings_yield)"},
        {"template_id": "m7v_02", "expression": "group_rank(forward_median_earnings_yield, industry)"},
        {"template_id": "m7v_03", "expression": "rank(equity_value_score)"},
        {"template_id": "m7v_04", "expression": "rank(normalized_earnings_yield)"},
        {"template_id": "m7v_05", "expression": "rank(income_statement_value_score)"},
        {"template_id": "m7v_06", "expression": "group_zscore(forward_median_earnings_yield, sector)"},
        {"template_id": "m7v_07", "expression": "ts_rank(normalized_earnings_yield, 252)"},
        {"template_id": "m7v_08", "expression": "signed_power(rank(forward_median_earnings_yield), 0.5) * signed_power(rank(-returns), 0.5)"},
        {"template_id": "m7v_09", "expression": "-ts_delta(tobins_q_ratio, 60)"},
        {"template_id": "m7v_10", "expression": "ts_quantile(equity_value_score, 252)"},
    ],

    # ============================================================
    # CATEGORY 2: PROFITABILITY
    # ============================================================

    "profit_gross": [
        {"template_id": "pg_01", "expression": "rank((revenue - cogs) / (equity + 0.001))"},
        {"template_id": "pg_02", "expression": "group_zscore((revenue - cogs) / (equity + 0.001), industry)"},
        {"template_id": "pg_03", "expression": "ts_delta((revenue - cogs) / (equity + 0.001), 252)"},
        {"template_id": "pg_04", "expression": "ts_decay_linear(rank((revenue - cogs) / (revenue + 0.001)), 60)"},
        {"template_id": "pg_05", "expression": "signed_power(rank((revenue - cogs) / (cap + 0.001)), 0.5)"},
        {"template_id": "pg_06", "expression": "ts_quantile((revenue - cogs) / (equity + 0.001), 252)"},
        {"template_id": "pg_07", "expression": "rank((revenue - cogs) / (equity + 0.001)) * rank(-returns)"},
        {"template_id": "pg_08", "expression": "-ts_corr(rank((revenue - cogs) / (cap + 0.001)), rank(returns), 120)"},
        {"template_id": "pg_09", "expression": "ts_zscore((revenue - cogs) / (equity + 0.001), 252)"},
        {"template_id": "pg_10", "expression": "group_rank(ts_delta((revenue - cogs) / (revenue + 0.001), 120), subindustry)"},
    ],

    "profit_margins": [
        {"template_id": "pm_01", "expression": "rank(ebitda / (revenue + 0.001))"},
        {"template_id": "pm_02", "expression": "group_rank(ebit / (revenue + 0.001), industry)"},
        {"template_id": "pm_03", "expression": "ts_rank(ebitda / (revenue + 0.001), 252)"},
        {"template_id": "pm_04", "expression": "rank(ts_delta(ebitda / (revenue + 0.001), 60))"},
        {"template_id": "pm_05", "expression": "rank(income / (revenue + 0.001))"},
        {"template_id": "pm_06", "expression": "group_rank(income / (revenue + 0.001), subindustry)"},
        {"template_id": "pm_07", "expression": "rank(ebitda / (revenue + 0.001)) * rank(-ts_mean(returns, 5))"},
        {"template_id": "pm_08", "expression": "ts_decay_linear(rank(ebit / (revenue + 0.001)), 60)"},
        {"template_id": "pm_09", "expression": "ts_zscore(ebitda / (revenue + 0.001), 252)"},
        {"template_id": "pm_10", "expression": "ts_delta(rank(ebit / (revenue + 0.001)), 120)"},
    ],

    "profit_return_on_capital": [
        {"template_id": "prc_01", "expression": "rank(ebit / (equity + debt_lt + debt_st + 0.001))"},
        {"template_id": "prc_02", "expression": "ts_zscore(income / (equity + 0.001), 252)"},
        {"template_id": "prc_03", "expression": "group_zscore(ebit / (equity + debt_lt + 0.001), industry)"},
        {"template_id": "prc_04", "expression": "ts_delta(income / (equity + 0.001), 252)"},
        {"template_id": "prc_05", "expression": "signed_power(rank(ebit / (equity + debt_lt + debt_st + 0.001)), 0.5)"},
        {"template_id": "prc_06", "expression": "ts_decay_linear(rank(ebit / (equity + debt_lt + 0.001)), 60)"},
        {"template_id": "prc_07", "expression": "group_rank(ts_delta(ebit / (equity + 0.001), 120), subindustry)"},
        {"template_id": "prc_08", "expression": "ts_quantile(income / (equity + debt_lt + debt_st + 0.001), 252)"},
        {"template_id": "prc_09", "expression": "group_rank(ts_delta(ebit / (equity + 0.001), 126), subindustry)"},
        {"template_id": "prc_10", "expression": "ts_quantile(income / (equity + debt_lt + debt_st + 0.001), 504)"},
    ],

    "profit_cash_return_quarterly": [
        {"template_id": "pcr_01", "expression": "rank(ts_backfill(cashflow_op, 60) / (ts_backfill(sales, 60) + 0.001))"},
        {"template_id": "pcr_02", "expression": "ts_zscore(ts_backfill(cashflow, 60) / (ts_backfill(sales, 60) + 0.001), 252)"},
        {"template_id": "pcr_03", "expression": "group_zscore(ts_backfill(cashflow_op, 60) / (ts_backfill(ebitda, 60) + 0.001), industry)"},
        {"template_id": "pcr_04", "expression": "ts_delta(ts_backfill(cashflow, 60) / (ts_backfill(sales, 60) + 0.001), 252)"},
        {"template_id": "pcr_05", "expression": "signed_power(rank(ts_backfill(cashflow_op, 60) / (ts_backfill(income, 60) + 0.001)), 0.5)"},
        {"template_id": "pcr_06", "expression": "ts_decay_linear(rank(ts_backfill(cashflow, 60) / (ts_backfill(ebitda, 60) + 0.001)), 60)"},
        {"template_id": "pcr_07", "expression": "group_rank(ts_backfill(cashflow, 60) / (ts_backfill(sales, 60) + 0.001), subindustry)"},
        {"template_id": "pcr_08", "expression": "ts_quantile(ts_backfill(cashflow_op, 60) / (ts_backfill(ebitda, 60) + 0.001), 252)"},
        {"template_id": "pcr_09", "expression": "group_rank(ts_backfill(cashflow, 60) / (ts_backfill(sales, 60) + 0.001), subindustry)"},
        {"template_id": "pcr_10", "expression": "ts_quantile(ts_backfill(cashflow_op, 60) / (ts_backfill(ebitda, 60) + 0.001), 504)"},
    ],

    "m77_profitability": [
        {"template_id": "m7p_01", "expression": "rank((revenue - cogs) / (assets + 0.001))"},
        {"template_id": "m7p_02", "expression": "group_zscore(cashflow_op / (assets + 0.001), industry)"},
        {"template_id": "m7p_03", "expression": "ts_zscore((revenue - cogs) / (assets + 0.001), 252)"},
        {"template_id": "m7p_04", "expression": "ts_decay_linear(rank(normalized_earnings_yield), 60)"},
        {"template_id": "m7p_05", "expression": "signed_power(rank(cash_earnings_return_on_equity), 0.5)"},
        {"template_id": "m7p_06", "expression": "-ts_corr((revenue - cogs) / (assets + 0.001), cashflow_op / (assets + 0.001), 252)"},
        {"template_id": "m7p_07", "expression": "group_rank(ts_delta((revenue - cogs) / (assets + 0.001), 252), subindustry)"},
        {"template_id": "m7p_08", "expression": "ts_quantile(cashflow_op / (assets + 0.001), 252)"},
        {"template_id": "m7p_09", "expression": "rank(ts_zscore((revenue - cogs) / (assets + 0.001), 120)) + rank(ts_zscore(cashflow_op / (assets + 0.001), 120))"},
        {"template_id": "m7p_10", "expression": "rank((revenue - cogs) / (assets + 0.001)) * rank(normalized_earnings_yield)"},
    ],

    # ============================================================
    # CATEGORY 3: QUALITY
    # ============================================================

    "quality_accruals": [
        {"template_id": "qa_01", "expression": "-rank((income - ebitda + capex) / (equity + 0.001))"},
        {"template_id": "qa_02", "expression": "-group_zscore((capex - depre_amort) / (equity + 0.001), industry)"},
        {"template_id": "qa_03", "expression": "-rank(ts_delta(working_capital, 252) / (equity + 0.001))"},
        {"template_id": "qa_04", "expression": "-ts_decay_linear(rank((income - ebitda + capex) / (revenue + 0.001)), 252)"},
        {"template_id": "qa_05", "expression": "-signed_power(group_zscore(ts_delta(working_capital, 252) / (cap + 0.001), sector), 0.5)"},
        {"template_id": "qa_06", "expression": "-ts_zscore((income - ebitda + capex) / (equity + 0.001), 252)"},
        {"template_id": "qa_07", "expression": "-group_rank(ts_delta(income - ebitda + capex, 252) / (revenue + 0.001), subindustry)"},
        {"template_id": "qa_08", "expression": "-ts_quantile(ts_delta(working_capital, 252) / (equity + 0.001), 252)"},
        {"template_id": "qa_09", "expression": "rank((ebitda - capex) / (abs(income) + 0.001))"},
        {"template_id": "qa_10", "expression": "-rank(ts_delta(working_capital / (abs(ebitda) + 0.001), 252))"},
    ],

    "quality_earnings_stability": [
        {"template_id": "qes_01", "expression": "-rank(ts_std_dev(eps, 252))"},
        {"template_id": "qes_02", "expression": "rank(ts_mean(eps, 252) / (ts_std_dev(eps, 252) + 0.001))"},
        {"template_id": "qes_03", "expression": "rank(ts_corr(ebitda, revenue, 252))"},
        {"template_id": "qes_04", "expression": "-group_zscore(ts_std_dev(income / (revenue + 0.001), 252), industry)"},
        {"template_id": "qes_05", "expression": "rank(inverse(ts_std_dev(ebitda / (revenue + 0.001), 252) + 0.001))"},
        {"template_id": "qes_06", "expression": "-ts_decay_linear(rank(ts_std_dev((revenue - cogs) / (revenue + 0.001), 252)), 60)"},
        {"template_id": "qes_07", "expression": "group_zscore(ts_mean(eps, 252) / (ts_std_dev(eps, 252) + 0.001), subindustry)"},
        {"template_id": "qes_08", "expression": "rank(ts_zscore(income / (revenue + 0.001), 252))"},
        {"template_id": "qes_09", "expression": "rank(ts_mean(ebitda / (revenue + 0.001), 504) * ts_corr(income, revenue, 504))"},
        {"template_id": "qes_10", "expression": "group_zscore(ts_mean(eps, 504) * inverse(ts_std_dev(eps, 504) + 0.001), subindustry)"},
    ],

    "quality_balance_sheet_quarterly": [
        {"template_id": "qbs_01", "expression": "-rank(ts_backfill(debt, 60) / (ts_backfill(cashflow_op, 60) + 0.001))"},
        {"template_id": "qbs_02", "expression": "rank(ts_delta(ts_backfill(working_capital, 60), 252))"},
        {"template_id": "qbs_03", "expression": "rank(ts_backfill(cash, 60) / (ts_backfill(debt, 60) + 0.001))"},
        {"template_id": "qbs_04", "expression": "-group_zscore(ts_backfill(interest_expense, 60) / (ts_backfill(cashflow_op, 60) + 0.001), industry)"},
        {"template_id": "qbs_05", "expression": "rank(ts_zscore(ts_backfill(working_capital, 60) / (ts_backfill(debt, 60) + 0.001), 252))"},
        {"template_id": "qbs_06", "expression": "ts_decay_linear(rank(ts_backfill(cashflow_op, 60) / (ts_backfill(debt, 60) + 0.001)), 120)"},
        {"template_id": "qbs_07", "expression": "-rank(ts_delta(ts_backfill(debt, 60) / (abs(ts_backfill(income, 60)) + 0.001), 252))"},
        {"template_id": "qbs_08", "expression": "group_rank(ts_backfill(cashflow_op, 60) - ts_backfill(interest_expense, 60), subindustry)"},
        {"template_id": "qbs_09", "expression": "-signed_power(group_zscore(ts_backfill(debt, 60) / (ts_backfill(cash, 60) + 0.001), sector), 0.5)"},
        {"template_id": "qbs_10", "expression": "rank(ts_corr(ts_backfill(cash, 60), ts_backfill(income, 60), 252))"},
    ],

    "quality_cash_earnings": [
        {"template_id": "qce_01", "expression": "rank((ebitda - capex) / (abs(income) + 0.001))"},
        {"template_id": "qce_02", "expression": "-rank((income - ebitda + capex) / (abs(ebitda) + 0.001))"},
        {"template_id": "qce_03", "expression": "group_zscore((income + depre_amort - capex) / (abs(income) + 0.001), industry)"},
        {"template_id": "qce_04", "expression": "-rank(ts_delta(working_capital / (abs(ebitda) + 0.001), 252))"},
        {"template_id": "qce_05", "expression": "rank(ts_corr(ebitda - capex, income, 252))"},
        {"template_id": "qce_06", "expression": "-ts_zscore(working_capital / (abs(ebitda) + 0.001), 252)"},
        {"template_id": "qce_07", "expression": "ts_decay_linear(rank((ebitda - capex) / (abs(income) + 0.001)), 120)"},
        {"template_id": "qce_08", "expression": "group_rank(ts_mean((income + depre_amort - capex) / (abs(income) + 0.001), 252), subindustry)"},
        {"template_id": "qce_09", "expression": "signed_power(rank((ebitda - capex) / (abs(income) + 0.001)) - 0.5, 0.5)"},
        {"template_id": "qce_10", "expression": "if_else(current_ratio > 1, rank((ebitda - capex) / (abs(income) + 0.001)), -rank(abs(income - ebitda + capex) / (abs(ebitda) + 0.001)))"},
    ],

    "m77_quality": [
        {"template_id": "m7q_01", "expression": "-rank(trailing_twelve_month_accruals)"},
        {"template_id": "m7q_02", "expression": "rank(financial_statement_value_score)"},
        {"template_id": "m7q_03", "expression": "-ts_zscore(earnings_torpedo_indicator, 252)"},
        {"template_id": "m7q_04", "expression": "rank(change_in_eps_surprise + standardized_unexpected_earnings)"},
        {"template_id": "m7q_05", "expression": "-group_zscore(book_leverage_ratio_3, industry)"},
        {"template_id": "m7q_06", "expression": "-ts_decay_linear(rank(debt / (equity + 0.001)), 120)"},
        {"template_id": "m7q_07", "expression": "rank(visibility_ratio * financial_statement_value_score)"},
        {"template_id": "m7q_08", "expression": "group_rank(-trailing_twelve_month_accruals + standardized_unexpected_earnings, subindustry)"},
        {"template_id": "m7q_09", "expression": "ts_decay_linear(rank(financial_statement_value_score) - rank(debt / (equity + 0.001)), 120)"},
        {"template_id": "m7q_10", "expression": "-signed_power(rank(credit_risk_premium_indicator), 0.5)"},
    ],

    # ============================================================
    # CATEGORY 4: INVESTMENT/GROWTH
    # ============================================================

    "invest_asset_growth": [
        {"template_id": "iag_01", "expression": "-rank(ts_delta(equity, 252) / (ts_delay(equity, 252) + 0.001))"},
        {"template_id": "iag_02", "expression": "-group_zscore(ts_delta(equity, 252) / (ts_delay(equity, 252) + 0.001), sector)"},
        {"template_id": "iag_03", "expression": "-ts_decay_linear(rank(ts_delta(equity, 252) / (ts_delay(equity, 252) + 0.001)), 60)"},
        {"template_id": "iag_04", "expression": "-ts_zscore(ts_delta(equity, 252) / (ts_delay(equity, 252) + 0.001), 252)"},
        {"template_id": "iag_05", "expression": "-signed_power(group_zscore(ts_delta(equity, 252) / (ts_delay(equity, 252) + 0.001), industry), 0.5)"},
        {"template_id": "iag_06", "expression": "-ts_quantile(ts_delta(equity, 120) / (ts_delay(equity, 120) + 0.001), 252)"},
        {"template_id": "iag_07", "expression": "-rank(ts_delta(retained_earnings, 252) / (revenue + 0.001))"},
        {"template_id": "iag_08", "expression": "inverse(group_rank(ts_delta(equity, 252) / (ts_delay(equity, 252) + 0.001), subindustry) + 0.001)"},
        {"template_id": "iag_09", "expression": "-ts_quantile(ts_delta(equity, 126) / (ts_delay(equity, 126) + 0.001), 504)"},
        {"template_id": "iag_10", "expression": "-rank(ts_delta(retained_earnings, 252) / (revenue + 0.001))"},
    ],

    "invest_capex": [
        {"template_id": "icx_01", "expression": "-rank(capex / (revenue + 0.001))"},
        {"template_id": "icx_02", "expression": "-group_zscore(capex / (depre_amort + 0.001), industry)"},
        {"template_id": "icx_03", "expression": "rank(depre_amort / (capex + 0.001))"},
        {"template_id": "icx_04", "expression": "-ts_decay_linear(ts_delta(capex / (revenue + 0.001), 252), 20)"},
        {"template_id": "icx_05", "expression": "-ts_zscore(capex / (ebitda + 0.001), 252)"},
        {"template_id": "icx_06", "expression": "rank(ebitda / (capex + 0.001)) * rank(revenue / (equity + 0.001))"},
        {"template_id": "icx_07", "expression": "-ts_corr(rank(capex / (revenue + 0.001)), rank(cap), 252)"},
        {"template_id": "icx_08", "expression": "-signed_power(group_zscore(ts_delta(capex, 252) / (ts_delay(capex, 252) + 0.001), subindustry), 0.5)"},
        {"template_id": "icx_09", "expression": "group_rank(revenue / (capex + rd_expense + 0.001), subindustry)"},
        {"template_id": "icx_10", "expression": "rank(rd_expense / (revenue + 0.001)) * rank((revenue - cogs) / (revenue + 0.001))"},
    ],

    "invest_net_issuance": [
        {"template_id": "ini_01", "expression": "-rank(ts_delta(sharesout, 252) / (ts_delay(sharesout, 252) + 0.001))"},
        {"template_id": "ini_02", "expression": "-group_zscore(ts_delta(sharesout, 252) / (ts_delay(sharesout, 252) + 0.001), industry)"},
        {"template_id": "ini_03", "expression": "-ts_decay_linear(ts_delta(sharesout, 120) / (ts_delay(sharesout, 120) + 0.001), 20)"},
        {"template_id": "ini_04", "expression": "-ts_zscore(ts_delta(sharesout, 252) / (ts_delay(sharesout, 252) + 0.001), 252)"},
        {"template_id": "ini_05", "expression": "-rank(ts_delta(sharesout, 252) / (ts_delay(sharesout, 252) + 0.001)) * rank(equity / (cap + 0.001))"},
        {"template_id": "ini_06", "expression": "-signed_power(group_zscore(ts_delta(sharesout, 252) / (ts_delay(sharesout, 252) + 0.001), subindustry), 0.5)"},
        {"template_id": "ini_07", "expression": "-ts_sum(sign(ts_delta(sharesout, 60)), 252)"},
        {"template_id": "ini_08", "expression": "-rank(ts_delta(sharesout, 60) / (ts_delay(sharesout, 60) + 0.001))"},
        {"template_id": "ini_09", "expression": "-ts_sum(sign(ts_delta(sharesout, 63)), 252)"},
        {"template_id": "ini_10", "expression": "if_else(ts_delta(sharesout, 252) > 0, -rank(ts_delta(sharesout, 252) / (ts_delay(sharesout, 252) + 0.001)), rank(abs(ts_delta(sharesout, 252)) / (ts_delay(sharesout, 252) + 0.001)))"},
    ],

    "invest_rnd": [
        {"template_id": "ird_01", "expression": "rank(rd_expense / (revenue + 0.001))"},
        {"template_id": "ird_02", "expression": "group_zscore(rd_expense / (cap + 0.001), industry)"},
        {"template_id": "ird_03", "expression": "ts_decay_linear(rank(rd_expense / (revenue + 0.001)), 60)"},
        {"template_id": "ird_04", "expression": "ts_zscore(rd_expense / ((revenue - cogs) + 0.001), 252)"},
        {"template_id": "ird_05", "expression": "signed_power(group_zscore(rd_expense / (revenue + 0.001), subindustry), 0.5)"},
        {"template_id": "ird_06", "expression": "group_rank(ts_delta(rd_expense, 252) / (ts_delay(rd_expense, 252) + 0.001), industry)"},
        {"template_id": "ird_07", "expression": "rank(rd_expense / (revenue + 0.001)) * rank((revenue - cogs) / (revenue + 0.001))"},
        {"template_id": "ird_08", "expression": "ts_rank(rd_expense / (cap + 0.001), 252)"},
        {"template_id": "ird_09", "expression": "group_rank(ts_delta(rd_expense, 252) / (ts_delay(rd_expense, 252) + 0.001), industry)"},
        {"template_id": "ird_10", "expression": "rank(rd_expense / (revenue + 0.001)) * rank((revenue - cogs) / (revenue + 0.001))"},
    ],

    "m77_growth": [
        {"template_id": "m7g_01", "expression": "-rank(asset_growth_rate)"},
        {"template_id": "m7g_02", "expression": "rank(sustainable_growth_rate) - rank(asset_growth_rate)"},
        {"template_id": "m7g_03", "expression": "-group_zscore(asset_growth_rate, industry) + group_zscore(sustainable_growth_rate, industry)"},
        {"template_id": "m7g_04", "expression": "-ts_decay_linear(rank(capex_to_total_assets), 20)"},
        {"template_id": "m7g_05", "expression": "rank(capex / (depre_amort + 0.001)) * rank(sustainable_growth_rate)"},
        {"template_id": "m7g_06", "expression": "-ts_zscore(reinvestment_rate, 252)"},
        {"template_id": "m7g_07", "expression": "signed_power(group_zscore(sustainable_growth_rate - asset_growth_rate, industry), 0.5)"},
        {"template_id": "m7g_08", "expression": "-rank(reinvestment_rate) * rank(news_short_interest)"},
        {"template_id": "m7g_09", "expression": "signed_power(group_zscore(sustainable_growth_rate - asset_growth_rate, industry), 0.5)"},
        {"template_id": "m7g_10", "expression": "-rank(reinvestment_rate) * rank(news_short_interest)"},
    ],

    # ============================================================
    # CATEGORY 5: MOMENTUM
    # ============================================================

    "momentum_price": [
        {"template_id": "mpr_01", "expression": "rank(ts_delay(close, 20) / (ts_delay(close, 120) + 0.001))"},
        {"template_id": "mpr_02", "expression": "group_zscore(ts_delay(close, 20) / (ts_delay(close, 180) + 0.001), industry)"},
        {"template_id": "mpr_03", "expression": "ts_decay_linear(rank(ts_delta(close, 120) / (ts_delay(close, 120) + 0.001)), 20)"},
        {"template_id": "mpr_04", "expression": "ts_quantile(close / (ts_delay(close, 252) + 0.001), 252)"},
        {"template_id": "mpr_05", "expression": "signed_power(rank(ts_delay(close, 20) / (ts_delay(close, 252) + 0.001)), 0.5)"},
        {"template_id": "mpr_06", "expression": "ts_rank(close / (ts_delay(close, 120) + 0.001), 252)"},
        {"template_id": "mpr_07", "expression": "group_rank(ts_delay(close, 20) / (ts_delay(close, 180) + 0.001), sector)"},
        {"template_id": "mpr_08", "expression": "rank(ts_corr(ts_delta(close, 60) / (ts_delay(close, 60) + 0.001), volume, 120))"},
        {"template_id": "mp_09", "expression": "ts_rank(close / (ts_delay(close, 120) + 0.001), 252)"},
        {"template_id": "mp_10", "expression": "group_rank(ts_delay(close, 20) / (ts_delay(close, 180) + 0.001), sector)"},
        {"template_id": "mp_11", "expression": "rank(ts_delay(close, 20) / (ts_delay(close, 252) + 0.001)) + rank(ts_delay(close, 20) / (ts_delay(close, 120) + 0.001))"},
        {"template_id": "mp_12", "expression": "ts_decay_linear(signed_power(group_zscore(ts_delta(close, 60) / (ts_delay(close, 60) + 0.001), subindustry), 0.5), 10)"},
    ],

    "momentum_earnings": [
        {"template_id": "mem_01", "expression": "rank(ts_delta(eps, 60) / (abs(ts_delay(eps, 60)) + 0.001))"},
        {"template_id": "mem_02", "expression": "rank(ts_zscore(eps, 252))"},
        {"template_id": "mem_03", "expression": "ts_decay_linear(rank(ts_delta(ebit, 60) / (abs(ts_delay(ebit, 60)) + 0.001)), 20)"},
        {"template_id": "mem_04", "expression": "rank(ts_mean(ts_delta(revenue, 60) / (abs(ts_delay(revenue, 60)) + 0.001), 120))"},
        {"template_id": "mem_05", "expression": "signed_power(group_zscore(ts_delta(income, 60) / (abs(ts_delay(income, 60)) + 0.001), subindustry), 0.5)"},
        {"template_id": "mem_06", "expression": "rank(ts_delta(eps, 60) / (abs(ts_delay(eps, 60)) + 0.001) - ts_delay(ts_delta(eps, 60) / (abs(ts_delay(eps, 60)) + 0.001), 60))"},
        {"template_id": "mem_07", "expression": "group_zscore(ts_delta(ebitda, 60) / (abs(ts_delay(ebitda, 60)) + 0.001), industry)"},
        {"template_id": "mem_08", "expression": "ts_rank(ts_delta(revenue, 120) / (abs(ts_delay(revenue, 120)) + 0.001), 252)"},
        {"template_id": "me_09", "expression": "ts_decay_linear(rank(ts_delta(eps, 60) / (abs(ts_delay(eps, 60)) + 0.001) - ts_delay(ts_delta(eps, 60) / (abs(ts_delay(eps, 60)) + 0.001), 60)), 20)"},
        {"template_id": "me_10", "expression": "signed_power(rank(ts_zscore(ebit, 252)) + rank(ts_delta(revenue, 120) / (abs(ts_delay(revenue, 120)) + 0.001)) - 1.0, 0.5)"},
    ],

    "momentum_estimate_revision": [
        {"template_id": "mer_01", "expression": "rank(ts_delta(est_sales, 60) / (abs(ts_delay(est_sales, 60)) + 0.001))"},
        {"template_id": "mer_02", "expression": "ts_decay_linear(rank(ts_delta(est_ebitda, 30) / (abs(ts_delay(est_ebitda, 30)) + 0.001)), 20)"},
        {"template_id": "mer_03", "expression": "rank(ts_zscore(est_eps, 120))"},
        {"template_id": "mer_04", "expression": "group_zscore(ts_delta(est_eps, 60) / (abs(ts_delay(est_eps, 60)) + 0.001), industry)"},
        {"template_id": "mer_05", "expression": "rank(ts_mean(ts_delta(est_sales, 20) / (abs(ts_delay(est_sales, 20)) + 0.001), 60))"},
        {"template_id": "mer_06", "expression": "signed_power(group_zscore(ts_delta(est_cashflow_op, 60) / (abs(ts_delay(est_cashflow_op, 60)) + 0.001), subindustry), 0.5)"},
        {"template_id": "mer_07", "expression": "ts_sum(sign(ts_delta(est_eps, 20)), 60)"},
        {"template_id": "mer_08", "expression": "rank(ts_delta(est_sales, 60) / (abs(ts_delay(est_sales, 60)) + 0.001)) + rank(ts_delta(est_ebitda, 60) / (abs(ts_delay(est_ebitda, 60)) + 0.001))"},
        {"template_id": "mer_09", "expression": "ts_decay_linear(ts_zscore(ts_delta(est_capex, 30), 120), 20)"},
        {"template_id": "mer_10", "expression": "rank(ts_delta(est_ptp, 60) / (abs(ts_delay(est_ptp, 60)) + 0.001))"},
    ],

    "momentum_industry": [
        {"template_id": "min_01", "expression": "group_zscore(ts_delta(close, 120) / (ts_delay(close, 120) + 0.001), industry)"},
        {"template_id": "min_02", "expression": "group_rank(ts_decay_linear(ts_delta(close, 60) / (ts_delay(close, 60) + 0.001), 20), sector)"},
        {"template_id": "min_03", "expression": "rank(group_neutralize(ts_delta(close, 120) / (ts_delay(close, 120) + 0.001), subindustry))"},
        {"template_id": "min_04", "expression": "ts_decay_linear(group_rank(ts_delta(close, 120) / (ts_delay(close, 120) + 0.001), subindustry), 20)"},
        {"template_id": "min_05", "expression": "rank(ts_corr(ts_delta(close, 60) / (ts_delay(close, 60) + 0.001), group_mean(ts_delta(close, 60) / (ts_delay(close, 60) + 0.001), 1, industry), 120))"},
        {"template_id": "min_06", "expression": "group_zscore(ts_delta(close, 60) / (ts_delay(close, 60) + 0.001), sector) * rank(volume)"},
        {"template_id": "min_07", "expression": "group_zscore(ts_mean(ts_delta(close, 20) / (ts_delay(close, 20) + 0.001), 60), industry)"},
        {"template_id": "min_08", "expression": "group_rank(ts_delta(close, 60) / (ts_delay(close, 60) + 0.001), industry) * rank(cap)"},
        {"template_id": "mi_09", "expression": "rank(ts_corr(ts_delta(close, 60) / (ts_delay(close, 60) + 0.001), group_mean(ts_delta(close, 60) / (ts_delay(close, 60) + 0.001), industry), 120))"},
        {"template_id": "mi_10", "expression": "group_zscore(ts_delta(close, 60) / (ts_delay(close, 60) + 0.001), sector) * rank(cap)"},
    ],

    "m77_momentum": [
        {"template_id": "m7m_01", "expression": "rank(ts_delta(close, 252) / (close + 0.001))"},
        {"template_id": "m7m_02", "expression": "group_zscore(earnings_momentum_analyst_score, industry)"},
        {"template_id": "m7m_03", "expression": "rank(ts_delta(close, 252) / (close + 0.001)) + rank(earnings_momentum_analyst_score)"},
        {"template_id": "m7m_04", "expression": "ts_decay_linear(rank(earnings_revision_magnitude), 20)"},
        {"template_id": "m7m_05", "expression": "rank(ts_zscore(standardized_unexpected_earnings, 120))"},
        {"template_id": "m7m_06", "expression": "ts_decay_linear(group_zscore(change_in_eps_surprise, subindustry), 20)"},
        {"template_id": "m7m_07", "expression": "ts_rank(treynor_ratio, 252)"},
        {"template_id": "m7m_08", "expression": "group_zscore(rank(ts_delta(close, 252) / (close + 0.001)) + rank(standardized_unexpected_earnings) + rank(treynor_ratio), subindustry)"},
        {"template_id": "m7m_09", "expression": "rank(ts_delta(close, 252) / (close + 0.001)) * 0.3 + rank(earnings_momentum_analyst_score) * 0.25 + rank(earnings_revision_magnitude) * 0.25 + rank(standardized_unexpected_earnings) * 0.2"},
        {"template_id": "m7m_10", "expression": "group_zscore(rank(ts_delta(close, 252) / (close + 0.001)) + rank(standardized_unexpected_earnings) + rank(treynor_ratio), subindustry)"},
    ],

    # ============================================================
    # CATEGORY 6: MEAN REVERSION
    # ============================================================

    "mr_short_term": [
        {"template_id": "mrs_01", "expression": "-rank(ts_decay_linear(close - vwap, 3))"},
        {"template_id": "mrs_02", "expression": "-ts_av_diff(close, 5) / (ts_std_dev(close, 10) + 0.001)"},
        {"template_id": "mrs_03", "expression": "rank(-ts_sum(returns, 3)) * rank(ts_arg_min(close, 5))"},
        {"template_id": "mrs_04", "expression": "group_rank(-ts_decay_linear((close - open) / (high - low + 0.001), 5), subindustry)"},
        {"template_id": "mrs_05", "expression": "rank(ts_quantile(-returns, 10)) - rank(ts_decay_linear(returns, 5))"},
        {"template_id": "mrs_06", "expression": "-rank(ts_arg_max(close, 5) - ts_arg_min(close, 5)) * sign(ts_mean(close, 5) - close)"},
        {"template_id": "mrs_07", "expression": "-ts_decay_linear(sign(returns) * sqrt(abs(returns)), 5)"},
        {"template_id": "mrs_08", "expression": "rank(0.5 - ts_quantile(returns, 10))"},
        {"template_id": "mrs_09", "expression": "-ts_av_diff(close, 5) / (ts_std_dev(close, 10) + 0.001)"},
        {"template_id": "mrs_10", "expression": "rank(ts_quantile(-returns, 10)) - rank(ts_decay_linear(returns, 5))"},
        {"template_id": "mrs_11", "expression": "-rank(ts_delta(close, 1)) * rank(ts_arg_min(close, 5))"},
        {"template_id": "mrs_12", "expression": "-ts_decay_linear(sign(returns) * power(abs(returns), 0.5), 5)"},
        {"template_id": "mrs_13", "expression": "-rank(ts_arg_max(close, 5) - ts_arg_min(close, 5)) * sign(ts_mean(close, 5) - close)"},
        {"template_id": "mrs_14", "expression": "(0.5 - ts_quantile(returns, 10)) * rank(ts_mean(close, 5) - close)"},
    ],

    "mr_vol_gated": [
        {"template_id": "mrv_01", "expression": "trade_when(ts_rank(historical_volatility_10, 252) > 0.7, -rank(ts_av_diff(close, 10) / (ts_std_dev(close, 20) + 0.001)), -1)"},
        {"template_id": "mrv_02", "expression": "trade_when(historical_volatility_30 > ts_mean(historical_volatility_30, 60), rank(-ts_delta(close, 5)), -1)"},
        {"template_id": "mrv_03", "expression": "trade_when(ts_rank(ts_std_dev(returns, 22), 252) > 0.55, rank(-ts_av_diff(close, 10) / (ts_std_dev(close, 20) + 0.001)), -1)"},
        {"template_id": "mrv_04", "expression": "trade_when(historical_volatility_10 > historical_volatility_90, -rank(ts_sum(returns, 3)), -1)"},
        {"template_id": "mrv_05", "expression": "trade_when(ts_zscore(historical_volatility_30, 120) > 1, group_rank(-ts_zscore(close, 10), subindustry), -1)"},
        {"template_id": "mrv_06", "expression": "trade_when(ts_rank(ts_std_dev(returns, 10), 252) > 0.6, -ts_av_diff(close, 10) / (ts_std_dev(close, 20) + 0.001), -1)"},
        {"template_id": "mrv_07", "expression": "trade_when(historical_volatility_30 > historical_volatility_180, rank(-ts_delta(close, 3)), -1)"},
        {"template_id": "mrv_08", "expression": "-rank(ts_decay_linear(returns, 5)) * rank(ts_rank(historical_volatility_30, 252))"},
        {"template_id": "mrvg_09", "expression": "trade_when(historical_volatility_10 > historical_volatility_90, -rank(ts_sum(returns, 3)), -1)"},
        {"template_id": "mrvg_10", "expression": "trade_when(ts_rank(ts_std_dev(returns, 10), 252) > 0.6, -ts_av_diff(close, 10) / (ts_std_dev(close, 20) + 0.001), -1)"},
        {"template_id": "mrvg_11", "expression": "trade_when(historical_volatility_30 > historical_volatility_180, rank(-ts_delta(close, 3)), -1)"},
    ],

    "mr_regression_residual": [
        {"template_id": "mrr_01", "expression": "-ts_regression(close, ts_mean(close, 20), 20, lag=0, rettype=2)"},
        {"template_id": "mrr_02", "expression": "-ts_regression(close, ts_mean(close, 60), 60, lag=0, rettype=2) * rank(inverse(ts_std_dev(returns, 20) + 0.001))"},
        {"template_id": "mrr_03", "expression": "-ts_regression(returns, ts_mean(returns, 10), 20, lag=0, rettype=2)"},
        {"template_id": "mrr_04", "expression": "-ts_regression(close / (vwap + 0.001), ts_mean(close / (vwap + 0.001), 10), 20, lag=0, rettype=2)"},
        {"template_id": "mrr_05", "expression": "-rank(ts_regression(close, ts_decay_linear(close, 30), 30, lag=0, rettype=2))"},
        {"template_id": "mrr_06", "expression": "-ts_regression(returns, ts_mean(returns, 5), 10, lag=0, rettype=2) + ts_regression(returns, ts_mean(returns, 20), 40, lag=0, rettype=2)"},
        {"template_id": "mrr_07", "expression": "rank(-ts_regression(vwap, ts_mean(vwap, 20), 20, lag=0, rettype=2)) * rank(-ts_regression(close, ts_mean(close, 20), 20, lag=0, rettype=2))"},
        {"template_id": "mrr_08", "expression": "-ts_decay_linear(ts_regression(close, ts_mean(close, 10), 10, lag=0, rettype=2), 5)"},
        {"template_id": "mrr_09", "expression": "rank(-ts_regression(vwap, ts_mean(vwap, 20), 20)) * rank(-ts_regression(close, ts_mean(close, 20), 20))"},
        {"template_id": "mrr_10", "expression": "-ts_regression(close, ts_mean(close, 60), 60) * rank(1 / (ts_std_dev(returns, 20) + 0.001))"},
        {"template_id": "mrr_11", "expression": "-ts_regression(close / (vwap + 0.001), ts_mean(close / (vwap + 0.001), 10), 20)"},
        {"template_id": "mrr_12", "expression": "ts_regression(ts_mean(close, 5), ts_mean(close, 20), 40)"},
    ],

    "mr_volume_conditioned": [
        {"template_id": "mvc_01", "expression": "-rank(returns) * rank(volume / (adv20 + 0.001))"},
        {"template_id": "mvc_02", "expression": "-ts_decay_linear(returns, 3) * rank(ts_zscore(volume, 20))"},
        {"template_id": "mvc_03", "expression": "trade_when(volume > adv20, -rank(ts_delta(close, 3)), -1)"},
        {"template_id": "mvc_04", "expression": "-ts_corr(returns, volume, 10) * rank(-ts_sum(returns, 3))"},
        {"template_id": "mvc_05", "expression": "rank(-ts_zscore(close, 10)) * rank(ts_rank(volume / (adv20 + 0.001), 20))"},
        {"template_id": "mvc_06", "expression": "-ts_regression(returns, volume / (adv20 + 0.001), 10, lag=0, rettype=2)"},
        {"template_id": "mvc_07", "expression": "trade_when(ts_rank(volume, 10) > 0.8, group_rank(-ts_decay_linear(returns, 5), subindustry), -1)"},
        {"template_id": "mvc_08", "expression": "-ts_decay_linear(returns, 5) * ts_corr(abs(returns), volume, 20)"},
        {"template_id": "mrvc_09", "expression": "-ts_decay_linear(returns, 5) * ts_corr(abs(returns), volume, 20)"},
        {"template_id": "mrvc_10", "expression": "trade_when(volume > 1.5 * adv20, -ts_av_diff(close, 10) / (ts_std_dev(close, 10) + 0.001), -1)"},
        {"template_id": "mrvc_11", "expression": "rank(-returns * (volume / (adv20 + 0.001))) * (1 - ts_quantile(returns, 20))"},
        {"template_id": "mrvc_12", "expression": "-ts_regression(returns, log(volume / (adv20 + 0.001)), 10)"},
    ],

    "mr_long_term": [
        {"template_id": "mrl_01", "expression": "-ts_decay_linear(ts_sum(returns, 252), 252)"},
        {"template_id": "mrl_02", "expression": "rank(-ts_quantile(close, 252))"},
        {"template_id": "mrl_03", "expression": "-ts_decay_linear(rank(ts_sum(returns, 252)), 120)"},
        {"template_id": "mrl_04", "expression": "-ts_decay_linear(ts_rank(close, 252), 120)"},
        {"template_id": "mrl_05", "expression": "-rank(ts_mean(returns, 252))"},
        {"template_id": "mrl_06", "expression": "-group_rank(ts_sum(returns, 252), industry)"},
        {"template_id": "mrl_07", "expression": "rank(-ts_quantile(close, 504)) - rank(ts_quantile(close, 252))"},
        {"template_id": "mrl_08", "expression": "-ts_decay_linear(group_rank(ts_sum(returns, 252), sector), 60)"},
        {"template_id": "mrl_09", "expression": "-ts_decay_linear(ts_sum(returns, 252), 504)"},
        {"template_id": "mrl_10", "expression": "-ts_decay_linear(rank(ts_sum(returns, 504)), 252)"},
        {"template_id": "mrl_11", "expression": "rank(-ts_quantile(close, 1260)) - rank(ts_quantile(close, 252))"},
        {"template_id": "mrl_12", "expression": "-ts_decay_linear(ts_rank(close, 504), 252)"},
    ],

    # ============================================================
    # CATEGORY 7: VOLATILITY
    # ============================================================

    "vol_low_vol": [
        {"template_id": "vlv_01", "expression": "-rank(historical_volatility_90)"},
        {"template_id": "vlv_02", "expression": "-rank(implied_volatility_mean_180)"},
        {"template_id": "vlv_03", "expression": "-ts_decay_linear(rank(historical_volatility_150), 20)"},
        {"template_id": "vlv_04", "expression": "-group_rank(beta_last_360_days_spy, sector) * rank(-historical_volatility_120)"},
        {"template_id": "vlv_05", "expression": "rank(inverse(beta_last_360_days_spy + 0.001)) - rank(historical_volatility_180)"},
        {"template_id": "vlv_06", "expression": "-rank(historical_volatility_90 + implied_volatility_mean_90)"},
        {"template_id": "vlv_07", "expression": "-rank(ts_mean(implied_volatility_mean_120, 20)) * rank(inverse(unsystematic_risk_last_60_days + 0.001))"},
        {"template_id": "vlv_08", "expression": "trade_when(ts_delta(historical_volatility_180, 20) < 0, -rank(beta_last_60_days_spy), -1)"},
        {"template_id": "vlv_09", "expression": "-group_rank(implied_volatility_mean_360 + historical_volatility_180, subindustry)"},
        {"template_id": "vlv_10", "expression": "rank(-historical_volatility_10) * rank(-beta_last_30_days_spy)"},
        {"template_id": "vlv_11", "expression": "rank(-historical_volatility_10) * rank(-beta_last_30_days_spy)"},
        {"template_id": "vlv_12", "expression": "-group_rank(implied_volatility_mean_360 + historical_volatility_180, subindustry)"},
    ],

    "vol_term_structure": [
        {"template_id": "vts_01", "expression": "rank(implied_volatility_mean_10 / (implied_volatility_mean_180 + 0.001))"},
        {"template_id": "vts_02", "expression": "-rank(implied_volatility_mean_20 / (implied_volatility_mean_360 + 0.001))"},
        {"template_id": "vts_03", "expression": "-ts_delta(implied_volatility_mean_10 / (implied_volatility_mean_180 + 0.001), 10)"},
        {"template_id": "vts_04", "expression": "ts_zscore(implied_volatility_mean_20 / (implied_volatility_mean_360 + 0.001), 60)"},
        {"template_id": "vts_05", "expression": "-rank(ts_decay_linear(implied_volatility_mean_10 - implied_volatility_mean_180, 20))"},
        {"template_id": "vts_06", "expression": "rank(implied_volatility_call_10 / (implied_volatility_call_360 + 0.001)) - rank(implied_volatility_put_10 / (implied_volatility_put_360 + 0.001))"},
        {"template_id": "vts_07", "expression": "-ts_rank(implied_volatility_mean_20 / (implied_volatility_mean_720 + 0.001), 120)"},
        {"template_id": "vts_08", "expression": "-rank(ts_corr(implied_volatility_mean_10 / (implied_volatility_mean_120 + 0.001), returns, 60))"},
        {"template_id": "vts_09", "expression": "rank(implied_volatility_call_10 / (implied_volatility_call_360 + 0.001)) - rank(implied_volatility_put_10 / (implied_volatility_put_360 + 0.001))"},
        {"template_id": "vts_10", "expression": "-ts_rank(implied_volatility_mean_20 / (implied_volatility_mean_720 + 0.001), 120)"},
        {"template_id": "vts_11", "expression": "rank(implied_volatility_mean_30 / (implied_volatility_mean_270 + 0.001)) - rank(implied_volatility_mean_60 / (implied_volatility_mean_720 + 0.001))"},
        {"template_id": "vts_12", "expression": "-rank(ts_corr(implied_volatility_mean_10 / (implied_volatility_mean_120 + 0.001), returns, 60))"},
    ],

    "vol_realized_vs_implied": [
        {"template_id": "vri_01", "expression": "-rank(implied_volatility_mean_30 - historical_volatility_30)"},
        {"template_id": "vri_02", "expression": "-rank((implied_volatility_mean_90 - historical_volatility_90) / (historical_volatility_90 + 0.001))"},
        {"template_id": "vri_03", "expression": "rank(historical_volatility_180 / (implied_volatility_mean_180 + 0.001))"},
        {"template_id": "vri_04", "expression": "-ts_zscore(implied_volatility_mean_60 - historical_volatility_90, 120)"},
        {"template_id": "vri_05", "expression": "-rank(ts_mean(implied_volatility_mean_30 - historical_volatility_30, 20))"},
        {"template_id": "vri_06", "expression": "ts_decay_linear(-rank(implied_volatility_call_90 - historical_volatility_90), 15)"},
        {"template_id": "vri_07", "expression": "trade_when(implied_volatility_mean_30 > historical_volatility_30 * 1.2, -rank(implied_volatility_mean_30 - historical_volatility_30), -1)"},
        {"template_id": "vri_08", "expression": "-rank(ts_delta(implied_volatility_mean_90 - historical_volatility_90, 10)) * rank(-historical_volatility_120)"},
        {"template_id": "vrvi_09", "expression": "-rank(ts_delta(implied_volatility_mean_90 - historical_volatility_90, 10)) * rank(-historical_volatility_120)"},
        {"template_id": "vrvi_10", "expression": "rank(ts_regression(historical_volatility_90, implied_volatility_mean_90, 120)) * rank(-implied_volatility_mean_180)"},
        {"template_id": "vrvi_11", "expression": "trade_when(implied_volatility_mean_30 > historical_volatility_30 * 1.2, -rank(implied_volatility_mean_30 - historical_volatility_30), -1)"},
        {"template_id": "vrvi_12", "expression": "-ts_rank(implied_volatility_mean_10 - historical_volatility_10, 60) * rank(1 / (beta_last_60_days_spy + 0.001))"},
    ],

    "vol_of_vol": [
        {"template_id": "vov_01", "expression": "-rank(ts_std_dev(historical_volatility_30, 60))"},
        {"template_id": "vov_02", "expression": "-rank(ts_std_dev(implied_volatility_mean_30, 120))"},
        {"template_id": "vov_03", "expression": "-ts_zscore(ts_std_dev(historical_volatility_90, 60), 252)"},
        {"template_id": "vov_04", "expression": "-rank(ts_std_dev(implied_volatility_call_90 - implied_volatility_put_90, 60))"},
        {"template_id": "vov_05", "expression": "-ts_decay_linear(rank(ts_std_dev(beta_last_60_days_spy, 60)), 20) * rank(-historical_volatility_150)"},
        {"template_id": "vov_06", "expression": "trade_when(ts_std_dev(historical_volatility_30, 20) > ts_mean(ts_std_dev(historical_volatility_30, 20), 252), -rank(historical_volatility_30), -1)"},
        {"template_id": "vov_07", "expression": "-group_neutralize(rank(ts_std_dev(implied_volatility_mean_90, 60)) + rank(ts_std_dev(historical_volatility_90, 60)), subindustry)"},
        {"template_id": "vov_08", "expression": "rank(-ts_std_dev(returns, 120)) * rank(-ts_std_dev(historical_volatility_30, 60))"},
        {"template_id": "vov_09", "expression": "trade_when(ts_std_dev(historical_volatility_30, 20) > ts_mean(ts_std_dev(historical_volatility_30, 20), 252), -rank(historical_volatility_30), -1)"},
        {"template_id": "vov_10", "expression": "-group_neutralize(rank(ts_std_dev(implied_volatility_mean_90, 60)) + rank(ts_std_dev(historical_volatility_90, 60)), subindustry)"},
        {"template_id": "vov_11", "expression": "-rank(ts_std_dev(implied_volatility_call_90 - implied_volatility_put_90, 60))"},
        {"template_id": "vov_12", "expression": "-ts_decay_linear(rank(ts_std_dev(beta_last_60_days_spy, 60)), 20) * rank(-historical_volatility_150)"},
    ],

    "m77_vol_risk": [
        {"template_id": "m7vr_01", "expression": "-rank(debt / (equity + 0.001)) * rank(-implied_volatility_mean_90)"},
        {"template_id": "m7vr_02", "expression": "-rank(credit_risk_premium_indicator) * rank(historical_volatility_90 / (implied_volatility_mean_90 + 0.001))"},
        {"template_id": "m7vr_03", "expression": "-rank(earnings_shortfall_metric) * rank(-ts_std_dev(returns, 60))"},
        {"template_id": "m7vr_04", "expression": "-rank(coefficient_variation_fy1_eps) * rank(-historical_volatility_150)"},
        {"template_id": "m7vr_05", "expression": "rank(treynor_ratio) * rank(-implied_volatility_mean_120) * rank(-debt / (equity + 0.001))"},
        {"template_id": "m7vr_06", "expression": "rank(fcf_yield_multiplied_forward_roe) * rank(-coefficient_variation_fy1_eps) * rank(-implied_volatility_mean_360)"},
        {"template_id": "m7vr_07", "expression": "trade_when(rank(debt / (equity + 0.001)) > 0.8, -rank(implied_volatility_mean_90 - historical_volatility_90), -1)"},
        {"template_id": "m7vr_08", "expression": "-rank(news_short_interest) * rank(-historical_volatility_180)"},
        {"template_id": "m7vr_09", "expression": "rank(treynor_ratio) * rank(-implied_volatility_mean_120) * rank(-debt / (equity + 0.001))"},
        {"template_id": "m7vr_10", "expression": "trade_when(rank(debt / (equity + 0.001)) > 0.8, -rank(implied_volatility_mean_90 - historical_volatility_90), -1)"},
        {"template_id": "m7vr_11", "expression": "rank(fcf_yield_multiplied_forward_roe) * rank(-coefficient_variation_fy1_eps) * rank(-implied_volatility_mean_360)"},
        {"template_id": "m7vr_12", "expression": "-rank(news_short_interest) * rank(-historical_volatility_180) * rank(1 / (beta_last_360_days_spy + 0.001))"},
    ],

    # ============================================================
    # CATEGORY 8: LIQUIDITY
    # ============================================================

    "liq_amihud": [
        {"template_id": "la_01", "expression": "-rank(ts_mean(abs(returns) / (volume * close + 0.001), 20))"},
        {"template_id": "la_02", "expression": "-ts_zscore(abs(returns) / (volume * close + 0.001), 60)"},
        {"template_id": "la_03", "expression": "-rank(ts_delta(ts_mean(abs(returns) / (volume * close + 0.001), 20), 20))"},
        {"template_id": "la_04", "expression": "-ts_rank(abs(returns) / (volume * close + 0.001), 252)"},
        {"template_id": "la_05", "expression": "-rank(ts_mean(abs(returns) / (volume * close + 0.001), 5) / (ts_mean(abs(returns) / (volume * close + 0.001), 60) + 0.001))"},
        {"template_id": "la_06", "expression": "-rank(hump(ts_zscore(abs(returns) / (volume * close + 0.001), 120), hump=0.5))"},
        {"template_id": "la_07", "expression": "-group_rank(ts_mean(abs(returns) / (volume * close + 0.001), 20), sector)"},
        {"template_id": "la_08", "expression": "-rank(ts_decay_linear(abs(returns) / (volume * close + 0.001), 10))"},
        {"template_id": "la_09", "expression": "-ts_rank(abs(returns) / (volume * close + 0.001), 252)"},
        {"template_id": "la_10", "expression": "-rank(ts_mean(abs(returns) / (volume * close + 0.001), 5) / (ts_mean(abs(returns) / (volume * close + 0.001), 60) + 0.001))"},
        {"template_id": "la_11", "expression": "-rank(ts_mean(abs(returns) / (volume * close + 0.001), 5) / (ts_mean(abs(returns) / (volume * close + 0.001), 60) + 0.001))"},
        {"template_id": "la_12", "expression": "-rank(ts_mean(abs(returns) / (volume * vwap + 0.001), 20))"},
    ],

    "liq_volume_trend": [
        {"template_id": "lvt_01", "expression": "rank(ts_decay_linear(volume, 20))"},
        {"template_id": "lvt_02", "expression": "-rank(ts_delta(ts_mean(volume, 5), 20))"},
        {"template_id": "lvt_03", "expression": "rank(ts_regression(volume, close, 20, lag=0, rettype=2))"},
        {"template_id": "lvt_04", "expression": "-rank(ts_zscore(ts_mean(volume, 5) / (ts_mean(volume, 60) + 0.001), 60))"},
        {"template_id": "lvt_05", "expression": "-group_rank(ts_corr(volume, close, 60), industry)"},
        {"template_id": "lvt_06", "expression": "-rank(ts_corr(volume, close, 60))"},
        {"template_id": "lvt_07", "expression": "-rank(ts_delta(kth_element(volume, 3, k=20), 10))"},
        {"template_id": "lvt_08", "expression": "rank(ts_delta(ts_delta(volume, 10), 10))"},
        {"template_id": "lvt_09", "expression": "-rank(ts_corr(volume, close, 60))"},
        {"template_id": "lvt_10", "expression": "rank(ts_regression(volume, close, 20))"},
        {"template_id": "lvt_11", "expression": "-rank(ts_delta(ts_delta(volume, 10), 10))"},
        {"template_id": "lvt_12", "expression": "-rank(ts_zscore(ts_mean(volume, 5) / (ts_mean(volume, 60) + 0.001), 60))"},
    ],

    "liq_turnover_reversal": [
        {"template_id": "ltr_01", "expression": "-rank(ts_zscore(volume / (sharesout + 0.001), 60))"},
        {"template_id": "ltr_02", "expression": "-ts_rank(volume / (sharesout + 0.001), 120)"},
        {"template_id": "ltr_03", "expression": "-rank((volume / (sharesout + 0.001)) / (ts_mean(volume / (sharesout + 0.001), 60) + 0.001))"},
        {"template_id": "ltr_04", "expression": "-rank(ts_decay_linear(volume / (sharesout + 0.001), 20))"},
        {"template_id": "ltr_05", "expression": "-rank(ts_quantile(volume / (sharesout + 0.001), 60))"},
        {"template_id": "ltr_06", "expression": "-group_rank(ts_delta(ts_mean(volume / (sharesout + 0.001), 5), 20), subindustry)"},
        {"template_id": "ltr_07", "expression": "-rank(ts_corr(volume / (sharesout + 0.001), abs(returns), 20))"},
        {"template_id": "ltr_08", "expression": "-rank(sqrt((volume / (sharesout + 0.001)) / (ts_mean(volume / (sharesout + 0.001), 252) + 0.001)))"},
        {"template_id": "ltr_09", "expression": "-rank(ts_corr(volume / (sharesout + 0.001), abs(returns), 20))"},
        {"template_id": "ltr_10", "expression": "-rank(power((volume / (sharesout + 0.001)) / (ts_mean(volume / (sharesout + 0.001), 252) + 0.001), hump=0.5))"},
        {"template_id": "ltr_11", "expression": "-rank(ts_quantile(volume / (sharesout + 0.001), 60))"},
        {"template_id": "ltr_12", "expression": "-group_rank(ts_delta(ts_mean(volume / (sharesout + 0.001), 5), 20), subindustry)"},
    ],

    "liq_risk_premium": [
        {"template_id": "lrp_01", "expression": "-rank(log(adv20 + 1))"},
        {"template_id": "lrp_02", "expression": "-rank(log(cap + 1) + log(adv20 + 1))"},
        {"template_id": "lrp_03", "expression": "rank(ts_decay_linear(inverse(adv20 + 0.001), 20))"},
        {"template_id": "lrp_04", "expression": "-rank(volume * vwap) + rank((high - low) / (close + 0.001))"},
        {"template_id": "lrp_05", "expression": "rank(group_rank(ts_mean(abs(returns) / (volume * close + 0.001), 20), sector))"},
        {"template_id": "lrp_06", "expression": "rank(ts_mean((high - low) / (vwap + 0.001), 20))"},
        {"template_id": "lrp_07", "expression": "-rank(ts_regression(returns, abs(returns) / (volume * close + 0.001), 60, lag=0, rettype=2))"},
        {"template_id": "lrp_08", "expression": "rank(group_neutralize(ts_mean((high - low) / (close + 0.001), 20), sector))"},
        {"template_id": "lrp_09", "expression": "rank(ts_mean((high - low) / (vwap + 0.001), 20))"},
        {"template_id": "lrp_10", "expression": "-rank(ts_regression(returns, abs(returns) / (volume * close + 0.001), 60))"},
        {"template_id": "lrp_11", "expression": "-rank(log(cap + 1) + log(adv20 + 1))"},
        {"template_id": "lrp_12", "expression": "-rank(volume * vwap) + rank((high - low) / (close + 0.001))"},
    ],

    "liq_volume_price_divergence": [
        {"template_id": "lvpd_01", "expression": "-rank(ts_corr(returns, volume, 20))"},
        {"template_id": "lvpd_02", "expression": "-rank(ts_corr(abs(returns), volume, 60))"},
        {"template_id": "lvpd_03", "expression": "-rank(ts_covariance(returns, log(volume + 1), 20))"},
        {"template_id": "lvpd_04", "expression": "-rank(ts_corr(abs(returns), volume, 10) - ts_corr(abs(returns), volume, 60))"},
        {"template_id": "lvpd_05", "expression": "-rank(ts_rank(abs(returns), 20) - ts_rank(volume, 20))"},
        {"template_id": "lvpd_06", "expression": "-rank(ts_regression(returns, ts_delta(volume, 1), 20, lag=0, rettype=2))"},
        {"template_id": "lvpd_07", "expression": "-rank(ts_delta(ts_corr(returns, volume, 20), 20))"},
        {"template_id": "lvpd_08", "expression": "-rank(ts_corr(news_curr_vol, abs(returns), 20))"},
        {"template_id": "lvpd_09", "expression": "-rank(ts_regression(returns, ts_delta(volume, 1), 20))"},
        {"template_id": "lvpd_10", "expression": "-rank(ts_delta(ts_corr(returns, volume, 20), 20))"},
        {"template_id": "lvpd_11", "expression": "-rank(ts_covariance(returns, log(volume + 1), 20))"},
        {"template_id": "lvpd_12", "expression": "-rank(ts_rank(abs(returns), 20) - ts_rank(volume, 20))"},
    ],

    # ============================================================
    # CATEGORY 9: SIZE
    # ============================================================

    "size_conditioned_value": [
        {"template_id": "scv_01", "expression": "group_rank(equity / (cap + 0.001), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scv_02", "expression": "group_neutralize(rank(equity / (cap + 0.001)), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scv_03", "expression": "group_rank(ebitda / (cap + 0.001), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scv_04", "expression": "group_neutralize(ts_rank(eps / (close + 0.001), 252), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scv_05", "expression": "group_neutralize(rank(ts_backfill(ebitda, 60) / (cap + 0.001)), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scv_06", "expression": "group_rank(ts_mean(equity / (cap + 0.001), 120), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scv_07", "expression": "group_neutralize(rank(income / (cap + 0.001)) + rank(equity / (cap + 0.001)), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scv_08", "expression": "group_rank(ts_delta(equity / (cap + 0.001), 60), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scv_09", "expression": "group_rank(ts_delta(equity / (cap + 0.001), 60), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scv_10", "expression": "group_neutralize(ts_zscore(ebitda / (cap + 0.001), 252), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scv_11", "expression": "group_rank(ts_backfill(income, 60) / (cap + 0.001), densify(bucket(rank(cap), range=\"0, 1, 0.1\")))"},
        {"template_id": "scv_12", "expression": "group_neutralize(rank(equity / (cap + 0.001)) * ts_decay_linear(rank(eps / (close + 0.001)), 20), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
    ],

    "size_conditioned_momentum": [
        {"template_id": "scm_01", "expression": "group_neutralize(ts_decay_linear(returns, 20), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scm_02", "expression": "group_rank(ts_sum(returns, 60), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scm_03", "expression": "group_neutralize(ts_sum(returns, 252) - ts_sum(returns, 21), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scm_04", "expression": "group_rank(-ts_delta(close, 5) / (close + 0.001), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scm_05", "expression": "group_neutralize(ts_rank(ts_sum(returns, 120), 252), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scm_06", "expression": "rank(-cap) * ts_decay_linear(returns, 60)"},
        {"template_id": "scm_07", "expression": "group_rank(ts_corr(returns, log(volume + 1), 60), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scm_08", "expression": "group_neutralize(-ts_arg_max(close, 60), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scm_09", "expression": "group_rank(ts_corr(returns, log(volume + 1), 60), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scm_10", "expression": "rank(-cap) * ts_decay_linear(returns, 60)"},
        {"template_id": "scm_11", "expression": "group_neutralize(-ts_arg_max(close, 60) / 60.0, densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scm_12", "expression": "group_rank(ts_decay_linear(returns, 40), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
    ],

    "size_conditioned_quality": [
        {"template_id": "scq_01", "expression": "group_neutralize(rank((revenue - cogs) / (cap + 0.001)), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scq_02", "expression": "group_rank(retained_earnings / (cap + 0.001), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scq_03", "expression": "group_neutralize(rank(current_ratio), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scq_04", "expression": "group_rank(rd_expense / (revenue + 0.001), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scq_05", "expression": "group_neutralize(ts_rank((revenue - cogs) / (revenue + 0.001), 252), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scq_06", "expression": "group_rank(ts_backfill(cashflow_op, 60) / (cap + 0.001), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scq_07", "expression": "group_neutralize(rank(ebit / (revenue + 0.001)) - rank(ts_std_dev(returns, 60)), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scq_08", "expression": "group_rank(ts_backfill(cashflow, 60) / (cap + 0.001), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scq_09", "expression": "group_neutralize(ts_delta(rank((revenue - cogs) / (cap + 0.001)), 60), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scq_10", "expression": "group_rank(ts_backfill(income, 60) / (ts_backfill(sales, 60) + 0.001), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "scq_11", "expression": "group_neutralize(rank(ebitda / (revenue + 0.001)) * rank(-historical_volatility_30), densify(bucket(rank(cap), range=\"0, 1, 0.1\")))"},
        {"template_id": "scq_12", "expression": "group_rank(ts_mean(ts_backfill(revenue, 60), 120) / (cap + 0.001), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
    ],

    "size_microcap_liquidity": [
        {"template_id": "sml_01", "expression": "rank(-cap) * rank(volume / (adv20 + 0.001))"},
        {"template_id": "sml_02", "expression": "-log(cap + 1) * rank(-volume / (adv20 + 0.001))"},
        {"template_id": "sml_03", "expression": "rank(-cap) * ts_decay_linear(volume / (adv20 + 0.001), 20)"},
        {"template_id": "sml_04", "expression": "ts_corr(rank(-cap), rank(volume), 60)"},
        {"template_id": "sml_05", "expression": "rank(-log(cap + 1)) * rank(ts_std_dev(volume / (adv20 + 0.001), 20))"},
        {"template_id": "sml_06", "expression": "group_neutralize(rank(-cap) * rank(volume / (adv20 + 0.001)), sector)"},
        {"template_id": "sml_07", "expression": "ts_regression(returns, log(cap + 1), 120, lag=0, rettype=2) * rank(adv20 / (cap + 0.001))"},
        {"template_id": "sml_08", "expression": "ts_zscore(-log(cap + 1), 252) * ts_zscore(volume / (adv20 + 0.001), 60)"},
        {"template_id": "sml_09", "expression": "ts_zscore(-log(cap + 1), 252) * ts_zscore(volume / (adv20 + 0.001), 60)"},
        {"template_id": "sml_10", "expression": "group_neutralize(log(adv20 + 1) / (log(cap + 1) + 0.001), densify(bucket(rank(cap), range=\"0, 1, 0.2\"))) * rank(-historical_volatility_30)"},
        {"template_id": "sml_11", "expression": "ts_decay_linear(rank(-cap) * rank(ts_delta(volume, 5) / (adv20 + 0.001)), 10)"},
        {"template_id": "sml_12", "expression": "rank(-cap) * rank(ts_std_dev(volume / (adv20 + 0.001), 20))"},
    ],

    # ============================================================
    # CATEGORY 10: LEVERAGE/CREDIT
    # ============================================================

    "lev_debt_ratios": [
        {"template_id": "ldr_01", "expression": "-rank((debt_lt + debt_st) / (cap + 0.001))"},
        {"template_id": "ldr_02", "expression": "-group_rank(debt_lt / (equity + 0.001), industry)"},
        {"template_id": "ldr_03", "expression": "-ts_decay_linear(rank(debt_lt / (ebitda + 0.001)), 20)"},
        {"template_id": "ldr_04", "expression": "-rank(ts_delta((debt_lt + debt_st) / (cap + 0.001), 60))"},
        {"template_id": "ldr_05", "expression": "-group_neutralize(ts_zscore(debt_lt / (cap + 0.001), 252), subindustry)"},
        {"template_id": "ldr_06", "expression": "-rank((debt_lt + debt_st) / (cap + 0.001)) * rank(debt_st / (debt_lt + debt_st + 0.001))"},
        {"template_id": "ldr_07", "expression": "-ts_decay_linear(rank(debt_lt / (equity + 0.001) * historical_volatility_30), 15)"},
        {"template_id": "ldr_08", "expression": "-group_neutralize(ts_quantile(debt_lt / (ebitda + 0.001), 252), sector)"},
        {"template_id": "ldr_09", "expression": "-rank((debt_lt + debt_st) / (cap + 0.001)) * (1 + rank(debt_st / (debt_lt + debt_st + 0.001)))"},
        {"template_id": "ldr_10", "expression": "-ts_decay_linear(rank(debt_lt / (equity + 0.001) * historical_volatility_30), 15)"},
        {"template_id": "ldr_11", "expression": "-group_neutralize(ts_quantile(debt_lt / (ebitda + 0.001), 504), sector)"},
        {"template_id": "ldr_12", "expression": "-ts_decay_linear(rank(debt_lt / (ebitda + 0.001)), 20)"},
    ],

    "lev_interest_coverage": [
        {"template_id": "lic_01", "expression": "rank(ebit / (interest_expense + 0.001))"},
        {"template_id": "lic_02", "expression": "ts_decay_linear(group_rank(ebitda / (interest_expense + 0.001), industry), 20)"},
        {"template_id": "lic_03", "expression": "rank(ts_backfill(cashflow_op, 60) / (ts_backfill(interest_expense, 60) + 0.001))"},
        {"template_id": "lic_04", "expression": "group_neutralize(ts_zscore(ebit / (interest_expense + 0.001), 252), subindustry)"},
        {"template_id": "lic_05", "expression": "rank(ts_delta(ebit / (interest_expense + 0.001), 60)) + rank(ebit / (interest_expense + 0.001))"},
        {"template_id": "lic_06", "expression": "rank(ts_backfill(cashflow_op, 60) / (ts_backfill(interest_expense, 60) + 0.001)) - rank(ts_backfill(debt, 60) / (cap + 0.001))"},
        {"template_id": "lic_07", "expression": "ts_quantile(ebitda / (interest_expense + 0.001), 252)"},
        {"template_id": "lic_08", "expression": "signed_power(group_zscore(ebit / (interest_expense + 0.001), industry), 0.5)"},
        {"template_id": "lic_09", "expression": "rank(ts_delta(ebit / (interest_expense + 0.001), 60)) + 0.5 * rank(ebit / (interest_expense + 0.001))"},
        {"template_id": "lic_10", "expression": "rank(ts_backfill(cashflow_op, 60) / (ts_backfill(interest_expense, 60) + 0.001)) - rank(ts_backfill(debt, 60) / (cap + 0.001))"},
        {"template_id": "lic_11", "expression": "rank(ebitda / (interest_expense + 0.001)) - rank(debt_lt / (cap + 0.001))"},
    ],

    "lev_distress": [
        {"template_id": "ldis_01", "expression": "rank(1.2 * working_capital / (cap + 0.001) + 1.4 * retained_earnings / (cap + 0.001) + 3.3 * ebit / (cap + 0.001) + 0.6 * cap / (debt_lt + debt_st + 0.001) + revenue / (cap + 0.001))"},
        {"template_id": "ldis_02", "expression": "ts_decay_linear(group_rank(1.2 * working_capital / (cap + 0.001) + 1.4 * retained_earnings / (cap + 0.001) + 3.3 * ebit / (cap + 0.001) + 0.6 * cap / (debt_lt + debt_st + 0.001), industry), 20)"},
        {"template_id": "ldis_03", "expression": "group_neutralize(-rank(inverse(current_ratio + 0.001)) - rank(debt_lt / (equity + 0.001)) - rank(historical_volatility_90), subindustry)"},
        {"template_id": "ldis_04", "expression": "rank(ts_delta(1.2 * working_capital / (cap + 0.001) + 1.4 * retained_earnings / (cap + 0.001) + 3.3 * ebit / (cap + 0.001) + 0.6 * cap / (debt_lt + debt_st + 0.001), 120))"},
        {"template_id": "ldis_05", "expression": "rank(ebit / (interest_expense + 0.001)) + rank(current_ratio) - rank((debt_lt + debt_st) / (cap + 0.001)) - rank(historical_volatility_90)"},
        {"template_id": "ldis_06", "expression": "-ts_decay_linear(rank(-ts_zscore(working_capital / (cap + 0.001), 252) + ts_zscore(debt_lt / (cap + 0.001), 252)), 15)"},
        {"template_id": "ldis_07", "expression": "rank(ts_delta(cash, 60) / (cap + 0.001)) - rank(debt_lt / (equity + 0.001))"},
        {"template_id": "ldis_08", "expression": "trade_when(current_ratio < 1, rank(ts_delta(cash, 60) / (cap + 0.001)), -1)"},
        {"template_id": "ld_09", "expression": "rank(ts_delta(1.2 * working_capital / (cap + 0.001) + 1.4 * retained_earnings / (cap + 0.001) + 3.3 * ebit / (cap + 0.001) + 0.6 * cap / (debt_lt + debt_st + 0.001), 120))"},
        {"template_id": "ld_10", "expression": "rank(ebit / (interest_expense + 0.001)) + rank(current_ratio) - rank((debt_lt + debt_st) / (cap + 0.001)) - rank(historical_volatility_90)"},
        {"template_id": "ld_11", "expression": "-rank(debt_lt / (equity + 0.001)) * (1 - rank(working_capital / (cap + 0.001))) + rank(ts_delta(cash, 60) / (cap + 0.001))"},
    ],

    "lev_credit_quality_qoq": [
        {"template_id": "lcq_01", "expression": "-rank(ts_delta(ts_backfill(debt, 60), 60) / (cap + 0.001))"},
        {"template_id": "lcq_02", "expression": "-group_rank(ts_delta(ts_backfill(debt, 60) / (ts_backfill(ebitda, 60) + 0.001), 60), industry)"},
        {"template_id": "lcq_03", "expression": "ts_decay_linear(rank(ts_delta(ts_backfill(ebitda, 60) / (ts_backfill(interest_expense, 60) + 0.001), 60)), 20)"},
        {"template_id": "lcq_04", "expression": "-group_neutralize(rank(ts_delta((ts_backfill(debt, 60) - ts_backfill(cash, 60)) / (cap + 0.001), 60)), subindustry)"},
        {"template_id": "lcq_05", "expression": "rank(ts_zscore(ts_backfill(cashflow, 60) / (ts_backfill(interest_expense, 60) + 0.001), 252))"},
        {"template_id": "lcq_06", "expression": "rank(ts_corr(ts_backfill(cashflow_op, 60) / (ts_backfill(debt, 60) + 0.001), returns, 120))"},
        {"template_id": "lcq_07", "expression": "ts_decay_linear(ts_delta(rank(ts_backfill(ebitda, 60) / (ts_backfill(interest_expense, 60) + 0.001)), 60), 15)"},
        {"template_id": "lcq_08", "expression": "-rank(ts_delta(ts_delta(ts_backfill(debt, 60), 60), 60))"},
        {"template_id": "lcq_09", "expression": "rank(ts_corr(ts_backfill(cashflow_op, 60) / (ts_backfill(debt, 60) + 0.001), returns, 120))"},
        {"template_id": "lcq_10", "expression": "-rank(ts_delta(ts_backfill(debt, 60), 60) / (cap + 0.001))"},
        {"template_id": "lcq_11", "expression": "ts_decay_linear(ts_delta(rank(ts_backfill(ebitda, 60) / (ts_backfill(interest_expense, 60) + 0.001)), 60) - ts_delta(rank(ts_backfill(debt, 60) / (cap + 0.001)), 60), 15)"},
    ],

    "m77_credit": [
        {"template_id": "m7c_01", "expression": "-rank(debt / (equity + 0.001))"},
        {"template_id": "m7c_02", "expression": "-ts_decay_linear(group_rank(credit_risk_premium_indicator, industry), 20)"},
        {"template_id": "m7c_03", "expression": "-rank(book_leverage_ratio_3) * rank(debt / (equity + 0.001))"},
        {"template_id": "m7c_04", "expression": "-group_neutralize(ts_zscore(current_liabilities_to_price, 252), subindustry)"},
        {"template_id": "m7c_05", "expression": "-rank(ts_delta(debt / (equity + 0.001), 60)) - rank(ts_delta(credit_risk_premium_indicator, 60))"},
        {"template_id": "m7c_06", "expression": "ts_decay_linear(rank(fcf_yield_multiplied_forward_roe) - rank(credit_risk_premium_indicator) - rank(book_leverage_ratio_3), 15)"},
        {"template_id": "m7c_07", "expression": "group_neutralize(rank(visibility_ratio) + rank(treynor_ratio) - rank(debt / (equity + 0.001)) - rank(current_liabilities_to_price), subindustry)"},
        {"template_id": "m7c_08", "expression": "-(rank(coefficient_variation_fy1_eps) + rank(debt / (equity + 0.001)) + rank(book_leverage_ratio_3))"},
        {"template_id": "m7c_09", "expression": "group_neutralize(rank(visibility_ratio) + rank(treynor_ratio) - rank(debt / (equity + 0.001)) - rank(current_liabilities_to_price), subindustry)"},
        {"template_id": "m7c_10", "expression": "-(rank(coefficient_variation_fy1_eps) + rank(debt / (equity + 0.001)) + rank(book_leverage_ratio_3) + rank(earnings_shortfall_metric))"},
        {"template_id": "m7c_11", "expression": "ts_decay_linear(rank(fcf_yield_multiplied_forward_roe) - rank(credit_risk_premium_indicator) - rank(book_leverage_ratio_3), 15)"},
        {"template_id": "m7c_12", "expression": "trade_when(ts_rank(historical_volatility_30, 252) > 0.5, -(rank(coefficient_variation_fy1_eps) + rank(credit_risk_premium_indicator) + rank(debt / (equity + 0.001))), -1)"},
    ],

    # ============================================================
    # CATEGORY 11: SENTIMENT
    # ============================================================

    "sent_social_buzz": [
        {"template_id": "ssb_01", "expression": "rank(ts_zscore(scl12_buzz, 20))"},
        {"template_id": "ssb_02", "expression": "-ts_decay_linear(ts_delta(snt_buzz, 5), 10)"},
        {"template_id": "ssb_03", "expression": "group_zscore(scl12_buzz_fast_d1, subindustry)"},
        {"template_id": "ssb_04", "expression": "rank(snt_buzz_fast_d1) - rank(ts_mean(snt_buzz, 40))"},
        {"template_id": "ssb_05", "expression": "ts_decay_linear(group_zscore(snt_buzz_ret, sector), 20)"},
        {"template_id": "ssb_06", "expression": "ts_decay_linear(ts_zscore(scl12_buzz, 10) - ts_zscore(scl12_buzz, 60), 5)"},
        {"template_id": "ssb_07", "expression": "rank(ts_mean(snt_buzz_ret, 5)) - rank(ts_mean(snt_buzz_ret, 40))"},
        {"template_id": "ssb_08", "expression": "group_zscore(ts_decay_linear(snt_value_fast_d1, 10), subindustry)"},
        {"template_id": "ssb_09", "expression": "rank(ts_mean(snt_buzz_ret, 5)) - rank(ts_mean(snt_buzz_ret, 40))"},
        {"template_id": "ssb_10", "expression": "group_zscore(ts_decay_linear(snt_value_fast_d1, 10), subindustry)"},
        {"template_id": "ssb_11", "expression": "-rank(ts_av_diff(vec_sum(scl12_buzzvec), 60))"},
        {"template_id": "ssb_12", "expression": "ts_decay_linear(snt_buzz_bfl_fast_d1 - snt_buzz_bfl, 10)"},
    ],

    "sent_level_change": [
        {"template_id": "slc_01", "expression": "rank(ts_delta(scl12_sentiment, 5))"},
        {"template_id": "slc_02", "expression": "ts_zscore(scl12_sentiment, 20) - ts_zscore(scl12_sentiment, 120)"},
        {"template_id": "slc_03", "expression": "ts_decay_linear(scl12_sentiment_fast_d1, 10)"},
        {"template_id": "slc_04", "expression": "group_neutralize(ts_rank(snt_value, 120), subindustry)"},
        {"template_id": "slc_05", "expression": "ts_decay_linear(ts_delta(snt_value, 10), 20)"},
        {"template_id": "slc_06", "expression": "rank(ts_delta(scl12_sentiment, 5) - ts_delta(scl12_sentiment, 20))"},
        {"template_id": "slc_07", "expression": "ts_rank(snt_value_fast_d1, 60) - ts_rank(snt_value, 60)"},
        {"template_id": "slc_08", "expression": "ts_decay_linear(group_zscore(scl12_sentiment, subindustry), 5)"},
        {"template_id": "slc_09", "expression": "ts_decay_linear(group_zscore(scl12_sentiment, subindustry), 5)"},
        {"template_id": "slc_10", "expression": "rank(ts_delta(snt_value, 5)) * rank(ts_delta(snt_buzz_ret, 5))"},
        {"template_id": "slc_11", "expression": "-rank(ts_delta(vec_avg(scl12_sentvec), 10))"},
        {"template_id": "slc_12", "expression": "group_neutralize(ts_decay_linear(snt_value_fast_d1, 40), subindustry)"},
    ],

    "sent_ravenpack": [
        {"template_id": "srp_01", "expression": "rank(ts_sum(ts_backfill(rp_css_earnings, 60), 10) + ts_sum(ts_backfill(rp_css_revenue, 60), 10))"},
        {"template_id": "srp_02", "expression": "ts_decay_linear(ts_backfill(rp_ess_earnings, 60) + ts_backfill(rp_ess_credit, 60), 20)"},
        {"template_id": "srp_03", "expression": "group_zscore(ts_sum(ts_backfill(rp_css_earnings, 60) + ts_backfill(rp_css_credit, 60) + ts_backfill(rp_css_equity, 60), 5), subindustry)"},
        {"template_id": "srp_04", "expression": "rank(ts_decay_linear(ts_backfill(rp_ess_product, 60) + ts_backfill(rp_ess_business, 60), 10))"},
        {"template_id": "srp_05", "expression": "rank(ts_sum(ts_backfill(rp_css_insider, 60) + ts_backfill(rp_css_labor, 60), 5))"},
        {"template_id": "srp_06", "expression": "ts_decay_linear(ts_backfill(rp_css_earnings, 60) + ts_backfill(rp_css_revenue, 60) + ts_backfill(rp_css_credit, 60) + ts_backfill(rp_css_dividends, 60), 40)"},
        {"template_id": "srp_07", "expression": "rank(ts_sum(ts_backfill(rp_ess_earnings, 60), 5)) - rank(ts_sum(ts_backfill(rp_ess_earnings, 60), 60))"},
        {"template_id": "srp_08", "expression": "ts_decay_linear(ts_backfill(rp_ess_dividends, 60) + ts_backfill(rp_ess_equity, 60) + ts_backfill(rp_ess_credit, 60), 5)"},
        {"template_id": "srp_09", "expression": "rank(ts_sum(ts_backfill(rp_css_technical, 60) + ts_backfill(rp_css_price, 60), 10)) + rank(ts_sum(ts_backfill(rp_ess_technical, 60), 10))"},
        {"template_id": "srp_10", "expression": "ts_decay_linear(ts_backfill(rp_ess_dividends, 60) + ts_backfill(rp_ess_equity, 60) + ts_backfill(rp_ess_credit, 60), 5)"},
        {"template_id": "srp_11", "expression": "rank(ts_sum(ts_backfill(rp_ess_earnings, 60), 5)) - rank(ts_sum(ts_backfill(rp_ess_earnings, 60), 60))"},
        {"template_id": "srp_12", "expression": "group_zscore(ts_sum(ts_backfill(rp_ess_insider, 60) + ts_backfill(rp_ess_labor, 60) + ts_backfill(rp_ess_assets, 60), 20), subindustry)"},
    ],

    "sent_news_reaction": [
        {"template_id": "snr_01", "expression": "rank(ts_mean(news_mins_1_chg, 10))"},
        {"template_id": "snr_02", "expression": "ts_decay_linear(news_mins_10_pct_up - news_mins_10_pct_dn, 20)"},
        {"template_id": "snr_03", "expression": "group_zscore(ts_mean(news_max_dn_ret, 20), subindustry)"},
        {"template_id": "snr_04", "expression": "rank(ts_zscore(news_mins_1_chg, 40))"},
        {"template_id": "snr_05", "expression": "ts_decay_linear(news_mins_20_pct_up - news_mins_20_pct_dn, 10)"},
        {"template_id": "snr_06", "expression": "rank(ts_mean(news_ls, 5)) * rank(news_curr_vol / (news_close_vol + 0.001))"},
        {"template_id": "snr_07", "expression": "rank(ts_mean(news_mins_1_pct_up, 5)) - rank(ts_mean(news_mins_1_pct_dn, 5))"},
        {"template_id": "snr_08", "expression": "group_zscore(ts_mean(news_mins_20_chg, 40), industry)"},
        {"template_id": "snr_09", "expression": "rank(ts_mean(news_mins_1_pct_up, 5)) - rank(ts_mean(news_mins_1_pct_dn, 5))"},
        {"template_id": "snr_10", "expression": "group_zscore(ts_mean(news_mins_20_chg, 40), industry)"},
        {"template_id": "snr_11", "expression": "ts_decay_linear(news_mins_20_pct_up - news_mins_20_pct_dn, 10)"},
        {"template_id": "snr_12", "expression": "rank(ts_mean(news_ls, 5)) * rank(news_curr_vol / (news_close_vol + 0.001))"},
    ],

    "sent_price_divergence": [
        {"template_id": "spd_01", "expression": "-rank(ts_corr(scl12_sentiment, close, 20))"},
        {"template_id": "spd_02", "expression": "rank(ts_mean(snt_value, 10)) - rank(ts_delta(close, 10))"},
        {"template_id": "spd_03", "expression": "-ts_corr(snt_buzz_ret, close, 40)"},
        {"template_id": "spd_04", "expression": "ts_zscore(snt_value, 20) * (-ts_zscore(close, 20))"},
        {"template_id": "spd_05", "expression": "rank(scl12_sentiment) * rank(-ts_delta(close, 20))"},
        {"template_id": "spd_06", "expression": "-group_zscore(ts_corr(snt_value, close, 120), subindustry)"},
        {"template_id": "spd_07", "expression": "ts_decay_linear(rank(ts_delta(snt_value, 5)) - rank(ts_delta(close, 5)), 10)"},
        {"template_id": "spd_08", "expression": "rank(snt_buzz_ret) * rank(-ts_delta(close, 10)) * rank(news_close_vol)"},
        {"template_id": "spd_09", "expression": "rank(snt_buzz_ret) * rank(-ts_delta(news_eod_close, 10)) * rank(news_close_vol)"},
        {"template_id": "spd_10", "expression": "rank(ts_regression(snt_value, news_eod_vwap, 60) - ts_mean(snt_value, 60))"},
        {"template_id": "spd_11", "expression": "ts_decay_linear(rank(ts_delta(snt_value, 5)) - rank(ts_delta(news_eod_close, 5)), 10)"},
    ],

    # ============================================================
    # CATEGORY 12: SEASONALITY
    # ============================================================

    "season_earnings_calendar": [
        {"template_id": "sec_01", "expression": "rank(-days_from_last_change(est_sales))"},
        {"template_id": "sec_02", "expression": "inverse(days_from_last_change(actual_eps_value_quarterly) + 1)"},
        {"template_id": "sec_03", "expression": "rank(-days_from_last_change(est_sales)) * rank(-days_from_last_change(est_ebitda))"},
        {"template_id": "sec_04", "expression": "rank(-days_from_last_change(actual_sales_value_quarterly))"},
        {"template_id": "sec_05", "expression": "inverse(days_from_last_change(est_ebitda) + 1) + inverse(days_from_last_change(est_eps) + 1)"},
        {"template_id": "sec_06", "expression": "if_else(days_from_last_change(est_sales) < 5, rank(-days_from_last_change(est_sales)), 0)"},
        {"template_id": "sec_07", "expression": "-rank(days_from_last_change(est_eps) + days_from_last_change(est_ebitda))"},
        {"template_id": "sec_08", "expression": "-ts_zscore(days_from_last_change(actual_eps_value_quarterly), 120)"},
        {"template_id": "sec_09", "expression": "if_else(days_from_last_change(est_eps) < 5, rank(-days_from_last_change(est_eps)), 0)"},
        {"template_id": "sec_10", "expression": "-rank(days_from_last_change(est_eps) + days_from_last_change(est_ebitda))"},
    ],

    "season_data_release_mr": [
        {"template_id": "sdr_01", "expression": "rank(-days_from_last_change(est_sales)) * rank(fscore_total)"},
        {"template_id": "sdr_02", "expression": "if_else(days_from_last_change(est_sales) < 10, -rank(historical_volatility_10), rank(fscore_momentum))"},
        {"template_id": "sdr_03", "expression": "rank(fscore_momentum) * inverse(days_from_last_change(est_sales) + 1)"},
        {"template_id": "sdr_04", "expression": "rank(composite_factor_score_derivative) * rank(-days_from_last_change(actual_eps_value_quarterly))"},
        {"template_id": "sdr_05", "expression": "if_else(days_from_last_change(est_sales) < 20, rank(fscore_value), 0)"},
        {"template_id": "sdr_06", "expression": "rank(fscore_value) * inverse(days_from_last_change(est_ebitda) + 1) * rank(-historical_volatility_30)"},
        {"template_id": "sdr_07", "expression": "rank(-days_from_last_change(est_sales)) * rank(composite_factor_score_derivative) * rank(-historical_volatility_10)"},
        {"template_id": "sdr_08", "expression": "ts_zscore(fscore_total, 120) * inverse(days_from_last_change(est_sales) + 1)"},
        {"template_id": "sdr_09", "expression": "rank(-days_from_last_change(est_eps)) * rank(composite_factor_score_derivative) * rank(-historical_volatility_10)"},
        {"template_id": "sdr_10", "expression": "ts_zscore(fscore_total, 120) * inverse(days_from_last_change(est_sales) + 1)"},
    ],

    # ============================================================
    # CATEGORY 13: OPTIONS
    # ============================================================

    "opt_call_breakeven_ts": [
        {"template_id": "ocb_01", "expression": "rank(call_breakeven_30 - call_breakeven_360)"},
        {"template_id": "ocb_02", "expression": "rank(call_breakeven_10 / (call_breakeven_1080 + 0.001))"},
        {"template_id": "ocb_03", "expression": "ts_zscore(call_breakeven_30 - call_breakeven_180, 20)"},
        {"template_id": "ocb_04", "expression": "group_zscore(call_breakeven_60 / (call_breakeven_720 + 0.001), industry)"},
        {"template_id": "ocb_05", "expression": "ts_delta(call_breakeven_20 / (call_breakeven_120 + 0.001), 10)"},
        {"template_id": "ocb_06", "expression": "rank(ts_delta(call_breakeven_30 - call_breakeven_270, 5))"},
        {"template_id": "ocb_07", "expression": "ts_decay_linear(rank(call_breakeven_60 - call_breakeven_360), 10)"},
        {"template_id": "ocb_08", "expression": "ts_delta(rank(call_breakeven_90 / (call_breakeven_720 + 0.001)), 20)"},
        {"template_id": "ocbt_09", "expression": "ts_decay_linear(rank(call_breakeven_60 - call_breakeven_360), 10)"},
        {"template_id": "ocbt_10", "expression": "rank((call_breakeven_30 + call_breakeven_360) / 2 - call_breakeven_120)"},
        {"template_id": "ocbt_11", "expression": "ts_delta(rank(call_breakeven_90 / (call_breakeven_720 + 0.001)), 20)"},
        {"template_id": "ocbt_12", "expression": "group_zscore(ts_delta(call_breakeven_20 - call_breakeven_150, 5), sector)"},
    ],

    "opt_put_breakeven_ts": [
        {"template_id": "opb_01", "expression": "rank(put_breakeven_30 - put_breakeven_360)"},
        {"template_id": "opb_02", "expression": "rank(put_breakeven_10 / (put_breakeven_1080 + 0.001))"},
        {"template_id": "opb_03", "expression": "-ts_zscore(put_breakeven_30 - put_breakeven_180, 20)"},
        {"template_id": "opb_04", "expression": "group_zscore(put_breakeven_60 / (put_breakeven_720 + 0.001), industry)"},
        {"template_id": "opb_05", "expression": "rank(put_breakeven_90 / (put_breakeven_30 + 0.001))"},
        {"template_id": "opb_06", "expression": "-rank(ts_delta(put_breakeven_10 - put_breakeven_120, 5))"},
        {"template_id": "opb_07", "expression": "ts_decay_linear(rank(put_breakeven_60 - put_breakeven_360), 15)"},
        {"template_id": "opb_08", "expression": "ts_zscore(put_breakeven_10 / (put_breakeven_360 + 0.001), 40)"},
        {"template_id": "opbt_09", "expression": "ts_decay_linear(rank(put_breakeven_60 - put_breakeven_360), 15)"},
        {"template_id": "opbt_10", "expression": "ts_zscore(put_breakeven_10 / (put_breakeven_360 + 0.001), 40)"},
        {"template_id": "opbt_11", "expression": "group_zscore(ts_delta(put_breakeven_60 - put_breakeven_180, 10), sector)"},
        {"template_id": "opbt_12", "expression": "rank(put_breakeven_90 / (put_breakeven_30 + 0.001))"},
    ],

    "opt_forward_price": [
        {"template_id": "ofp_01", "expression": "rank(forward_price_30 / (forward_price_360 + 0.001))"},
        {"template_id": "ofp_02", "expression": "ts_delta(forward_price_60 / (forward_price_720 + 0.001), 10)"},
        {"template_id": "ofp_03", "expression": "ts_zscore(forward_price_30 - forward_price_180, 20)"},
        {"template_id": "ofp_04", "expression": "group_zscore(forward_price_90 / (forward_price_360 + 0.001), industry)"},
        {"template_id": "ofp_05", "expression": "rank(ts_delta(forward_price_10 - forward_price_120, 5))"},
        {"template_id": "ofp_06", "expression": "ts_decay_linear(rank(forward_price_60 - forward_price_360), 10)"},
        {"template_id": "ofp_07", "expression": "ts_delta(rank(forward_price_90 / (forward_price_270 + 0.001)), 20)"},
        {"template_id": "ofp_08", "expression": "rank(forward_price_10 / (forward_price_30 + 0.001) - forward_price_360 / (forward_price_720 + 0.001))"},
        {"template_id": "ofp_09", "expression": "ts_delta(rank(forward_price_90 / (forward_price_270 + 0.001)), 20)"},
        {"template_id": "ofp_10", "expression": "rank(forward_price_10 / (forward_price_30 + 0.001) - forward_price_360 / (forward_price_720 + 0.001))"},
        {"template_id": "ofp_11", "expression": "group_zscore(ts_delta(forward_price_30 / (forward_price_180 + 0.001), 5), sector)"},
        {"template_id": "ofp_12", "expression": "ts_decay_linear(ts_zscore(forward_price_60 - forward_price_270, 10), 20)"},
    ],

    "opt_breakeven_dynamics": [
        {"template_id": "obd_01", "expression": "ts_delta(option_breakeven_30, 5)"},
        {"template_id": "obd_02", "expression": "ts_zscore(option_breakeven_60, 20)"},
        {"template_id": "obd_03", "expression": "ts_decay_linear(ts_delta(option_breakeven_90, 10), 15)"},
        {"template_id": "obd_04", "expression": "rank(ts_zscore(option_breakeven_30, 60))"},
        {"template_id": "obd_05", "expression": "group_zscore(ts_delta(option_breakeven_120, 10), industry)"},
        {"template_id": "obd_06", "expression": "ts_delta(option_breakeven_30 - option_breakeven_360, 5)"},
        {"template_id": "obd_07", "expression": "rank(ts_zscore(option_breakeven_10, 10) - ts_zscore(option_breakeven_180, 10))"},
        {"template_id": "obd_08", "expression": "rank(ts_delta(option_breakeven_60, 20) / (ts_std_dev(option_breakeven_60, 20) + 0.001))"},
        {"template_id": "obd_09", "expression": "-rank(ts_delta(ts_std_dev(option_breakeven_90, 20), 10))"},
        {"template_id": "obd_10", "expression": "ts_zscore(option_breakeven_270 - option_breakeven_1080, 40)"},
        {"template_id": "obd_11", "expression": "rank(ts_delta(option_breakeven_60, 20) / (ts_std_dev(option_breakeven_60, 20) + 0.001))"},
        {"template_id": "obd_12", "expression": "ts_delta(option_breakeven_30 - option_breakeven_360, 5)"},
    ],

    "opt_call_put_skew": [
        {"template_id": "ocps_01", "expression": "rank(call_breakeven_30 / (put_breakeven_30 + 0.001))"},
        {"template_id": "ocps_02", "expression": "rank(call_breakeven_90 - put_breakeven_90)"},
        {"template_id": "ocps_03", "expression": "ts_delta(call_breakeven_60 / (put_breakeven_60 + 0.001), 10)"},
        {"template_id": "ocps_04", "expression": "ts_zscore(call_breakeven_30 - put_breakeven_30, 20)"},
        {"template_id": "ocps_05", "expression": "group_zscore(call_breakeven_120 / (put_breakeven_120 + 0.001), industry)"},
        {"template_id": "ocps_06", "expression": "rank(call_breakeven_360 / (put_breakeven_360 + 0.001)) - rank(call_breakeven_30 / (put_breakeven_30 + 0.001))"},
        {"template_id": "ocps_07", "expression": "ts_decay_linear(rank(call_breakeven_180 - put_breakeven_180), 15)"},
        {"template_id": "ocps_08", "expression": "rank(call_breakeven_30 / (put_breakeven_30 + 0.001) - call_breakeven_720 / (put_breakeven_720 + 0.001))"},
        {"template_id": "ocps_09", "expression": "group_zscore(ts_delta(call_breakeven_90 - put_breakeven_90, 5), subindustry)"},
        {"template_id": "ocps_10", "expression": "rank(call_breakeven_30 / (put_breakeven_30 + 0.001) - call_breakeven_720 / (put_breakeven_720 + 0.001))"},
        {"template_id": "ocps_11", "expression": "ts_delta(rank(call_breakeven_270 / (put_breakeven_270 + 0.001)), 20)"},
        {"template_id": "ocps_12", "expression": "group_zscore(call_breakeven_60 / (put_breakeven_60 + 0.001) - call_breakeven_360 / (put_breakeven_360 + 0.001), sector)"},
    ],

    # ============================================================
    # CATEGORY 14: SUPPLY CHAIN
    # ============================================================

    "sc_network_centrality": [
        {"template_id": "snc_01", "expression": "rank(ts_backfill(pv13_com_page_rank, 60))"},
        {"template_id": "snc_02", "expression": "ts_delta(rank(ts_backfill(pv13_com_page_rank, 60)), 20)"},
        {"template_id": "snc_03", "expression": "ts_zscore(rank(ts_backfill(pv13_com_page_rank, 60)), 60)"},
        {"template_id": "snc_04", "expression": "group_zscore(pv13_com_page_rank, industry)"},
        {"template_id": "snc_05", "expression": "ts_decay_linear(rank(ts_backfill(pv13_com_rk_au, 60)), 15)"},
        {"template_id": "snc_06", "expression": "group_zscore(ts_delta(rank(ts_backfill(pv13_com_page_rank, 60)), 10), subindustry)"},
        {"template_id": "snc_07", "expression": "rank(ts_backfill(pv13_com_page_rank, 60)) - rank(ts_backfill(pv13_com_rk_au, 60))"},
        {"template_id": "snc_08", "expression": "ts_decay_linear(ts_zscore(rank(ts_backfill(pv13_com_page_rank, 60)), 20), 10)"},
        {"template_id": "snc_09", "expression": "rank(ts_backfill(pv13_com_page_rank, 60)) - rank(ts_backfill(pv13_com_rk_au, 60))"},
        {"template_id": "snc_10", "expression": "ts_decay_linear(ts_zscore(rank(ts_backfill(pv13_com_page_rank, 60)), 20), 10)"},
    ],

    "sc_breadth": [
        {"template_id": "scbr_01", "expression": "rank(rank(pv13_5l_scibr) / (pv13_1l_scibr + 0.001))"},
        {"template_id": "scbr_02", "expression": "rank(rank(pv13_6l_scibr) / (pv13_2l_scibr + 0.001))"},
        {"template_id": "scbr_03", "expression": "ts_delta(rank(pv13_3l_scibr) - rank(pv13_1l_scibr), 20)"},
        {"template_id": "scbr_04", "expression": "group_zscore(pv13_5l_scibr, industry)"},
        {"template_id": "scbr_05", "expression": "ts_zscore(rank(pv13_4l_scibr) / (pv13_2l_scibr + 0.001), 60)"},
        {"template_id": "scbr_06", "expression": "ts_delta(rank(pv13_6l_scibr), 10)"},
        {"template_id": "scbr_07", "expression": "rank(rank(ts_backfill(pv13_di_5l, 60)) - rank(ts_backfill(pv13_di_6l, 60)))"},
        {"template_id": "scbr_08", "expression": "ts_decay_linear(rank(rank(pv13_5l_scibr) - rank(pv13_1l_scibr)), 15)"},
        {"template_id": "scb_09", "expression": "ts_decay_linear(rank(rank(pv13_5l_scibr) - rank(pv13_1l_scibr)), 15)"},
        {"template_id": "scb_10", "expression": "ts_zscore(rank(ts_backfill(pv13_di_6l, 60)), 40)"},
    ],

    "sc_customer_returns": [
        {"template_id": "sccr_01", "expression": "rank(ts_backfill(pv13_custretsig_retsig, 60))"},
        {"template_id": "sccr_02", "expression": "ts_zscore(rank(ts_backfill(pv13_custretsig_retsig, 60)), 20)"},
        {"template_id": "sccr_03", "expression": "ts_delta(rank(ts_backfill(pv13_custretsig_retsig, 60)), 5)"},
        {"template_id": "sccr_04", "expression": "ts_decay_linear(rank(ts_backfill(pv13_custretsig_retsig, 60)), 10)"},
        {"template_id": "sccr_05", "expression": "group_zscore(pv13_custretsig_retsig, industry)"},
        {"template_id": "sccr_06", "expression": "ts_mean(rank(ts_backfill(pv13_custretsig_retsig, 60)), 5) - ts_mean(rank(ts_backfill(pv13_custretsig_retsig, 60)), 60)"},
        {"template_id": "sccr_07", "expression": "group_zscore(ts_delta(rank(ts_backfill(pv13_custretsig_retsig, 60)), 5), subindustry)"},
        {"template_id": "sccr_08", "expression": "ts_decay_linear(rank(ts_backfill(pv13_custretsig_retsig, 60)), 20)"},
        {"template_id": "sccr_09", "expression": "group_zscore(ts_delta(rank(ts_backfill(pv13_custretsig_retsig, 60)), 5), subindustry)"},
        {"template_id": "sccr_10", "expression": "ts_zscore(rank(ts_backfill(pv13_custretsig_retsig, 60)), 60)"},
    ],

    "sc_hierarchy_sector": [
        {"template_id": "schs_01", "expression": "rank(ts_backfill(pv13_com_page_rank, 60))"},
        {"template_id": "schs_02", "expression": "ts_delta(rank(ts_backfill(pv13_com_page_rank, 60)), 20)"},
        {"template_id": "schs_03", "expression": "group_zscore(pv13_com_page_rank, industry)"},
        {"template_id": "schs_04", "expression": "rank(ts_backfill(pv13_com_page_rank, 60)) - rank(ts_backfill(pv13_com_page_rank, 60))"},
        {"template_id": "schs_05", "expression": "ts_zscore(rank(ts_backfill(pv13_com_page_rank, 60)), 60)"},
        {"template_id": "schs_06", "expression": "ts_decay_linear(rank(ts_backfill(pv13_com_page_rank, 60)), 15)"},
        {"template_id": "schs_07", "expression": "rank(ts_backfill(pv13_com_page_rank, 60)) - rank(ts_backfill(pv13_com_page_rank, 60))"},
        {"template_id": "schs_08", "expression": "ts_delta(rank(pv13_h2_sector), 5)"},
        {"template_id": "schs_09", "expression": "rank(ts_backfill(pv13_com_page_rank, 60)) - rank(ts_backfill(pv13_com_page_rank, 60))"},
        {"template_id": "schs_10", "expression": "ts_delta(rank(pv13_h2_sector), 5)"},
        {"template_id": "schs_11", "expression": "group_zscore(rank(ts_backfill(pv13_com_page_rank, 60)) - rank(ts_backfill(pv13_com_page_rank, 60)), sector)"},
        {"template_id": "schs_12", "expression": "ts_zscore(rank(ts_backfill(pv13_com_page_rank, 60)), 40)"},
    ],

    # ============================================================
    # CATEGORY 15: RISK
    # ============================================================

    "risk_bab": [
        {"template_id": "rbab_01", "expression": "-rank(beta_last_60_days_spy)"},
        {"template_id": "rbab_02", "expression": "-rank(beta_last_60_days_spy) - rank(beta_last_360_days_spy)"},
        {"template_id": "rbab_03", "expression": "group_zscore(-beta_last_60_days_spy, subindustry)"},
        {"template_id": "rbab_04", "expression": "inverse(beta_last_60_days_spy + 0.001)"},
        {"template_id": "rbab_05", "expression": "ts_decay_linear(rank(-beta_last_60_days_spy), 10)"},
        {"template_id": "rbab_06", "expression": "rank(-ts_delta(beta_last_30_days_spy, 20))"},
        {"template_id": "rbab_07", "expression": "rank(-(beta_last_30_days_spy + beta_last_360_days_spy) / 2)"},
        {"template_id": "rbab_08", "expression": "group_zscore(-ts_mean(beta_last_60_days_spy, 20), industry)"},
        {"template_id": "rb_09", "expression": "ts_zscore(-beta_last_60_days_spy, 60)"},
        {"template_id": "rb_10", "expression": "group_zscore(-ts_mean(beta_last_60_days_spy, 20), industry)"},
        {"template_id": "rb_11", "expression": "rank(-beta_last_60_days_spy) * sign(-(beta_last_30_days_spy - beta_last_360_days_spy))"},
        {"template_id": "rb_12", "expression": "rank(-(beta_last_30_days_spy + beta_last_360_days_spy) / 2)"},
    ],

    "risk_idiosyncratic": [
        {"template_id": "rid_01", "expression": "rank(-unsystematic_risk_last_60_days)"},
        {"template_id": "rid_02", "expression": "group_zscore(-unsystematic_risk_last_60_days, subindustry)"},
        {"template_id": "rid_03", "expression": "rank(-ts_delta(unsystematic_risk_last_30_days, 20))"},
        {"template_id": "rid_04", "expression": "ts_zscore(-unsystematic_risk_last_60_days, 60)"},
        {"template_id": "rid_05", "expression": "rank(-(unsystematic_risk_last_30_days - unsystematic_risk_last_360_days))"},
        {"template_id": "rid_06", "expression": "ts_decay_linear(rank(-unsystematic_risk_last_60_days), 15)"},
        {"template_id": "rid_07", "expression": "group_zscore(-ts_delta(unsystematic_risk_last_60_days, 20), industry)"},
        {"template_id": "rid_08", "expression": "rank(-unsystematic_risk_last_360_days) * rank(-ts_delta(unsystematic_risk_last_60_days, 30))"},
        {"template_id": "ri_09", "expression": "group_zscore(-(unsystematic_risk_last_30_days - unsystematic_risk_last_60_days), subindustry)"},
        {"template_id": "ri_10", "expression": "rank(-unsystematic_risk_last_360_days) * rank(-ts_delta(unsystematic_risk_last_60_days, 30))"},
        {"template_id": "ri_11", "expression": "ts_decay_linear(rank(-unsystematic_risk_last_60_days), 15)"},
        {"template_id": "ri_12", "expression": "rank(-unsystematic_risk_last_30_days) + rank(-unsystematic_risk_last_60_days)"},
    ],

    "risk_systematic_decomp": [
        {"template_id": "rsd_01", "expression": "-rank(systematic_risk_last_60_days / (systematic_risk_last_60_days + unsystematic_risk_last_60_days + 0.001))"},
        {"template_id": "rsd_02", "expression": "group_zscore(unsystematic_risk_last_60_days - systematic_risk_last_60_days, subindustry)"},
        {"template_id": "rsd_03", "expression": "rank(ts_delta(unsystematic_risk_last_60_days, 20) - ts_delta(systematic_risk_last_60_days, 20))"},
        {"template_id": "rsd_04", "expression": "group_zscore(systematic_risk_last_60_days / (unsystematic_risk_last_60_days + 0.001), industry)"},
        {"template_id": "rsd_05", "expression": "ts_decay_linear(rank(unsystematic_risk_last_60_days - systematic_risk_last_60_days), 10)"},
        {"template_id": "rsd_06", "expression": "rank(-beta_last_60_days_spy * systematic_risk_last_60_days / (unsystematic_risk_last_60_days + 0.001))"},
        {"template_id": "rsd_07", "expression": "group_zscore(-ts_delta(systematic_risk_last_30_days / (systematic_risk_last_30_days + unsystematic_risk_last_30_days + 0.001), 20), subindustry)"},
        {"template_id": "rsd_08", "expression": "rank(systematic_risk_last_360_days / (systematic_risk_last_360_days + unsystematic_risk_last_360_days + 0.001) - systematic_risk_last_30_days / (systematic_risk_last_30_days + unsystematic_risk_last_30_days + 0.001))"},
        {"template_id": "rsd_09", "expression": "rank(-beta_last_60_days_spy * systematic_risk_last_60_days / (unsystematic_risk_last_60_days + 0.001))"},
        {"template_id": "rsd_10", "expression": "group_zscore(-ts_delta(systematic_risk_last_30_days / (systematic_risk_last_30_days + unsystematic_risk_last_30_days + 0.001), 20), subindustry)"},
        {"template_id": "rsd_11", "expression": "ts_decay_linear(rank(unsystematic_risk_last_60_days - systematic_risk_last_60_days), 10)"},
        {"template_id": "rsd_12", "expression": "group_zscore(unsystematic_risk_last_60_days - systematic_risk_last_60_days, subindustry)"},
    ],

    "risk_correlation_regime": [
        {"template_id": "rcr_01", "expression": "rank(-(correlation_last_30_days_spy - correlation_last_60_days_spy))"},
        {"template_id": "rcr_02", "expression": "rank(-ts_delta(correlation_last_60_days_spy, 20))"},
        {"template_id": "rcr_03", "expression": "group_zscore(-correlation_last_60_days_spy, subindustry)"},
        {"template_id": "rcr_04", "expression": "group_zscore(-(correlation_last_30_days_spy - correlation_last_360_days_spy), industry)"},
        {"template_id": "rcr_05", "expression": "ts_decay_linear(rank(-correlation_last_30_days_spy), 10)"},
        {"template_id": "rcr_06", "expression": "rank(correlation_last_360_days_spy - correlation_last_30_days_spy)"},
        {"template_id": "rcr_07", "expression": "rank(-ts_delta(correlation_last_60_days_spy, 30)) * rank(-correlation_last_60_days_spy)"},
        {"template_id": "rcr_08", "expression": "rank(-ts_mean(correlation_last_60_days_spy, 20)) + rank(-ts_delta(correlation_last_30_days_spy, 10))"},
        {"template_id": "rcr_09", "expression": "group_zscore(ts_delta(correlation_last_30_days_spy, 10) - ts_delta(correlation_last_60_days_spy, 10), subindustry)"},
        {"template_id": "rcr_10", "expression": "rank(-ts_mean(correlation_last_60_days_spy, 20)) + rank(-ts_delta(correlation_last_30_days_spy, 10))"},
        {"template_id": "rcr_11", "expression": "rank(correlation_last_360_days_spy - correlation_last_30_days_spy)"},
        {"template_id": "rcr_12", "expression": "rank(-ts_delta(correlation_last_60_days_spy, 30)) * rank(-correlation_last_60_days_spy)"},
    ],

    # ============================================================
    # CATEGORY 16: ANALYST
    # ============================================================

    "analyst_revision_breadth": [
        {"template_id": "arb_01", "expression": "rank(sign(ts_delta(est_sales, 20)) + sign(ts_delta(est_ebitda, 20)) + sign(ts_delta(est_eps, 20)) + sign(ts_delta(est_cashflow_op, 20)))"},
        {"template_id": "arb_02", "expression": "group_zscore(sign(ts_delta(est_sales, 60)) + sign(ts_delta(est_ebitda, 60)) + sign(ts_delta(est_ptp, 60)) + sign(ts_delta(est_eps, 60)), subindustry)"},
        {"template_id": "arb_03", "expression": "ts_decay_linear(rank(ts_delta(est_eps, 20) + ts_delta(est_sales, 20)), 10)"},
        {"template_id": "arb_04", "expression": "ts_zscore(ts_delta(est_sales, 20) + ts_delta(est_ebitda, 20), 60)"},
        {"template_id": "arb_05", "expression": "rank(sign(ts_delta(est_sales, 20)) + sign(ts_delta(est_eps, 20)) + sign(ts_delta(est_sales, 60)) + sign(ts_delta(est_eps, 60)))"},
        {"template_id": "arb_06", "expression": "group_zscore(ts_delta(est_sales, 20) - ts_delta(est_sales, 60), industry)"},
        {"template_id": "arb_07", "expression": "ts_decay_linear(sign(ts_delta(est_sales, 20)) + sign(ts_delta(est_ebitda, 20)) + sign(ts_delta(est_eps, 20)) + sign(ts_delta(est_cashflow_op, 20)) + sign(ts_delta(est_eps, 20)), 15)"},
        {"template_id": "arb_08", "expression": "rank(ts_delta(est_eps, 20)) + rank(ts_delta(bookvalue_ps, 20))"},
        {"template_id": "arb_09", "expression": "ts_decay_linear(sign(ts_delta(est_sales, 20)) + sign(ts_delta(est_ebitda, 20)) + sign(ts_delta(est_eps, 20)) + sign(ts_delta(est_cashflow_op, 20)) + sign(ts_delta(est_eps, 20)), 15)"},
        {"template_id": "arb_10", "expression": "group_zscore(ts_delta(est_sales, 20) - ts_delta(est_sales, 60), industry)"},
    ],

    "analyst_coverage_dispersion": [
        {"template_id": "acd_01", "expression": "rank(-(anl4_ady_high - anl4_ady_low) / (anl4_ady_mean + 0.001))"},
        {"template_id": "acd_02", "expression": "group_zscore((anl4_ady_mean - anl4_ady_median) / (anl4_ady_high - anl4_ady_low + 0.001), subindustry)"},
        {"template_id": "acd_03", "expression": "rank(ts_delta(anl4_ady_numest, 20))"},
        {"template_id": "acd_04", "expression": "group_zscore(-(anl4_ady_high - anl4_ady_low) / (anl4_ady_mean + 0.001), industry)"},
        {"template_id": "acd_05", "expression": "rank(-anl4_ady_down / (anl4_ady_numest + 0.001))"},
        {"template_id": "acd_06", "expression": "group_zscore(anl4_ady_pu / (anl4_ady_numest + 0.001), subindustry)"},
        {"template_id": "acd_07", "expression": "rank(-ts_delta((anl4_ady_high - anl4_ady_low) / (anl4_ady_mean + 0.001), 20))"},
        {"template_id": "acd_08", "expression": "ts_decay_linear(rank(ts_delta(anl4_ady_mean, 20)), 10)"},
        {"template_id": "acd_09", "expression": "group_zscore(anl4_ady_pu / (anl4_ady_numest + 0.001), subindustry)"},
        {"template_id": "acd_10", "expression": "rank(ts_delta(anl4_ady_numest, 20) - ts_delta(anl4_ady_numest, 60))"},
    ],

    "analyst_target_price": [
        {"template_id": "atp_01", "expression": "rank(snt1_d1_uptargetpercent - snt1_d1_downtargetpercent)"},
        {"template_id": "atp_02", "expression": "group_zscore(snt1_d1_nettargetpercent, subindustry)"},
        {"template_id": "atp_03", "expression": "rank(ts_delta(snt1_d1_nettargetpercent, 20))"},
        {"template_id": "atp_04", "expression": "ts_decay_linear(rank(snt1_d1_nettargetpercent), 10)"},
        {"template_id": "atp_05", "expression": "rank(-snt1_d1_stockrank) + rank(snt1_d1_nettargetpercent)"},
        {"template_id": "atp_06", "expression": "group_zscore(snt1_d1_uptargetpercent / (snt1_d1_downtargetpercent + 0.001), industry)"},
        {"template_id": "atp_07", "expression": "rank(-snt1_d1_dynamicfocusrank) * rank(snt1_d1_nettargetpercent)"},
        {"template_id": "atp_08", "expression": "group_zscore(ts_delta(snt1_d1_nettargetpercent, 10) - ts_delta(snt1_d1_nettargetpercent, 40), subindustry)"},
        {"template_id": "atp_09", "expression": "rank(-snt1_d1_dynamicfocusrank) * rank(snt1_d1_nettargetpercent)"},
        {"template_id": "atp_10", "expression": "group_zscore(ts_delta(snt1_d1_nettargetpercent, 10) - ts_delta(snt1_d1_nettargetpercent, 40), subindustry)"},
    ],

    "analyst_recommendations": [
        {"template_id": "arc_01", "expression": "rank(snt1_d1_buyrecpercent - snt1_d1_sellrecpercent)"},
        {"template_id": "arc_02", "expression": "group_zscore(snt1_d1_netrecpercent, subindustry)"},
        {"template_id": "arc_03", "expression": "rank(ts_delta(snt1_d1_netrecpercent, 20))"},
        {"template_id": "arc_04", "expression": "ts_decay_linear(rank(snt1_d1_netrecpercent), 15)"},
        {"template_id": "arc_05", "expression": "group_zscore(snt1_d1_netrecpercent * snt1_d1_analystcoverage, industry)"},
        {"template_id": "arc_06", "expression": "rank(snt1_d1_earningsrevision) + rank(snt1_d1_netrecpercent)"},
        {"template_id": "arc_07", "expression": "rank(snt1_cored1_score) * rank(snt1_d1_buyrecpercent - snt1_d1_sellrecpercent)"},
        {"template_id": "arc_08", "expression": "group_zscore(ts_delta(snt1_d1_netrecpercent, 10) - ts_delta(snt1_d1_netrecpercent, 40), subindustry)"},
        {"template_id": "ar_09", "expression": "rank(snt1_cored1_score) * rank(snt1_d1_buyrecpercent - snt1_d1_sellrecpercent)"},
        {"template_id": "ar_10", "expression": "group_zscore(ts_delta(snt1_d1_netrecpercent, 10) - ts_delta(snt1_d1_netrecpercent, 40), subindustry)"},
    ],

    "analyst_derivative_scores": [
        {"template_id": "ads_01", "expression": "rank(analyst_revision_rank_derivative + earnings_certainty_rank_derivative)"},
        {"template_id": "ads_02", "expression": "group_zscore(composite_factor_score_derivative, subindustry)"},
        {"template_id": "ads_03", "expression": "rank(ts_delta(multi_factor_acceleration_score_derivative, 20))"},
        {"template_id": "ads_04", "expression": "ts_zscore(growth_potential_rank_derivative, 60)"},
        {"template_id": "ads_05", "expression": "ts_decay_linear(rank(analyst_revision_rank_derivative), 15)"},
        {"template_id": "ads_06", "expression": "group_zscore(analyst_revision_rank_derivative + earnings_certainty_rank_derivative + growth_potential_rank_derivative + relative_valuation_rank_derivative, industry)"},
        {"template_id": "ads_07", "expression": "rank(multi_factor_static_score_derivative) + rank(multi_factor_acceleration_score_derivative)"},
        {"template_id": "ads_08", "expression": "ts_decay_linear(rank(cashflow_efficiency_rank_derivative) + rank(relative_valuation_rank_derivative), 10)"},
        {"template_id": "ads_09", "expression": "ts_decay_linear(rank(cashflow_efficiency_rank_derivative) + rank(relative_valuation_rank_derivative), 10)"},
        {"template_id": "ads_10", "expression": "rank(ts_zscore(analyst_revision_rank_derivative, 40)) * rank(ts_zscore(earnings_certainty_rank_derivative, 40))"},
    ],

    # ============================================================
    # CATEGORY 17: EARNINGS
    # ============================================================

    "earnings_sue_pead": [
        {"template_id": "esp_01", "expression": "rank(ts_decay_linear(standardized_unexpected_earnings, 5))"},
        {"template_id": "esp_02", "expression": "rank(ts_decay_linear(standardized_unexpected_earnings, 40))"},
        {"template_id": "esp_03", "expression": "signed_power(zscore(standardized_unexpected_earnings), 0.5)"},
        {"template_id": "esp_04", "expression": "ts_decay_linear(standardized_unexpected_earnings, 20) * rank(volume / (adv20 + 0.001))"},
        {"template_id": "esp_05", "expression": "rank(ts_decay_linear(change_in_eps_surprise, 10))"},
        {"template_id": "esp_06", "expression": "rank(ts_decay_linear(earnings_revision_magnitude, 20))"},
        {"template_id": "esp_07", "expression": "rank(standardized_unexpected_earnings) + rank(earnings_revision_magnitude)"},
        {"template_id": "esp_08", "expression": "ts_zscore(standardized_unexpected_earnings, 60) + ts_decay_linear(rank(earnings_revision_magnitude), 20)"},
        {"template_id": "esp_09", "expression": "rank(standardized_unexpected_earnings) + rank(earnings_revision_magnitude)"},
        {"template_id": "esp_10", "expression": "ts_zscore(standardized_unexpected_earnings, 60) + ts_decay_linear(rank(earnings_revision_magnitude), 20)"},
    ],

    "earnings_quality_quarterly": [
        {"template_id": "eqq_01", "expression": "rank(ts_backfill(cashflow_op, 60) / (ts_backfill(income, 60) + 0.001))"},
        {"template_id": "eqq_02", "expression": "rank(ts_backfill(cashflow, 60) / (ts_backfill(ebitda, 60) + 0.001))"},
        {"template_id": "eqq_03", "expression": "rank(ts_backfill(cashflow_op, 60) / (ts_backfill(sales, 60) + 0.001))"},
        {"template_id": "eqq_04", "expression": "rank(ts_delta(ts_backfill(cashflow_op, 60) / (ts_backfill(income, 60) + 0.001), 60))"},
        {"template_id": "eqq_05", "expression": "rank(ts_backfill(cashflow_op, 60) - ts_backfill(income, 60))"},
        {"template_id": "eqq_06", "expression": "group_zscore(ts_backfill(revenue, 60) / (ts_backfill(sales, 60) + 0.001), subindustry)"},
        {"template_id": "eqq_07", "expression": "zscore(ts_backfill(cashflow, 60) / (cap + 0.001))"},
        {"template_id": "eqq_08", "expression": "rank(ts_backfill(cashflow_op, 60) / (ts_backfill(income, 60) + 0.001)) + rank(ts_backfill(cashflow, 60) / (ts_backfill(ebitda, 60) + 0.001))"},
        {"template_id": "eqq_09", "expression": "rank(ts_delta(ts_backfill(cashflow, 60) / (ts_backfill(ebitda, 60) + 0.001), 60))"},
        {"template_id": "eqq_10", "expression": "rank(ts_backfill(cashflow_op, 60) / (ts_backfill(income, 60) + 0.001)) + rank(ts_backfill(cashflow, 60) / (ts_backfill(ebitda, 60) + 0.001))"},
    ],

    "earnings_surprise_magnitude": [
        {"template_id": "esm_01", "expression": "rank(ts_delta(eps, 60) / (ts_std_dev(eps, 252) + 0.001))"},
        {"template_id": "esm_02", "expression": "rank((ts_backfill(actual_sales_value_quarterly, 60) - est_sales) / (abs(est_sales) + 0.001))"},
        {"template_id": "esm_03", "expression": "ts_decay_linear(rank(ts_delta(eps, 60) / (ts_std_dev(eps, 252) + 0.001)), 20)"},
        {"template_id": "esm_04", "expression": "rank(ts_delta(eps, 60) / (ts_std_dev(eps, 252) + 0.001)) * rank(volume / (adv20 + 0.001))"},
        {"template_id": "esm_05", "expression": "group_zscore(ts_delta(eps, 60) / (ts_std_dev(eps, 252) + 0.001), subindustry)"},
        {"template_id": "esm_06", "expression": "signed_power(rank(ts_delta(eps, 60) / (ts_std_dev(eps, 252) + 0.001)), 0.5)"},
        {"template_id": "esm_07", "expression": "rank(ts_delta(ebit, 60) / (ts_std_dev(ebit, 252) + 0.001))"},
        {"template_id": "esm_08", "expression": "rank(ts_delta(revenue, 60) / (ts_std_dev(revenue, 252) + 0.001))"},
        {"template_id": "esm_09", "expression": "group_zscore(ts_backfill(actual_eps_value_quarterly, 60) - est_eps / (sharesout + 0.001), subindustry)"},
        {"template_id": "esm_10", "expression": "signed_power(rank(ts_backfill(actual_eps_value_quarterly, 60) - est_eps / (sharesout + 0.001)) + rank((ts_backfill(actual_sales_value_quarterly, 60) - est_sales) / (abs(est_sales) + 0.001)), 0.5)"},
    ],

    "earnings_torpedo": [
        {"template_id": "etr_01", "expression": "-rank(ts_decay_linear(earnings_torpedo_indicator, 10))"},
        {"template_id": "etr_02", "expression": "-ts_decay_linear(rank(earnings_torpedo_indicator), 40)"},
        {"template_id": "etr_03", "expression": "rank(ts_decay_linear(earnings_shortfall_metric, 20))"},
        {"template_id": "etr_04", "expression": "-rank(earnings_torpedo_indicator) + rank(visibility_ratio)"},
        {"template_id": "etr_05", "expression": "-rank(coefficient_variation_fy1_eps)"},
        {"template_id": "etr_06", "expression": "rank(snt1_d1_buyrecpercent) - rank(earnings_torpedo_indicator)"},
        {"template_id": "etr_07", "expression": "-rank(ts_delta(earnings_torpedo_indicator, 20))"},
        {"template_id": "etr_08", "expression": "-rank(earnings_torpedo_indicator) - rank(coefficient_variation_fy1_eps) + rank(visibility_ratio)"},
        {"template_id": "et_09", "expression": "-rank(ts_delta(earnings_torpedo_indicator, 20))"},
        {"template_id": "et_10", "expression": "ts_decay_linear(rank(earnings_shortfall_metric), 40) - ts_decay_linear(rank(earnings_torpedo_indicator), 40)"},
    ],

    # ============================================================
    # CATEGORY 18: CORPORATE EVENTS
    # ============================================================

    "event_mna": [
        {"template_id": "emna_01", "expression": "rank(group_zscore(ts_decay_linear(ts_backfill(rp_css_mna, 60), 5), subindustry))"},
        {"template_id": "emna_02", "expression": "rank(ts_decay_linear(ts_backfill(rp_nip_mna, 60), 10)) - rank(ts_decay_linear(ts_backfill(rp_nip_mna, 60), 40))"},
        {"template_id": "emna_03", "expression": "rank(ts_decay_linear(ts_backfill(rp_css_mna, 60), 5) * ts_decay_linear(ts_backfill(rp_nip_mna, 60), 5))"},
        {"template_id": "emna_04", "expression": "ts_decay_linear(ts_backfill(rp_css_mna, 60), 5) - ts_decay_linear(ts_backfill(rp_css_mna, 60), 20)"},
        {"template_id": "emna_05", "expression": "rank(ts_delta(ts_backfill(rp_css_mna, 60), 5))"},
        {"template_id": "emna_06", "expression": "rank(sign(ts_backfill(rp_css_mna, 60)) / (days_from_last_change(ts_backfill(rp_css_mna, 60)) + 0.001))"},
        {"template_id": "emna_07", "expression": "signed_power(quantile(ts_decay_linear(ts_backfill(rp_css_mna, 60), 5)), 0.5)"},
        {"template_id": "emna_08", "expression": "rank(ts_decay_linear(ts_backfill(rp_css_mna, 60), 10) + ts_decay_linear(ts_backfill(rp_nip_mna, 60), 10))"},
        {"template_id": "em_09", "expression": "rank(sign(ts_backfill(rp_css_mna, 60)) / (days_from_last_change(ts_backfill(rp_css_mna, 60)) + 0.001))"},
        {"template_id": "em_10", "expression": "rank(0.6 * ts_decay_linear(ts_backfill(rp_css_mna, 60), 10) + 0.4 * ts_decay_linear(ts_backfill(rp_nip_mna, 60), 10))"},
    ],

    "event_insider": [
        {"template_id": "ein_01", "expression": "group_zscore(rank(ts_decay_linear(ts_backfill(rp_css_insider, 60), 5)), subindustry)"},
        {"template_id": "ein_02", "expression": "rank(ts_decay_linear(ts_backfill(rp_ess_insider, 60), 10) - ts_decay_linear(ts_backfill(rp_ess_insider, 60), 40))"},
        {"template_id": "ein_03", "expression": "rank(ts_decay_linear(ts_backfill(rp_css_insider, 60), 5) * ts_decay_linear(ts_backfill(rp_ess_insider, 60), 5))"},
        {"template_id": "ein_04", "expression": "rank(ts_delta(ts_backfill(rp_css_insider, 60), 5) + ts_delta(ts_backfill(rp_ess_insider, 60), 20))"},
        {"template_id": "ein_05", "expression": "rank(ts_decay_linear(ts_backfill(rp_css_insider, 60), 5) / (historical_volatility_30 + 0.001))"},
        {"template_id": "ein_06", "expression": "-ts_corr(ts_backfill(rp_css_insider, 60), returns, 20)"},
        {"template_id": "ein_07", "expression": "signed_power(rank(ts_decay_linear(ts_backfill(rp_css_insider, 60), 10) + ts_decay_linear(ts_backfill(rp_ess_insider, 60), 10)), 0.5)"},
        {"template_id": "ein_08", "expression": "rank(ts_decay_linear(ts_backfill(rp_css_insider, 60), 5) * ts_zscore(volume, 20))"},
        {"template_id": "ei_09", "expression": "-ts_corr(ts_backfill(rp_css_insider, 60), returns, 20)"},
        {"template_id": "ei_10", "expression": "signed_power(rank(ts_decay_linear(ts_backfill(rp_css_insider, 60), 10) + ts_decay_linear(ts_backfill(rp_ess_insider, 60), 10)), 0.5)"},
    ],

    "event_business": [
        {"template_id": "ebs_01", "expression": "rank(group_zscore(ts_decay_linear(ts_backfill(rp_css_business, 60), 5), subindustry))"},
        {"template_id": "ebs_02", "expression": "rank(ts_decay_linear(ts_backfill(rp_ess_business, 60), 10) - ts_decay_linear(ts_backfill(rp_ess_business, 60), 40))"},
        {"template_id": "ebs_03", "expression": "rank(ts_decay_linear(ts_backfill(rp_css_business, 60), 5) * ts_decay_linear(ts_backfill(rp_ess_business, 60), 5))"},
        {"template_id": "ebs_04", "expression": "sign(ts_backfill(rp_css_business, 60)) * rank(-days_from_last_change(ts_backfill(rp_css_business, 60)))"},
        {"template_id": "ebs_05", "expression": "trade_when(ts_zscore(volume, 20) > 1, rank(ts_decay_linear(ts_backfill(rp_css_business, 60), 5)), -1)"},
        {"template_id": "ebs_06", "expression": "group_zscore(ts_zscore(ts_backfill(rp_css_business, 60), 60), subindustry)"},
        {"template_id": "ebs_07", "expression": "rank(ts_decay_linear(ts_backfill(rp_css_business, 60), 10) + ts_decay_linear(ts_backfill(rp_ess_business, 60), 20))"},
        {"template_id": "ebs_08", "expression": "rank(ts_delta(ts_backfill(rp_css_business, 60), 10))"},
        {"template_id": "eb_09", "expression": "sign(ts_backfill(rp_css_business, 60)) * rank(-days_from_last_change(ts_backfill(rp_css_business, 60)))"},
        {"template_id": "eb_10", "expression": "rank(ts_decay_linear(ts_backfill(rp_css_business, 60), 10) + ts_decay_linear(ts_backfill(rp_ess_business, 60), 20))"},
    ],

    "event_credit": [
        {"template_id": "ecr_01", "expression": "rank(group_zscore(ts_decay_linear(ts_backfill(rp_css_credit, 60), 5), subindustry))"},
        {"template_id": "ecr_02", "expression": "rank(ts_decay_linear(ts_backfill(rp_ess_credit, 60), 10) - ts_decay_linear(ts_backfill(rp_ess_credit, 60), 40))"},
        {"template_id": "ecr_03", "expression": "rank(ts_decay_linear(ts_backfill(rp_nip_credit, 60), 10))"},
        {"template_id": "ecr_04", "expression": "rank(ts_decay_linear(ts_backfill(rp_css_credit, 60), 5) * (debt_lt + debt_st) / (cap + 0.001))"},
        {"template_id": "ecr_05", "expression": "rank(ts_delta(ts_backfill(rp_css_credit, 60), 10))"},
        {"template_id": "ecr_06", "expression": "rank(ts_decay_linear(ts_backfill(rp_css_credit, 60), 5) * ts_backfill(debt, 60) / (cap + 0.001))"},
        {"template_id": "ecr_07", "expression": "-ts_corr(ts_backfill(rp_css_credit, 60), returns, 20)"},
        {"template_id": "ecr_08", "expression": "sign(ts_backfill(rp_css_credit, 60)) * rank(-days_from_last_change(ts_backfill(rp_css_credit, 60))) + rank(ts_decay_linear(ts_backfill(rp_css_credit, 60), 10))"},
        {"template_id": "ec_09", "expression": "-ts_corr(ts_backfill(rp_css_credit, 60), returns, 20)"},
        {"template_id": "ec_10", "expression": "sign(ts_backfill(rp_css_credit, 60)) * rank(-days_from_last_change(ts_backfill(rp_css_credit, 60))) + rank(ts_decay_linear(ts_backfill(rp_css_credit, 60), 10))"},
    ],

    # ============================================================
    # CATEGORY 19: TECHNICAL
    # ============================================================

    "tech_rsi_like": [
        {"template_id": "trl_01", "expression": "rank(-ts_quantile(returns, 20))"},
        {"template_id": "trl_02", "expression": "rank(1 - ts_rank(returns, 40))"},
        {"template_id": "trl_03", "expression": "rank(-(ts_quantile(returns, 10) + ts_quantile(returns, 40) + ts_quantile(returns, 120)) / 3)"},
        {"template_id": "trl_04", "expression": "rank(-ts_quantile(close / (vwap + 0.001), 20))"},
        {"template_id": "trl_05", "expression": "group_zscore(-ts_quantile(returns, 60), subindustry)"},
        {"template_id": "trl_06", "expression": "rank(-(ts_sum(max(returns, 0), 14) / (ts_sum(abs(returns), 14) + 0.001)))"},
        {"template_id": "trl_07", "expression": "rank(-((close - ts_mean(low, 14)) / (ts_mean(high, 14) - ts_mean(low, 14) + 0.001)))"},
        {"template_id": "trl_08", "expression": "rank(-signed_power(ts_quantile(returns, 40) - 0.5, 2))"},
        {"template_id": "trl_09", "expression": "rank(-(ts_sum(max(returns, 0), 14) / (ts_sum(abs(returns), 14) + 0.001)))"},
        {"template_id": "trl_10", "expression": "rank(-((close - ts_mean(low, 14)) / (ts_mean(high, 14) - ts_mean(low, 14) + 0.001)))"},
    ],

    "tech_bollinger_like": [
        {"template_id": "tbl_01", "expression": "rank(-(close - ts_mean(close, 20)) / (ts_std_dev(close, 20) + 0.001))"},
        {"template_id": "tbl_02", "expression": "rank(-(close - ts_mean(close, 60)) / (ts_std_dev(close, 60) + 0.001))"},
        {"template_id": "tbl_03", "expression": "rank(ts_std_dev(close, 20) / (ts_mean(close, 20) + 0.001))"},
        {"template_id": "tbl_04", "expression": "rank(-(returns - ts_mean(returns, 20)) / (ts_std_dev(returns, 20) + 0.001))"},
        {"template_id": "tbl_05", "expression": "rank(-(vwap - ts_mean(vwap, 20)) / (ts_std_dev(vwap, 20) + 0.001))"},
        {"template_id": "tbl_06", "expression": "rank(-((close - ts_mean(close, 10)) / (ts_std_dev(close, 10) + 0.001) + (close - ts_mean(close, 40)) / (ts_std_dev(close, 40) + 0.001)) / 2)"},
        {"template_id": "tbl_07", "expression": "group_zscore(-(close - ts_mean(close, 20)) / (ts_std_dev(close, 20) + 0.001), subindustry)"},
        {"template_id": "tbl_08", "expression": "rank(-(close - ts_mean(close, 20)) / (ts_std_dev(close, 20) + 0.001)) * rank(volume / (adv20 + 0.001))"},
        {"template_id": "tbl_09", "expression": "rank(-((close - ts_mean(close, 20)) / (ts_std_dev(close, 20) + 0.001)) * volume / (adv20 + 0.001))"},
        {"template_id": "tbl_10", "expression": "group_zscore(-(close - ts_mean(close, 20)) / (ts_std_dev(close, 20) + 0.001), subindustry)"},
    ],

    "tech_macd_like": [
        {"template_id": "tml_01", "expression": "rank(ts_mean(close, 5) - ts_mean(close, 20))"},
        {"template_id": "tml_02", "expression": "rank(ts_mean(close, 10) - ts_mean(close, 40))"},
        {"template_id": "tml_03", "expression": "rank((ts_mean(close, 5) - ts_mean(close, 20)) / (close + 0.001))"},
        {"template_id": "tml_04", "expression": "rank(ts_mean(ts_mean(close, 5) - ts_mean(close, 20), 9))"},
        {"template_id": "tml_05", "expression": "rank((ts_mean(close, 5) - ts_mean(close, 20)) - ts_mean(ts_mean(close, 5) - ts_mean(close, 20), 9))"},
        {"template_id": "tml_06", "expression": "rank(ts_mean(vwap, 5) - ts_mean(vwap, 20))"},
        {"template_id": "tml_07", "expression": "group_rank((ts_mean(close, 5) - ts_mean(close, 60)) / (close + 0.001), subindustry)"},
        {"template_id": "tml_08", "expression": "rank(((ts_mean(close, 5) - ts_mean(close, 20)) + (ts_mean(close, 10) - ts_mean(close, 40)) + (ts_mean(close, 20) - ts_mean(close, 60))) / (close + 0.001))"},
        {"template_id": "tml_09", "expression": "rank(ts_delta(ts_mean(close, 5) - ts_mean(close, 20), 5))"},
        {"template_id": "tml_10", "expression": "rank(((ts_mean(close, 5) - ts_mean(close, 20)) + (ts_mean(close, 10) - ts_mean(close, 40)) + (ts_mean(close, 20) - ts_mean(close, 60))) / (close + 0.001))"},
    ],

    "tech_breakout": [
        {"template_id": "tbr_01", "expression": "rank(-ts_arg_max(close, 20))"},
        {"template_id": "tbr_02", "expression": "rank(-ts_arg_max(close, 60))"},
        {"template_id": "tbr_03", "expression": "rank(ts_arg_min(close, 60))"},
        {"template_id": "tbr_04", "expression": "rank(ts_arg_min(close, 60) - ts_arg_max(close, 60))"},
        {"template_id": "tbr_05", "expression": "rank(close / (kth_element(close, 1, k=60) + 0.001))"},
        {"template_id": "tbr_06", "expression": "rank((close - kth_element(close, -1, k=60)) / (kth_element(close, 1, k=60) - kth_element(close, -1, k=60) + 0.001))"},
        {"template_id": "tbr_07", "expression": "rank(volume / (adv20 + 0.001) * close / (kth_element(close, 1, k=60) + 0.001))"},
        {"template_id": "tbr_08", "expression": "rank(-(ts_arg_max(close, 20) + ts_arg_max(close, 60) + ts_arg_max(close, 120)))"},
        {"template_id": "tbr_09", "expression": "group_rank(-ts_arg_max(close, 40) + ts_arg_min(close, 40), subindustry)"},
        {"template_id": "tbr_10", "expression": "rank(ts_decay_linear(1 / (1 + ts_arg_max(close, 20)), 20))"},
    ],

    "tech_trend_strength": [
        {"template_id": "tts_01", "expression": "rank(ts_regression(close, ts_step(20), 20, lag=0, rettype=2))"},
        {"template_id": "tts_02", "expression": "rank(ts_regression(close, ts_step(60), 60, lag=0, rettype=2) / (close + 0.001))"},
        {"template_id": "tts_03", "expression": "rank(ts_regression(close, ts_step(40), 40, lag=0, rettype=6))"},
        {"template_id": "tts_04", "expression": "rank(ts_regression(close, ts_step(40), 40, lag=0, rettype=2) * ts_regression(close, ts_step(40), 40, lag=0, rettype=6))"},
        {"template_id": "tts_05", "expression": "rank(ts_corr(close, ts_step(60), 60))"},
        {"template_id": "tts_06", "expression": "rank(ts_corr(close, ts_step(60), 60) - ts_corr(volume, ts_step(60), 60))"},
        {"template_id": "tts_07", "expression": "rank((ts_corr(close, ts_step(20), 20) + ts_corr(close, ts_step(40), 40) + ts_corr(close, ts_step(60), 60)) / 3)"},
        {"template_id": "tts_08", "expression": "rank(ts_delta(ts_corr(close, ts_step(20), 20), 10))"},
        {"template_id": "tts_09", "expression": "rank((ts_corr(close, ts_step(20), 20) + ts_corr(close, ts_step(40), 40) + ts_corr(close, ts_step(60), 60)) / 3)"},
        {"template_id": "tts_10", "expression": "rank(ts_delta(ts_corr(close, ts_step(20), 20), 10))"},
    ],

    # ============================================================
    # CATEGORIES 20-22: INTERACTIONS, DERIVATIVES, COMPOSITES
    # ============================================================

    "interact_value_x_quality": [
        {"template_id": "ivq_01", "expression": "rank(equity / (cap + 0.001)) * rank((revenue - cogs) / (revenue + 0.001))"},
        {"template_id": "ivq_02", "expression": "group_zscore(ebitda / (cap + 0.001), sector) * group_zscore(current_ratio, sector)"},
        {"template_id": "ivq_03", "expression": "signed_power(rank(equity / (cap + 0.001)) * rank(fscore_quality), 0.5)"},
        {"template_id": "ivq_04", "expression": "ts_decay_linear(rank(ts_backfill(ebitda, 60) / (cap + 0.001)) * rank(ts_backfill(revenue, 60) / (ts_backfill(sales, 60) + 0.001)), 20)"},
        {"template_id": "ivq_05", "expression": "if_else(rank(fscore_quality) > 0.5, group_zscore(equity / (cap + 0.001), subindustry), -group_zscore(equity / (cap + 0.001), subindustry))"},
        {"template_id": "ivq_06", "expression": "winsorize(group_zscore(ebitda / (cap + 0.001), subindustry) * group_zscore(retained_earnings / (cap + 0.001), subindustry) * group_zscore(current_ratio, subindustry))"},
        {"template_id": "ivq_07", "expression": "ts_rank(rank(equity / (cap + 0.001)) * rank(fscore_profitability), 60)"},
        {"template_id": "ivq_08", "expression": "signed_power(group_zscore(ebit / (cap + 0.001), industry), 0.5) * signed_power(group_zscore((revenue - cogs) / (revenue + 0.001), industry), 0.5)"},
    ],

    "interact_momentum_x_quality": [
        {"template_id": "imq_01", "expression": "rank(ts_delta(close, 60) / (close + 0.001)) * rank(fscore_quality)"},
        {"template_id": "imq_02", "expression": "rank(ts_delta(close, 20) / (close + 0.001)) * rank((revenue - cogs) / (revenue + 0.001))"},
        {"template_id": "imq_03", "expression": "group_zscore(ts_decay_linear(returns, 40), sector) * group_zscore(fscore_profitability, sector)"},
        {"template_id": "imq_04", "expression": "signed_power(rank(ts_delta(close, 120) / (close + 0.001)) * rank(fscore_quality), 0.5)"},
        {"template_id": "imq_05", "expression": "if_else(rank(fscore_quality) > 0.6, ts_decay_linear(returns, 60), -abs(ts_decay_linear(returns, 60)))"},
        {"template_id": "imq_06", "expression": "ts_rank(ts_delta(close, 60) / (close + 0.001), 252) * group_zscore(ts_backfill(revenue, 60) / (ts_backfill(sales, 60) + 0.001), sector)"},
        {"template_id": "imq_07", "expression": "winsorize(rank(ts_decay_linear(returns, 90)) * rank(fscore_profitability))"},
        {"template_id": "imq_08", "expression": "ts_zscore(close, 120) * rank(fscore_quality) * rank(current_ratio)"},
    ],

    "interact_value_x_momentum": [
        {"template_id": "ivm_01", "expression": "rank(equity / (cap + 0.001)) * rank(ts_delta(close, 120) / (close + 0.001))"},
        {"template_id": "ivm_02", "expression": "rank(ebitda / (cap + 0.001)) * rank(ts_decay_linear(returns, 60))"},
        {"template_id": "ivm_03", "expression": "signed_power(rank(equity / (cap + 0.001)) * rank(ts_delta(close, 60) / (close + 0.001)), 0.5)"},
        {"template_id": "ivm_04", "expression": "group_zscore(ebitda / (cap + 0.001), sector) * group_zscore(ts_decay_linear(returns, 120), sector)"},
        {"template_id": "ivm_05", "expression": "if_else(ts_delta(close, 120) / (close + 0.001) > 0, rank(equity / (cap + 0.001)), -rank(equity / (cap + 0.001)) * 0.5)"},
        {"template_id": "ivm_06", "expression": "ts_decay_linear(group_zscore(ebit / (cap + 0.001), industry) * ts_zscore(close, 60), 20)"},
        {"template_id": "ivm_07", "expression": "winsorize(group_zscore(equity / (cap + 0.001), industry) * ts_rank(returns, 120))"},
        {"template_id": "ivm_08", "expression": "rank(equity / (cap + 0.001)) * rank(ts_decay_linear(returns, 40)) + rank(ebitda / (cap + 0.001)) * rank(ts_delta(close, 120) / (close + 0.001))"},
    ],

    "interact_sentiment_x_fundamental": [
        {"template_id": "isf_01", "expression": "rank(scl12_sentiment) * rank(ebitda / (cap + 0.001))"},
        {"template_id": "isf_02", "expression": "rank(snt_value) * rank(fscore_total)"},
        {"template_id": "isf_03", "expression": "ts_corr(scl12_sentiment, returns, 60) * rank(fscore_total)"},
        {"template_id": "isf_04", "expression": "if_else(rank(snt_value) > 0.6, group_zscore(ebitda / (cap + 0.001), sector), -group_zscore(ebitda / (cap + 0.001), sector) * 0.5)"},
        {"template_id": "isf_05", "expression": "signed_power(rank(ts_decay_linear(ts_backfill(rp_css_earnings, 60), 5)) * rank(fscore_quality), 0.5)"},
        {"template_id": "isf_06", "expression": "ts_decay_linear(rank(scl12_sentiment) * rank(ebitda / (cap + 0.001)), 20)"},
        {"template_id": "isf_07", "expression": "group_zscore(snt_value, sector) * group_zscore(fscore_total, sector)"},
        {"template_id": "isf_08", "expression": "ts_delta(scl12_sentiment, 20) * rank(fscore_quality) * rank(ebitda / (cap + 0.001))"},
    ],

    # --- Derivative Scores ---

    "deriv_fscore_composites": [
        {"template_id": "dfc_01", "expression": "(rank(fscore_value) + rank(fscore_growth) + rank(fscore_momentum) + rank(fscore_profitability) + rank(fscore_quality) + rank(fscore_total)) / 6"},
        {"template_id": "dfc_02", "expression": "group_zscore(2 * fscore_value + 2 * fscore_quality + fscore_growth + fscore_momentum + fscore_profitability, sector)"},
        {"template_id": "dfc_03", "expression": "rank(fscore_growth + fscore_momentum) + rank(fscore_surface + fscore_surface_accel)"},
        {"template_id": "dfc_04", "expression": "winsorize(group_zscore(fscore_profitability + fscore_quality + fscore_surface, subindustry))"},
        {"template_id": "dfc_05", "expression": "signed_power(rank(fscore_value) * rank(fscore_quality), 0.5)"},
        {"template_id": "dfc_06", "expression": "ts_decay_linear(rank(fscore_total + fscore_profitability + fscore_quality), 10)"},
        {"template_id": "dfc_07", "expression": "rank(3 * fscore_total + 2 * fscore_value + fscore_momentum)"},
        {"template_id": "dfc_08", "expression": "signed_power(rank(fscore_value) + rank(fscore_profitability) + rank(fscore_quality), 0.5)"},
    ],

    "deriv_fscore_bfl": [
        {"template_id": "dfb_01", "expression": "(rank(fscore_bfl_value) + rank(fscore_bfl_growth) + rank(fscore_bfl_momentum) + rank(fscore_bfl_profitability) + rank(fscore_bfl_quality)) / 5"},
        {"template_id": "dfb_02", "expression": "ts_decay_linear(fscore_bfl_total + fscore_bfl_quality + fscore_bfl_profitability, 20)"},
        {"template_id": "dfb_03", "expression": "rank(ts_delta(fscore_bfl_total, 20)) + rank(ts_delta(fscore_bfl_value, 20))"},
        {"template_id": "dfb_04", "expression": "group_zscore(fscore_bfl_total + fscore_bfl_surface + fscore_bfl_surface_accel, subindustry)"},
        {"template_id": "dfb_05", "expression": "signed_power(rank(fscore_bfl_surface) + rank(fscore_bfl_surface_accel), 0.5) * sign(fscore_bfl_surface_accel)"},
        {"template_id": "dfb_06", "expression": "winsorize(rank(fscore_bfl_total) + rank(fscore_bfl_value) + rank(fscore_bfl_quality))"},
        {"template_id": "dfb_07", "expression": "ts_mean(fscore_bfl_profitability + fscore_bfl_quality, 10) - ts_mean(fscore_bfl_profitability + fscore_bfl_quality, 40)"},
        {"template_id": "dfb_08", "expression": "rank(fscore_bfl_growth + fscore_bfl_momentum) * rank(fscore_bfl_surface_accel)"},
    ],

    "deriv_fscore_momentum": [
        {"template_id": "dfm_01", "expression": "rank(ts_delta(fscore_total, 20))"},
        {"template_id": "dfm_02", "expression": "ts_zscore(fscore_momentum, 60)"},
        {"template_id": "dfm_03", "expression": "rank(ts_delta(fscore_value, 5) + ts_delta(fscore_quality, 5))"},
        {"template_id": "dfm_04", "expression": "ts_decay_linear(ts_delta(fscore_profitability, 10), 20)"},
        {"template_id": "dfm_05", "expression": "ts_av_diff(fscore_total, 20)"},
        {"template_id": "dfm_06", "expression": "rank(ts_delta(fscore_value, 20)) + rank(ts_delta(fscore_growth, 20)) + rank(ts_delta(fscore_profitability, 20))"},
        {"template_id": "dfm_07", "expression": "rank(ts_delta(fscore_total, 5) - ts_delta(fscore_total, 20))"},
        {"template_id": "dfm_08", "expression": "ts_zscore(fscore_quality, 40) * rank(ts_delta(fscore_surface_accel, 10))"},
    ],

    "deriv_fscore_x_price": [
        {"template_id": "dfp_01", "expression": "fscore_total * rank(ts_delta(close, 60) / (close + 0.001))"},
        {"template_id": "dfp_02", "expression": "rank(fscore_quality) * rank(ts_decay_linear(returns, 20))"},
        {"template_id": "dfp_03", "expression": "fscore_momentum * rank(volume / (adv20 + 0.001))"},
        {"template_id": "dfp_04", "expression": "rank(fscore_value) * rank(-ts_delta(close, 5) / (close + 0.001))"},
        {"template_id": "dfp_05", "expression": "group_zscore(fscore_total, sector) * rank(ts_decay_linear(returns, 10))"},
        {"template_id": "dfp_06", "expression": "if_else(fscore_total > ts_mean(fscore_total, 60), rank(ts_delta(close, 20) / (close + 0.001)), -rank(ts_delta(close, 20) / (close + 0.001)))"},
        {"template_id": "dfp_07", "expression": "signed_power(rank(fscore_quality) * rank(fscore_profitability), 0.5) * sign(ts_delta(vwap, 10))"},
        {"template_id": "dfp_08", "expression": "ts_decay_linear(fscore_total * rank(returns), 15)"},
    ],

    "deriv_rank_composites": [
        {"template_id": "drc_01", "expression": "rank(analyst_revision_rank_derivative + cashflow_efficiency_rank_derivative + composite_factor_score_derivative + earnings_certainty_rank_derivative + growth_potential_rank_derivative + multi_factor_acceleration_score_derivative + multi_factor_static_score_derivative + relative_valuation_rank_derivative)"},
        {"template_id": "drc_02", "expression": "signed_power(rank(analyst_revision_rank_derivative + cashflow_efficiency_rank_derivative + earnings_certainty_rank_derivative + relative_valuation_rank_derivative), 0.5)"},
        {"template_id": "drc_03", "expression": "rank(growth_potential_rank_derivative + multi_factor_acceleration_score_derivative + composite_factor_score_derivative)"},
        {"template_id": "drc_04", "expression": "rank(ts_delta(composite_factor_score_derivative, 10)) + rank(ts_delta(multi_factor_acceleration_score_derivative, 10))"},
        {"template_id": "drc_05", "expression": "group_zscore(analyst_revision_rank_derivative + earnings_certainty_rank_derivative + multi_factor_static_score_derivative, subindustry)"},
        {"template_id": "drc_06", "expression": "winsorize(group_zscore(multi_factor_acceleration_score_derivative + multi_factor_static_score_derivative + composite_factor_score_derivative, sector))"},
        {"template_id": "drc_07", "expression": "ts_decay_linear(rank(analyst_revision_rank_derivative + growth_potential_rank_derivative + cashflow_efficiency_rank_derivative), 15)"},
        {"template_id": "drc_08", "expression": "signed_power(rank(multi_factor_static_score_derivative), 0.5) * sign(multi_factor_acceleration_score_derivative) + rank(relative_valuation_rank_derivative)"},
    ],

    # --- Composites ---

    "composite_vqm": [
        {"template_id": "cvqm_01", "expression": "rank(equity / (cap + 0.001)) + rank((revenue - cogs) / (revenue + 0.001)) + rank(ts_delta(close, 60) / (close + 0.001))"},
        {"template_id": "cvqm_02", "expression": "rank(ebitda / (cap + 0.001)) * rank(current_ratio) * rank(ts_delta(close, 20) / (close + 0.001))"},
        {"template_id": "cvqm_03", "expression": "rank(eps / (close + 0.001)) + rank(rd_expense / (revenue + 0.001)) + rank(ts_delta(close, 40) / (close + 0.001))"},
        {"template_id": "cvqm_04", "expression": "rank(equity / (cap + 0.001)) + rank(ts_backfill(revenue, 60) / (ts_backfill(sales, 60) + 0.001)) + rank(ts_delta(close, 60) / (close + 0.001))"},
        {"template_id": "cvqm_05", "expression": "group_zscore(ebitda / (cap + 0.001), sector) + group_zscore(ebit / (revenue + 0.001), sector) + group_zscore(ts_delta(close, 120) / (close + 0.001), sector)"},
        {"template_id": "cvqm_06", "expression": "rank(ebitda / (cap + 0.001)) + rank(income / (revenue + 0.001)) + ts_rank(close, 120)"},
        {"template_id": "cvqm_07", "expression": "rank(equity / (cap + 0.001)) + rank(dividend / (close + 0.001)) + rank((revenue - cogs) / (revenue + 0.001)) + rank(ts_delta(close, 60) / (close + 0.001))"},
        {"template_id": "cvqm_08", "expression": "ts_decay_linear(rank(equity / (cap + 0.001)) + rank(ebit / (revenue + 0.001)) + rank(ts_delta(close, 60) / (close + 0.001)), 10)"},
    ],

    "composite_risk_adjusted": [
        {"template_id": "cra_01", "expression": "rank(ebitda / (cap * (historical_volatility_90 + 0.001)))"},
        {"template_id": "cra_02", "expression": "rank(fscore_total / (beta_last_60_days_spy + 0.001))"},
        {"template_id": "cra_03", "expression": "rank(equity / (cap + 0.001)) * rank(-historical_volatility_30)"},
        {"template_id": "cra_04", "expression": "rank((revenue - cogs) / (revenue + 0.001)) * rank(-unsystematic_risk_last_60_days)"},
        {"template_id": "cra_05", "expression": "rank(ebit / (cap + 0.001)) * rank(-correlation_last_60_days_spy)"},
        {"template_id": "cra_06", "expression": "rank(composite_factor_score_derivative / (beta_last_60_days_spy + 0.001))"},
        {"template_id": "cra_07", "expression": "rank(ts_backfill(cashflow, 60) / (cap * (historical_volatility_90 + 0.001) + 0.001))"},
        {"template_id": "cra_08", "expression": "rank(fscore_value) * rank(-unsystematic_risk_last_360_days)"},
    ],

    "composite_cross_category": [
        {"template_id": "cxc_01", "expression": "rank(call_breakeven_60 / (close + 0.001)) + rank(ebitda / (cap + 0.001)) + rank(scl12_sentiment)"},
        {"template_id": "cxc_02", "expression": "rank(ts_backfill(pv13_com_page_rank, 60)) * rank(est_sales / (revenue + 0.001)) * rank(-historical_volatility_90)"},
        {"template_id": "cxc_03", "expression": "rank(snt_value) + rank(est_ebitda / (ebitda + 0.001)) + rank(-beta_last_60_days_spy) + rank(ts_delta(close, 60) / (close + 0.001))"},
        {"template_id": "cxc_04", "expression": "rank(forward_price_120 / (close + 0.001)) + rank(income / (cap + 0.001)) + rank(scl12_buzz) + rank(ts_backfill(pv13_custretsig_retsig, 60))"},
        {"template_id": "cxc_05", "expression": "rank(ts_backfill(pv13_com_page_rank, 60)) + rank(-unsystematic_risk_last_60_days) + rank(current_ratio)"},
        {"template_id": "cxc_06", "expression": "rank(snt_buzz) + rank(ts_delta(est_eps, 20)) + rank(equity / (cap + 0.001))"},
        {"template_id": "cxc_07", "expression": "rank(forward_price_60 / (close + 0.001)) + rank(ts_backfill(income, 60) / (cap + 0.001)) + rank(snt_buzz_ret) + rank(-beta_last_360_days_spy)"},
        {"template_id": "cxc_08", "expression": "rank(call_breakeven_120 / (close + 0.001)) + rank(ebit / (revenue + 0.001)) + rank(scl12_sentiment) + rank(pv13_5l_scibr)"},
    ],

    "composite_adaptive": [
        {"template_id": "cad_01", "expression": "if_else(historical_volatility_90 > ts_mean(historical_volatility_90, 120), rank(equity / (cap + 0.001)), rank(ts_delta(close, 60) / (close + 0.001)))"},
        {"template_id": "cad_02", "expression": "trade_when(ts_delta(est_sales, 20) > 0, rank(fscore_growth), -1)"},
        {"template_id": "cad_03", "expression": "if_else(beta_last_60_days_spy > 1, rank((revenue - cogs) / (revenue + 0.001)), rank(ts_delta(close, 40) / (close + 0.001)))"},
        {"template_id": "cad_04", "expression": "trade_when(historical_volatility_90 > ts_mean(historical_volatility_90, 252), rank(equity / (cap + 0.001)) + rank(ebit / (revenue + 0.001)), -1)"},
        {"template_id": "cad_05", "expression": "trade_when(snt_buzz > ts_mean(snt_buzz, 60), rank(snt_value), -1)"},
        {"template_id": "cad_06", "expression": "if_else(ts_delta(est_eps, 20) > 0, rank(fscore_growth) + rank(ts_delta(close, 60) / (close + 0.001)), rank(fscore_value) + rank(equity / (cap + 0.001)))"},
        {"template_id": "cad_07", "expression": "if_else(historical_volatility_30 > ts_mean(historical_volatility_30, 60), rank(ts_delta(close, 120) / (close + 0.001)) + rank(fscore_value), rank(ts_delta(close, 20) / (close + 0.001)) + rank(fscore_momentum))"},
        {"template_id": "cad_08", "expression": "trade_when(correlation_last_60_days_spy < 0.7, rank(equity / (cap + 0.001)) + rank((revenue - cogs) / (revenue + 0.001)) + rank(ts_delta(close, 60) / (close + 0.001)), -1)"},
        {"template_id": "ca_09", "expression": "if_else(historical_volatility_30 > ts_mean(historical_volatility_30, 60), rank(ts_delta(close, 120) / (close + 0.001)) + rank(fscore_value), rank(ts_delta(close, 20) / (close + 0.001)) + rank(fscore_momentum))"},
        {"template_id": "ca_10", "expression": "trade_when(correlation_last_60_days_spy < 0.7, rank(equity / (cap + 0.001)) + rank((revenue - cogs) / (revenue + 0.001)) + rank(ts_delta(close, 60) / (close + 0.001)), -1)"},
    ],

    "m77_mega_composite": [
        {"template_id": "m7mc_01", "expression": "rank(forward_median_earnings_yield) + rank((revenue - cogs) / (assets + 0.001)) + rank(cashflow_op / (assets + 0.001)) + rank(-trailing_twelve_month_accruals) + rank(sustainable_growth_rate)"},
        {"template_id": "m7mc_02", "expression": "rank(-tobins_q_ratio) + rank((revenue - cogs) / (assets + 0.001)) + rank(-debt / (equity + 0.001)) + rank(ts_delta(close, 252) / (close + 0.001)) + rank(earnings_revision_magnitude)"},
        {"template_id": "m7mc_03", "expression": "rank(forward_median_earnings_yield) + rank(financial_statement_value_score) + rank(ts_delta(close, 252) / (close + 0.001)) + rank(earnings_momentum_analyst_score) + rank(-news_short_interest)"},
        {"template_id": "m7mc_04", "expression": "rank(normalized_earnings_yield) + rank(fcf_yield_multiplied_forward_roe) + rank(earnings_revision_magnitude) + rank(visibility_ratio)"},
        {"template_id": "m7mc_05", "expression": "rank(forward_median_earnings_yield) + rank(cashflow_op / (assets + 0.001)) + rank(sustainable_growth_rate) + rank(-book_leverage_ratio_3) + rank(-credit_risk_premium_indicator)"},
        {"template_id": "m7mc_06", "expression": "ts_decay_linear(rank(forward_median_earnings_yield) + rank((revenue - cogs) / (assets + 0.001)) + rank(earnings_momentum_analyst_score) + rank(cashflow_op / (assets + 0.001)) + rank(fcf_yield_multiplied_forward_roe), 5)"},
        {"template_id": "m7mc_07", "expression": "group_zscore(forward_median_earnings_yield, sector) + group_zscore((revenue - cogs) / (assets + 0.001), sector) + group_zscore(normalized_earnings_yield, sector) + group_zscore(cashflow_op / (assets + 0.001), sector)"},
        {"template_id": "m7mc_08", "expression": "rank(forward_median_earnings_yield) + rank((revenue - cogs) / (assets + 0.001)) + rank(treynor_ratio) + rank(-asset_growth_rate) + rank(standardized_unexpected_earnings) + rank(cashflow_op / (assets + 0.001))"},
    ],

    # ============================================================
    # GAP AREAS (Part 5)
    # ============================================================

    "gap_piotroski": [
        {"template_id": "gp_01", "expression": "rank(sign(income) + sign(ts_backfill(cashflow_op, 60)) + sign(ts_delta(ebit / (equity + 0.001), 252)) + sign(ts_backfill(cashflow_op, 60) - income))"},
        {"template_id": "gp_02", "expression": "rank(sign(ts_delta(-debt_lt / (equity + 0.001), 252)) + sign(ts_delta(current_ratio, 252)) + sign(-ts_delta(sharesout, 252)))"},
        {"template_id": "gp_03", "expression": "rank(sign(ts_delta((revenue - cogs) / (revenue + 0.001), 252)) + sign(ts_delta(revenue / (equity + 0.001), 252)))"},
        {"template_id": "gp_04", "expression": "rank(sign(income) + sign(ts_backfill(cashflow_op, 60)) + sign(ts_delta(ebit / (equity + 0.001), 252)) + sign(ts_backfill(cashflow_op, 60) - income) + sign(ts_delta(-debt_lt / (equity + 0.001), 252)) + sign(ts_delta(current_ratio, 252)) + sign(-ts_delta(sharesout, 252)) + sign(ts_delta((revenue - cogs) / (revenue + 0.001), 252)) + sign(ts_delta(revenue / (equity + 0.001), 252)))"},
        {"template_id": "gp_05", "expression": "group_zscore(sign(income) + sign(ts_backfill(cashflow_op, 60)) + sign(ts_delta(ebit / (equity + 0.001), 252)) + sign(ts_backfill(cashflow_op, 60) - income) + sign(ts_delta(-debt_lt / (equity + 0.001), 252)) + sign(ts_delta(current_ratio, 252)) + sign(-ts_delta(sharesout, 252)) + sign(ts_delta((revenue - cogs) / (revenue + 0.001), 252)) + sign(ts_delta(revenue / (equity + 0.001), 252)), industry)"},
        {"template_id": "gp_06", "expression": "ts_decay_linear(rank(sign(income) + sign(ts_backfill(cashflow_op, 60)) + sign(ts_delta(ebit / (equity + 0.001), 252)) + sign(ts_backfill(cashflow_op, 60) - income)), 20)"},
        {"template_id": "gp_07", "expression": "trade_when(sign(income) + sign(ts_backfill(cashflow_op, 60)) > 1, rank(sign(ts_delta((revenue - cogs) / (revenue + 0.001), 252)) + sign(ts_delta(current_ratio, 252)) + sign(-ts_delta(sharesout, 252))), -1)"},
        {"template_id": "gp_08", "expression": "signed_power(rank(sign(income) + sign(ts_backfill(cashflow_op, 60)) + sign(ts_delta(ebit / (equity + 0.001), 252)) + sign(ts_backfill(cashflow_op, 60) - income) + sign(ts_delta(-debt_lt / (equity + 0.001), 252)) + sign(ts_delta(current_ratio, 252)) + sign(-ts_delta(sharesout, 252)) + sign(ts_delta((revenue - cogs) / (revenue + 0.001), 252)) + sign(ts_delta(revenue / (equity + 0.001), 252))), 0.5)"},
        {"template_id": "gp_09", "expression": "rank(sign(ts_backfill(cashflow_op, 60) - ts_backfill(income, 60))) * rank(sign(ts_delta(current_ratio, 252))) * rank(sign(-ts_delta(ts_backfill(debt, 60), 252)))"},
        {"template_id": "gp_10", "expression": "signed_power(rank(sign(income) + sign(ts_backfill(cashflow_op, 60)) + sign(ts_delta(ebit / (equity + 0.001), 252)) + sign(ts_backfill(cashflow_op, 60) - income) + sign(ts_delta(-debt_lt / (equity + 0.001), 252)) + sign(ts_delta(current_ratio, 252)) + sign(-ts_delta(sharesout, 252)) + sign(ts_delta((revenue - cogs) / (revenue + 0.001), 252)) + sign(ts_delta(revenue / (equity + 0.001), 252))), 0.5)"},
    ],

    "gap_cash_conversion": [
        {"template_id": "gcc_01", "expression": "rank(inventory_turnover)"},
        {"template_id": "gcc_02", "expression": "group_zscore(inventory_turnover, industry)"},
        {"template_id": "gcc_03", "expression": "rank(-working_capital / (revenue + 0.001))"},
        {"template_id": "gcc_04", "expression": "group_zscore(-ts_backfill(working_capital, 60) / (ts_backfill(sales, 60) + 0.001), industry)"},
        {"template_id": "gcc_05", "expression": "rank(ts_delta(inventory_turnover, 252))"},
        {"template_id": "gcc_06", "expression": "rank(ts_delta(-working_capital / (revenue + 0.001), 252))"},
        {"template_id": "gcc_07", "expression": "group_zscore(ts_delta(inventory_turnover, 60), subindustry)"},
        {"template_id": "gcc_08", "expression": "rank(cash / (working_capital + 0.001)) * rank(inventory_turnover)"},
        {"template_id": "gcc_09", "expression": "ts_decay_linear(group_zscore(inventory_turnover, industry), 20)"},
        {"template_id": "gcc_10", "expression": "rank(ts_delta(ts_backfill(working_capital, 60) / (ts_backfill(sales, 60) + 0.001), 60)) * rank(ts_delta(inventory_turnover, 60))"},
    ],

    "gap_corr_regime_shift": [
        {"template_id": "gcrs_01", "expression": "-rank(ts_delta(ts_corr(volume, close, 20), 10))"},
        {"template_id": "gcrs_02", "expression": "-rank(ts_delta(ts_corr(ebitda / (cap + 0.001), returns, 60), 20))"},
        {"template_id": "gcrs_03", "expression": "-rank(ts_delta(ts_corr((revenue - cogs) / (revenue + 0.001), close, 120), 20))"},
        {"template_id": "gcrs_04", "expression": "-ts_corr(beta_last_60_days_spy, returns, 60)"},
        {"template_id": "gcrs_05", "expression": "-rank(ts_delta(ts_corr(implied_volatility_mean_90, close, 60), 10))"},
        {"template_id": "gcrs_06", "expression": "rank(-ts_corr(volume, close, 10)) - rank(-ts_corr(volume, close, 60))"},
        {"template_id": "gcrs_07", "expression": "-rank(ts_delta(ts_corr(historical_volatility_90, returns, 60), 20))"},
        {"template_id": "gcrs_08", "expression": "rank(-ts_corr(ebitda / (cap + 0.001), close, 120)) * rank(ebitda / (cap + 0.001))"},
        {"template_id": "gcrs_09", "expression": "group_zscore(-ts_delta(ts_corr(volume, close, 20), 10), subindustry)"},
        {"template_id": "gcrs_10", "expression": "ts_decay_linear(-ts_delta(ts_corr((revenue - cogs) / (revenue + 0.001), returns, 120), 20), 10)"},
    ],

    "gap_iv_momentum": [
        {"template_id": "givm_01", "expression": "rank(-ts_delta(implied_volatility_mean_30, 10))"},
        {"template_id": "givm_02", "expression": "rank(-ts_delta(implied_volatility_mean_90, 20))"},
        {"template_id": "givm_03", "expression": "rank(-(ts_delta(implied_volatility_mean_30, 5) - ts_delta(implied_volatility_mean_180, 5)))"},
        {"template_id": "givm_04", "expression": "rank(-ts_zscore(ts_delta(implied_volatility_mean_60, 10), 60))"},
        {"template_id": "givm_05", "expression": "rank(-ts_delta(ts_delta(implied_volatility_mean_30, 10), 10))"},
        {"template_id": "givm_06", "expression": "group_zscore(-ts_delta(implied_volatility_mean_90, 10), industry)"},
        {"template_id": "givm_07", "expression": "rank(-ts_delta(implied_volatility_mean_30, 5)) * rank(-ts_delta(implied_volatility_mean_120, 10))"},
        {"template_id": "givm_08", "expression": "trade_when(ts_delta(implied_volatility_mean_30, 5) < 0, rank(-implied_volatility_mean_90), -1)"},
        {"template_id": "givm_09", "expression": "ts_decay_linear(rank(-ts_delta(implied_volatility_mean_60, 10)), 15)"},
        {"template_id": "givm_10", "expression": "rank(-ts_delta(implied_volatility_mean_30, 5)) - rank(-ts_delta(implied_volatility_mean_360, 20))"},
    ],

    "gap_beta_mean_reversion": [
        {"template_id": "gbmr_01", "expression": "rank(-(beta_last_30_days_spy - beta_last_360_days_spy))"},
        {"template_id": "gbmr_02", "expression": "rank(-ts_zscore(beta_last_60_days_spy, 252))"},
        {"template_id": "gbmr_03", "expression": "rank(ts_delta(-beta_last_30_days_spy, 20))"},
        {"template_id": "gbmr_04", "expression": "group_zscore(-(beta_last_30_days_spy - beta_last_360_days_spy), industry)"},
        {"template_id": "gbmr_05", "expression": "rank(-(beta_last_60_days_spy - beta_last_360_days_spy)) * rank(-historical_volatility_90)"},
        {"template_id": "gbmr_06", "expression": "trade_when(beta_last_30_days_spy > beta_last_360_days_spy, rank(-beta_last_30_days_spy), -1)"},
        {"template_id": "gbmr_07", "expression": "ts_decay_linear(rank(-(beta_last_30_days_spy - beta_last_60_days_spy)), 10)"},
        {"template_id": "gbmr_08", "expression": "rank(-ts_delta(beta_last_60_days_spy, 30)) * rank(beta_last_360_days_spy - beta_last_30_days_spy)"},
        {"template_id": "gbmr_09", "expression": "rank(-ts_delta(beta_last_60_days_spy, 30)) * rank(beta_last_360_days_spy - beta_last_30_days_spy)"},
        {"template_id": "gbmr_10", "expression": "group_zscore(-ts_delta(beta_last_30_days_spy, 10) + ts_delta(beta_last_360_days_spy, 10), industry)"},
    ],

    # ============================================================
    # CATEGORY 20: FACTOR INTERACTIONS
    # ============================================================

    "interact_value_x_quality": [
        {"template_id": "vxq_01", "expression": "rank(equity / (cap + 0.001)) * rank((revenue - cogs) / (revenue + 0.001))"},
        {"template_id": "vxq_02", "expression": "group_zscore(ebitda / (cap + 0.001), sector) * group_zscore(current_ratio, sector)"},
        {"template_id": "vxq_03", "expression": "signed_power(rank(equity / (cap + 0.001)) * rank(fscore_quality), 0.5)"},
        {"template_id": "vxq_04", "expression": "ts_decay_linear(rank(ts_backfill(ebitda, 60) / (cap + 0.001)) * rank(ts_backfill(revenue, 60) / (ts_backfill(sales, 60) + 0.001)), 20)"},
        {"template_id": "vxq_05", "expression": "signed_power(group_zscore(ebit / (cap + 0.001), industry), 0.5) * signed_power(group_zscore((revenue - cogs) / (revenue + 0.001), industry), 0.5)"},
        {"template_id": "vxq_06", "expression": "ts_rank(rank(equity / (cap + 0.001)) * rank(fscore_profitability), 60)"},
        {"template_id": "vxq_07", "expression": "winsorize(group_zscore(ebitda / (cap + 0.001), subindustry) * group_zscore(retained_earnings / (cap + 0.001), subindustry) * group_zscore(current_ratio, subindustry))"},
        {"template_id": "vxq_08", "expression": "if_else(rank(fscore_quality) > 0.5, group_zscore(equity / (cap + 0.001), subindustry), -group_zscore(equity / (cap + 0.001), subindustry))"},
        {"template_id": "vxq_09", "expression": "rank(ebitda / (cap + 0.001)) * rank((revenue - cogs) / (revenue + 0.001)) * rank(current_ratio)"},
        {"template_id": "vxq_10", "expression": "rank(equity / (cap + 0.001)) * rank(fscore_quality) * rank(fscore_profitability)"},
    ],

    "interact_momentum_x_quality": [
        {"template_id": "mxq_01", "expression": "rank(ts_delta(close, 60) / (close + 0.001)) * rank(fscore_quality)"},
        {"template_id": "mxq_02", "expression": "rank(ts_delta(close, 20) / (close + 0.001)) * rank((revenue - cogs) / (revenue + 0.001))"},
        {"template_id": "mxq_03", "expression": "group_zscore(ts_decay_linear(returns, 40), sector) * group_zscore(fscore_profitability, sector)"},
        {"template_id": "mxq_04", "expression": "signed_power(rank(ts_delta(close, 120) / (close + 0.001)) * rank(fscore_quality), 0.5)"},
        {"template_id": "mxq_05", "expression": "group_zscore(ts_decay_linear(returns, 60), industry) * group_zscore((revenue - cogs) / (revenue + 0.001), industry)"},
        {"template_id": "mxq_06", "expression": "ts_zscore(close, 120) * rank(fscore_quality) * rank(current_ratio)"},
        {"template_id": "mxq_07", "expression": "if_else(rank(fscore_quality) > 0.6, ts_decay_linear(returns, 60), -abs(ts_decay_linear(returns, 60)))"},
        {"template_id": "mxq_08", "expression": "ts_rank(ts_delta(close, 60) / (close + 0.001), 252) * group_zscore(ts_backfill(revenue, 60) / (ts_backfill(sales, 60) + 0.001), sector)"},
        {"template_id": "mxq_09", "expression": "winsorize(rank(ts_decay_linear(returns, 90)) * rank(fscore_profitability))"},
        {"template_id": "mxq_10", "expression": "rank(ts_delta(close, 40) / (close + 0.001)) * rank(fscore_quality) * rank(-historical_volatility_90)"},
    ],

    "interact_value_x_momentum": [
        {"template_id": "vxm_01", "expression": "rank(equity / (cap + 0.001)) * rank(ts_delta(close, 120) / (close + 0.001))"},
        {"template_id": "vxm_02", "expression": "rank(ebitda / (cap + 0.001)) * rank(ts_decay_linear(returns, 60))"},
        {"template_id": "vxm_03", "expression": "signed_power(rank(equity / (cap + 0.001)) * rank(ts_delta(close, 60) / (close + 0.001)), 0.5)"},
        {"template_id": "vxm_04", "expression": "group_zscore(ebitda / (cap + 0.001), sector) * group_zscore(ts_decay_linear(returns, 120), sector)"},
        {"template_id": "vxm_05", "expression": "if_else(ts_delta(close, 120) / (close + 0.001) > 0, rank(equity / (cap + 0.001)), -rank(equity / (cap + 0.001)) * 0.5)"},
        {"template_id": "vxm_06", "expression": "ts_decay_linear(group_zscore(ebit / (cap + 0.001), industry) * ts_zscore(close, 60), 20)"},
        {"template_id": "vxm_07", "expression": "rank(equity / (cap + 0.001)) * rank(ts_delta(close, 60) / (close + 0.001)) * rank(-historical_volatility_30)"},
        {"template_id": "vxm_08", "expression": "winsorize(group_zscore(equity / (cap + 0.001), industry) * ts_rank(returns, 120))"},
        {"template_id": "vxm_09", "expression": "rank(ebit / (cap + 0.001)) * rank(ts_decay_linear(returns, 40)) + rank(ebitda / (cap + 0.001)) * rank(ts_delta(close, 120) / (close + 0.001))"},
        {"template_id": "vxm_10", "expression": "ts_rank(rank(equity / (cap + 0.001)) * rank(ts_delta(close, 60) / (close + 0.001)), 120)"},
    ],

    "interact_size_x_value_x_quality": [
        {"template_id": "sxvxq_01", "expression": "(1 - rank(cap)) * rank(equity / (cap + 0.001)) * rank(fscore_quality)"},
        {"template_id": "sxvxq_02", "expression": "group_zscore(rank(ebitda / (cap + 0.001)) * rank((revenue - cogs) / (revenue + 0.001)), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "sxvxq_03", "expression": "signed_power((1 - rank(cap)) * rank(ebitda / (cap + 0.001)) * rank(current_ratio), 0.33)"},
        {"template_id": "sxvxq_04", "expression": "group_zscore(signed_power(rank(equity / (cap + 0.001)) * rank(fscore_profitability), 0.5), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "sxvxq_05", "expression": "if_else(rank(cap) < 0.4, rank(equity / (cap + 0.001)) * rank((revenue - cogs) / (revenue + 0.001)) * 2, rank(equity / (cap + 0.001)) * rank((revenue - cogs) / (revenue + 0.001)))"},
        {"template_id": "sxvxq_06", "expression": "(1 - rank(cap)) * ts_rank(rank(ebitda / (cap + 0.001)) * rank(fscore_quality), 60)"},
        {"template_id": "sxvxq_07", "expression": "group_zscore(rank(ts_backfill(ebitda, 60) / (cap + 0.001)) * rank(ts_backfill(revenue, 60) / (ts_backfill(sales, 60) + 0.001)), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "sxvxq_08", "expression": "group_neutralize(rank(equity / (cap + 0.001)) * rank(current_ratio), densify(bucket(rank(cap), range=\"0, 1, 0.2\")))"},
        {"template_id": "isxvxq_09", "expression": "(1 - rank(cap)) * ts_rank(rank(ebitda / (cap + 0.001)) * rank(fscore_quality), 60)"},
        {"template_id": "isxvxq_10", "expression": "if_else(rank(cap) < 0.4, rank(equity / (cap + 0.001)) * rank((revenue - cogs) / (revenue + 0.001)) * 2, rank(equity / (cap + 0.001)) * rank((revenue - cogs) / (revenue + 0.001)))"},
    ],

    "interact_sentiment_x_fundamental": [
        {"template_id": "sxf_01", "expression": "rank(scl12_sentiment) * rank(ebitda / (cap + 0.001))"},
        {"template_id": "sxf_02", "expression": "rank(snt_value) * rank(fscore_total)"},
        {"template_id": "sxf_03", "expression": "ts_corr(scl12_sentiment, returns, 60) * rank(fscore_total)"},
        {"template_id": "sxf_04", "expression": "if_else(rank(snt_value) > 0.6, group_zscore(ebitda / (cap + 0.001), sector), -group_zscore(ebitda / (cap + 0.001), sector) * 0.5)"},
        {"template_id": "sxf_05", "expression": "signed_power(rank(ts_decay_linear(ts_backfill(rp_css_earnings, 60), 5)) * rank(fscore_quality), 0.5)"},
        {"template_id": "sxf_06", "expression": "ts_decay_linear(rank(scl12_sentiment) * rank(ebitda / (cap + 0.001)), 20)"},
        {"template_id": "sxf_07", "expression": "group_zscore(snt_value, sector) * group_zscore(fscore_total, sector)"},
        {"template_id": "sxf_08", "expression": "ts_delta(scl12_sentiment, 20) * rank(fscore_quality) * rank(ebitda / (cap + 0.001))"},
        {"template_id": "sxf_09", "expression": "rank(snt_buzz_ret) * rank(fscore_total) * rank(ebitda / (cap + 0.001))"},
        {"template_id": "sxf_10", "expression": "rank(snt_value) * rank((revenue - cogs) / (revenue + 0.001)) * rank(-historical_volatility_90)"},
    ],

    # ============================================================
    # CATEGORY 21: DERIVATIVE SCORES
    # ============================================================

    "deriv_fscore_composites": [
        {"template_id": "dfc_01", "expression": "rank(fscore_value + fscore_growth + fscore_momentum + fscore_profitability + fscore_quality + fscore_total)"},
        {"template_id": "dfc_02", "expression": "(rank(fscore_value) + rank(fscore_growth) + rank(fscore_momentum) + rank(fscore_profitability) + rank(fscore_quality) + rank(fscore_total)) / 6"},
        {"template_id": "dfc_03", "expression": "group_zscore(2 * fscore_value + 2 * fscore_quality + fscore_growth + fscore_momentum + fscore_profitability, sector)"},
        {"template_id": "dfc_04", "expression": "rank(fscore_growth + fscore_momentum) + rank(fscore_surface + fscore_surface_accel)"},
        {"template_id": "dfc_05", "expression": "winsorize(group_zscore(fscore_profitability + fscore_quality + fscore_surface, subindustry))"},
        {"template_id": "dfc_06", "expression": "signed_power(rank(fscore_value) * rank(fscore_quality), 0.5) + signed_power(rank(fscore_growth) * rank(fscore_momentum), 0.5)"},
        {"template_id": "dfc_07", "expression": "ts_decay_linear(rank(fscore_total + fscore_profitability + fscore_quality), 10)"},
        {"template_id": "dfc_08", "expression": "group_zscore(signed_power(fscore_value + 0.001, 0.5) + signed_power(fscore_profitability + 0.001, 0.5) + signed_power(fscore_quality + 0.001, 0.5), sector)"},
        {"template_id": "dfc_09", "expression": "rank(3 * fscore_total + 2 * fscore_value + fscore_momentum)"},
        {"template_id": "dfc_10", "expression": "rank(fscore_value) * rank(fscore_quality) * rank(fscore_momentum)"},
    ],

    "deriv_fscore_bfl": [
        {"template_id": "dbfl_01", "expression": "rank(fscore_bfl_total + fscore_bfl_value + fscore_bfl_growth + fscore_bfl_momentum + fscore_bfl_profitability + fscore_bfl_quality)"},
        {"template_id": "dbfl_02", "expression": "ts_decay_linear(fscore_bfl_total + fscore_bfl_quality + fscore_bfl_profitability, 20)"},
        {"template_id": "dbfl_03", "expression": "rank(ts_delta(fscore_bfl_total, 20)) + rank(ts_delta(fscore_bfl_value, 20))"},
        {"template_id": "dbfl_04", "expression": "(rank(fscore_bfl_value) + rank(fscore_bfl_growth) + rank(fscore_bfl_momentum) + rank(fscore_bfl_profitability) + rank(fscore_bfl_quality)) / 5"},
        {"template_id": "dbfl_05", "expression": "group_zscore(fscore_bfl_total + 2 * fscore_bfl_surface + fscore_bfl_surface_accel, subindustry)"},
        {"template_id": "dbfl_06", "expression": "signed_power(rank(fscore_bfl_surface) + rank(fscore_bfl_surface_accel), 0.5) * sign(fscore_bfl_surface_accel)"},
        {"template_id": "dbfl_07", "expression": "winsorize(rank(fscore_bfl_total) + rank(fscore_bfl_value) + rank(fscore_bfl_quality))"},
        {"template_id": "dbfl_08", "expression": "ts_mean(fscore_bfl_profitability + fscore_bfl_quality, 10) - ts_mean(fscore_bfl_profitability + fscore_bfl_quality, 40)"},
        {"template_id": "dbfl_09", "expression": "rank(fscore_bfl_growth + fscore_bfl_momentum) * rank(fscore_bfl_surface_accel)"},
        {"template_id": "dbfl_10", "expression": "signed_power(group_zscore(fscore_bfl_total, sector), 0.5)"},
    ],

    "deriv_fscore_momentum": [
        {"template_id": "dfm_01", "expression": "rank(ts_delta(fscore_total, 20))"},
        {"template_id": "dfm_02", "expression": "ts_zscore(fscore_momentum, 60)"},
        {"template_id": "dfm_03", "expression": "rank(ts_delta(fscore_value, 5) + ts_delta(fscore_quality, 5))"},
        {"template_id": "dfm_04", "expression": "ts_decay_linear(ts_delta(fscore_profitability, 10), 20)"},
        {"template_id": "dfm_05", "expression": "ts_delta(fscore_total, 40) + ts_delta(fscore_total, 10)"},
        {"template_id": "dfm_06", "expression": "ts_av_diff(fscore_total, 20)"},
        {"template_id": "dfm_07", "expression": "rank(ts_delta(fscore_value, 20)) + rank(ts_delta(fscore_growth, 20)) + rank(ts_delta(fscore_profitability, 20))"},
        {"template_id": "dfm_08", "expression": "rank(ts_delta(fscore_total, 5) - ts_delta(fscore_total, 20))"},
        {"template_id": "dfm_09", "expression": "ts_zscore(fscore_quality, 40) * rank(ts_delta(fscore_surface_accel, 10))"},
        {"template_id": "dfm_10", "expression": "fscore_total + 2 * rank(ts_delta(fscore_total, 20))"},
    ],

    "deriv_fscore_x_price": [
        {"template_id": "dfxp_01", "expression": "fscore_total * rank(ts_delta(close, 60) / (close + 0.001))"},
        {"template_id": "dfxp_02", "expression": "rank(fscore_quality) * rank(ts_decay_linear(returns, 20))"},
        {"template_id": "dfxp_03", "expression": "fscore_momentum * rank(volume / (adv20 + 0.001))"},
        {"template_id": "dfxp_04", "expression": "rank(fscore_value) * rank(-ts_delta(close, 5) / (close + 0.001))"},
        {"template_id": "dfxp_05", "expression": "fscore_profitability * ts_zscore(close, 120)"},
        {"template_id": "dfxp_06", "expression": "group_zscore(fscore_total, sector) * rank(ts_decay_linear(returns, 10))"},
        {"template_id": "dfxp_07", "expression": "if_else(fscore_total > ts_mean(fscore_total, 60), rank(ts_delta(close, 20) / (close + 0.001)), -rank(ts_delta(close, 20) / (close + 0.001)))"},
        {"template_id": "dfxp_08", "expression": "rank(fscore_growth) * rank(-ts_corr(volume, close, 20))"},
        {"template_id": "dfxp_09", "expression": "signed_power(rank(fscore_quality) * rank(fscore_profitability), 0.5) * sign(ts_delta(vwap, 10))"},
        {"template_id": "dfxp_10", "expression": "ts_decay_linear(fscore_total * rank(returns), 15)"},
    ],

    "deriv_rank_composites": [
        {"template_id": "drc_01", "expression": "rank(analyst_revision_rank_derivative + cashflow_efficiency_rank_derivative + composite_factor_score_derivative + earnings_certainty_rank_derivative + growth_potential_rank_derivative + multi_factor_acceleration_score_derivative + multi_factor_static_score_derivative + relative_valuation_rank_derivative)"},
        {"template_id": "drc_02", "expression": "signed_power(rank(analyst_revision_rank_derivative + cashflow_efficiency_rank_derivative + earnings_certainty_rank_derivative + relative_valuation_rank_derivative), 0.5)"},
        {"template_id": "drc_03", "expression": "rank(growth_potential_rank_derivative + multi_factor_acceleration_score_derivative + composite_factor_score_derivative)"},
        {"template_id": "drc_04", "expression": "rank(ts_delta(composite_factor_score_derivative, 10)) + rank(ts_delta(multi_factor_acceleration_score_derivative, 10))"},
        {"template_id": "drc_05", "expression": "group_zscore(analyst_revision_rank_derivative + earnings_certainty_rank_derivative + multi_factor_static_score_derivative, subindustry)"},
        {"template_id": "drc_06", "expression": "3 * rank(analyst_revision_rank_derivative) + 2 * rank(earnings_certainty_rank_derivative) + rank(relative_valuation_rank_derivative) + rank(cashflow_efficiency_rank_derivative)"},
        {"template_id": "drc_07", "expression": "winsorize(group_zscore(multi_factor_acceleration_score_derivative + multi_factor_static_score_derivative + composite_factor_score_derivative, sector))"},
        {"template_id": "drc_08", "expression": "ts_decay_linear(rank(analyst_revision_rank_derivative + growth_potential_rank_derivative + cashflow_efficiency_rank_derivative), 15)"},
        {"template_id": "drc_09", "expression": "signed_power(rank(multi_factor_static_score_derivative), 0.5) * sign(multi_factor_acceleration_score_derivative) + rank(relative_valuation_rank_derivative)"},
        {"template_id": "drc_10", "expression": "rank(cashflow_efficiency_rank_derivative + earnings_certainty_rank_derivative + relative_valuation_rank_derivative)"},
    ],

    # ============================================================
    # CATEGORY 22: COMPOSITE MULTI-FACTOR
    # ============================================================

    "composite_vqm": [
        {"template_id": "cvqm_01", "expression": "(rank(equity / (cap + 0.001)) + rank((revenue - cogs) / (revenue + 0.001)) + rank(ts_delta(close, 60) / (close + 0.001))) / 3"},
        {"template_id": "cvqm_02", "expression": "rank(ebitda / (cap + 0.001)) * rank(current_ratio) * rank(ts_delta(close, 20) / (close + 0.001))"},
        {"template_id": "cvqm_03", "expression": "signed_power(group_zscore(equity / (cap + 0.001), sector) + group_zscore(ebit / (revenue + 0.001), sector) + group_zscore(ts_delta(close, 120) / (close + 0.001), sector), 0.5)"},
        {"template_id": "cvqm_04", "expression": "rank(equity / (cap + 0.001)) + rank(ts_backfill(revenue, 60) / (ts_backfill(sales, 60) + 0.001)) + rank(ts_delta(close, 60) / (close + 0.001))"},
        {"template_id": "cvqm_05", "expression": "rank(ebitda / (cap + 0.001)) + rank(income / (revenue + 0.001)) + ts_rank(close, 120)"},
        {"template_id": "cvqm_06", "expression": "winsorize(group_zscore(eps / (close + 0.001), industry) + group_zscore(current_ratio, industry) + group_zscore(ts_delta(close, 40) / (close + 0.001), industry))"},
        {"template_id": "cvqm_07", "expression": "rank(retained_earnings / (cap + 0.001)) + rank((revenue - cogs) / (revenue + 0.001)) + rank(ts_delta(close, 20) / (close + 0.001)) + rank(ts_delta(close, 120) / (close + 0.001))"},
        {"template_id": "cvqm_08", "expression": "ts_decay_linear(rank(equity / (cap + 0.001)) + rank((revenue - cogs) / (revenue + 0.001)) + rank(ts_delta(close, 60) / (close + 0.001)), 5)"},
        {"template_id": "cvqm_09", "expression": "rank(dividend / (close + 0.001)) + rank(ebitda / (cap + 0.001)) + rank((revenue - cogs) / (revenue + 0.001)) + rank(ts_delta(close, 60) / (close + 0.001))"},
        {"template_id": "cvqm_10", "expression": "rank(equity / (cap + 0.001)) * rank((revenue - cogs) / (revenue + 0.001)) * signed_power(rank(ts_delta(close, 60) / (close + 0.001)), 0.5)"},
    ],

    "composite_risk_adjusted": [
        {"template_id": "cra_01", "expression": "rank(ebitda / (cap * (historical_volatility_90 + 0.001)))"},
        {"template_id": "cra_02", "expression": "rank(fscore_total / (beta_last_60_days_spy + 0.001))"},
        {"template_id": "cra_03", "expression": "rank(equity / (cap + 0.001)) * rank(-historical_volatility_30)"},
        {"template_id": "cra_04", "expression": "(rank((revenue - cogs) / (revenue + 0.001)) + rank(current_ratio)) * rank(-unsystematic_risk_last_60_days)"},
        {"template_id": "cra_05", "expression": "rank(ebit / (cap + 0.001)) * rank(-correlation_last_60_days_spy)"},
        {"template_id": "cra_06", "expression": "rank(composite_factor_score_derivative / (beta_last_60_days_spy + 0.001))"},
        {"template_id": "cra_07", "expression": "rank(ts_backfill(cashflow, 60) / (cap * (historical_volatility_90 + 0.001) + 0.001))"},
        {"template_id": "cra_08", "expression": "rank(equity / (cap + 0.001)) * rank(fscore_growth) * inverse(historical_volatility_120 + 0.001)"},
        {"template_id": "cra_09", "expression": "group_zscore(equity / (cap + 0.001), sector) * inverse(historical_volatility_150 + 0.001)"},
        {"template_id": "cra_10", "expression": "rank(fscore_value) * rank(-unsystematic_risk_last_360_days)"},
    ],

    "composite_cross_category": [
        {"template_id": "ccc_01", "expression": "(rank(call_breakeven_60 / (close + 0.001)) + rank(ebitda / (cap + 0.001)) + rank(scl12_sentiment)) / 3"},
        {"template_id": "ccc_02", "expression": "rank(ts_backfill(pv13_com_page_rank, 60)) * rank(est_sales / (revenue + 0.001)) * rank(-historical_volatility_90)"},
        {"template_id": "ccc_03", "expression": "(rank(put_breakeven_60 / (close + 0.001)) + rank(pv13_5l_scibr) + rank((revenue - cogs) / (revenue + 0.001)) + rank(fscore_quality)) / 4"},
        {"template_id": "ccc_04", "expression": "rank(snt_value) + rank(est_ebitda / (ebitda + 0.001)) + rank(-beta_last_60_days_spy) + rank(ts_delta(close, 60) / (close + 0.001))"},
        {"template_id": "ccc_05", "expression": "(rank(forward_price_120 / (close + 0.001)) + rank(income / (cap + 0.001)) + rank(scl12_buzz) + rank(ts_backfill(pv13_custretsig_retsig, 60))) / 4"},
        {"template_id": "ccc_06", "expression": "rank(implied_volatility_mean_90 - implied_volatility_mean_30) + rank(ts_backfill(ebitda, 60) / (cap + 0.001)) + rank(fscore_momentum)"},
        {"template_id": "ccc_07", "expression": "rank(ts_backfill(pv13_com_page_rank, 60)) + rank(-call_breakeven_360 / (close + 0.001)) + rank(-unsystematic_risk_last_60_days) + rank(current_ratio)"},
        {"template_id": "ccc_08", "expression": "rank(snt_buzz) + rank(ts_delta(est_eps, 20)) + rank(implied_volatility_mean_180 - implied_volatility_mean_30) + rank(equity / (cap + 0.001))"},
        {"template_id": "ccc_09", "expression": "signed_power(rank(call_breakeven_120 / (close + 0.001)) + rank(ebit / (revenue + 0.001)) + rank(scl12_sentiment) + rank(pv13_5l_scibr), 0.5)"},
        {"template_id": "ccc_10", "expression": "ts_decay_linear(rank(forward_price_60 / (close + 0.001)) + rank(ts_backfill(income, 60) / (cap + 0.001)) + rank(snt_buzz_ret) + rank(-beta_last_360_days_spy), 10)"},
    ],

    "composite_adaptive_regime": [
        {"template_id": "car_01", "expression": "if_else(historical_volatility_90 > ts_mean(historical_volatility_90, 120), rank(equity / (cap + 0.001)), rank(ts_delta(close, 60) / (close + 0.001)))"},
        {"template_id": "car_02", "expression": "trade_when(ts_delta(est_sales, 20) > 0, rank(fscore_growth), -1)"},
        {"template_id": "car_03", "expression": "if_else(beta_last_60_days_spy > 1, rank((revenue - cogs) / (revenue + 0.001)), rank(ts_delta(close, 40) / (close + 0.001)))"},
        {"template_id": "car_04", "expression": "trade_when(historical_volatility_90 > ts_mean(historical_volatility_90, 252), rank(equity / (cap + 0.001)) + rank(ebit / (revenue + 0.001)), -1)"},
        {"template_id": "car_05", "expression": "trade_when(snt_buzz > ts_mean(snt_buzz, 60), rank(snt_value), -1)"},
        {"template_id": "car_06", "expression": "if_else(ts_delta(est_eps, 20) > 0, rank(fscore_growth) + rank(ts_delta(close, 60) / (close + 0.001)), rank(fscore_value) + rank(equity / (cap + 0.001)))"},
        {"template_id": "car_07", "expression": "if_else(historical_volatility_90 < ts_mean(historical_volatility_90, 120), rank(ebitda / (cap + 0.001)) * rank(ts_delta(close, 20) / (close + 0.001)), rank(dividend / (close + 0.001)) + rank(current_ratio))"},
        {"template_id": "car_08", "expression": "trade_when(correlation_last_60_days_spy < 0.7, rank(equity / (cap + 0.001)) + rank((revenue - cogs) / (revenue + 0.001)) + rank(ts_delta(close, 60) / (close + 0.001)), -1)"},
        {"template_id": "car_09", "expression": "if_else(historical_volatility_30 > ts_mean(historical_volatility_30, 60), rank(ts_delta(close, 120) / (close + 0.001)) + rank(fscore_value), rank(ts_delta(close, 20) / (close + 0.001)) + rank(fscore_momentum))"},
        {"template_id": "car_10", "expression": "trade_when(ts_delta(est_ebitda, 20) > 0, rank(ebitda / (cap + 0.001)) * rank(ts_decay_linear(returns, 40)), -1)"},
    ],

    "m77_mega_composite": [
        {"template_id": "m7mc_09", "expression": "(rank(forward_median_earnings_yield) + rank(normalized_earnings_yield) + rank((revenue - cogs) / (assets + 0.001)) + rank(financial_statement_value_score) + rank(fcf_yield_multiplied_forward_roe)) / 5"},
        {"template_id": "m7mc_10", "expression": "signed_power(rank((revenue - cogs) / (assets + 0.001)) + rank(cashflow_op / (assets + 0.001)) + rank(-trailing_twelve_month_accruals) + rank(sustainable_growth_rate), 0.5)"},
        {"template_id": "m7mc_11", "expression": "rank(forward_median_earnings_yield) * rank(normalized_earnings_yield) + rank(cashflow_op / (assets + 0.001)) * rank(-trailing_twelve_month_accruals) + rank(sustainable_growth_rate) * rank(earnings_revision_magnitude)"},
        {"template_id": "m7mc_12", "expression": "ts_decay_linear(rank(forward_median_earnings_yield) + rank((revenue - cogs) / (assets + 0.001)) + rank(earnings_momentum_analyst_score) + rank(cashflow_op / (assets + 0.001)) + rank(fcf_yield_multiplied_forward_roe), 5)"},
        {"template_id": "m7mc_13", "expression": "rank(-tobins_q_ratio) + rank((revenue - cogs) / (assets + 0.001)) + rank(-debt / (equity + 0.001)) + rank(ts_delta(close, 252) / (close + 0.001)) + rank(earnings_revision_magnitude)"},
        {"template_id": "m7mc_14", "expression": "group_zscore(forward_median_earnings_yield, sector) + group_zscore((revenue - cogs) / (assets + 0.001), sector) + group_zscore(normalized_earnings_yield, sector) + group_zscore(cashflow_op / (assets + 0.001), sector)"},
        {"template_id": "m7mc_15", "expression": "rank(forward_median_earnings_yield) + rank(financial_statement_value_score) + rank(ts_delta(close, 252) / (close + 0.001)) + rank(earnings_momentum_analyst_score) + rank(-news_short_interest)"},
        {"template_id": "m7mc_16", "expression": "rank(normalized_earnings_yield) + rank(fcf_yield_multiplied_forward_roe) + rank(earnings_revision_magnitude) + rank(visibility_ratio)"},
        {"template_id": "m7mc_17", "expression": "rank(forward_median_earnings_yield) + rank(cashflow_op / (assets + 0.001)) + rank(sustainable_growth_rate) + rank(-book_leverage_ratio_3) + rank(-credit_risk_premium_indicator)"},
        {"template_id": "m7mc_18", "expression": "rank(forward_median_earnings_yield) + rank((revenue - cogs) / (assets + 0.001)) + rank(treynor_ratio) + rank(-asset_growth_rate) + rank(standardized_unexpected_earnings) + rank(cashflow_op / (assets + 0.001))"},
    ],
    "season_event_recency": [
        {"template_id": "ser_01", "expression": "trade_when(not(is_nan(vec_avg(nws18_bee))), rank(-returns), days_from_last_change(vec_avg(nws18_bee)) > 20)"},
        {"template_id": "ser_02", "expression": "trade_when(not(is_nan(vec_avg(nws18_ber))), rank(ebitda / (cap + 0.001)), days_from_last_change(vec_avg(nws18_ber)) > 20)"},
        {"template_id": "ser_03", "expression": "trade_when(not(is_nan(vec_avg(nws18_qep))), rank(-ts_delta(close, 3)), days_from_last_change(vec_avg(nws18_qep)) > 15)"},
        {"template_id": "ser_04", "expression": "trade_when(not(is_nan(vec_avg(nws18_ssc))), rank(equity / (cap + 0.001)), days_from_last_change(vec_avg(nws18_ssc)) > 20)"},
        {"template_id": "ser_05", "expression": "trade_when(not(is_nan(vec_avg(nws18_bam))), (rp_ess_mna + rp_css_mna) / 2, days_from_last_change(vec_avg(nws18_bam)) > 20)"},
        {"template_id": "ser_06", "expression": "zscore(trade_when(not(is_nan(vec_avg(nws18_bee))), rank(-returns), days_from_last_change(vec_avg(nws18_bee)) > 20))"},
        {"template_id": "ser_07", "expression": "zscore(trade_when(not(is_nan(vec_avg(nws18_ber))), rank(ebitda / (cap + 0.001)), days_from_last_change(vec_avg(nws18_ber)) > 15))"},
        {"template_id": "ser_08", "expression": "zscore(trade_when(not(is_nan(vec_avg(nws18_qep))), rank(-ts_zscore(returns, 10)), days_from_last_change(vec_avg(nws18_qep)) > 20))"},
        {"template_id": "ser_09", "expression": "ts_decay_linear(rank(vec_avg(nws18_bee)), 10)"},
        {"template_id": "ser_10", "expression": "rank(ts_mean(vec_avg(nws18_ber), 5)) * rank(-returns)"},
    ],
}

EWAN_ONLY_FAMILIES = {
    "m77_value", "m77_profitability", "m77_quality", "m77_growth", "m77_momentum",
    "m77_vol_risk", "m77_credit", "m77_mega_composite",
    "earnings_sue_pead", "earnings_torpedo",
    "analyst_target_price", "analyst_recommendations",
    "risk_treynor",  # uses model77 earnings signals / beta

}

RESEARCH_NEUTRALIZATIONS = {f: ["SUBINDUSTRY", "INDUSTRY"] for f in RESEARCH_TEMPLATES}
# Override specific families
for f in ["mr_short_term", "mr_vol_gated", "mr_volume_conditioned", "liq_turnover_reversal",
          "size_conditioned_value", "size_conditioned_momentum", "size_conditioned_quality",
          "interact_value_x_quality", "interact_momentum_x_quality", "interact_value_x_momentum",
          "interact_sentiment_x_fundamental"]:
    RESEARCH_NEUTRALIZATIONS[f] = ["SUBINDUSTRY"]
for f in ["lev_distress", "gap_piotroski", "composite_vqm", "composite_cross_category"]:
    RESEARCH_NEUTRALIZATIONS[f] = ["INDUSTRY", "SECTOR"]
for f in ["risk_bab", "risk_correlation_regime", "composite_risk_adjusted",
          "gap_beta_mean_reversion", "gap_iv_momentum"]:
    RESEARCH_NEUTRALIZATIONS[f] = ["MARKET", "SECTOR"]
for f in ["interact_size_x_value_x_quality"]:
    RESEARCH_NEUTRALIZATIONS[f] = ["MARKET", "SECTOR"]
for f in ["composite_adaptive_regime"]:
    RESEARCH_NEUTRALIZATIONS[f] = ["SECTOR", "INDUSTRY"]

# Weights: new families get 5.0, model77 get 4.0
RESEARCH_WEIGHTS = {}
for fam in RESEARCH_TEMPLATES:
    if fam in EWAN_ONLY_FAMILIES:
        RESEARCH_WEIGHTS[fam] = 4.0
    elif fam.startswith("gap_") or fam.startswith("opt_") or fam.startswith("sc_"):
        RESEARCH_WEIGHTS[fam] = 6.0  # Highest priority — untouched data categories
    elif fam.startswith("deriv_") or fam.startswith("composite_") or fam.startswith("interact_"):
        RESEARCH_WEIGHTS[fam] = 5.0
    else:
        RESEARCH_WEIGHTS[fam] = 4.5
