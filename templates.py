from __future__ import annotations

TEMPLATE_LIBRARY: dict[str, list[dict[str, str]]] = {
    "mean_reversion": [
        {"template_id": "mr_01", "expression": "rank(ts_mean(close, {n}) - close)"},
        {"template_id": "mr_02", "expression": "rank(-ts_delta(close, {n}))"},
        {"template_id": "mr_03", "expression": "rank(-(close / ts_mean(close, {n}) - 1))"},
        {"template_id": "mr_04", "expression": "rank(-(returns - ts_mean(returns, {n})))"},
    ],
    "cross_sectional": [
        {"template_id": "cs_01", "expression": "-rank(ts_zscore(close, {n}))"},
        {"template_id": "cs_02", "expression": "-rank(ts_zscore(returns, {n}))"},
        {"template_id": "cs_03", "expression": "rank(ts_mean(returns, {m}) - ts_mean(returns, {n}))"},
        {"template_id": "cs_04", "expression": "group_rank(-ts_zscore(close, {n}), subindustry)"},
    ],
    "liquidity_scaled": [
        {"template_id": "liq_01", "expression": "rank(-(returns - ts_mean(returns, {n})) * rank(adv20))"},
        {"template_id": "liq_02", "expression": "rank(-ts_zscore(returns, {n}) * rank(cap))"},
        {"template_id": "liq_03", "expression": "rank((ts_mean(close, {n}) - close) * rank(adv20) / (ts_std_dev(returns, {m}) + 0.001))"},
    ],
    "conditional": [
        {"template_id": "cond_01", "expression": "trade_when(volume > ts_mean(volume, {n}), rank(-returns), -1)"},
        {"template_id": "cond_02", "expression": "trade_when(abs(returns) > ts_std_dev(returns, {n}), rank(-returns), -1)"},
        {"template_id": "cond_03", "expression": "trade_when(volume > ts_mean(volume, {n}), rank(ts_delta(close, {m})), -1)"},
    ],
    "vol_adjusted": [
        {"template_id": "va_01", "expression": "rank(ts_delta(close, {n}) / ts_std_dev(returns, {m}))"},
        {"template_id": "va_02", "expression": "rank((ts_mean(close, {n}) - close) / ts_std_dev(returns, {m}))"},
    ],
    "fundamental_value": [
        {"template_id": "fv_01", "expression": "ts_zscore({deep_field} / cap, {n})"},
        {"template_id": "fv_02", "expression": "rank(ts_delta({deep_field}, {n}) / ({deep_field} + 0.001))"},
        {"template_id": "fv_03", "expression": "-rank(close / ({deep_field} + 0.001))"},
        {"template_id": "fv_04", "expression": "group_rank(-ts_zscore({deep_field}, {n}), industry)"},
        {"template_id": "fv_05", "expression": "ts_rank({deep_field} / cap, 252)"},
        {"template_id": "fv_06", "expression": "group_rank(ts_rank({deep_field} / cap, 252), subindustry)"},
        {"template_id": "fv_07", "expression": "rank({deep_field} / enterprise_value)"},
        # v6.2.1: Multi-period — short+long agreement as a signal (structurally different)
        {"template_id": "fv_08", "expression": "rank(ts_rank({deep_field} / cap, 22)) * rank(ts_rank({deep_field} / cap, 252))"},
        {"template_id": "fv_09", "expression": "rank(ts_rank({deep_field} / cap, 60)) * rank(ts_rank({deep_field} / cap, 252))"},
    ],
    "size_value": [
        {"template_id": "sv_01", "expression": "rank(ts_zscore(eps / close, {n}))"},
        {"template_id": "sv_02", "expression": "rank(ts_delta(ebitda / enterprise_value, {n}))"},
        {"template_id": "sv_03", "expression": "-rank(ts_zscore(close / (bookvalue_ps + 0.001), {n}))"},
    ],
    "quality_trend": [
        {"template_id": "qt_01", "expression": "rank(ts_delta(cashflow_op / assets, {n}))"},
        {"template_id": "qt_02", "expression": "rank(ts_delta(current_ratio, {n}))"},
        {"template_id": "qt_03", "expression": "rank(cashflow_op / (income + 0.001))"},
        {"template_id": "qt_04", "expression": "ts_rank(cashflow_op / debt, 252)"},
        {"template_id": "qt_05", "expression": "rank(ts_delta(equity / assets, {n}))"},
        {"template_id": "qt_06", "expression": "group_rank(ts_rank(cashflow_op / debt, 252), subindustry)"},
    ],
    "fundamental_scores": [
        {"template_id": "fs_01", "expression": "rank(ts_delta({fscore_field}, {n}))"},
        {"template_id": "fs_02", "expression": "rank(ts_zscore({fscore_field}, {n}))"},
        {"template_id": "fs_03", "expression": "group_rank(-ts_zscore({fscore_field}, {n}), industry)"},
        {"template_id": "fs_04", "expression": "rank(ts_zscore({fscore_field}, {n})) * rank(cap)"},
        {"template_id": "fs_05", "expression": "rank(ts_zscore({fscore_field}, {n})) * rank(adv20)"},
        {"template_id": "fs_06", "expression": "rank(ts_zscore({fscore_field}, {n}) * rank(cap))"},
        {"template_id": "fs_07", "expression": "rank({derivative_field}) * rank(adv20)"},
        {"template_id": "fs_08", "expression": "ts_decay_linear(rank({derivative_field}) * rank(cap), {n})"},
    ],
    "earnings_momentum": [
        {"template_id": "em_01", "expression": "rank(snt1_d1_netearningsrevision)"},
        {"template_id": "em_02", "expression": "rank(ts_delta(snt1_d1_earningssurprise, {n}))"},
        {"template_id": "em_03", "expression": "rank(snt1_d1_dynamicfocusrank)"},
        {"template_id": "em_04", "expression": "ts_decay_linear(rank(consensus_analyst_rating), {n})"},
        {"template_id": "em_05", "expression": "rank(snt1_d1_stockrank)"},
        {"template_id": "em_06", "expression": "rank(ts_zscore(snt1_d1_netearningsrevision, {n}))"},
        {"template_id": "em_07", "expression": "group_rank(snt1_d1_earningssurprise, subindustry)"},
        {"template_id": "em_08", "expression": "rank(ts_decay_linear(snt1_d1_earningssurprise, 60))"},
        {"template_id": "em_09", "expression": "rank(ts_decay_linear(snt1_d1_earningssurprise, 60)) + rank(ts_decay_linear(snt1_d1_netearningsrevision, 40))"},
    ],
    "options_vol": [
        {"template_id": "opt_01", "expression": "rank(ts_backfill(implied_volatility_call_{opt_window}, 60) - ts_backfill(implied_volatility_put_{opt_window}, 60))"},
        {"template_id": "opt_02", "expression": "rank(ts_backfill(implied_volatility_call_{opt_window}, 60) / (ts_backfill(historical_volatility_{opt_window}, 60) + 0.001))"},
        {"template_id": "opt_03", "expression": "rank((ts_backfill(implied_volatility_call_{opt_window}, 60) - ts_backfill(implied_volatility_put_{opt_window}, 60)) * rank(adv20))"},
        {"template_id": "opt_04", "expression": "rank((ts_backfill(implied_volatility_call_{opt_window}, 60) / (ts_backfill(historical_volatility_{opt_window}, 60) + 0.001)) * rank(adv20))"},
        {"template_id": "opt_05", "expression": "trade_when(ts_arg_max(ts_backfill(pcr_vol_{pcr_window}, 60), 7) < 1, rank(-returns), -1)"},
        {"template_id": "opt_06", "expression": "ts_decay_linear(ts_delta(ts_backfill(implied_volatility_call_{opt_window}, 60), 25) > 0, 20)"},
        {"template_id": "opt_07", "expression": "rank(-ts_backfill(pcr_oi_{pcr_window}, 60)) * rank(adv20)"},
        {"template_id": "opt_08", "expression": "rank(ts_backfill(implied_volatility_mean_skew_{opt_window}, 60)) * rank(cap)"},
        {"template_id": "opt_09", "expression": "trade_when(ts_backfill(pcr_oi_270, 60) < 1, ts_backfill(implied_volatility_call_270, 60) - ts_backfill(implied_volatility_put_270, 60), -1)"},
        # v6.2.1: PROVEN PORTFOLIO-ADDITIVE — IV/realized ratio (all 4 overnight winners used this)
        {"template_id": "opt_10", "expression": "rank(ts_backfill(implied_volatility_call_120, 60) / (ts_backfill(parkinson_volatility_120, 60) + 0.001))"},
        {"template_id": "opt_11", "expression": "group_rank(ts_backfill(implied_volatility_call_120, 60) / (ts_backfill(parkinson_volatility_120, 60) + 0.001), industry)"},
        # v6.2.1: IV/realized × fundamentals cross-category (the +48 winning pattern)
        {"template_id": "opt_12", "expression": "rank(ts_backfill(implied_volatility_call_120, 60) / (ts_backfill(parkinson_volatility_120, 60) + 0.001) * snt1_d1_netearningsrevision)"},
        {"template_id": "opt_13", "expression": "rank(ts_backfill(implied_volatility_call_120, 60) / (ts_backfill(parkinson_volatility_120, 60) + 0.001) * forward_ebitda_to_enterprise_value_2)"},
        # v6.2.1: Options term structure (long vs short IV spread)
        {"template_id": "opt_14", "expression": "rank(ts_backfill(implied_volatility_call_120, 60) - ts_backfill(implied_volatility_call_30, 60))"},
        {"template_id": "opt_15", "expression": "rank((ts_backfill(implied_volatility_call_120, 60) - ts_backfill(implied_volatility_call_30, 60)) * rank(adv20))"},
        # v6.2.1: PCR mean reversion (high PCR → oversold → buy)
        {"template_id": "opt_16", "expression": "rank(ts_zscore(ts_backfill(pcr_oi_60, 60), 20))"},
    ],
    "news_sentiment": [
        {"template_id": "ns_01", "expression": "rank(ts_backfill(scl12_sentiment, 60))"},
        {"template_id": "ns_02", "expression": "rank(ts_delta(ts_backfill(scl12_buzz, 60), {n}))"},
        {"template_id": "ns_03", "expression": "rank(ts_backfill(rp_ess_earnings, 60))"},
        {"template_id": "ns_04", "expression": "ts_decay_linear(rank(ts_backfill(news_pct_1min, 60)), {n})"},
        {"template_id": "ns_05", "expression": "rank(ts_backfill(snt_buzz_bfl, 60)) * rank(adv20)"},
        {"template_id": "ns_06", "expression": "rank(ts_backfill(rp_css_earnings, 60) - ts_backfill(rp_css_credit, 60))"},
        {"template_id": "ns_07", "expression": "-ts_std_dev(ts_backfill(scl12_buzz, 60), {n})"},
        {"template_id": "ns_08", "expression": "-rank(ts_backfill(scl12_buzz, 60) / (ts_mean(ts_backfill(scl12_buzz, 60), {n}) + 0.001))"},
        # v6.2.1: News × reversion combos (cross-category, likely uncorrelated with portfolio)
        {"template_id": "ns_09", "expression": "rank(-ts_backfill(news_max_up_ret, 60) * ts_mean(-returns, {n}))"},
        {"template_id": "ns_10", "expression": "rank(ts_backfill(rp_ess_earnings, 60) * -ts_mean(returns, {n}))"},
        {"template_id": "ns_11", "expression": "rank(ts_backfill(scl12_sentiment, 60) * rank(adv20) * -returns)"},
        # v6.2.1: News momentum (buzz acceleration)
        {"template_id": "ns_12", "expression": "rank(ts_delta(ts_backfill(scl12_buzz, 60), 5) * ts_backfill(scl12_sentiment, 60))"},
    ],
    "vol_regime": [
        {"template_id": "vr_01", "expression": "trade_when(ts_rank(ts_std_dev(returns, 22), 252) > 0.55, -ts_regression(returns, ts_delay(returns, 1), 252, rettype=2), -1)"},
        {"template_id": "vr_02", "expression": "trade_when(ts_rank(ts_std_dev(returns, {n}), 252) > 0.5, rank(-returns), -1)"},
        {"template_id": "vr_03", "expression": "rank(-ts_regression(returns, ts_delay(returns, 1), {n}, rettype=2))"},
        {"template_id": "vr_04", "expression": "rank(ts_regression(close, ts_step(1), {n}, rettype=2))"},
    ],
    "combo_factor": [
        {"template_id": "cf_01", "expression": "-rank(close / {deep_field}) + -rank(ts_mean(returns, {n}))"},
        {"template_id": "cf_02", "expression": "-rank(ts_zscore({deep_field}, {n})) + -rank(ts_mean((close - vwap) / vwap, {m}))"},
        {"template_id": "cf_03", "expression": "rank({deep_field} / assets) + -rank(ts_mean((close - vwap) / vwap, {n}))"},
        {"template_id": "cf_04", "expression": "rank({deep_field} / cap) + -rank(ts_mean(returns, {n}))"},
        {"template_id": "cf_05", "expression": "rank({deep_field} / cap) + rank(trade_when(ts_rank(ts_std_dev(returns, 22), 252) > 0.5, rank(-returns), -1))"},
        {"template_id": "cf_06", "expression": "rank(snt1_d1_netearningsrevision) + -rank(ts_mean(returns, {n}))"},
        {"template_id": "cf_07", "expression": "rank(snt1_d1_dynamicfocusrank) + -rank(ts_mean((close - vwap) / vwap, {n}))"},
        {"template_id": "cf_08", "expression": "rank(ts_backfill(implied_volatility_call_{opt_window}, 60) / (ts_backfill(historical_volatility_{opt_window}, 60) + 0.001)) + -rank(ts_mean(returns, {n}))"},
        {"template_id": "cf_09", "expression": "rank(ts_zscore({fscore_field}, {n})) * rank(adv20) + -rank(ts_mean(returns, {m}))"},
        {"template_id": "cf_10", "expression": "rank(ts_backfill(scl12_sentiment, 60)) + rank({deep_field} / cap)"},
        # v6.1: RAW MULTIPLICATIVE combo factors — rank(A * B) form
        {"template_id": "cf_11", "expression": "rank(({deep_field} / cap) * (-ts_mean(returns, {n})))"},
        {"template_id": "cf_12", "expression": "rank(ts_zscore({deep_field}, {n}) * (-(close - vwap) / vwap))"},
        {"template_id": "cf_13", "expression": "rank(({deep_field} / cap) * (-ts_mean((close - vwap) / vwap, {m})))"},
        # v6.1: GROUP_RANK of fundamental × reversion product
        {"template_id": "cf_14", "expression": "group_rank(({deep_field} / cap) * (-ts_mean(returns, {n})), industry)"},
        {"template_id": "cf_15", "expression": "group_rank(ts_zscore({deep_field}, {n}) * (-ts_mean(returns, {m})), subindustry)"},
    ],

    # ══════════════════════════════════════════════════════════════════
    # v5.9 NEW FAMILIES
    # ══════════════════════════════════════════════════════════════════
    "model77_anomaly": [
        {"template_id": "m77_01", "expression": "rank({model77_field})"},
        {"template_id": "m77_02", "expression": "-rank({model77_field})"},
        {"template_id": "m77_03", "expression": "rank(ts_zscore({model77_field}, {n}))"},
        {"template_id": "m77_04", "expression": "rank(ts_delta({model77_field}, {n}))"},
        {"template_id": "m77_05", "expression": "ts_decay_linear(rank({model77_field}), {n})"},
        {"template_id": "m77_06", "expression": "group_rank({model77_field}, industry)"},
        {"template_id": "m77_07", "expression": "group_rank({model77_field}, subindustry)"},
        {"template_id": "m77_08", "expression": "rank({model77_field}) * rank(adv20)"},
        {"template_id": "m77_09", "expression": "rank({model77_field}) * rank(cap)"},
    ],
    "model77_combo": [
        # Additive (original)
        {"template_id": "m7c_01", "expression": "rank({model77_field}) + -rank(ts_mean(returns, {m}))"},
        {"template_id": "m7c_02", "expression": "-rank({model77_field}) + -rank(ts_mean(returns, {m}))"},
        {"template_id": "m7c_03", "expression": "rank({model77_field}) + rank(trade_when(ts_rank(ts_std_dev(returns, 22), 252) > 0.5, rank(-returns), -1))"},
        {"template_id": "m7c_04", "expression": "group_rank({model77_field}, industry) + -rank(ts_mean((close - vwap) / vwap, {m}))"},
        # v5.9.1: MULTIPLICATIVE combos (research: outperform additive)
        {"template_id": "m7c_05", "expression": "rank({model77_field}) * rank(-ts_mean(returns, {m}))"},
        {"template_id": "m7c_06", "expression": "rank({model77_field}) * rank(trade_when(ts_rank(ts_std_dev(returns, 22), 252) > 0.5, rank(-returns), -1))"},
        {"template_id": "m7c_07", "expression": "rank(group_zscore({model77_field}, industry) * group_zscore(-returns, industry))"},
        {"template_id": "m7c_08", "expression": "rank(group_zscore({model77_field}, subindustry) * group_zscore(-ts_mean(returns, {m}), subindustry))"},
        # v5.9.1: Q-theory composite (Hou, Xue & Zhang — strongest academic signal)
        {"template_id": "m7c_09", "expression": "rank(gross_profit_to_assets_ratio) - rank(asset_growth_rate)"},
        {"template_id": "m7c_10", "expression": "rank(gross_profit_to_assets_ratio) - rank(asset_growth_rate) + rank(-ts_mean(returns, {m}))"},
        {"template_id": "m7c_11", "expression": "group_rank(gross_profit_to_assets_ratio, subindustry) - group_rank(asset_growth_rate, subindustry)"},
        # v5.9.1: Earnings quality × value (multiplicative model77 × model77)
        {"template_id": "m7c_12", "expression": "rank(five_year_eps_stability * forward_ebitda_to_enterprise_value_2)"},
        {"template_id": "m7c_13", "expression": "rank(five_year_eps_stability * forward_ebitda_to_enterprise_value_2) * rank(-ts_mean(returns, {m}))"},
        # v5.9.1: 3-signal composites (weak signals → strong combined)
        {"template_id": "m7c_14", "expression": "rank({model77_field}) + rank(gross_profit_to_assets_ratio) + rank(-ts_mean(returns, {m}))"},
        {"template_id": "m7c_15", "expression": "rank({model77_field}) + rank(-asset_growth_rate) + rank(-ts_mean(returns, {m}))"},
        # v6.1: RAW MULTIPLICATIVE — rank(A * B) form (research: outperforms rank(A) + rank(B) and rank(A) * rank(B))
        {"template_id": "m7c_16", "expression": "rank({model77_field} * (-ts_mean(returns, {m})))"},
        {"template_id": "m7c_17", "expression": "rank({model77_field} * (-(close - vwap) / vwap))"},
        {"template_id": "m7c_18", "expression": "rank({model77_field} * (-ts_mean((close - vwap) / vwap, {m})))"},
        {"template_id": "m7c_19", "expression": "rank({model77_field} * ts_backfill(implied_volatility_mean_skew_30, 60))"},
        {"template_id": "m7c_20", "expression": "rank({model77_field} * snt1_d1_netearningsrevision)"},
        # v6.1: 3-signal with multiplicative pair + additive third
        {"template_id": "m7c_21", "expression": "rank({model77_field} * gross_profit_to_assets_ratio) + rank(-ts_mean(returns, {m}))"},
        {"template_id": "m7c_22", "expression": "rank({model77_field} * (-asset_growth_rate)) + rank(-ts_mean(returns, {m}))"},
        # v6.1: GROUP_RANK of product — research: outperforms rank() for fundamentals 78% of the time
        {"template_id": "m7c_23", "expression": "group_rank({model77_field} * (-ts_mean(returns, {m})), industry)"},
        {"template_id": "m7c_24", "expression": "group_rank({model77_field} * (-ts_mean((close - vwap) / vwap, {m})), subindustry)"},
        # v6.1: TS_RANK temporal ranking × reversion — best for quarterly fundamental data
        {"template_id": "m7c_25", "expression": "rank(ts_rank({model77_field}, 252) * (-ts_mean(returns, {m})))"},
        {"template_id": "m7c_26", "expression": "group_rank(ts_rank({model77_field}, 252) * (-ts_mean(returns, {m})), industry)"},
    ],
    "relationship": [
        {"template_id": "rel_01", "expression": "rank(rel_ret_cust)"},
        {"template_id": "rel_02", "expression": "-rank(rel_ret_comp)"},
        {"template_id": "rel_03", "expression": "rank(ts_delta(rel_ret_cust, {n}))"},
        {"template_id": "rel_04", "expression": "rank(ts_zscore(rel_ret_cust, {n}))"},
        {"template_id": "rel_05", "expression": "rank(rel_ret_cust) * rank(rel_num_cust)"},
        {"template_id": "rel_06", "expression": "rank(pv13_ustomergraphrank_page_rank) * rank(adv20)"},
        {"template_id": "rel_07", "expression": "rank(rel_ret_cust) + -rank(ts_mean(returns, {m}))"},
        {"template_id": "rel_08", "expression": "-rank(rel_ret_comp) + -rank(ts_mean(returns, {m}))"},
        # v5.9.1: Decayed customer momentum (Cohen & Frazzini 2008 — 150bp monthly)
        {"template_id": "rel_09", "expression": "ts_decay_linear(ts_mean(rel_ret_cust, 5), 10)"},
        {"template_id": "rel_10", "expression": "ts_decay_linear(rank(rel_ret_cust) - rank(rel_ret_comp), 10)"},
        # v5.9.1: Multiplicative customer momentum × profitability
        {"template_id": "rel_11", "expression": "rank(ts_decay_linear(rel_ret_cust, 10) * group_zscore(operating_income / (sales + 0.001), sector))"},
    ],
    "risk_beta": [
        {"template_id": "rb_01", "expression": "-rank(beta_last_60_days_spy)"},
        {"template_id": "rb_02", "expression": "-rank(beta_last_360_days_spy)"},
        {"template_id": "rb_03", "expression": "rank(unsystematic_risk_last_60_days)"},
        {"template_id": "rb_04", "expression": "-rank(ts_zscore(beta_last_60_days_spy, {n}))"},
        {"template_id": "rb_05", "expression": "-rank(correlation_last_60_days_spy) * rank(adv20)"},
        {"template_id": "rb_06", "expression": "-rank(beta_last_60_days_spy) + rank({deep_field} / cap)"},
        {"template_id": "rb_07", "expression": "-rank(beta_last_60_days_spy) + -rank(ts_mean(returns, {m}))"},
        # v6.2.1: Cross-category beta × fundamentals (beta is genuinely different data)
        {"template_id": "rb_08", "expression": "rank(-beta_last_60_days_spy * forward_ebitda_to_enterprise_value_2)"},
        {"template_id": "rb_09", "expression": "rank(-beta_last_60_days_spy) + rank(est_eps / close)"},
        {"template_id": "rb_10", "expression": "rank(unsystematic_risk_last_60_days * gross_profit_to_assets_ratio)"},
        {"template_id": "rb_11", "expression": "rank(-beta_last_60_days_spy * ts_backfill(implied_volatility_call_120, 60))"},
    ],
    "expanded_fundamental": [
        {"template_id": "ef_01", "expression": "rank(cashflow_op / assets) - rank(income / assets)"},
        {"template_id": "ef_02", "expression": "-group_rank(ts_delta(assets, 252) / (ts_delay(assets, 252) + 0.001), subindustry)"},
        {"template_id": "ef_03", "expression": "-rank(ts_delta(sharesout, 252) / (ts_delay(sharesout, 252) + 0.001))"},
        {"template_id": "ef_04", "expression": "rank((sales - cogs) / assets)"},
        {"template_id": "ef_05", "expression": "group_rank((sales - cogs) / assets, subindustry)"},
        {"template_id": "ef_06", "expression": "-ts_rank(retained_earnings, 500)"},
        {"template_id": "ef_07", "expression": "rank(rd_expense / cap)"},
        {"template_id": "ef_08", "expression": "rank(ts_delta(working_capital / assets, {n}))"},
        {"template_id": "ef_09", "expression": "rank(ts_delta(inventory_turnover, {n}))"},
        {"template_id": "ef_10", "expression": "group_rank((sales - ebitda) / assets, subindustry)"},
        {"template_id": "ef_11", "expression": "-ts_zscore(enterprise_value / (ebitda + 0.001), 63)"},
        {"template_id": "ef_12", "expression": "-rank(ebit / (capex + 0.001))"},
        {"template_id": "ef_13", "expression": "rank(cashflow_op / enterprise_value)"},
        {"template_id": "ef_14", "expression": "rank(sales / assets) + rank(bookvalue_ps / close)"},
        # v5.9.1: R&D intensity (Lev & Sougiannis — 1.52% monthly FF5 alpha)
        {"template_id": "ef_15", "expression": "group_rank(rd_expense / (sales + 0.001), industry)"},
        {"template_id": "ef_16", "expression": "rank(rd_expense / (sales + 0.001)) * rank(1 / (tobins_q_ratio + 0.001))"},
        # v5.9.1: Piotroski-style 3-factor quality composite
        {"template_id": "ef_17", "expression": "rank(group_zscore(cashflow_op / assets, subindustry) + group_zscore(operating_income / (sales + 0.001), subindustry) - group_zscore(debt / assets, subindustry))"},
        # v5.9.1: Altman Z-Score composite
        {"template_id": "ef_18", "expression": "rank(1.2 * working_capital / (assets + 0.001) + 1.4 * retained_earnings / (assets + 0.001) + 3.3 * ebit / (assets + 0.001))"},
        # v6.2.1: Proven academic anomalies (from previous research session)
        # Accrual anomaly (Sloan 1996) — cash flow vs earnings gap
        {"template_id": "ef_19", "expression": "rank(cashflow_op / assets) - rank(income / assets)"},
        {"template_id": "ef_20", "expression": "-rank(ts_delta(assets_curr - liabilities_curr + debt_st, 252) / (ts_delay(assets, 252) + 0.001))"},
        # Retained earnings reversion — proven S=1.55 in previous chat
        {"template_id": "ef_21", "expression": "-ts_rank(retained_earnings, 500)"},
        # Investment anomaly (Titman, Wei & Xie) — overinvestors underperform
        {"template_id": "ef_22", "expression": "-rank(ts_delta(assets, 252) / (ts_delay(assets, 252) + 0.001))"},
    ],
    "analyst_estimates": [
        {"template_id": "ae_01", "expression": "group_rank(ts_rank(est_eps / close, 60), industry)"},
        {"template_id": "ae_02", "expression": "ts_decay_linear(ts_scale(est_cashflow_op, 252), 22) - ts_decay_linear(ts_scale(est_capex, 252), 22)"},
        {"template_id": "ae_03", "expression": "-ts_corr(est_ptp, est_fcf, 252)"},
        {"template_id": "ae_04", "expression": "rank(ts_zscore(est_fcf / cap, {n}))"},
        {"template_id": "ae_05", "expression": "rank(est_eps / close) + rank(snt1_d1_netearningsrevision)"},
    ],
    "wq_proven": [
        {"template_id": "wp_01", "expression": "group_rank(-ts_zscore(enterprise_value / cashflow, 63), industry)"},
        {"template_id": "wp_02", "expression": "ts_backfill(implied_volatility_call_120, 60) / (ts_backfill(parkinson_volatility_120, 60) + 0.001)"},
        {"template_id": "wp_03", "expression": "ts_regression(ts_sum(ts_backfill(operating_income, 60), 252), ts_step(1), 756, rettype=2)"},
        {"template_id": "wp_04", "expression": "winsorize(-ts_backfill(news_max_up_ret, 60) * abs(ts_regression(ts_backfill(news_pct_1min, 60), ts_step(1), 5, rettype=2)), std=4)"},
        {"template_id": "wp_05", "expression": "ts_rank(operating_income / cap, 252)"},
        {"template_id": "wp_06", "expression": "-ts_rank(fn_liab_fair_val_l1_a, 252)"},
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
    "price_vol_corr": [
        {"template_id": "pvc_01", "expression": "-rank(ts_corr(rank(close), rank(volume), {n}))"},
        {"template_id": "pvc_02", "expression": "rank(ts_corr(ts_delta(close, {m}), volume, {n}))"},
        {"template_id": "pvc_03", "expression": "-rank(ts_corr(vwap, volume, {n}))"},
        {"template_id": "pvc_04", "expression": "-rank((close - vwap) / (vwap + 0.001) * log(volume + 1))"},
    ],
    "volatility": [
        {"template_id": "vlty_01", "expression": "rank(1 / (ts_std_dev(returns, {n}) + 0.001))"},
        {"template_id": "vlty_04", "expression": "rank(ts_mean(returns, {m}) / (ts_std_dev(returns, {n}) + 0.001))"},
    ],
    "intraday": [
        {"template_id": "iday_01", "expression": "rank(-(high - low) / (close + 0.001))"},
        {"template_id": "iday_02", "expression": "rank((close - low) / (high - low + 0.001))"},
        {"template_id": "iday_03", "expression": "-rank(ts_mean(high - low, {n}) / (close + 0.001))"},
        {"template_id": "iday_04", "expression": "rank(open / close - 1)"},
        {"template_id": "iday_05", "expression": "-rank(ts_mean(open / close - 1, {n}))"},
        # v6.2.1: Intraday cross-category (genuinely different data from price returns)
        {"template_id": "iday_06", "expression": "rank((close - open) / (high - low + 0.001) * snt1_d1_earningssurprise)"},
        {"template_id": "iday_07", "expression": "rank(ts_zscore((high - low) / (close + 0.001), {n})) * rank(adv20)"},
        {"template_id": "iday_08", "expression": "group_rank((close - low) / (high - low + 0.001), industry)"},
    ],
    "fundamental": [
        {"template_id": "fund_01", "expression": "rank({field})"},
        {"template_id": "fund_02", "expression": "rank(ts_delta({field}, {n}))"},
        {"template_id": "fund_03", "expression": "rank(({field} - ts_mean({field}, {n})))"},
    ],
    "analyst_sentiment": [
        {"template_id": "ans_01", "expression": "rank(ts_delta({analyst_field}, {n}))"},
        {"template_id": "ans_02", "expression": "-rank({sentiment_field} / (ts_mean({sentiment_field}, {n}) + 0.001))"},
        {"template_id": "ans_03", "expression": "rank(ts_zscore({analyst_field}, {n}))"},
        # v6.2.1: Sentiment × reversion combos (cross-category, proven in +46 resim winner)
        {"template_id": "ans_04", "expression": "rank(ts_backfill(snt1_d1_netearningsrevision, 60) * -ts_mean(returns, {n}))"},
        {"template_id": "ans_05", "expression": "rank(ts_backfill(snt1_d1_earningssurprise, 60) * forward_ebitda_to_enterprise_value_2)"},
        {"template_id": "ans_06", "expression": "rank(ts_backfill(consensus_analyst_rating, 60) * -ts_mean((close - vwap) / vwap, {n}))"},
    ],

    # ══════════════════════════════════════════════════════════════════
    # v6.2.1: UNTAPPED DATA CATEGORIES — virtually zero portfolio correlation
    # ══════════════════════════════════════════════════════════════════

    # Vector datasets — multiple values per stock per day, use vec_* operators
    # Most competitors skip these entirely. Proven: S=1.94 buzz alpha.
    "vector_data": [
        # Social buzz vectors — proven pattern: aggregate then smooth
        {"template_id": "vec_01", "expression": "ts_av_diff(ts_backfill(-vec_sum(scl12_alltype_buzzvec), 20), 60)"},
        {"template_id": "vec_02", "expression": "rank(ts_backfill(vec_avg(scl12_alltype_sentvec), 20))"},
        # News vectors — after-hours significance
        {"template_id": "vec_04", "expression": "rank(ts_backfill(vec_sum(nws12_afterhsz_sl), 20))"},
        {"template_id": "vec_05", "expression": "rank(ts_backfill(vec_avg(nws12_afterhsz_sl), 20) * -ts_mean(returns, {n}))"},
        # Buzz × sentiment interaction
        {"template_id": "vec_06", "expression": "rank(ts_backfill(vec_sum(scl12_alltype_buzzvec), 20) * ts_backfill(vec_avg(scl12_alltype_sentvec), 20))"},
        # Buzz IR (information ratio — signal-to-noise of social media)
        # v6.2.1: News-conditional regime switching — proven S=1.84
        {"template_id": "vec_11", "expression": "trade_when(rank(ts_sum(ts_backfill(vec_avg(nws12_afterhsz_sl), 20), 60)) > 0.5, rank(-ts_delta(close, 2)), -1)"},
    ],

    # ══════════════════════════════════════════════════════════════════════
    # v6.2.1: 10 NEW FAMILIES — completely untapped data categories
    # ══════════════════════════════════════════════════════════════════════

    # SUPPLY CHAIN — 165 pv13_* fields, 0 submitted alphas, fewest users on platform
    "supply_chain": [
        {"template_id": "sc_01", "expression": "rank(ts_mean(rel_ret_cust, {n}))"},
        {"template_id": "sc_02", "expression": "rank(ts_mean(rel_ret_comp, {n})) * rank(-returns)"},
        {"template_id": "sc_03", "expression": "rank(pv13_com_page_rank) * rank(ts_rank(operating_income / cap, 252))"},
        {"template_id": "sc_04", "expression": "trade_when(ts_rank(ts_std_dev(returns, 22), 252) > 0.5, rank(ts_mean(rel_ret_cust, {n})), -1)"},
        {"template_id": "sc_05", "expression": "rank(ts_mean(rel_ret_cust, {n}) - ts_mean(rel_ret_comp, {n}))"},
        {"template_id": "sc_06", "expression": "rank(pv13_com_page_rank) * rank(forward_ebitda_to_enterprise_value_2)"},
        {"template_id": "sc_07", "expression": "rank(ts_delta(rel_num_cust, {n}))"},
        {"template_id": "sc_08", "expression": "rank(pv13_custretsig_retsig) * group_rank(ts_rank(est_eps / close, 60), industry)"},
    ],

    # RAVENPACK CATEGORY SENTIMENT — 75 fields, 0 submitted alphas
    "ravenpack_cat": [
        {"template_id": "rp_01", "expression": "rank(ts_backfill(rp_ess_mna, 60))"},
        {"template_id": "rp_02", "expression": "rank(ts_backfill(rp_ess_earnings, 60)) * rank(-ts_mean(returns, {n}))"},
        {"template_id": "rp_03", "expression": "-rank(ts_av_diff(ts_backfill(rp_css_credit, 60), 30))"},
        {"template_id": "rp_04", "expression": "rank(ts_backfill(rp_ess_insider, 60)) * rank(-(close - vwap) / vwap)"},
        {"template_id": "rp_05", "expression": "rank(ts_backfill(rp_ess_revenue, 60)) * rank(snt1_d1_netearningsrevision)"},
        {"template_id": "rp_06", "expression": "-rank(ts_backfill(rp_css_legal, 60))"},
        {"template_id": "rp_07", "expression": "rank(ts_delta(ts_backfill(rp_ess_product, 60), 10))"},
        {"template_id": "rp_08", "expression": "rank(ts_backfill(rp_nip_earnings, 60) * ts_backfill(rp_ess_earnings, 60))"},
        {"template_id": "rp_09", "expression": "trade_when(ts_backfill(nws18_event_relevance, 60) > 50, rank(ts_backfill(rp_ess_earnings, 60)), -1)"},
    ],

    # OPTIONS ANALYTICS — breakeven & forward price, 74 fields, 0 submitted
    "options_analytics": [
        {"template_id": "oa_01", "expression": "rank(ts_backfill(call_breakeven_30, 60) / close - 1)"},
        {"template_id": "oa_02", "expression": "rank(ts_backfill(call_breakeven_120, 60) - ts_backfill(call_breakeven_30, 60))"},
        {"template_id": "oa_03", "expression": "rank(ts_delta(ts_backfill(forward_price_60, 60), 10) / close)"},
        {"template_id": "oa_04", "expression": "rank(1 - ts_backfill(put_breakeven_30, 60) / close)"},
        {"template_id": "oa_05", "expression": "rank((ts_backfill(call_breakeven_60, 60) - close) / (close - ts_backfill(put_breakeven_60, 60) + 0.001))"},
        {"template_id": "oa_06", "expression": "rank(ts_delta(ts_backfill(option_breakeven_60, 60), {n})) * rank(volume / adv20)"},
    ],

    # HISTORICAL VOLATILITY — vol risk premium, 8 fields, 0 submitted
    "hist_vol": [
        {"template_id": "hv_01", "expression": "rank(ts_backfill(implied_volatility_call_60, 60) / (historical_volatility_60 + 0.001))"},
        {"template_id": "hv_02", "expression": "trade_when(historical_volatility_20 > ts_mean(historical_volatility_20, 60), rank(-returns), -1)"},
        {"template_id": "hv_03", "expression": "rank(historical_volatility_20 / (historical_volatility_90 + 0.001))"},
        {"template_id": "hv_04", "expression": "rank(ts_delta(ts_backfill(implied_volatility_mean_skew_60, 60), 10))"},
        {"template_id": "hv_05", "expression": "-rank(ts_backfill(implied_volatility_mean_60, 60))"},
    ],

    # FUNDAMENTAL SCORES (fscore_*) — 24 fields, 0 submitted
    "fscore": [
        {"template_id": "fs_01", "expression": "rank(ts_backfill(fscore_bfl_quality, 60)) * rank(ts_backfill(fscore_bfl_momentum, 60))"},
        {"template_id": "fs_02", "expression": "rank(ts_backfill(fscore_bfl_surface_accel, 60)) * rank(cap)"},
        {"template_id": "fs_03", "expression": "rank(ts_backfill(fscore_bfl_total, 60) * rank(cap))"},
        {"template_id": "fs_04", "expression": "rank(ts_backfill(fscore_bfl_value, 60)) * rank(ts_backfill(fscore_bfl_profitability, 60))"},
        {"template_id": "fs_05", "expression": "rank(ts_backfill(growth_potential_rank_derivative, 60))"},
        {"template_id": "fs_06", "expression": "rank(ts_delta(ts_backfill(composite_factor_score_derivative, 60), 20))"},
    ],

    # RISK METRICS — beta, correlation, systematic risk, 16 fields, 0 submitted
    "risk_metrics": [
        {"template_id": "rm_01", "expression": "-rank(beta_last_60_days_spy)"},
        {"template_id": "rm_02", "expression": "rank(unsystematic_risk_last_60_days / (systematic_risk_last_60_days + 0.001))"},
        {"template_id": "rm_03", "expression": "rank(beta_last_30_days_spy - beta_last_90_days_spy)"},
        {"template_id": "rm_04", "expression": "-rank(ts_delta(correlation_last_60_days_spy, 20))"},
        {"template_id": "rm_05", "expression": "-rank(beta_last_60_days_spy) * rank(forward_ebitda_to_enterprise_value_2)"},
        {"template_id": "rm_06", "expression": "trade_when(systematic_risk_last_30_days > systematic_risk_last_90_days, rank(-returns), -1)"},
    ],

    # INTRADAY PATTERNS — open/high/low/close relationships, 0 submitted
    "intraday_pattern": [
        {"template_id": "id_01", "expression": "rank(-(open - ts_delay(close, 1)) / (ts_delay(close, 1) + 0.001))"},
        {"template_id": "id_02", "expression": "rank((close - open) / (high - low + 0.001))"},
        {"template_id": "id_03", "expression": "-rank((high - max(open, close)) / (high - low + 0.001))"},
        {"template_id": "id_04", "expression": "rank(-ts_mean(high - low, {n}) / (ts_mean(high - low, 60) + 0.001))"},
        {"template_id": "id_05", "expression": "rank(ts_mean((high - low) / (volume + 0.001), {n}))"},
    ],

    # DEEP ANALYST SENTIMENT — snt1_d1_* deep fields, mostly untapped
    "analyst_deep": [
        {"template_id": "as_01", "expression": "-rank(snt1_d1_earningstorpedo)"},
        {"template_id": "as_02", "expression": "rank(snt1_d1_buyrecpercent - snt1_d1_sellrecpercent)"},
        {"template_id": "as_03", "expression": "rank(snt1_d1_uptargetpercent - snt1_d1_downtargetpercent)"},
        {"template_id": "as_04", "expression": "rank(snt1_d1_analystcoverage) * rank(snt1_d1_earningsrevision)"},
        {"template_id": "as_05", "expression": "rank(snt1_d1_longtermepsgrowthest) * rank(-returns)"},
        {"template_id": "as_06", "expression": "rank(snt1_d1_stockrank)"},
        {"template_id": "as_07", "expression": "rank(snt1_d1_fundamentalfocusrank - snt1_d1_dynamicfocusrank)"},
    ],

    # SOCIAL MEDIA SCALAR — non-vector buzz/sentiment, 0 submitted
    "social_scalar": [
        {"template_id": "sm_01", "expression": "-rank(scl12_buzz)"},
        {"template_id": "sm_02", "expression": "rank(scl12_buzz_fast_d1)"},
        {"template_id": "sm_03", "expression": "rank(snt_value)"},
        {"template_id": "sm_04", "expression": "rank(scl12_buzz * scl12_sentiment)"},
        {"template_id": "sm_05", "expression": "rank(snt_buzz_ret)"},
    ],

    # WILD COMBOS — cross-category novel interactions
    "wild_combos": [
        {"template_id": "wild_01", "expression": "rank(ts_mean(rel_ret_cust, 5)) * rank(ts_backfill(implied_volatility_call_60, 60) / (historical_volatility_60 + 0.001))"},
        {"template_id": "wild_02", "expression": "group_zscore(ts_backfill(rp_css_credit, 60), industry) * rank(ts_rank(cashflow_op / cap, 252))"},
        {"template_id": "wild_03", "expression": "trade_when(historical_volatility_20 > ts_mean(historical_volatility_20, 60), rank(-returns) * rank(volume / adv20), -1)"},
        {"template_id": "wild_04", "expression": "rank(ts_backfill(forward_price_60, 60) / close - 1) * rank(gross_profit_to_assets_ratio)"},
        {"template_id": "wild_05", "expression": "rank(-snt1_d1_earningstorpedo) * rank(ts_mean(rel_ret_comp, 5))"},
        {"template_id": "wild_06", "expression": "rank(ts_backfill(fscore_bfl_surface_accel, 60)) * rank(ts_backfill(implied_volatility_mean_skew_60, 60))"},
        {"template_id": "wild_07", "expression": "rank(correlation_last_30_days_spy - correlation_last_360_days_spy) * rank(ts_rank(operating_income / cap, 252))"},
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

FSCORE_FIELDS = [
    "fscore_bfl_value", "fscore_bfl_momentum", "fscore_bfl_quality",
    "fscore_bfl_growth", "fscore_bfl_profitability", "fscore_bfl_total",
]

DERIVATIVE_FIELDS = [
    "cashflow_efficiency_rank_derivative",
    "composite_factor_score_derivative",
    "earnings_certainty_rank_derivative",
    "growth_potential_rank_derivative",
    "analyst_revision_rank_derivative",
    "relative_valuation_rank_derivative",
]

OPTIONS_WINDOWS = [30, 60, 90, 120, 180]
PCR_WINDOWS = [10, 30, 60, 90, 120, 180, 270]

# v5.9: model77 pre-computed anomaly fields — Tier 1 (highest expected impact)
MODEL77_TIER1_FIELDS = [
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
]

MODEL77_TIER2_FIELDS = [
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
]

MODEL77_ALL_FIELDS = MODEL77_TIER1_FIELDS + MODEL77_TIER2_FIELDS

# Direction: fields where HIGHER value = LOWER expected returns (need -rank)
MODEL77_NEGATIVE_DIRECTION = {
    "asset_growth_rate", "one_year_change_total_assets",
    "trailing_twelve_month_accruals", "twelve_month_total_debt_change_2",
    "yearly_change_leverage", "distress_risk_measure",
    "credit_risk_premium_indicator", "twelve_month_short_interest_change",
    "cash_burn_rate", "book_leverage_ratio_3",
    "implied_minus_realized_volatility_2",
}

SAFE_PARAM_RANGES = {
    "n": [3, 5, 10, 20, 40, 60],
    "m": [5, 10, 20, 60],
}

FUNDAMENTAL_PARAM_RANGES = {
    "n": [60, 120, 180, 252],
    "m": [60, 120, 252],
}

# v5.9: Dataset-aware neutralization (from official Neutralisation.csv)
DATASET_NEUTRALIZATION = {
    "fundamental_value": ["INDUSTRY", "SUBINDUSTRY"],
    "quality_trend": ["INDUSTRY", "SUBINDUSTRY"],
    "size_value": ["INDUSTRY", "SUBINDUSTRY"],
    "expanded_fundamental": ["INDUSTRY", "SUBINDUSTRY"],
    "fundamental": ["INDUSTRY", "SUBINDUSTRY"],
    "earnings_momentum": ["INDUSTRY", "NONE"],
    "analyst_sentiment": ["INDUSTRY", "SUBINDUSTRY"],
    "analyst_estimates": ["INDUSTRY", "SUBINDUSTRY"],
    "fundamental_scores": ["SUBINDUSTRY", "INDUSTRY", "MARKET", "SECTOR"],
    "model77_anomaly": ["INDUSTRY", "SUBINDUSTRY", "MARKET", "SECTOR"],
    "model77_combo": ["INDUSTRY", "SUBINDUSTRY", "MARKET"],
    "news_sentiment": ["SUBINDUSTRY", "INDUSTRY"],
    "options_vol": ["MARKET", "SECTOR"],
    "mean_reversion": ["MARKET", "SECTOR", "NONE"],
    "cross_sectional": ["SUBINDUSTRY", "MARKET"],
    "liquidity_scaled": ["MARKET", "SECTOR"],
    "conditional": ["MARKET", "SECTOR"],
    "vol_adjusted": ["MARKET", "SECTOR"],
    "momentum": ["MARKET", "SECTOR"],
    "volume_flow": ["MARKET", "SECTOR"],
    "price_vol_corr": ["MARKET", "SECTOR"],
    "volatility": ["MARKET", "SECTOR"],
    "intraday": ["MARKET", "SECTOR"],
    "vol_regime": ["MARKET", "NONE"],
    "relationship": ["SUBINDUSTRY", "INDUSTRY"],
    "risk_beta": ["MARKET", "INDUSTRY"],
    "combo_factor": ["MARKET", "INDUSTRY", "SUBINDUSTRY"],
    "wq_proven": ["INDUSTRY", "SUBINDUSTRY", "SECTOR", "MARKET"],
}
