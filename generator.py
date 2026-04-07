from __future__ import annotations

import json
import random
from typing import Any

import config
from canonicalize import canonicalize_expression, hash_candidate
from models import Candidate, SimulationSettings
from templates import (
    FUNDAMENTAL_FIELDS, DEEP_FUNDAMENTAL_FIELDS, ANALYST_FIELDS,
    SENTIMENT_FIELDS, OPTIONS_WINDOWS, FSCORE_FIELDS,
    SAFE_PARAM_RANGES, TEMPLATE_LIBRARY,
    DERIVATIVE_FIELDS, PCR_WINDOWS,
    MODEL77_ALL_FIELDS, MODEL77_TIER1_FIELDS, MODEL77_TIER2_FIELDS, MODEL77_TIER3_FIELDS,
    MODEL77_NEGATIVE_DIRECTION, DATASET_NEUTRALIZATION,
    NEWS_EVENT_FIELDS, RP_UNDERUSED_FIELDS,
)
import templates as templates_mod


DEFAULT_BASE_FAMILY_WEIGHTS = {
    "mean_reversion": 2.5,
    "momentum": 0.01,
    "volume_flow": 2.5,
    "vol_adjusted": 1.5,
    "fundamental": 0.05,
    "conditional": 2.0,
    # New families — high initial weight to explore them quickly
    "price_vol_corr": 3.0,
    "volatility": 2.8,
    "intraday": 2.5,
    "cross_sectional": 2.2,
    "fundamental_value": 2.0,
    "analyst_sentiment": 2.5,
    "options_vol": 1.8,
    # v5.4 new families
    "liquidity_scaled": 3.5,  # High — these naturally pass sub-universe tests
    "fundamental_scores": 3.0,  # fscore data = unique data category
    "size_value": 2.5,  # Fundamental ratios = different from price signals
    # v5.8: Multi-factor combinations — HIGHEST PRIORITY
    "combo_factor": 4.0,  # Griff's proven strategy (S=2.15)
    # v5.9: NEW families — model77 pre-computed anomalies (HIGHEST PRIORITY)
    "model77_anomaly": 5.0,   # 168 untapped pre-computed academic anomaly fields
    "model77_combo": 4.5,     # model77 + price reversion combos
    "relationship": 3.5,      # Supply chain / customer-competitor spillover
    "risk_beta": 3.5,         # Betting against beta / idiosyncratic vol
    "expanded_fundamental": 4.0,  # Accruals, asset growth, profitability, net issuance
    "analyst_estimates": 3.5,     # Forward-looking analyst data
    "wq_proven": 4.0,            # WQ-documented proven expressions
}


class AlphaGenerator:
    def __init__(self, seed: int | None = None):
        self.rng = random.Random(seed)

    # ============================
    # PUBLIC
    # ============================

    def generate_candidate(self, family_bias=None, template_bias=None, settings_bias=None):
        family = self._sample_family(family_bias)
        template = self._sample_template(family, template_bias)

        params = self._sample_params(template["expression"], family=family)
        expr, fields = self._render(template["expression"], params)
        expr = self._post_process(expr, family=family, template_id=template["template_id"], light=False)

        settings = self._sample_settings(family, settings_bias=settings_bias)
        canon = canonicalize_expression(expr)
        h = hash_candidate(canon, settings.to_dict())

        return Candidate.create(
            expression=expr,
            canonical_expression=canon,
            expression_hash=h,
            template_id=template["template_id"],
            family=family,
            fields=fields,
            params=params,
            settings=settings,
        )

    def create_from_expression(self, raw_expr: str, settings_bias=None, settings_override=None) -> Candidate:
        """
        v5.6: Create a Candidate from a raw LLM-generated expression.
        Classifies the expression into a family, generates settings, and wraps it.
        v6.0: settings_override dict directly sets universe/neutralization/decay/truncation.
        """
        expr = raw_expr.strip()
        family = self._classify_llm_family(expr)
        template_id = f"llm_{family[:4]}"

        if settings_override:
            # v6.0: Optuna-suggested settings — build SimulationSettings object directly
            from models import SimulationSettings
            settings = SimulationSettings(
                region="USA",
                universe=settings_override.get("universe", "TOP3000"),
                delay=1,
                decay=int(settings_override.get("decay", 6)),
                neutralization=settings_override.get("neutralization", "SUBINDUSTRY"),
                truncation=float(settings_override.get("truncation", 0.08)),
            )
        else:
            settings = self._sample_settings(family, settings_bias=settings_bias)

        canon = canonicalize_expression(expr)
        h = hash_candidate(canon, settings.to_dict())
        fields = self._extract_fields(expr, {})

        return Candidate.create(
            expression=expr,
            canonical_expression=canon,
            expression_hash=h,
            template_id=template_id,
            family=family,
            fields=fields,
            params={},
            settings=settings,
        )

    def _classify_llm_family(self, expr: str) -> str:
        """Classify an LLM-generated expression into a family for tracking."""
        expr_lower = expr.lower()

        # v5.9: Detect model77 pre-computed anomaly fields
        model77_keywords = [
            "standardized_unexpected_earnings", "earnings_momentum_composite",
            "earnings_revision_magnitude", "asset_growth_rate", "gross_profit_to_assets",
            "tobins_q_ratio", "distress_risk_measure", "trailing_twelve_month_accruals",
            "forward_median_earnings_yield", "cash_flow_return_on_invested",
            "twelve_month_short_interest", "financial_statement_value_score",
            "fcf_yield_times_forward", "value_momentum_analyst", "momentum_analyst_composite",
            "normalized_earnings_yield", "equity_value_score", "income_statement_value_score",
            "credit_risk_premium", "sustainable_growth_rate", "reinvestment_rate",
            "price_momentum_module", "fundamental_growth_module", "sales_surprise_score",
            "ttm_operating_cash_flow", "ttm_operating_income_to_ev", "ttm_sales_to_enterprise",
            "industry_relative_return", "industry_relative_book", "industry_relative_fcf",
            "implied_minus_realized_volatility_2", "out_of_money_put_call",
            "visibility_ratio", "treynor_ratio", "cash_burn_rate",
            "capex_to_total_assets", "capex_to_depreciation",
        ]
        has_model77 = any(f in expr_lower for f in model77_keywords)
        if has_model77:
            # Check if it's a combo (model77 + price)
            has_price = any(f in expr_lower for f in ["returns", "close", "vwap"])
            if has_price and (" + " in expr_lower or ") + -" in expr_lower):
                return "model77_combo"
            return "model77_anomaly"

        # v5.9: Detect relationship/supply chain data
        if any(f in expr_lower for f in ["rel_ret_", "rel_num_", "pv13_"]):
            return "relationship"

        # v5.9: Detect risk/beta fields
        if any(f in expr_lower for f in ["beta_last_", "correlation_last_", "unsystematic_risk", "systematic_risk"]):
            return "risk_beta"

        # v5.9: Detect expanded fundamental fields
        if any(f in expr_lower for f in [
            "retained_earnings", "working_capital", "inventory_turnover",
            "rd_expense", "sharesout", "operating_income", "return_assets",
            "return_equity", "goodwill", "fn_liab_fair_val",
        ]):
            return "expanded_fundamental"

        # v5.9: Detect analyst estimate fields (forward-looking)
        if any(f in expr_lower for f in ["est_eps", "est_fcf", "est_ptp", "est_cashflow_op", "est_capex"]):
            return "analyst_estimates"

        # v5.8: Detect multi-factor combinations (A + B pattern with different data sources)
        if " + " in expr_lower or ") + -" in expr_lower or ") + rank" in expr_lower:
            has_fundamental = any(f in expr_lower for f in [
                "debt", "equity", "assets", "sales", "cashflow", "ebitda", "bookvalue",
                "fscore_", "income", "eps", "enterprise_value",
            ])
            has_price = any(f in expr_lower for f in [
                "returns", "close", "vwap", "open", "high", "low",
            ])
            has_options = any(f in expr_lower for f in ["implied_volatility", "pcr_"])
            has_earnings = any(f in expr_lower for f in ["snt1_d1_", "consensus_analyst"])
            has_sentiment = any(f in expr_lower for f in ["scl12_", "news_", "rp_ess_", "rp_css_"])
            categories = sum([has_fundamental, has_price, has_options, has_earnings, has_sentiment])
            if categories >= 2:
                return "combo_factor"

        if any(f in expr_lower for f in ["implied_volatility", "historical_volatility", "pcr_"]):
            return "options_vol"
        if any(f in expr_lower for f in ["news_", "scl12_", "snt_social"]):
            return "sentiment"
        if any(f in expr_lower for f in ["est_", "consensus_analyst", "snt1_d1_"]):
            return "analyst_sentiment"
        if any(f in expr_lower for f in ["fscore_"]):
            return "fundamental_scores"
        if any(f in expr_lower for f in [
            "cashflow_op", "ebitda", "ebit", "enterprise_value",
            "bookvalue_ps", "current_ratio", "capex", "cogs",
        ]):
            return "fundamental_value"
        if any(f in expr_lower for f in ["eps", "debt", "equity", "assets", "sales", "income"]):
            return "size_value"
        if "trade_when" in expr_lower:
            return "conditional"
        if "group_rank" in expr_lower:
            return "cross_sectional"
        if "ts_corr" in expr_lower and "volume" in expr_lower:
            return "price_vol_corr"
        if "adv20" in expr_lower or ("cap" in expr_lower and "returns" in expr_lower):
            return "liquidity_scaled"
        if "volume" in expr_lower and "returns" in expr_lower:
            return "volume_flow"
        if "ts_std_dev" in expr_lower or "volatility" in expr_lower:
            return "volatility"
        if any(f in expr_lower for f in ["open", "high", "low", "vwap"]):
            return "intraday"
        if "returns" in expr_lower or "close" in expr_lower:
            return "cross_sectional"

        return "llm_novel"

    def mutate_candidate(self, row, metrics_hint: dict[str, Any] | None = None):
        family = row["family"]
        template_id = row["template_id"]
        reason = str(row["reason"]) if "reason" in row.keys() else ""

        # v6.1: Families not in TEMPLATE_LIBRARY (signal_combo, evolved, llm_*)
        # can't be mutated via template — use settings-only refinement
        if family not in TEMPLATE_LIBRARY:
            original_expr = row["canonical_expression"]
            settings = self._json_or_dict(row["settings_json"])
            settings = self._mutate_settings(settings, mode="settings_sweep", family=family)
            sim = SimulationSettings(**settings)
            canon = canonicalize_expression(original_expr)
            h = hash_candidate(canon, sim.to_dict())
            return Candidate.create(
                expression=original_expr,
                canonical_expression=canon,
                expression_hash=h,
                template_id=template_id,
                family=family,
                fields=self._extract_fields(original_expr, {}),
                params={},
                settings=sim,
            )

        template = next(
            (t for t in TEMPLATE_LIBRARY[family] if t["template_id"] == template_id),
            None,
        )
        if template is None:
            return self.generate_candidate()

        params = self._json_or_dict(row["params_json"])
        settings = self._json_or_dict(row["settings_json"])
        metrics_hint = metrics_hint or {}

        mode = self._refinement_mode(reason=reason, metrics_hint=metrics_hint)

        # Settings-only refinement: sub_universe_sharpe is a settings problem
        if mode == "sub_universe_sharpe":
            original_expr = row["canonical_expression"]
            settings = self._mutate_settings(settings, mode=mode, family=family)
            sim = SimulationSettings(**settings)
            canon = canonicalize_expression(original_expr)
            h = hash_candidate(canon, sim.to_dict())

            return Candidate.create(
                expression=original_expr,
                canonical_expression=canon,
                expression_hash=h,
                template_id=template_id,
                family=family,
                fields=self._extract_fields(original_expr, params),
                params=params,
                settings=sim,
            )

        # v5.9: Settings sweep — keep same expression, try DIFFERENT neutralization/universe
        # This is the "legendary refinement" for near-passers like Sharpe 1.34 / Fitness 0.71
        # The expression is good — it just needs the right settings context
        if mode == "settings_sweep":
            original_expr = row["canonical_expression"]
            settings = self._mutate_settings(settings, mode=mode, family=family)
            sim = SimulationSettings(**settings)
            canon = canonicalize_expression(original_expr)
            h = hash_candidate(canon, sim.to_dict())

            return Candidate.create(
                expression=original_expr,
                canonical_expression=canon,
                expression_hash=h,
                template_id=template_id,
                family=family,
                fields=self._extract_fields(original_expr, params),
                params=params,
                settings=sim,
            )

        # v5.5: CONCENTRATED_WEIGHT = expression-level problem, not settings-level
        # fscore/options data only covers certain stocks → rank() concentrates weight
        # Fix: multiply by rank(cap) or rank(adv20) to spread weight to liquid stocks
        if mode == "concentrated_weight":
            original_expr = row["canonical_expression"]
            modified_expr = self._apply_concentration_fix(original_expr)
            settings = self._mutate_settings(settings, mode=mode, family=family)
            sim = SimulationSettings(**settings)
            canon = canonicalize_expression(modified_expr)
            h = hash_candidate(canon, sim.to_dict())

            # Determine template_id for the modified expression
            modified_template_id = template_id
            if family == "fundamental_scores" and "ts_zscore" in modified_expr:
                if "* rank(adv20)" in modified_expr:
                    modified_template_id = "fs_05"
                elif modified_expr.endswith("* rank(cap))"):
                    # rank(ts_zscore(...) * rank(cap)) — cap INSIDE outer rank
                    modified_template_id = "fs_06"
                elif "* rank(cap)" in modified_expr:
                    # rank(ts_zscore(...)) * rank(cap) — cap OUTSIDE outer rank
                    modified_template_id = "fs_04"

            return Candidate.create(
                expression=modified_expr,
                canonical_expression=canon,
                expression_hash=h,
                template_id=modified_template_id,
                family=family,
                fields=self._extract_fields(modified_expr, params),
                params=params,
                settings=sim,
            )

        # Normal expression + settings refinement
        chosen_template = self._choose_refinement_template(
            family=family,
            template_id=template_id,
            mode=mode,
            metrics_hint=metrics_hint,
        )

        params = self._mutate_params_for_mode(params, chosen_template["expression"], mode, metrics_hint=metrics_hint)
        settings = self._mutate_settings(settings, mode=mode, family=family)

        expr, fields = self._render(chosen_template["expression"], params)
        expr, realized_template_id = self._apply_refinement_variants(
            expr=expr,
            family=family,
            template_id=chosen_template["template_id"],
            params=params,
            mode=mode,
            metrics_hint=metrics_hint,
        )
        final_template_id = realized_template_id or chosen_template["template_id"]
        expr = self._post_process(
            expr,
            family=family,
            template_id=final_template_id,
            light=True,
            force_smoothing=(mode in {"fitness", "turnover"}),
        )

        sim = SimulationSettings(**settings)
        canon = canonicalize_expression(expr)
        h = hash_candidate(canon, sim.to_dict())

        return Candidate.create(
            expression=expr,
            canonical_expression=canon,
            expression_hash=h,
            template_id=chosen_template["template_id"],
            family=family,
            fields=self._extract_fields(expr, params, fields),
            params=params,
            settings=sim,
        )

    # ============================
    # SAMPLING
    # ============================

    def _sample_family(self, bias):
        fams = list(TEMPLATE_LIBRARY.keys())
        if hasattr(config, "DEFAULT_FAMILY_ORDER"):
            ordered = [f for f in config.DEFAULT_FAMILY_ORDER if f in TEMPLATE_LIBRARY]
            fams = ordered + [f for f in fams if f not in ordered]

        # v7.0: Hard-block families that need data this bot doesn't have
        try:
            from datasets import get_blocked_families
            blocked = get_blocked_families()
        except Exception:
            blocked = set()

        config_weights = getattr(config, "FAMILY_BASE_WEIGHTS", {})
        weights = []

        for fam in fams:
            if fam in blocked:
                weights.append(0.0)
                continue
            base = DEFAULT_BASE_FAMILY_WEIGHTS.get(fam, 1.0)
            if fam in config_weights:
                base *= float(config_weights[fam])
            if bias:
                base *= bias.get(fam, 1.0)
            weights.append(max(base, 0.001))

        return self.rng.choices(fams, weights=weights, k=1)[0]

    def _sample_template(self, family, bias):
        templates = TEMPLATE_LIBRARY[family]
        base_boosts = getattr(config, "TEMPLATE_BASE_WEIGHTS", {})
        preferred_boosts = getattr(config, "PREFERRED_TEMPLATE_BOOSTS", {})
        penalties = getattr(config, "TEMPLATE_WEIGHT_PENALTIES", {})

        weights = []
        for t in templates:
            tid = t["template_id"]
            w = 1.0
            w *= base_boosts.get(tid, 1.0)
            w *= preferred_boosts.get(tid, 1.0)
            w *= penalties.get(tid, 1.0)
            if bias:
                w *= bias.get(tid, 1.0)
            weights.append(max(w, 0.001))

        return self.rng.choices(templates, weights=weights, k=1)[0]

    # ============================
    # REFINEMENT
    # ============================

    def _refinement_mode(self, reason: str, metrics_hint: dict[str, Any] | None) -> str:
        txt = (reason or "").upper()

        # v6.1: Eligible alpha optimization — always try different settings
        if "ELIGIBLE_OPTIMIZE" in txt:
            return "settings_sweep"

        if "CONCENTRATED_WEIGHT" in txt:
            return "concentrated_weight"
        if "LOW_SUB_UNIVERSE_SHARPE" in txt or "SUB_UNIVERSE" in txt:
            return "sub_universe_sharpe"

        # v5.9: Settings sweep — for near-passers, 40% chance of trying
        # a completely different neutralization/universe combo instead of
        # tweaking parameters. This is the single highest-impact refinement.
        if metrics_hint:
            sharpe = self._safe_float(metrics_hint.get("sharpe"))
            if sharpe is not None and sharpe >= 1.20 and self.rng.random() < 0.40:
                return "settings_sweep"

        if "LOW_FITNESS" in txt:
            return "fitness"
        if "HIGH_TURNOVER" in txt:
            return "turnover"
        if "LOW_SHARPE" in txt:
            return "sharpe"

        if metrics_hint:
            turnover = self._safe_float(metrics_hint.get("turnover"))
            sharpe = self._safe_float(metrics_hint.get("sharpe"))
            fitness = self._safe_float(metrics_hint.get("fitness"))

            if turnover is not None and turnover > 0.60:
                return "turnover"
            if fitness is not None and fitness < config.MIN_FITNESS:
                return "fitness"
            if sharpe is not None and sharpe < config.MIN_SHARPE:
                return "sharpe"

        return "general"

    def _choose_refinement_template(
        self,
        family: str,
        template_id: str,
        mode: str,
        metrics_hint: dict[str, Any] | None = None,
    ) -> dict[str, str]:
        templates = TEMPLATE_LIBRARY[family]
        current = next((t for t in templates if t["template_id"] == template_id), templates[0])

        elite_templates = set(getattr(config, "ELITE_TEMPLATES", set()))
        stay_prob = 0.0
        if current["template_id"] in elite_templates:
            if mode == "turnover":
                stay_prob = getattr(config, "REFINEMENT_ELITE_TURNOVER_STAY_PROB", 0.97)
            elif mode == "fitness":
                stay_prob = getattr(config, "REFINEMENT_ELITE_FITNESS_STAY_PROB", 0.95)
            elif mode == "sharpe":
                stay_prob = getattr(config, "REFINEMENT_ELITE_SHARPE_STAY_PROB", 0.82)
            else:
                stay_prob = getattr(config, "REFINEMENT_ELITE_STAY_PROB", 0.90)

            sharpe = self._safe_float((metrics_hint or {}).get("sharpe"))
            fitness = self._safe_float((metrics_hint or {}).get("fitness"))
            turnover = self._safe_float((metrics_hint or {}).get("turnover"))

            if mode == "fitness" and turnover is not None and turnover <= config.MAX_TURNOVER:
                stay_prob = max(stay_prob, 0.97)
            if mode == "sharpe" and fitness is not None and fitness >= config.MIN_FITNESS:
                stay_prob = max(stay_prob, 0.92)
            if sharpe is not None and sharpe >= 1.45 and current["template_id"] in elite_templates:
                stay_prob = max(stay_prob, 0.94)

            if self.rng.random() < stay_prob:
                return current

        sisters = [t for t in templates if t["template_id"] != template_id]
        if not sisters:
            return current

        switch_prob = getattr(config, "REFINEMENT_TEMPLATE_SWITCH_PROB", 0.12)
        if current["template_id"] in elite_templates:
            if mode in {"fitness", "turnover"}:
                switch_prob *= 0.45
            elif mode == "sharpe":
                switch_prob *= 0.70

        if self.rng.random() >= switch_prob:
            return current

        filtered = []
        weights = []
        disabled = set(getattr(config, "DISABLED_REFINEMENT_TEMPLATES", set()))

        for t in sisters:
            tid = t["template_id"]

            if tid in disabled:
                continue

            if tid in getattr(config, "SOFT_BLOCK_REFINEMENT_TEMPLATES", set()):
                if self.rng.random() > getattr(config, "SOFT_BLOCK_REFINEMENT_PROB", 0.08):
                    continue

            w = 1.0

            if family == "mean_reversion":
                if current["template_id"] == "mr_04":
                    if tid == "mr_01":
                        w = 0.92
                    elif tid == "mr_02":
                        w = 0.58
                    elif tid == "mr_03":
                        w = 0.68
                elif current["template_id"] in {"mr_01", "mr_02", "mr_03"} and tid == "mr_04":
                    w = 1.50
                elif tid == "mr_04":
                    w = 1.25
            elif family == "conditional":
                if current["template_id"] == "cond_01":
                    if tid == "cond_02":
                        w = 0.20
                    elif tid == "cond_03":
                        w = 0.02
                elif tid == "cond_01":
                    w = 1.35
            elif family == "volume_flow":
                if tid == "vol_03":
                    w = 1.28
                elif tid == "vol_01":
                    w = 0.78
                elif tid == "vol_02":
                    w = 0.00
            elif family == "vol_adjusted":
                if tid == "va_02":
                    w = 1.30
                elif tid == "va_01":
                    w = 0.10

            if mode == "fitness":
                if tid in {"mr_04", "cond_01", "vol_03", "va_02"}:
                    w *= 1.15
            elif mode == "turnover":
                if tid in {"mr_04", "cond_01", "va_02"}:
                    w *= 1.12
            elif mode == "sharpe":
                if tid in {"mr_04", "cond_01", "vol_03"}:
                    w *= 1.08

            filtered.append(t)
            weights.append(max(w, 0.001))

        if not filtered:
            return current

        return self.rng.choices(filtered, weights=weights, k=1)[0]

    # ============================
    # POST PROCESSING
    # ============================

    def _post_process(self, expr, family=None, template_id=None, light=False, force_smoothing: bool = False):
        if expr.count("rank(") >= 3 or expr.count("ts_mean(") >= 3 or expr.count("ts_decay_linear(") >= 2:
            return expr

        # v5.9.1 + v6.1: Signal-aware smoothing — complex templates get light processing only
        light_templates = {
            "m7c_05", "m7c_06", "m7c_07", "m7c_08", "m7c_09", "m7c_10", "m7c_11",
            "m7c_12", "m7c_13", "m7c_14", "m7c_15",
            # v6.1: Raw multiplicative and group_rank templates — already complex
            "m7c_16", "m7c_17", "m7c_18", "m7c_19", "m7c_20", "m7c_21", "m7c_22",
            "m7c_23", "m7c_24", "m7c_25", "m7c_26",
            "cf_11", "cf_12", "cf_13", "cf_14", "cf_15",
        }
        if template_id and template_id in light_templates:
            force_smoothing = False
            light = True  # Only light smoothing for complex expressions

        # Family-specific smoothing parameters
        if family in {"model77_anomaly", "model77_combo", "expanded_fundamental", "fundamental_value", "quality_trend", "size_value"}:
            # Fundamentals: longer windows, ts_decay_linear preferred
            smooth_windows = [5, 8, 10]
            decay_prob = 0.60  # Prefer ts_decay_linear for fundamentals
        elif family in {"news_sentiment"}:
            # News: short windows only (signal decays fast)
            smooth_windows = [3, 5]
            decay_prob = 0.70  # Strong preference for ts_decay_linear
        elif family in {"relationship"}:
            # Customer momentum: longer windows (persists for weeks)
            smooth_windows = [5, 10, 15]
            decay_prob = 0.65
        elif family in {"analyst_estimates", "earnings_momentum"}:
            # Analyst: medium windows, slight decay preference
            smooth_windows = [5, 10]
            decay_prob = 0.55
        else:
            # Price-based: default
            smooth_windows = [3, 5, 10]
            decay_prob = 0.40

        if force_smoothing:
            smoothing_prob = 0.78
        elif light:
            smoothing_prob = getattr(config, "LIGHT_POST_PROCESS_SMOOTH_PROB", 0.30)
        else:
            smoothing_prob = getattr(config, "FRESH_FORCE_SMOOTH_PROB", 0.72)

        already_smoothed = expr.startswith("ts_mean(") or expr.startswith("ts_decay_linear(")
        if self.rng.random() < smoothing_prob and not already_smoothed:
            if force_smoothing:
                win = self.rng.choice([5, 8, 10, 10])
                if self.rng.random() < 0.55:
                    expr = f"ts_decay_linear(rank({expr}), {win})"
                else:
                    expr = f"ts_mean(rank({expr}), {win})"
            else:
                win = self.rng.choice(smooth_windows)
                roll = self.rng.random()
                if roll < decay_prob:
                    expr = f"ts_decay_linear(rank({expr}), {win})"
                elif roll < decay_prob + 0.08:
                    # v6.1: rank(rank(X)) — double ranking, normalizes extreme distributions
                    # Shows up in several near-passers from logs
                    expr = f"rank(rank({expr}))"
                elif roll < decay_prob + 0.13:
                    # v6.1: winsorize — cap outliers, improves fitness (used in wp_04, our best proven template)
                    expr = f"winsorize(rank({expr}), std=4)"
                else:
                    expr = f"ts_mean(rank({expr}), {win})"

        rank_prob = getattr(config, "FRESH_RAW_RANK_PROB", 0.02) if not light else 0.05
        if self.rng.random() < rank_prob and not expr.startswith("rank("):
            expr = f"rank({expr})"

        return expr

    # ============================
    # PARAMS
    # ============================

    def _grid(self, key):
        desired = [3, 5, 10, 20, 40, 60]
        allowed = SAFE_PARAM_RANGES.get(key, desired)
        return [x for x in desired if x in allowed] or list(allowed)

    def _sample_params(self, template, family=None):
        p = {}
        # v5.7: Use fundamental param ranges for families with quarterly data
        fundamental_families = {"fundamental_value", "quality_trend", "size_value"}
        use_fund_params = family in fundamental_families

        # v5.8: combo_factor uses MIXED params — {n} for fundamental (moderate lookback), {m} for price (short)
        is_combo = family == "combo_factor"

        if "{n}" in template:
            if is_combo:
                # Fundamental component lookback (Griff used 30-40)
                p["n"] = self.rng.choice([20, 30, 40, 60])
            elif use_fund_params:
                p["n"] = self.rng.choice(getattr(templates_mod, "FUNDAMENTAL_PARAM_RANGES", {}).get("n", self._grid("n")))
            else:
                p["n"] = self.rng.choice(self._grid("n"))
        if "{m}" in template:
            if is_combo:
                # Price reversion component (Griff used 3-5)
                p["m"] = self.rng.choice([3, 5, 10])
            elif use_fund_params:
                p["m"] = self.rng.choice(getattr(templates_mod, "FUNDAMENTAL_PARAM_RANGES", {}).get("m", self._grid("m")))
            else:
                p["m"] = self.rng.choice(self._grid("m"))
            # Ensure m ≠ n (fixes cs_03: ts_mean(returns, m) - ts_mean(returns, n) = 0 when m==n)
            if "n" in p and p["m"] == p["n"]:
                grid = getattr(templates_mod, "FUNDAMENTAL_PARAM_RANGES", {}).get("m", self._grid("m")) if use_fund_params else self._grid("m")
                alternatives = [v for v in grid if v != p["n"]]
                if alternatives:
                    p["m"] = self.rng.choice(alternatives)
        if "{field}" in template:
            p["field"] = self.rng.choice(FUNDAMENTAL_FIELDS)
        if "{deep_field}" in template:
            p["deep_field"] = self.rng.choice(DEEP_FUNDAMENTAL_FIELDS)
        if "{analyst_field}" in template:
            p["analyst_field"] = self.rng.choice(ANALYST_FIELDS)
        if "{sentiment_field}" in template:
            p["sentiment_field"] = self.rng.choice(SENTIMENT_FIELDS)
        if "{opt_window}" in template:
            p["opt_window"] = self.rng.choice(OPTIONS_WINDOWS)
        if "{fscore_field}" in template:
            p["fscore_field"] = self.rng.choice(FSCORE_FIELDS)
        # v5.7: New field types
        if "{derivative_field}" in template:
            p["derivative_field"] = self.rng.choice(DERIVATIVE_FIELDS)
        if "{pcr_window}" in template:
            p["pcr_window"] = self.rng.choice(PCR_WINDOWS)
        # v5.9: model77 field sampling with tier weighting
        if "{model77_field}" in template:
            _m77_roll = self.rng.random()
            if _m77_roll < 0.70 and MODEL77_TIER1_FIELDS:
                p["model77_field"] = self.rng.choice(MODEL77_TIER1_FIELDS)
            elif _m77_roll < 0.95 or not MODEL77_TIER3_FIELDS:
                p["model77_field"] = self.rng.choice(MODEL77_TIER2_FIELDS if MODEL77_TIER2_FIELDS else MODEL77_ALL_FIELDS)
            else:
                p["model77_field"] = self.rng.choice(MODEL77_TIER3_FIELDS)
        # v7.1: New signal dimension field pools
        if "{news_event_field}" in template:
            if NEWS_EVENT_FIELDS:
                p["news_event_field"] = self.rng.choice(NEWS_EVENT_FIELDS)
        if "{rp_field}" in template:
            if RP_UNDERUSED_FIELDS:
                p["rp_field"] = self.rng.choice(RP_UNDERUSED_FIELDS)
        return p

    def _mutate_params_for_mode(self, params, template, mode: str, metrics_hint: dict[str, Any] | None = None):
        out = dict(params)
        grid_n = self._grid("n")
        grid_m = self._grid("m")

        if "n" in out:
            if mode == "turnover":
                out["n"] = self._push_param_wider(out["n"], grid_n)
            elif mode == "fitness":
                # KEY INSIGHT from live data: shorter n + longer smoothing = better fitness
                # pushing n wider kills sharpe without reliably improving fitness
                # instead: mutate freely with slight bias toward staying or going shorter
                turnover = self._safe_float((metrics_hint or {}).get("turnover"))
                if turnover is not None and turnover > 0.55:
                    # high turnover: wider n is justified here
                    out["n"] = self._push_param_wider(out["n"], grid_n)
                else:
                    # normal/low turnover: keep n tight, let smoothing handle fitness
                    if self.rng.random() < 0.35:
                        out["n"] = self._push_param_narrower(out["n"], grid_n)
                    else:
                        out["n"] = self._mutate(out["n"], grid_n, stay_prob=0.30)
            elif mode == "sharpe":
                out["n"] = self._mutate(out["n"], grid_n, stay_prob=0.10)
            else:
                out["n"] = self._mutate(out["n"], grid_n, stay_prob=0.25)

        if "{n}" in template and "n" not in out:
            out["n"] = self.rng.choice(grid_n)

        if "m" in out:
            if mode == "turnover":
                out["m"] = self._push_param_wider(out["m"], grid_m)
            elif mode == "fitness":
                out["m"] = self._push_param_wider(out["m"], grid_m) if self.rng.random() < 0.65 else self._mutate(out["m"], grid_m, stay_prob=0.10)
            else:
                out["m"] = self._mutate(out["m"], grid_m, stay_prob=0.20)

        if "{m}" in template and "m" not in out:
            out["m"] = self.rng.choice(grid_m)

        if "field" in out and self.rng.random() < 0.12:
            out["field"] = self.rng.choice(FUNDAMENTAL_FIELDS)

        if "{field}" in template and "field" not in out:
            out["field"] = self.rng.choice(FUNDAMENTAL_FIELDS)

        if "deep_field" in out and self.rng.random() < 0.15:
            out["deep_field"] = self.rng.choice(DEEP_FUNDAMENTAL_FIELDS)

        if "{deep_field}" in template and "deep_field" not in out:
            out["deep_field"] = self.rng.choice(DEEP_FUNDAMENTAL_FIELDS)

        if "analyst_field" in out and self.rng.random() < 0.15:
            out["analyst_field"] = self.rng.choice(ANALYST_FIELDS)

        if "{analyst_field}" in template and "analyst_field" not in out:
            out["analyst_field"] = self.rng.choice(ANALYST_FIELDS)

        if "sentiment_field" in out and self.rng.random() < 0.15:
            out["sentiment_field"] = self.rng.choice(SENTIMENT_FIELDS)

        if "{sentiment_field}" in template and "sentiment_field" not in out:
            out["sentiment_field"] = self.rng.choice(SENTIMENT_FIELDS)

        if "opt_window" in out and self.rng.random() < 0.15:
            out["opt_window"] = self.rng.choice(OPTIONS_WINDOWS)

        if "{opt_window}" in template and "opt_window" not in out:
            out["opt_window"] = self.rng.choice(OPTIONS_WINDOWS)

        if "fscore_field" in out and self.rng.random() < 0.15:
            out["fscore_field"] = self.rng.choice(FSCORE_FIELDS)

        if "{fscore_field}" in template and "fscore_field" not in out:
            out["fscore_field"] = self.rng.choice(FSCORE_FIELDS)

        # v5.9: model77 field mutation
        if "model77_field" in out and self.rng.random() < 0.20:
            # Mutate to a different model77 field (higher mutation rate — huge field space)
            _m77_roll = self.rng.random()
            if _m77_roll < 0.70 and MODEL77_TIER1_FIELDS:
                out["model77_field"] = self.rng.choice(MODEL77_TIER1_FIELDS)
            elif _m77_roll < 0.95 or not MODEL77_TIER3_FIELDS:
                out["model77_field"] = self.rng.choice(MODEL77_TIER2_FIELDS if MODEL77_TIER2_FIELDS else MODEL77_ALL_FIELDS)
            else:
                out["model77_field"] = self.rng.choice(MODEL77_TIER3_FIELDS)

        if "{model77_field}" in template and "model77_field" not in out:
            _m77_roll = self.rng.random()
            if _m77_roll < 0.70 and MODEL77_TIER1_FIELDS:
                out["model77_field"] = self.rng.choice(MODEL77_TIER1_FIELDS)
            elif _m77_roll < 0.95 or not MODEL77_TIER3_FIELDS:
                out["model77_field"] = self.rng.choice(MODEL77_TIER2_FIELDS if MODEL77_TIER2_FIELDS else MODEL77_ALL_FIELDS)
            else:
                out["model77_field"] = self.rng.choice(MODEL77_TIER3_FIELDS)

        # v7.1: New signal dimension field mutation/filling
        if "news_event_field" in out and self.rng.random() < 0.20:
            if NEWS_EVENT_FIELDS:
                out["news_event_field"] = self.rng.choice(NEWS_EVENT_FIELDS)
        if "{news_event_field}" in template and "news_event_field" not in out:
            if NEWS_EVENT_FIELDS:
                out["news_event_field"] = self.rng.choice(NEWS_EVENT_FIELDS)

        if "rp_field" in out and self.rng.random() < 0.20:
            if RP_UNDERUSED_FIELDS:
                out["rp_field"] = self.rng.choice(RP_UNDERUSED_FIELDS)
        if "{rp_field}" in template and "rp_field" not in out:
            if RP_UNDERUSED_FIELDS:
                out["rp_field"] = self.rng.choice(RP_UNDERUSED_FIELDS)

        return out

    def _mutate(self, val, grid, stay_prob: float = 0.35):
        if val not in grid:
            return self.rng.choice(grid)

        if self.rng.random() < stay_prob:
            return val

        i = grid.index(val)
        choices = [val]
        if i > 0:
            choices.append(grid[i - 1])
        if i < len(grid) - 1:
            choices.append(grid[i + 1])

        if self.rng.random() < 0.25 and i > 1:
            choices.append(grid[i - 2])
        if self.rng.random() < 0.25 and i < len(grid) - 2:
            choices.append(grid[i + 2])

        return self.rng.choice(choices)

    def _push_param_wider(self, val, grid=None):
        grid = grid or [3, 5, 10, 20, 40, 60]
        if val not in grid:
            return self.rng.choice(grid)
        i = grid.index(val)
        if i >= len(grid) - 1:
            return val
        if i == len(grid) - 2:
            return grid[-1]
        return self.rng.choice(grid[i + 1 : min(len(grid), i + 3)])

    def _push_param_narrower(self, val, grid=None):
        grid = grid or [3, 5, 10, 20, 40, 60]
        if val not in grid:
            return self.rng.choice(grid)
        i = grid.index(val)
        if i <= 0:
            return val
        lo = max(0, i - 2)
        return self.rng.choice(grid[lo:i])

    # ============================
    # SETTINGS
    # ============================

    def _mutate_settings(self, s, mode: str = "general", family: str = ""):
        out = dict(s)

        # v5.9: Settings sweep — systematically try ALL neutralization × universe combos
        # Triggered for high-Sharpe near-passers (e.g., Sharpe 1.34 / Fitness 0.71)
        # This is the "legendary refinement" — the single highest-impact settings change
        if mode == "settings_sweep":
            # ALWAYS change neutralization to something DIFFERENT from current
            current_neut = out.get("neutralization", "")
            # Use dataset-aware recommendations first, then all options
            neut_pool = DATASET_NEUTRALIZATION.get(family, config.DEFAULT_NEUTRALIZATIONS)
            neut_options = [n for n in neut_pool if n != current_neut]
            if not neut_options:
                neut_options = [n for n in config.DEFAULT_NEUTRALIZATIONS if n != current_neut]
            if neut_options:
                out["neutralization"] = self.rng.choice(neut_options)

            # Also try a different universe (50% of the time)
            if self.rng.random() < 0.50:
                current_univ = out.get("universe", "")
                univ_options = [u for u in config.DEFAULT_UNIVERSES if u != current_univ]
                if univ_options:
                    out["universe"] = self.rng.choice(univ_options)

            # Also try a different decay (40% of the time)
            if self.rng.random() < 0.40:
                decay_grid = list(config.DEFAULT_DECAYS)
                out["decay"] = self.rng.choice(decay_grid)

            return out

        if "decay" in out:
            decay_grid = list(config.DEFAULT_DECAYS)
            if family == "volume_flow" and mode in {"fitness", "turnover"}:
                high_decay = [d for d in decay_grid if d >= 8] or decay_grid[-2:]
                out["decay"] = self.rng.choice(high_decay)
            elif mode in {"fitness", "turnover"}:
                out["decay"] = self._push_param_wider(out["decay"], decay_grid)
            elif mode == "sharpe":
                out["decay"] = self._mutate(out["decay"], decay_grid, stay_prob=0.10)
            else:
                out["decay"] = self._mutate(out["decay"], decay_grid, stay_prob=0.25)

        # v5.9: Dataset-aware neutralization during refinement
        neut_prob = 0.40 if mode == "fitness" else (0.25 if mode == "turnover" else 0.18)
        if mode == "concentrated_weight":
            neut_prob = 0.50
        if "neutralization" in out and self.rng.random() < neut_prob:
            if family in DATASET_NEUTRALIZATION and self.rng.random() < 0.70:
                out["neutralization"] = self.rng.choice(DATASET_NEUTRALIZATION[family])
            else:
                out["neutralization"] = self.rng.choice(config.DEFAULT_NEUTRALIZATIONS)

        # Truncation — CRITICAL for concentrated_weight failures
        if mode == "concentrated_weight":
            out["truncation"] = self.rng.choice([0.03, 0.05])
        else:
            trunc_prob = 0.30 if mode in {"fitness", "turnover"} else 0.10
            if "truncation" in out and self.rng.random() < trunc_prob:
                out["truncation"] = self.rng.choice(config.DEFAULT_TRUNCATIONS)

        # Universe — CRITICAL for sub_universe_sharpe failures
        if mode == "sub_universe_sharpe":
            sub_universe_options = ["TOP1000", "TOP500", "TOPSP500"]
            out["universe"] = self.rng.choice(sub_universe_options)
        else:
            universe_prob = 0.12 if mode == "fitness" else 0.05
            if "universe" in out and self.rng.random() < universe_prob:
                out["universe"] = self.rng.choice(config.DEFAULT_UNIVERSES)

        return out

    def _sample_settings(self, family, settings_bias=None):
        """
        v5.9: Dataset-aware settings sampling.

        Uses DATASET_NEUTRALIZATION from official WQ Neutralisation.csv (75% of time).
        Falls back to adaptive sampling for exploration (25%).
        """
        EXPLORE_PROB = 0.20
        FLOOR_WEIGHT = 0.25
        CEILING_WEIGHT = 2.50

        def weighted_choice(options, dimension_bias):
            """Pick from options using bias weights, with exploration."""
            if self.rng.random() < EXPLORE_PROB or not dimension_bias:
                return self.rng.choice(options)

            weights = []
            for opt in options:
                key = str(opt)
                w = dimension_bias.get(key, 1.0)
                w = max(FLOOR_WEIGHT, min(CEILING_WEIGHT, w))
                weights.append(w)

            return self.rng.choices(options, weights=weights, k=1)[0]

        # v5.7: Use signal-class settings profile if available
        profile = getattr(config, "SIGNAL_CLASS_SETTINGS", {}).get(family)
        if profile and self.rng.random() > 0.15:  # 85% use profile, 15% explore
            # v5.9: Override neutralization with dataset-aware recommendation
            neut_options = DATASET_NEUTRALIZATION.get(family, profile.get("neutralizations", config.DEFAULT_NEUTRALIZATIONS))
            return SimulationSettings(
                region=config.DEFAULT_REGION,
                universe=self.rng.choice(profile.get("universes", config.DEFAULT_UNIVERSES)),
                delay=config.DEFAULT_DELAY,
                decay=self.rng.choice(profile.get("decays", config.DEFAULT_DECAYS)),
                neutralization=self.rng.choice(neut_options),
                truncation=self.rng.choice(profile.get("truncations", config.DEFAULT_TRUNCATIONS)),
            )

        bias = settings_bias or {}

        # v5.9: Dataset-aware neutralization — 75% use recommended, 25% explore
        if family in DATASET_NEUTRALIZATION and self.rng.random() < 0.75:
            neut = self.rng.choice(DATASET_NEUTRALIZATION[family])
        else:
            neut = weighted_choice(
                config.DEFAULT_NEUTRALIZATIONS,
                bias.get("neutralization"),
            )

        return SimulationSettings(
            region=config.DEFAULT_REGION,
            universe=weighted_choice(
                config.DEFAULT_UNIVERSES,
                bias.get("universe"),
            ),
            delay=config.DEFAULT_DELAY,
            decay=weighted_choice(
                config.DEFAULT_DECAYS,
                bias.get("decay"),
            ),
            neutralization=neut,
            truncation=weighted_choice(
                config.DEFAULT_TRUNCATIONS,
                bias.get("truncation"),
            ),
            pasteurization=config.DEFAULT_PASTEURIZATION,
            unit_handling=config.DEFAULT_UNIT_HANDLING,
            nan_handling=config.DEFAULT_NAN_HANDLING,
            max_stock_weight=config.DEFAULT_MAX_STOCK_WEIGHT,
            language=config.DEFAULT_LANGUAGE,
        )

    # ============================
    # RENDER / VARIANTS
    # ============================

    def _classify_expression_template(self, family: str, expr: str, fallback_template_id: str) -> str:
        expr_no_space = expr.replace(" ", "")
        if family == "volume_flow":
            if "*-returns" in expr_no_space or "*-returns)" in expr_no_space or "*-returns," in expr_no_space or "*-returns" in expr_no_space:
                return "vol_03"
            if "ts_delta(volume" in expr:
                return "vol_02"
            if "volume/ts_mean(volume" in expr_no_space:
                return "vol_01"
        if family == "mean_reversion":
            if "returns-ts_mean(returns" in expr_no_space:
                return "mr_04"
            if "close/ts_mean(close" in expr_no_space:
                return "mr_03"
            if "-ts_delta(close" in expr_no_space:
                return "mr_02"
            if "ts_mean(close" in expr_no_space and "-close" in expr_no_space:
                return "mr_01"
        if family == "vol_adjusted":
            if "ts_mean(close" in expr_no_space and "/ts_std_dev(returns" in expr_no_space:
                return "va_02"
            if "ts_delta(close" in expr_no_space and "/ts_std_dev(returns" in expr_no_space:
                return "va_01"
        if family == "conditional":
            if "rank(-returns)" in expr and "volume > ts_mean(volume" in expr:
                return "cond_01"
            if "abs(returns) > ts_std_dev(returns" in expr:
                return "cond_02"
            if "rank(ts_delta(close" in expr and "volume > ts_mean(volume" in expr:
                return "cond_03"
        return fallback_template_id

    def _apply_refinement_variants(
        self,
        *,
        expr: str,
        family: str,
        template_id: str,
        params: dict[str, Any],
        mode: str,
        metrics_hint: dict[str, Any] | None = None,
    ) -> tuple[str, str | None]:
        metrics_hint = metrics_hint or {}
        n = params.get("n", 10)
        m = params.get("m", 10)
        wider_n = self._push_param_wider(n)
        wider_m = self._push_param_wider(m, self._grid("m"))
        narrower_n = self._push_param_narrower(n)
        narrower_m = self._push_param_narrower(m, self._grid("m"))

        candidates: list[str] = [expr]
        weights: list[float] = [1.0]

        def add(candidate: str, weight: float = 1.0):
            candidates.append(candidate)
            weights.append(weight)

        if family == "mean_reversion":
            if mode == "fitness":
                # LIVE DATA INSIGHT: eligible alpha was ts_decay_linear(..., n=5, smooth=10)
                # near-passers at fitness=0.990 were n=10, smooth=5
                # wider_n (20/40/60) consistently kills sharpe → deprioritise
                add(f"ts_decay_linear(rank(rank(-(returns - ts_mean(returns, {n})))), 10)", 1.50)
                add(f"ts_decay_linear(rank(rank(-(returns - ts_mean(returns, {narrower_n})))), 10)", 1.45)
                add(f"ts_decay_linear(rank(rank(-(returns - ts_mean(returns, {n})))), 8)", 1.35)
                add(f"ts_mean(rank(rank(-(returns - ts_mean(returns, {n})))), 10)", 1.30)
                add(f"ts_mean(rank(rank(-(returns - ts_mean(returns, {narrower_n})))), 10)", 1.25)
                add(f"ts_decay_linear(rank(rank(ts_mean(close, {n}) - close)), 10)", 1.20)
                add(f"ts_mean(rank(rank(-(returns - ts_mean(returns, {wider_n})))), 10)", 0.80)
                add(f"rank(ts_mean(rank(rank(-(returns - ts_mean(returns, {n})))), 5))", 0.90)
            elif mode == "turnover":
                add(f"ts_mean(rank(rank(-(returns - ts_mean(returns, {wider_n})))), 10)", 1.35)
                add(f"ts_decay_linear(rank(rank(-(returns - ts_mean(returns, {wider_n})))), 10)", 1.40)
                add(f"ts_mean(rank(rank(-(close / ts_mean(close, {wider_n}) - 1))), 5)", 1.15)
                add(f"ts_decay_linear(rank(rank(-ts_delta(close, {wider_n}))), 10)", 1.10)
                add(f"ts_mean(rank(rank(-ts_delta(close, {wider_n}))), 10)", 1.05)
            elif mode == "sharpe":
                add(f"ts_mean(rank(rank(-(returns - ts_mean(returns, {n})))), 3)", 1.35)
                add(f"ts_decay_linear(rank(rank(-(returns - ts_mean(returns, {n})))), 3)", 1.30)
                add(f"ts_mean(rank(rank(ts_mean(close, {n}) - close)), 3)", 1.20)
                add(f"rank(ts_mean(rank(rank(-ts_delta(close, {n}))), 3))", 1.00)
                add(f"ts_mean(rank(rank(-(returns - ts_mean(returns, {narrower_n})))), 3)", 0.95)
            else:
                add(f"ts_mean(rank(rank(-(returns - ts_mean(returns, {n})))), 5)", 1.10)
                add(f"ts_decay_linear(rank(rank(-(returns - ts_mean(returns, {n})))), 5)", 1.15)
                add(f"ts_mean(rank(rank(ts_mean(close, {n}) - close)), 5)", 1.00)

        elif family == "conditional":
            cond_volume = f"volume > ts_mean(volume, {n})"
            cond_tight = f"volume > ts_mean(volume, {n}) * 1.1"
            cond_abs = f"abs(returns) > ts_std_dev(returns, {n})"
            base_sig = "rank(-returns)"
            if mode == "fitness":
                add(f"trade_when({cond_tight}, {base_sig}, -1)", 1.15)
                add(f"ts_mean(rank(trade_when({cond_tight}, {base_sig}, -1)), 5)", 1.30)
                add(f"ts_decay_linear(rank(trade_when({cond_tight}, {base_sig}, -1)), 5)", 1.35)
                add(f"trade_when({cond_abs}, {base_sig}, -1)", 1.00)
            elif mode == "turnover":
                cond_wider = f"volume > ts_mean(volume, {wider_n}) * 1.1"
                add(f"trade_when({cond_wider}, {base_sig}, -1)", 1.20)
                add(f"ts_mean(rank(trade_when({cond_wider}, {base_sig}, -1)), 10)", 1.35)
                add(f"ts_decay_linear(rank(trade_when({cond_wider}, {base_sig}, -1)), 10)", 1.40)
            elif mode == "sharpe":
                add(f"ts_mean(rank(trade_when({cond_volume}, {base_sig}, -1)), 3)", 1.30)
                add(f"ts_decay_linear(rank(trade_when({cond_volume}, {base_sig}, -1)), 3)", 1.25)
                add(f"trade_when({cond_volume}, rank(-ts_delta(close, {n})), -1)", 1.00)
                add(f"trade_when({cond_abs}, {base_sig}, -1)", 0.95)
            else:
                add(f"ts_mean(rank(trade_when({cond_volume}, {base_sig}, -1)), 5)", 1.10)
                add(f"ts_decay_linear(rank(trade_when({cond_volume}, {base_sig}, -1)), 5)", 1.15)

        elif family == "volume_flow":
            if mode == "fitness":
                add(f"ts_mean(rank(rank((volume / ts_mean(volume, {wider_n})) * -returns)), 10)", 1.35)
                add(f"ts_decay_linear(rank(rank((volume / ts_mean(volume, {wider_n})) * -returns)), 10)", 1.40)
                add(f"rank(ts_mean(rank((volume / ts_mean(volume, {n})) * -returns), 5))", 1.05)
                add(f"ts_mean(rank(rank((volume / ts_mean(volume, {wider_n})) * -returns)), 5)", 1.10)
                add(f"ts_decay_linear(rank(rank((volume / ts_mean(volume, {wider_n})) * -returns)), 5)", 1.15)
            elif mode == "turnover":
                add(f"ts_mean(rank(rank((volume / ts_mean(volume, {wider_n})) * -returns)), 10)", 1.35)
                add(f"ts_decay_linear(rank(rank((volume / ts_mean(volume, {wider_n})) * -returns)), 10)", 1.45)
                add(f"trade_when(abs(returns) > ts_std_dev(returns, {wider_n}), rank((volume / ts_mean(volume, {wider_n})) * -returns), -1)", 1.10)
            elif mode == "sharpe":
                add(f"ts_mean(rank(rank((volume / ts_mean(volume, {n})) * -returns)), 3)", 1.30)
                add(f"ts_decay_linear(rank(rank((volume / ts_mean(volume, {n})) * -returns)), 3)", 1.25)
                add(f"rank(ts_mean(rank((volume / ts_mean(volume, {n})) * -returns), 3))", 1.05)
                add(f"ts_mean(rank(rank((volume / ts_mean(volume, {narrower_n})) * -returns)), 3)", 0.90)
            else:
                add(f"ts_mean(rank(rank((volume / ts_mean(volume, {n})) * -returns)), 5)", 1.10)
                add(f"ts_decay_linear(rank(rank((volume / ts_mean(volume, {n})) * -returns)), 5)", 1.15)

        elif family == "vol_adjusted":
            if mode == "fitness":
                add(f"ts_mean(rank(rank((ts_mean(close, {wider_n}) - close) / ts_std_dev(returns, {wider_m}))), 5)", 1.30)
                add(f"ts_decay_linear(rank(rank((ts_mean(close, {wider_n}) - close) / ts_std_dev(returns, {wider_m}))), 5)", 1.35)
                add(f"rank(ts_mean(rank((ts_mean(close, {wider_n}) - close) / ts_std_dev(returns, {wider_m})), 5))", 1.00)
            elif mode == "turnover":
                add(f"ts_mean(rank(rank((ts_mean(close, {wider_n}) - close) / ts_std_dev(returns, {wider_m}))), 10)", 1.35)
                add(f"ts_decay_linear(rank(rank((ts_mean(close, {wider_n}) - close) / ts_std_dev(returns, {wider_m}))), 10)", 1.40)
            elif mode == "sharpe":
                add(f"ts_mean(rank(rank((ts_mean(close, {n}) - close) / ts_std_dev(returns, {m}))), 3)", 1.30)
                add(f"rank(ts_mean(rank((ts_mean(close, {narrower_n}) - close) / ts_std_dev(returns, {narrower_m})), 3))", 1.00)
            else:
                add(f"ts_mean(rank(rank((ts_mean(close, {n}) - close) / ts_std_dev(returns, {m}))), 5)", 1.10)

        elif family == "fundamental":
            field = params.get("field", "sales")
            add(f"rank({field})", 1.10)
            add(f"rank(ts_delta({field}, {n}))", 1.00)
            add(f"rank(({field} - ts_mean({field}, {n})))", 1.00)

        deduped, deduped_weights = self._dedupe_weighted(candidates, weights)
        simple_candidates = []
        simple_weights = []
        for candidate, weight in zip(deduped, deduped_weights):
            smooth_count = candidate.count("ts_mean(") + candidate.count("ts_decay_linear(")
            if smooth_count <= 2 and candidate.count("rank(") <= 4:
                simple_candidates.append(candidate)
                simple_weights.append(weight)

        pool = simple_candidates or deduped
        pool_weights = simple_weights or deduped_weights
        chosen = self.rng.choices(pool, weights=pool_weights, k=1)[0]
        realized_template_id = self._classify_expression_template(family=family, expr=chosen, fallback_template_id=template_id)
        return chosen, realized_template_id

    def _render(self, template, params):
        expr = template.format(**params)
        fields = self._extract_fields(expr, params)
        return expr, fields

    def _extract_fields(self, expr: str, params: dict[str, Any], existing_fields: list[str] | None = None):
        fields = list(existing_fields or [])

        # Extract named params
        for key in ["field", "deep_field", "analyst_field", "sentiment_field", "fscore_field"]:
            if key in params and params[key] not in fields:
                fields.append(params[key])

        # Detect known fields in expression text
        known_fields = [
            "close", "returns", "volume", "cap", "assets", "sales", "income", "cash",
            "open", "high", "low", "vwap", "adv20",
            "cashflow_op", "ebit", "ebitda", "enterprise_value",
            "bookvalue_ps", "debt", "equity", "current_ratio",
            "eps", "capex", "cashflow", "cogs", "cash_st",
            "consensus_analyst_rating",
            "scl12_buzz", "scl12_sentiment", "snt_social_value",
            "implied_volatility_call", "implied_volatility_put", "historical_volatility",
            "fscore_bfl_value", "fscore_bfl_momentum", "fscore_bfl_quality",
            "fscore_bfl_growth", "fscore_bfl_profitability", "fscore_bfl_total",
        ]
        for f in known_fields:
            if f in expr and f not in fields:
                fields.append(f)

        return fields

    def _safe_float(self, x):
        try:
            return None if x is None else float(x)
        except (TypeError, ValueError):
            return None

    def _json_or_dict(self, value):
        if isinstance(value, (dict, list)):
            return value
        if isinstance(value, str):
            return json.loads(value)
        return dict(value)

    def _apply_concentration_fix(self, expr: str) -> str:
        """
        v5.5: Fix CONCENTRATED_WEIGHT by modifying the expression to spread weight.

        CONCENTRATED_WEIGHT means the expression only produces non-zero values for
        a small subset of stocks (e.g., fscore data covers ~1500 stocks out of 3000).
        rank() then concentrates all weight into those stocks.

        Fix strategies (randomly chosen):
        1. Multiply by rank(cap) — weights by market cap, spreads to large-caps
        2. Multiply by rank(adv20) — weights by average daily volume, spreads to liquid stocks
        3. Wrap inner signal inside rank(... * rank(cap)) — tighter integration
        """
        transform = self.rng.choice([
            "multiply_cap",
            "multiply_adv20",
            "inner_cap",
            "multiply_cap",       # Double weight for cap — most reliable fix
            "multiply_adv20",     # Double weight for adv20 — also reliable
        ])

        # Strip outer rank() if present, to avoid rank(rank(...))
        inner = expr
        had_outer_rank = False
        if expr.startswith("rank(") and expr.endswith(")"):
            # Verify balanced parens
            depth = 0
            balanced = True
            test = expr[5:-1]
            for ch in test:
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                if depth < 0:
                    balanced = False
                    break
            if balanced and depth == 0:
                inner = test
                had_outer_rank = True

        if transform == "multiply_cap":
            if had_outer_rank:
                return f"rank({inner}) * rank(cap)"
            else:
                return f"({expr}) * rank(cap)"
        elif transform == "multiply_adv20":
            if had_outer_rank:
                return f"rank({inner}) * rank(adv20)"
            else:
                return f"({expr}) * rank(adv20)"
        elif transform == "inner_cap":
            if had_outer_rank:
                return f"rank({inner} * rank(cap))"
            else:
                return f"rank(({expr}) * rank(cap))"
        else:
            # Fallback: multiply by rank(cap)
            return f"({expr}) * rank(cap)"

    def _dedupe_weighted(self, candidates: list[str], weights: list[float]):
        seen = {}
        ordered = []
        ordered_weights = []
        for candidate, weight in zip(candidates, weights):
            if candidate in seen:
                idx = seen[candidate]
                ordered_weights[idx] = max(ordered_weights[idx], weight)
                continue
            seen[candidate] = len(ordered)
            ordered.append(candidate)
            ordered_weights.append(weight)
        return ordered, ordered_weights
