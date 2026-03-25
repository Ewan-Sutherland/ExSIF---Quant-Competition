from __future__ import annotations

import json
import random
from typing import Any

import config
from canonicalize import canonicalize_expression, hash_candidate
from models import Candidate, SimulationSettings
from templates import (
    FUNDAMENTAL_FIELDS, DEEP_FUNDAMENTAL_FIELDS, ANALYST_FIELDS,
    SENTIMENT_FIELDS, OPTIONS_WINDOWS,
    SAFE_PARAM_RANGES, TEMPLATE_LIBRARY,
)


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
}


class AlphaGenerator:
    def __init__(self, seed: int | None = None):
        self.rng = random.Random(seed)

    # ============================
    # PUBLIC
    # ============================

    def generate_candidate(self, family_bias=None, template_bias=None):
        family = self._sample_family(family_bias)
        template = self._sample_template(family, template_bias)

        params = self._sample_params(template["expression"])
        expr, fields = self._render(template["expression"], params)
        expr = self._post_process(expr, family=family, template_id=template["template_id"], light=False)

        settings = self._sample_settings(family)
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

    def mutate_candidate(self, row, metrics_hint: dict[str, Any] | None = None):
        family = row["family"]
        template_id = row["template_id"]
        reason = str(row["reason"]) if "reason" in row.keys() else ""

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

        config_weights = getattr(config, "FAMILY_BASE_WEIGHTS", {})
        weights = []

        for fam in fams:
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

        if force_smoothing:
            smoothing_prob = 0.78
        elif light:
            smoothing_prob = getattr(config, "LIGHT_POST_PROCESS_SMOOTH_PROB", 0.30)
        else:
            smoothing_prob = getattr(config, "FRESH_FORCE_SMOOTH_PROB", 0.72)

        already_smoothed = expr.startswith("ts_mean(") or expr.startswith("ts_decay_linear(")
        if self.rng.random() < smoothing_prob and not already_smoothed:
            if force_smoothing:
                # Fitness/turnover: data shows window=8-10 + ts_decay_linear = best fitness
                win = self.rng.choice([5, 8, 10, 10])
                if self.rng.random() < 0.55:
                    expr = f"ts_decay_linear(rank({expr}), {win})"
                else:
                    expr = f"ts_mean(rank({expr}), {win})"
            elif light:
                win_choices = getattr(config, "PREFER_TS_MEAN_WINDOW", [3, 5])
                win = self.rng.choice(win_choices)
                if self.rng.random() < 0.35:
                    expr = f"ts_decay_linear(rank({expr}), {win})"
                else:
                    expr = f"ts_mean(rank({expr}), {win})"
            else:
                win = self.rng.choice([3, 5, 10])
                if self.rng.random() < 0.40:
                    expr = f"ts_decay_linear(rank({expr}), {win})"
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

    def _sample_params(self, template):
        p = {}
        if "{n}" in template:
            p["n"] = self.rng.choice(self._grid("n"))
        if "{m}" in template:
            p["m"] = self.rng.choice(self._grid("m"))
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

        if "decay" in out:
            decay_grid = list(config.DEFAULT_DECAYS)
            if family == "volume_flow" and mode in {"fitness", "turnover"}:
                # Volume-flow alphas are inherently high-turnover — push decay hard
                # Data shows vol_03 near-passers at fitness 0.92-0.98 need decay 8-12
                high_decay = [d for d in decay_grid if d >= 8] or decay_grid[-2:]
                out["decay"] = self.rng.choice(high_decay)
            elif mode in {"fitness", "turnover"}:
                out["decay"] = self._push_param_wider(out["decay"], decay_grid)
            elif mode == "sharpe":
                out["decay"] = self._mutate(out["decay"], decay_grid, stay_prob=0.10)
            else:
                out["decay"] = self._mutate(out["decay"], decay_grid, stay_prob=0.25)

        # More aggressive neutralization mutation for fitness
        neut_prob = 0.40 if mode == "fitness" else (0.25 if mode == "turnover" else 0.18)
        if "neutralization" in out and self.rng.random() < neut_prob:
            out["neutralization"] = self.rng.choice(config.DEFAULT_NEUTRALIZATIONS)

        # Truncation matters for fitness and turnover
        trunc_prob = 0.30 if mode in {"fitness", "turnover"} else 0.10
        if "truncation" in out and self.rng.random() < trunc_prob:
            out["truncation"] = self.rng.choice(config.DEFAULT_TRUNCATIONS)

        # Universe swap — sub-universe Sharpe test is a common failure
        universe_prob = 0.12 if mode == "fitness" else 0.05
        if "universe" in out and self.rng.random() < universe_prob:
            out["universe"] = self.rng.choice(config.DEFAULT_UNIVERSES)

        return out

    def _sample_settings(self, family):
        return SimulationSettings(
            region=config.DEFAULT_REGION,
            universe=self.rng.choice(config.DEFAULT_UNIVERSES),
            delay=config.DEFAULT_DELAY,
            decay=self.rng.choice(config.DEFAULT_DECAYS),
            neutralization=self.rng.choice(config.DEFAULT_NEUTRALIZATIONS),
            truncation=self.rng.choice(config.DEFAULT_TRUNCATIONS),
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
        for key in ["field", "deep_field", "analyst_field", "sentiment_field"]:
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
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            return json.loads(value)
        return dict(value)

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
