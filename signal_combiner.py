"""
v6.1: Signal Combination Engine

Automatically combines 2-3 near-passer signals from DIFFERENT data categories
into composite expressions. Three uncorrelated S=1.0 signals combine to S≈1.46.

Usage in bot.py:
    from signal_combiner import SignalCombiner
    combiner = SignalCombiner(storage)
    combo_expr = combiner.generate_combo()
"""
from __future__ import annotations

import random
from typing import Optional


# Data category classification for diversity enforcement
CATEGORY_KEYWORDS = {
    "fundamental": [
        "debt", "equity", "assets", "sales", "income", "ebit", "ebitda",
        "cashflow", "cogs", "capex", "bookvalue", "enterprise_value",
        "operating_income", "gross_profit", "retained_earnings", "inventory",
        "current_ratio", "rd_expense",
    ],
    "model77": [
        "earnings_momentum", "five_year_eps", "forward_ebitda", "forward_cash_flow",
        "cash_burn", "fcf_yield", "sustainable_growth", "normalized_earnings",
        "gross_profit_to_assets", "asset_growth_rate", "industry_relative",
        "gross_profit_margin", "parkinson_volatility",
    ],
    "analyst_estimates": ["est_eps", "est_ptp", "est_fcf", "est_cashflow_op", "est_capex"],
    "sentiment": [
        "snt1_d1_", "scl12_", "snt_", "consensus_analyst_rating",
    ],
    "options_vol": [
        "implied_volatility", "pcr_oi", "pcr_vol", "call_breakeven", "forward_price",
    ],
    "news": ["rp_css_", "rp_ess_", "news_pct", "news_max", "news_ls"],
    "price_returns": ["returns", "close", "open", "high", "low", "vwap"],
    "volume": ["volume", "adv20"],
    "relationship": ["rel_ret_cust", "rel_ret_comp", "rel_num_cust", "rel_num_comp"],
    "risk": ["beta_last", "unsystematic_risk", "systematic_risk"],
    "fscore": ["fscore_", "cashflow_efficiency_rank", "growth_potential_rank",
               "composite_factor_score", "earnings_certainty_rank"],
    # v6.2.1: Untapped data categories
    "vector_data": ["vec_sum", "vec_avg", "vec_count", "buzzvec", "sentvec",
                     "nws12_", "scl15_"],
    "model_data": ["mdf_nps", "mdf_oey", "mdf_rds", "mdf_eg3", "mdf_sg3",
                    "mdf_pbk", "mdl175_"],
    "event_driven": ["fnd6_", "fam_earn_surp", "fam_roe_rank",
                      "days_from_last_change", "last_diff_value"],
}


def classify_expression(expr: str) -> str:
    """Classify an expression into a data category."""
    expr_lower = expr.lower()
    scores = {}
    for category, keywords in CATEGORY_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in expr_lower)
        if score > 0:
            scores[category] = score
    if not scores:
        return "unknown"
    return max(scores, key=scores.get)


class SignalCombiner:
    """
    Generates composite alpha expressions by combining near-passers
    from different data categories.
    """

    def __init__(self, storage=None):
        self.storage = storage
        self.rng = random.Random()
        self._near_passers_by_category: dict[str, list[dict]] = {}
        self._last_refresh = 0
        self._submitted_fields: set[str] = set()  # v7.1: fields in existing submissions

    def refresh_near_passers(self, min_sharpe: float = 0.90, min_fitness: float = 0.40) -> None:
        """Load near-passers from storage, grouped by data category."""
        if self.storage is None:
            return

        self._near_passers_by_category.clear()

        # v7.1: Load submitted expression fields for novelty scoring
        try:
            submitted = self.storage.get_submitted_candidate_rows(limit=200)
            self._submitted_fields = set()
            for row in submitted:
                expr = row.get("canonical_expression", "")
                if expr:
                    self._submitted_fields.update(self._extract_fields(expr))
        except Exception:
            pass

        try:
            rows = self.storage.get_similarity_reference_candidates(
                limit=100, min_sharpe=min_sharpe, min_fitness=min_fitness,
            )
        except Exception:
            return

        for row in rows:
            expr = row.get("canonical_expression", "")
            if not expr:
                continue

            # v7.1: Filter out expressions using fields not in this bot's dataset
            # Prevents combiner from building combos with teammates' fields
            try:
                from datasets import expression_uses_valid_fields
                if not expression_uses_valid_fields(expr):
                    continue
            except Exception:
                pass

            category = classify_expression(expr)
            if category == "unknown":
                continue

            entry = {
                "expression": expr,
                "sharpe": float(row.get("sharpe", 0) or 0),
                "fitness": float(row.get("fitness", 0) or 0),
                "category": category,
            }

            if category not in self._near_passers_by_category:
                self._near_passers_by_category[category] = []
            self._near_passers_by_category[category].append(entry)

        # v7.3: Sort by composite score: fitness matters MORE than sharpe for score impact
        for cat in self._near_passers_by_category:
            self._near_passers_by_category[cat].sort(
                key=lambda x: x.get("fitness", 0) * 0.6 + x["sharpe"] * 0.4, reverse=True,
            )

        total = sum(len(v) for v in self._near_passers_by_category.values())
        cats = list(self._near_passers_by_category.keys())
        print(
            f"[COMBINER] Loaded {total} near-passers across {len(cats)} categories: "
            f"{', '.join(f'{c}={len(self._near_passers_by_category[c])}' for c in cats)}"
        )

    # v6.2.1: Categories that are proven portfolio-ADDITIVE (genuinely different data)
    PORTFOLIO_ADDITIVE_CATS = {"options_vol", "news", "sentiment", "risk", "vector_data", "model_data", "event_driven"}

    # v7.3: Fields that DOMINATE PnL profiles — wrapping diverse signals with these
    # makes PnL correlate with the 40% of portfolio that already uses returns/IV
    PNL_SATURATED_KEYWORDS = {"returns", "close", "open", "high", "low", "vwap",
        "implied_volatility", "parkinson_volatility", "historical_volatility", "adv20", "volume"}

    # v7.3: Non-price categories for pure-data combos
    NON_PRICE_CATS = {"fundamental", "analyst_estimates", "news", "sentiment",
        "fscore", "event_driven", "vector_data", "relationship"}

    def _pnl_saturation_score(self, expr: str) -> float:
        """Return 0.0 (pure non-price) to 1.0 (fully price-driven)."""
        fields = self._extract_fields(expr)
        if not fields:
            return 0.5
        saturated = sum(1 for f in fields
                       if any(s in f.lower() for s in self.PNL_SATURATED_KEYWORDS))
        return saturated / len(fields)

    def generate_combo(self, n_signals: int = 2) -> Optional[str]:
        """
        v7.3: Three generation modes for PnL diversity:
          50% PURE DATA  — both components non-price, additive combination
          30% DIVERSITY   — one additive cat (NOT options_vol) + one other
          20% EXPLORATION — random categories
        """
        if len(self._near_passers_by_category) < n_signals:
            return None

        available_cats = [
            c for c, entries in self._near_passers_by_category.items()
            if entries
        ]
        if len(available_cats) < n_signals:
            return None

        # v7.3: Determine generation mode
        roll = self.rng.random()
        non_price_avail = [c for c in available_cats if c in self.NON_PRICE_CATS]

        if roll < 0.50 and len(non_price_avail) >= n_signals:
            chosen_cats = self.rng.sample(non_price_avail, n_signals)
            combo_mode = "pure_data"
        elif roll < 0.80:
            # DIVERSITY mode — exclude options_vol from first pick to reduce IV contamination
            additive_no_iv = [c for c in available_cats
                             if c in self.PORTFOLIO_ADDITIVE_CATS and c != "options_vol"]
            other_available = [c for c in available_cats if c not in self.PORTFOLIO_ADDITIVE_CATS]
            if additive_no_iv and other_available:
                first_cat = self.rng.choice(additive_no_iv)
                remaining_pool = [c for c in available_cats if c != first_cat and c != "options_vol"]
                if len(remaining_pool) >= n_signals - 1:
                    rest = self.rng.sample(remaining_pool, n_signals - 1)
                    chosen_cats = [first_cat] + rest
                else:
                    chosen_cats = self.rng.sample(available_cats, n_signals)
            else:
                chosen_cats = self.rng.sample(available_cats, n_signals)
            combo_mode = "diversity"
        else:
            chosen_cats = self.rng.sample(available_cats, n_signals)
            combo_mode = "exploration"

        # Pick components with novelty + saturation-aware weighting
        max_component_ops = 25 if n_signals == 2 else 18
        components = []
        for cat in chosen_cats:
            entries = self._near_passers_by_category[cat]
            simple_entries = [e for e in entries if self._count_operators(e["expression"]) <= max_component_ops]
            if not simple_entries:
                simple_entries = entries[:3]
            pool = simple_entries[:min(8, len(simple_entries))]

            # v7.3: Score by novelty + PnL saturation penalty + fitness bonus
            if self._submitted_fields and len(pool) > 1:
                scored = []
                for entry in pool:
                    fields = set(self._extract_fields(entry["expression"]))
                    if fields:
                        overlap = len(fields & self._submitted_fields) / len(fields)
                        novelty = 1.0 - overlap
                    else:
                        novelty = 0.5
                    # v7.3: Penalize components using PnL-saturated fields (returns, IV, etc)
                    saturation = self._pnl_saturation_score(entry["expression"])
                    pnl_penalty = 1.0 - (saturation * 0.6)  # 0.4 to 1.0

                    # v7.3: In pure_data mode, STRONGLY penalize any price-field usage
                    if combo_mode == "pure_data":
                        pnl_penalty = max(0.05, 1.0 - saturation * 2.0)

                    # v7.3: Fitness bonus — high fitness = low turnover = better score impact
                    fitness_bonus = min(entry.get("fitness", 0.5), 2.0) * 0.5

                    weight = max(0.01, novelty * 2.0 + fitness_bonus + pnl_penalty + entry["sharpe"] * 0.3)
                    scored.append((entry, weight))
                chosen = self.rng.choices([s[0] for s in scored], weights=[s[1] for s in scored], k=1)[0]
            else:
                chosen = self.rng.choice(pool)
            components.append(chosen)

        # v7.3: Pick combination mode based on combo_mode
        if combo_mode == "pure_data":
            # Pure data combos: ALWAYS additive — keeps PnL profiles independent
            mode = "additive"
        else:
            roll2 = self.rng.random()
            if roll2 < 0.35:
                mode = "mult_raw"
            elif roll2 < 0.65:
                mode = "mult_ranked"
            else:
                mode = "additive"

        if n_signals == 2:
            expr = self._build_two_signal_combo(components[0], components[1], mode)
        else:
            expr = self._build_three_signal_combo(components, mode)

        # v6.2.1: Tighter operator count — combos get wrapped further by generator
        # WQ limit is 64, but generator adds rank/ts_decay_linear wrappers (+4-6 ops)
        op_count = self._count_operators(expr)
        if op_count > 58:
            cats_str = "+".join(c["category"] for c in components)
            print(f"[COMBO_OP_LIMIT] {op_count} operators in {cats_str} combo (limit 60), skipping")
            return None

        cats_str = "+".join(c["category"] for c in components)
        sharpes = [c["sharpe"] for c in components]
        sat_scores = [f"{self._pnl_saturation_score(c['expression']):.1f}" for c in components]
        print(
            f"[COMBO_GEN] categories={cats_str} mode={mode} combo_mode={combo_mode} "
            f"component_sharpes={[f'{s:.2f}' for s in sharpes]} "
            f"pnl_saturation={sat_scores}"
        )

        # v7.3: Fix rank(x, group) → group_rank(x, group)
        expr = self._fix_rank_group(expr)

        return expr

    @staticmethod
    def _fix_rank_group(expr: str) -> str:
        """Fix rank(x, industry) → group_rank(x, industry) using balanced paren matching."""
        result = []
        i = 0
        while i < len(expr):
            if expr[i:i+5] == 'rank(' and (i == 0 or not (expr[i-1].isalpha() or expr[i-1] == '_')):
                depth = 1
                j = i + 5
                while j < len(expr) and depth > 0:
                    if expr[j] == '(': depth += 1
                    elif expr[j] == ')': depth -= 1
                    j += 1
                inner = expr[i+5:j-1]
                for grp in ['industry', 'subindustry', 'sector', 'market']:
                    if inner.rstrip().endswith(f', {grp}'):
                        result.append('group_rank(' + inner + ')')
                        i = j
                        break
                else:
                    result.append(expr[i])
                    i += 1
            else:
                result.append(expr[i])
                i += 1
        return ''.join(result)

    @staticmethod
    def _count_operators(expr: str) -> int:
        """Count ALL operators in an expression — must match WQ's method.
        WQ counts: function calls + arithmetic (+,-,*,/) + comparisons (>,<,>=,<=,!=,==)."""
        import re
        func_ops = len(re.findall(r'[a-z_]+\s*\(', expr))
        arith_ops = len(re.findall(r'(?<![!=<>])[+\-*/](?![!=])', expr))
        compare_ops = len(re.findall(r'[<>]=?|[!=]=', expr))
        return func_ops + arith_ops + compare_ops

    def _build_two_signal_combo(
        self, sig_a: dict, sig_b: dict, mode: str,
    ) -> str:
        """Combine two signals using the specified mode."""
        if mode == "mult_raw":
            # rank(A * B) — research says this is best
            raw_a = self._extract_raw_signal(sig_a["expression"])
            raw_b = self._extract_raw_signal(sig_b["expression"])
            return f"rank({raw_a} * {raw_b})"
        elif mode == "mult_ranked":
            # rank(A) * rank(B)
            rank_a = self._wrap_as_rank_component(sig_a["expression"])
            rank_b = self._wrap_as_rank_component(sig_b["expression"])
            return f"{rank_a} * {rank_b}"
        else:
            # rank(A) + rank(B) — additive baseline
            rank_a = self._wrap_as_rank_component(sig_a["expression"])
            rank_b = self._wrap_as_rank_component(sig_b["expression"])
            return f"{rank_a} + {rank_b}"

    def _build_three_signal_combo(
        self, components: list[dict], mode: str,
    ) -> str:
        """Combine three signals."""
        if mode == "mult_raw":
            raw_a = self._extract_raw_signal(components[0]["expression"])
            raw_b = self._extract_raw_signal(components[1]["expression"])
            rank_c = self._wrap_as_rank_component(components[2]["expression"])
            return f"rank({raw_a} * {raw_b}) + {rank_c}"
        elif mode == "mult_ranked":
            rank_a = self._wrap_as_rank_component(components[0]["expression"])
            rank_b = self._wrap_as_rank_component(components[1]["expression"])
            rank_c = self._wrap_as_rank_component(components[2]["expression"])
            return f"{rank_a} * {rank_b} + {rank_c}"
        else:
            exprs = [self._wrap_as_rank_component(c["expression"]) for c in components]
            return f"{exprs[0]} + {exprs[1]} + {exprs[2]}"

    @staticmethod
    def _extract_fields(expr: str) -> list[str]:
        """v7.1: Extract data field names from expression for novelty scoring."""
        import re
        operators = {
            "rank", "ts_rank", "ts_zscore", "ts_delta", "ts_mean", "ts_std_dev",
            "ts_decay_linear", "ts_corr", "ts_regression", "ts_sum", "ts_backfill",
            "ts_delay", "ts_av_diff", "ts_arg_max", "ts_arg_min", "ts_step",
            "ts_product", "ts_min", "ts_max", "ts_count_nans", "ts_scale",
            "group_rank", "group_zscore", "group_neutralize", "group_vector_neut",
            "zscore", "winsorize", "trade_when", "hump", "scale", "log", "abs",
            "max", "min", "power", "vec_avg", "vec_sum", "vec_count",
            "bucket", "quantile", "range", "subindustry", "industry", "sector",
            "market", "returns", "close", "open", "high", "low",
            "volume", "vwap", "cap", "adv20",
        }
        tokens = re.findall(r'[a-z][a-z0-9_]+', expr.lower())
        return [t for t in tokens if t not in operators and len(t) > 3]

    def _extract_raw_signal(self, expr: str) -> str:
        """
        Extract the raw signal from an expression, stripping outer wrappers.
        
        rank(ts_zscore(debt, 40))          → ts_zscore(debt, 40)
        ts_decay_linear(rank(X), 5)        → X
        -rank(ts_mean(returns, 5))         → -ts_mean(returns, 5)
        group_rank(X, industry)            → X  (extract first arg)
        rank(A) + rank(B)                  → A + B  (strip ranks)
        
        For rank(A * B) combinations we want the raw signal inside,
        so the final output becomes rank(raw_A * raw_B).
        """
        stripped = expr.strip()
        
        # Strip leading negation
        negate = False
        if stripped.startswith("-"):
            negate = True
            stripped = stripped[1:].strip()
        
        # rank(something) → something
        if stripped.startswith("rank(") and stripped.endswith(")"):
            inner = stripped[5:-1]
            # Check balanced — make sure this close paren matches the open
            depth = 0
            for ch in inner:
                if ch == "(": depth += 1
                elif ch == ")": depth -= 1
                if depth < 0:
                    break
            if depth == 0:
                result = f"-{inner}" if negate else inner
                return result
        
        # group_rank(something, group) → something
        if stripped.startswith("group_rank("):
            inner = stripped[11:-1]
            # Extract first argument (before the last comma+group)
            depth = 0
            for i, ch in enumerate(inner):
                if ch == "(": depth += 1
                elif ch == ")": depth -= 1
                elif ch == "," and depth == 0:
                    result = inner[:i].strip()
                    return f"-{result}" if negate else result
        
        # ts_decay_linear(rank(X), N) → X
        for prefix in ["ts_decay_linear(", "ts_mean("]:
            if stripped.startswith(prefix):
                inner = stripped[len(prefix):-1]
                if inner.startswith("rank("):
                    # Extract what's inside the rank
                    rank_inner = inner[5:]
                    depth = 0
                    for i, ch in enumerate(rank_inner):
                        if ch == "(": depth += 1
                        elif ch == ")":
                            if depth == 0:
                                result = rank_inner[:i]
                                return f"-{result}" if negate else result
                            depth -= 1
        
        # Fallback — return as-is
        result = f"-{stripped}" if negate else stripped
        return result

    def _wrap_as_rank_component(self, expr: str) -> str:
        """
        Wrap an expression as a rank component for combination.
        If it's already a simple rank(...), use as-is.
        Otherwise wrap the core signal in rank().
        """
        stripped = expr.strip()

        # If it's already rank(something), use as-is
        if stripped.startswith("rank(") and stripped.endswith(")"):
            return stripped

        # If it starts with ts_decay_linear(rank(... or ts_mean(rank(...
        # extract the inner rank expression
        for prefix in ["ts_decay_linear(", "ts_mean("]:
            if stripped.startswith(prefix) and "rank(" in stripped:
                # Extract inner content — use the rank portion
                inner_start = stripped.find("rank(")
                # Find matching close paren for rank(
                depth = 0
                for i in range(inner_start + 5, len(stripped)):
                    if stripped[i] == "(":
                        depth += 1
                    elif stripped[i] == ")":
                        if depth == 0:
                            return stripped[inner_start:i + 1]
                        depth -= 1

        # If it starts with group_rank(...), use as-is
        if stripped.startswith("group_rank("):
            return stripped

        # If it's a negative signal like -rank(...)
        if stripped.startswith("-rank(") or stripped.startswith("-group_rank("):
            return stripped

        # For compound expressions like rank(A) + rank(B), use as-is but wrap
        if " + " in stripped or " - " in stripped or " * " in stripped:
            return f"rank({stripped})"

        # Otherwise just wrap in rank()
        return f"rank({stripped})"

    def stats(self) -> dict:
        return {
            "categories": len(self._near_passers_by_category),
            "total_near_passers": sum(
                len(v) for v in self._near_passers_by_category.values()
            ),
            "category_counts": {
                k: len(v) for k, v in self._near_passers_by_category.items()
            },
        }
