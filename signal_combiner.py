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

    def refresh_near_passers(self, min_sharpe: float = 0.90, min_fitness: float = 0.40) -> None:
        """Load near-passers from storage, grouped by data category."""
        if self.storage is None:
            return

        self._near_passers_by_category.clear()

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

        # Sort each category by sharpe descending
        for cat in self._near_passers_by_category:
            self._near_passers_by_category[cat].sort(
                key=lambda x: x["sharpe"], reverse=True,
            )

        total = sum(len(v) for v in self._near_passers_by_category.values())
        cats = list(self._near_passers_by_category.keys())
        print(
            f"[COMBINER] Loaded {total} near-passers across {len(cats)} categories: "
            f"{', '.join(f'{c}={len(self._near_passers_by_category[c])}' for c in cats)}"
        )

    # v6.2.1: Categories that are proven portfolio-ADDITIVE (genuinely different data)
    # The portfolio is saturated with price_returns/model77/fundamental.
    # Combos that include one of these categories are far more likely to improve score.
    PORTFOLIO_ADDITIVE_CATS = {"options_vol", "news", "sentiment", "risk", "vector_data", "model_data", "event_driven"}

    def generate_combo(self, n_signals: int = 2) -> Optional[str]:
        """
        Generate a composite expression combining n_signals from different categories.
        Returns expression string or None if not enough diversity.
        
        v6.2.1: Biased toward including at least one portfolio-additive category
        (options_vol, news, sentiment, risk) since those are proven to improve score.
        
        Research finding: rank(A * B) outperforms rank(A) + rank(B).
        Three forms (picked randomly):
          - rank(raw_A * raw_B)           — multiplicative raw (best per research)
          - rank(raw_A) * rank(raw_B)     — multiplicative ranked 
          - rank(raw_A) + rank(raw_B)     — additive (baseline)
        """
        if len(self._near_passers_by_category) < n_signals:
            return None

        # Pick n_signals different categories
        available_cats = [
            c for c, entries in self._near_passers_by_category.items()
            if entries
        ]
        if len(available_cats) < n_signals:
            return None

        # v6.2.1: Bias toward portfolio-additive categories
        # 70% of the time, force one component from the additive set
        additive_available = [c for c in available_cats if c in self.PORTFOLIO_ADDITIVE_CATS]
        other_available = [c for c in available_cats if c not in self.PORTFOLIO_ADDITIVE_CATS]

        if additive_available and other_available and self.rng.random() < 0.70:
            # Pick one additive + rest from other categories
            first_cat = self.rng.choice(additive_available)
            remaining_pool = [c for c in available_cats if c != first_cat]
            if len(remaining_pool) >= n_signals - 1:
                rest = self.rng.sample(remaining_pool, n_signals - 1)
                chosen_cats = [first_cat] + rest
            else:
                chosen_cats = self.rng.sample(available_cats, n_signals)
        else:
            chosen_cats = self.rng.sample(available_cats, n_signals)

        # Pick the best signal from each category (with some randomization)
        components = []
        for cat in chosen_cats:
            entries = self._near_passers_by_category[cat]
            pool = entries[:min(3, len(entries))]
            chosen = self.rng.choice(pool)
            components.append(chosen)

        # Pick combination mode — research says multiplicative raw is best
        roll = self.rng.random()
        if roll < 0.45:
            mode = "mult_raw"      # rank(A * B) — best per research
        elif roll < 0.75:
            mode = "mult_ranked"   # rank(A) * rank(B)
        else:
            mode = "additive"      # rank(A) + rank(B) — baseline

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
        print(
            f"[COMBO_GEN] categories={cats_str} mode={mode} "
            f"component_sharpes={[f'{s:.2f}' for s in sharpes]}"
        )

        return expr

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
