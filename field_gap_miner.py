"""
Field Gap Miner — systematically finds alpha in unused fields.

The portfolio uses 59 fields out of 5,904 available (1%).
Every positive-scoring alpha used a field NOT in the portfolio.
This module mines the other 99%.

Strategy:
1. Extract fields from all submissions → "saturated set"
2. Get all available fields from datasets → "full set"
3. gap = full - saturated
4. Generate simple expressions using gap fields in proven patterns
5. Rotate through gap fields systematically, not randomly
"""

from __future__ import annotations
import re
import random
from typing import Optional
from functools import lru_cache


# Proven expression patterns that produce eligible alphas.
# {F} = gap field, {G} = grouping (industry/subindustry)
# These 12 patterns cover the structures behind every submitted alpha.
GAP_PATTERNS = [
    # ── STANDALONE (simple, like Luca's +158) ──
    ("gap_standalone_rank", "rank(ts_rank({F} / (cap + 0.001), {long_window}))"),
    ("gap_standalone_zscore", "rank(ts_zscore({F}, {short_window}))"),
    ("gap_standalone_delta", "rank(ts_rank(ts_delta({F}, {mid_window}) / (abs(ts_delay({F}, {mid_window})) + 0.001), {long_window}))"),
    ("gap_standalone_smooth", "ts_mean(rank(ts_rank({F} / (cap + 0.001), {long_window})), {smooth_window})"),
    ("gap_standalone_neg_zscore", "rank(-ts_zscore({F}, {mid_window}))"),
    
    # ── GROUP RELATIVE (like the +198 fn_ alpha) ──
    ("gap_group_rank", "group_rank(ts_rank({F} / (cap + 0.001), {long_window}), {G})"),
    ("gap_group_zscore", "group_rank(ts_zscore({F}, {mid_window}), {G})"),
    ("gap_group_neutralize", "group_neutralize(ts_rank({F} / (cap + 0.001), {long_window}), {G})"),
    
    # ── VALUE + REVERSION (like Griff's +28) ──
    ("gap_plus_reversion", "rank(ts_rank({F} / (cap + 0.001), {long_window})) + rank(-ts_mean(returns, {reversion_window}))"),
    ("gap_backfill_times_rev", "rank(ts_backfill({F}, 60)) * rank(-returns)"),
    ("gap_group_plus_rev", "group_rank(ts_rank({F} / (cap + 0.001), {long_window}), {G}) + rank(-ts_mean(returns, {reversion_window}))"),
    ("gap_vwap_reversion", "rank(ts_rank({F} / (cap + 0.001), {long_window})) + -rank(ts_mean((close - vwap) / vwap, {reversion_window}))"),
    ("gap_debt_combo", "rank(ts_rank({F} / (cap + 0.001), {long_window})) + -rank(ts_zscore(debt, 30))"),
    
    # ── MULTIPLICATIVE (proven in submitted portfolio) ──
    ("gap_mult_reversion", "rank({F} / (cap + 0.001)) * rank(-returns)"),
    ("gap_mult_volume", "rank(ts_rank({F} / (cap + 0.001), {long_window})) * rank(volume / (adv20 + 0.001))"),
    ("gap_mult_liquidity", "rank({F}) * rank(adv20)"),
    
    # ── MULTI-TIMEFRAME (fv_08 pattern — proven submitted) ──
    ("gap_multi_tf", "rank(ts_rank({F} / (cap + 0.001), 22)) * rank(ts_rank({F} / (cap + 0.001), 252))"),
    ("gap_multi_tf_60_252", "rank(ts_rank({F} / (cap + 0.001), 60)) * rank(ts_rank({F} / (cap + 0.001), 252))"),
    
    # ── VOL REGIME CONDITIONAL (trade_when — proven submitted) ──
    ("gap_vol_regime", "trade_when(ts_rank(ts_std_dev(returns, 22), 252) > 0.5, rank({F} / (cap + 0.001)), -1)"),
    ("gap_vol_regime_rev", "rank(trade_when(ts_rank(ts_std_dev(returns, 22), 252) > 0.5, rank(-returns), -1)) * rank({F} / (cap + 0.001))"),
    
    # ── CROSS-FIELD CORRELATION (the +16 alpha used this) ──
    ("gap_cross_corr", "rank(-ts_corr(rank({F}), rank({F2}), {long_window}))"),
    ("gap_cross_corr_short", "rank(-ts_corr(rank({F}), rank({F2}), {mid_window}))"),
    
    # ── WINSORIZED / ROBUST ──
    ("gap_winsorized_group", "rank(winsorize(group_rank(ts_rank({F} / (cap + 0.001), {long_window}), {G}), std=4))"),
    ("gap_signed_power", "rank(signed_power(group_rank({F} / (cap + 0.001), {G}), 0.5))"),
    
    # ── TREND EXTRACTION (ts_regression residual) ──
    ("gap_regression_trend", "rank(ts_regression({F}, ts_step(1), {mid_window}, rettype=2))"),
    
    # ── BACKFILL (for sparse/event fields like rp_*, news) ──
    ("gap_backfill_rank", "rank(ts_backfill({F}, 60))"),
    ("gap_backfill_reversion", "rank(ts_decay_linear(ts_backfill({F}, 60), {smooth_window})) * rank(-returns)"),
    ("gap_backfill_group", "group_rank(ts_backfill({F}, 60), {G})"),
    ("gap_backfill_plus_rev", "rank(ts_decay_linear(ts_backfill({F}, 60), {smooth_window})) + rank(-ts_mean(returns, {reversion_window}))"),
]

# Fields that need ts_backfill() wrapping (sparse/event data)
# NOTE: pv13_ fields REMOVED — they have Unit[Group:] type and crash ts_backfill
SPARSE_FIELD_PREFIXES = (
    'rp_css_', 'rp_ess_', 'rp_nip_', 'nws12_', 'nws18_',
    'scl12_', 'scl15_', 'snt_', 'snt1_',
    'implied_volatility_', 'historical_volatility_', 'pcr_',
    'news_', 'rel_ret_',
)

# v7.2.1: Group-typed fields that only work with group_* operators.
# These fields have Unit[Group:N] type and FAIL with ts_backfill, ts_rank,
# divide-by-cap, or most scalar operators. Skip entirely for now.
GROUP_TYPED_PREFIXES = (
    'pv13_',              # supply chain hierarchy — Unit[Group:N]
    'fnd6_newqeventv',    # quarterly event fundamentals — "event inputs" errors
    'fnd6_eventv',        # event-type fundamentals
    'fnd6_newa',          # annual grouped fundamentals
    'fnd2_',              # fundamental grouped variant
)

# Fields that are ratios (don't divide by cap)
RATIO_FIELDS = {
    'current_ratio', 'eps', 'bookvalue_ps', 'sales_ps', 'dividend_yield',
    'payout_ratio', 'consensus_analyst_rating', 'beta_last_30_days_spy',
    'beta_last_60_days_spy', 'beta_last_90_days_spy',
}

# vec_ fields need vec_avg() wrapping
VECTOR_FIELD_PREFIXES = ('scl12_', 'scl15_', 'nws12_', 'nws18_')

OPERATORS = {
    'rank', 'ts_mean', 'ts_decay_linear', 'ts_zscore', 'ts_rank', 'ts_delta',
    'ts_std_dev', 'ts_corr', 'ts_backfill', 'ts_sum', 'ts_product', 'ts_regression',
    'ts_count_nans', 'ts_covariance', 'ts_delay', 'ts_av_diff', 'ts_scale',
    'ts_quantile', 'ts_step', 'ts_arg_max', 'ts_arg_min',
    'group_rank', 'group_zscore', 'group_neutralize', 'group_mean',
    'group_backfill', 'group_scale',
    'normalize', 'quantile', 'winsorize', 'zscore', 'scale',
    'abs', 'log', 'sqrt', 'sign', 'max', 'min', 'power', 'signed_power',
    'trade_when', 'if_else', 'densify', 'bucket', 'hump',
    'and', 'or', 'not', 'is_nan',
    'vec_avg', 'vec_sum', 'vec_count', 'vec_max', 'vec_min',
    'vec_stddev', 'vec_range', 'vec_ir',
    'days_from_last_change', 'last_diff_value', 'kth_element',
    'add', 'subtract', 'multiply', 'divide', 'inverse', 'reverse',
}
SKIP_TOKENS = {
    'industry', 'subindustry', 'sector', 'market', 'country',
    'true', 'false', 'nan', 'range', 'rettype', 'lag', 'std',
    'on', 'off', 'verify', 'fastexpr', 'usa', 'equity',
    'filter', 'rate', 'lookback', 'driver', 'gaussian',
    'condition', 'raw_signal',
}

# Weighted category selection — based on 40K+ sims of evidence.
# Higher weight = more gap mining attempts in that category.
CATEGORY_WEIGHTS = {
    "fn_financial": 10.0,        # 316 fields, only 2 used, proven +198 score
    "news_events": 8.0,          # rp_css/rp_ess fields, proven +28 score
    "supply_chain": 7.0,         # pv13_*, rel_ret_* — untapped network effects
    "options": 6.0,              # different IV tenors — proven category
    "fundamental": 5.0,          # core fundamentals not yet tried
    "hist_vol": 5.0,             # vol fields not yet tried
    "social_sentiment": 4.0,     # scl12/scl15 sentiment
    "research_sentiment": 4.0,   # snt1_ fields
    "analyst_estimates": 3.0,    # est_ebitda etc (NOT anl4_)
    "news_data": 3.0,            # news microstructure
    "vector_data": 3.0,          # vec fields
    "derivative_scores": 2.0,    # fscore derivatives
    "risk_beta": 2.0,            # beta fields
    "model77": 0.5,              # 3238 fields but 0% eligible in 214 sims — near-dead
    "price_volume": 0.1,         # mostly metadata (cusip, currency)
    "universe_membership": 0.0,  # not tradable signals
}

# Field name patterns that are metadata, not tradable signals
METADATA_PATTERNS = (
    'currency', 'cusip', 'isin', 'sedol', 'ticker', 'country', 'exchange',
    'reporting', 'fiscal', 'flag', '_item', '_code', 'gvkey', 'permno',
    'date', 'sector_code', 'industry_code',
)

# Field-to-pattern compatibility. Some patterns don't suit some field types.
# Key = field prefix, Value = pattern name substrings to EXCLUDE
FIELD_PATTERN_EXCLUSIONS = {
    # Ravenpack/news event scores: don't divide by cap (they're scores 0-100)
    'rp_css_': ('_rank', '_zscore', '_delta', '_smooth', '_group_rank', '_group_zscore',
                '_multi_tf', '_vol_regime', '_regression', '_winsorized', '_signed'),
    'rp_ess_': ('_rank', '_zscore', '_delta', '_smooth', '_group_rank', '_group_zscore',
                '_multi_tf', '_vol_regime', '_regression', '_winsorized', '_signed'),
    'rp_nip_': ('_rank', '_zscore', '_delta', '_smooth', '_group_rank', '_group_zscore',
                '_multi_tf', '_vol_regime', '_regression', '_winsorized', '_signed'),
    # Beta fields: already ratios, don't divide by cap
    'beta_': ('_rank', '_delta', '_smooth', '_multi_tf'),
    # Derivative scores: already composite scores
    'composite_factor_score': ('_rank', '_delta', '_multi_tf'),
}


def extract_fields_from_expr(expr: str) -> set[str]:
    """Extract data field names from an expression string."""
    if not expr:
        return set()
    tokens = re.findall(r'[a-zA-Z_][a-zA-Z0-9_]*', expr.lower())
    fields = set()
    for t in tokens:
        if t in OPERATORS or t in SKIP_TOKENS or len(t) <= 2:
            continue
        fields.add(t)
    return fields


class FieldGapMiner:
    """Mines the gap between portfolio fields and available fields."""

    def __init__(self, storage=None, rng=None):
        self.storage = storage
        self.rng = rng or random.Random()
        self._portfolio_fields: set[str] = set()
        self._all_fields: dict[str, list[str]] = {}  # category -> [fields]
        self._gap_fields: list[str] = []
        self._gap_by_category: dict[str, list[str]] = {}
        self._field_index: int = 0  # Rotate through gap fields systematically
        self._tried_combos: set[str] = set()  # track expr+settings already generated
        self._stats = {"generated": 0, "fields_tried": 0}

    def refresh(self) -> None:
        """Reload portfolio fields from submissions and compute gap."""
        self._load_portfolio_fields()
        self._load_all_fields()
        self._compute_gap()

    def _load_portfolio_fields(self) -> None:
        """Extract all fields used in submitted alphas — TEAM-WIDE.
        
        Self-correlation is team-wide, so a field used by ANY teammate's
        submission is saturated for ALL bots.
        """
        self._portfolio_fields = set()
        if self.storage is not None:
            try:
                # Use ALL team submissions, not just this owner's
                if hasattr(self.storage, 'get_all_team_submissions'):
                    rows = self.storage.get_all_team_submissions(limit=500)
                else:
                    rows = self.storage.get_submitted_candidate_rows(limit=300)
                for row in rows:
                    expr = row.get("canonical_expression", "")
                    self._portfolio_fields.update(extract_fields_from_expr(expr))
            except Exception as exc:
                print(f"[GAP_MINER] Failed to load portfolio fields: {exc}")

        # Always include commonly saturated fields even if not in DB
        # (manual submissions with null expressions, or missing JOIN partners)
        # This is the complete set from our portfolio analysis of 108 submissions.
        self._portfolio_fields.update({
            # price_volume basics — always correlated
            'returns', 'close', 'cap', 'adv20', 'volume', 'vwap', 'open',
            'high', 'low', 'sharesout',
            # Fields confirmed in portfolio from 93 expressions
            'implied_volatility_call_120', 'parkinson_volatility_120',
            'operating_income', 'debt', 'nws12_afterhsz_sl',
            'rel_ret_comp', 'est_eps', 'implied_volatility_put_120', 'assets',
            'implied_volatility_put_30', 'implied_volatility_call_30',
            'fn_liab_fair_val_l1_a', 'rp_ess_revenue',
            'implied_volatility_call_270', 'implied_volatility_put_270', 'pcr_oi_270',
            'fn_oth_income_loss_fx_transaction_and_tax_translation_adj_a',
            'scl12_alltype_buzzvec', 'rp_css_earnings', 'liabilities',
            'historical_volatility_20', 'news_pct_1min', 'news_max_up_ret',
            'one_year_change_total_assets', 'rp_ess_mna', 'rp_ess_legal',
            'snt_buzz_ret', 'snt1_d1_earningssurprise', 'snt1_d1_netearningsrevision',
            'rel_ret_cust', 'est_fcf', 'est_ptp', 'equity', 'eps', 'sales',
            'cashflow', 'cashflow_op', 'cogs', 'revenue', 'enterprise_value',
            'cash_burn_rate', 'rp_css_insider', 'rp_ess_insider', 'rp_ess_ratings',
            'pv13_com_page_rank', 'est_dividend_ps', 'capex', 'rp_ess_product',
            'sales_ps', 'income', 'earnings_momentum_composite_score',
            'historical_volatility_60', 'implied_volatility_call_60',
            'historical_volatility_20',
        })
        print(f"[GAP_MINER] Portfolio uses {len(self._portfolio_fields)} fields")

    def _load_all_fields(self) -> None:
        """Load all available fields from datasets."""
        try:
            from datasets import get_all_field_names, is_blocked_event_field
            self._all_fields = {}
            for category, fields in get_all_field_names().items():
                valid = [f for f in fields if f and not is_blocked_event_field(f)]
                if valid:
                    self._all_fields[category] = valid
        except Exception as exc:
            print(f"[GAP_MINER] Failed to load datasets: {exc}")
            self._all_fields = {}

    def _compute_gap(self) -> None:
        """Compute gap = all_fields - portfolio_fields, prioritized."""
        self._gap_fields = []
        self._gap_by_category = {}

        # v7.2.1: Fields with these prefixes are ECONOMICALLY similar to
        # portfolio fields even if the exact name is different.
        # anl4_* = analyst estimates (same signal as est_eps)
        # actual_* = actuals (same signal as eps/revenue)
        # These pass self-corr at 0.63 but score -34 to -180.
        economically_saturated_prefixes = set()
        # Only suppress if the economic category is already represented
        if any(f.startswith('est_') for f in self._portfolio_fields):
            economically_saturated_prefixes.update(('anl4_', 'actual_'))

        # Priority categories — these have proven alpha potential
        priority_order = [
            "fn_financial",      # 5000+ fn_ fields, only 2 used — HIGHEST PRIORITY
            "supply_chain",      # rel_ret_sup, pv13_* etc
            "fundamental",       # gross_profit, working_capital etc
            "options",           # IV tenors not yet tried
            "news_data",         # sentiment fields
            "social_sentiment",  # scl15_*, snt_ fields
            "analyst_estimates", # est_ebitda, est_revenue (NOT anl4_*)
            "news_events",       # nws18_ (need vec_avg wrapper)
            "vector_data",       # vec fields
            "risk_beta",         # beta fields
            "derivative_scores", # fscore derivatives
            "hist_vol",          # historical vol
        ]

        all_gap = []
        for category in priority_order:
            fields = self._all_fields.get(category, [])
            gap = [f for f in fields if f.lower() not in self._portfolio_fields
                   and f.lower() not in {'industry', 'subindustry', 'sector', 'market'}
                   and not any(f.lower().startswith(p) for p in economically_saturated_prefixes)
                   and not any(f.lower().startswith(p) for p in GROUP_TYPED_PREFIXES)
                   and not any(m in f.lower() for m in METADATA_PATTERNS)]
            if gap:
                self._gap_by_category[category] = gap
                all_gap.extend(gap)

        # Add remaining categories not in priority list
        for category, fields in self._all_fields.items():
            if category in priority_order:
                continue
            gap = [f for f in fields if f.lower() not in self._portfolio_fields
                   and f.lower() not in {'industry', 'subindustry', 'sector', 'market'}
                   and not any(f.lower().startswith(p) for p in economically_saturated_prefixes)
                   and not any(f.lower().startswith(p) for p in GROUP_TYPED_PREFIXES)
                   and not any(m in f.lower() for m in METADATA_PATTERNS)]
            if gap:
                self._gap_by_category[category] = gap
                all_gap.extend(gap)

        self._gap_fields = all_gap
        self._field_index = 0

        print(f"[GAP_MINER] Found {len(self._gap_fields)} untouched fields across {len(self._gap_by_category)} categories")
        for cat, fields in list(self._gap_by_category.items())[:8]:
            print(f"  {cat}: {len(fields)} gap fields (e.g. {', '.join(fields[:3])})")

    def _next_field(self) -> Optional[str]:
        """Get next gap field using WEIGHTED category selection.
        
        Categories are weighted by proven alpha potential — fn_financial
        gets 10x the weight of model77 because fn_ fields have proven
        +198 score impact while model77 has 0% eligible rate.
        Within each category, pick a random field.
        """
        if not self._gap_by_category:
            return None
        cats = list(self._gap_by_category.keys())
        if not cats:
            return None
        
        # Weighted selection by category
        weights = [CATEGORY_WEIGHTS.get(c, 1.0) for c in cats]
        total_w = sum(weights)
        if total_w <= 0:
            return None
        
        # Weighted random choice
        r = self.rng.random() * total_w
        cumulative = 0
        chosen_cat = cats[0]
        for cat, w in zip(cats, weights):
            cumulative += w
            if r <= cumulative:
                chosen_cat = cat
                break
        
        fields = self._gap_by_category[chosen_cat]
        if not fields:
            return None
        self._field_index += 1
        return self.rng.choice(fields)

    def _needs_backfill(self, field: str) -> bool:
        """Check if field needs ts_backfill() wrapping."""
        fl = field.lower()
        return any(fl.startswith(p) for p in SPARSE_FIELD_PREFIXES)

    def _needs_vec_avg(self, field: str) -> bool:
        """Check if field needs vec_avg() wrapping."""
        fl = field.lower()
        return any(fl.startswith(p) for p in VECTOR_FIELD_PREFIXES)

    def _wrap_field(self, field: str) -> str:
        """Wrap field with necessary operators (backfill, vec_avg)."""
        if self._needs_vec_avg(field):
            return f"ts_backfill(vec_avg({field}), 60)"
        if self._needs_backfill(field):
            return f"ts_backfill({field}, 60)"
        return field

    def _is_ratio_field(self, field: str) -> bool:
        """Check if field is already a ratio (don't divide by cap)."""
        fl = field.lower()
        return fl in RATIO_FIELDS or fl.startswith('beta_') or fl.startswith('pcr_')

    def generate(self) -> Optional[dict]:
        """Generate a gap-field expression. Returns dict with expression, family, template_id, fields."""
        if not self._gap_fields:
            return None

        field = self._next_field()
        if not field:
            return None

        wrapped = self._wrap_field(field)
        is_ratio = self._is_ratio_field(field)

        # Pick random parameters
        long_window = self.rng.choice([120, 252])
        mid_window = self.rng.choice([20, 40, 60])
        short_window = self.rng.choice([5, 10, 20])
        smooth_window = self.rng.choice([3, 5, 8, 10])
        reversion_window = self.rng.choice([3, 5, 10])
        group = self.rng.choice(["industry", "subindustry"])

        # Pick a pattern
        # For sparse fields, ONLY use backfill patterns (they handle wrapping)
        # For non-sparse fields, EXCLUDE backfill patterns
        is_sparse = self._needs_backfill(field) or self._needs_vec_avg(field)
        if is_sparse:
            eligible_patterns = [p for p in GAP_PATTERNS if 'backfill' in p[0]]
            if not eligible_patterns:
                eligible_patterns = GAP_PATTERNS[:5]
            # Backfill patterns already have ts_backfill in template,
            # so use RAW field name — don't double-wrap
            wrapped = field
            if self._needs_vec_avg(field):
                wrapped = f"vec_avg({field})"  # vec_avg only, backfill is in pattern
        else:
            eligible_patterns = [p for p in GAP_PATTERNS if 'backfill' not in p[0]]
            if not eligible_patterns:
                eligible_patterns = GAP_PATTERNS[:5]

        # v7.2.1: Additional field-pattern compatibility filter
        # Skip patterns that divide by cap for score/ratio type fields
        fl = field.lower()
        for prefix, excluded_suffixes in FIELD_PATTERN_EXCLUSIONS.items():
            if fl.startswith(prefix):
                eligible_patterns = [p for p in eligible_patterns
                                     if not any(s in p[0] for s in excluded_suffixes)]
                break
        if not eligible_patterns:
            eligible_patterns = [p for p in GAP_PATTERNS if 'backfill' in p[0]] if is_sparse else GAP_PATTERNS[:5]

        pattern_id, pattern_template = self.rng.choice(eligible_patterns)

        # For cross-correlation pattern, pick a second gap field
        f2_wrapped = wrapped  # default
        if '{F2}' in pattern_template:
            other_fields = [f for f in self._gap_fields if f != field]
            if other_fields:
                f2 = self.rng.choice(other_fields[:20])  # Pick from first 20 for diversity
                f2_wrapped = self._wrap_field(f2)
            else:
                # Fall back to a different pattern
                eligible_patterns = [p for p in GAP_PATTERNS if '{F2}' not in p[1]]
                pattern_id, pattern_template = self.rng.choice(eligible_patterns)

        # Build field reference — for ratio fields, don't divide by cap
        if is_ratio:
            field_ref = wrapped
            # Replace "/ (cap + 0.001)" patterns
            pattern_template = pattern_template.replace("{F} / (cap + 0.001)", "{F}")
        else:
            field_ref = wrapped

        # Format expression
        try:
            expr = pattern_template.format(
                F=field_ref,
                F2=f2_wrapped,
                G=group,
                long_window=long_window,
                mid_window=mid_window,
                short_window=short_window,
                smooth_window=smooth_window,
                reversion_window=reversion_window,
            )
        except (KeyError, IndexError):
            return None

        # Dedup check
        combo_key = f"{expr}:{pattern_id}"
        if combo_key in self._tried_combos:
            return None
        self._tried_combos.add(combo_key)

        self._stats["generated"] += 1
        if self._field_index % len(self._gap_fields) == 0:
            self._stats["fields_tried"] += 1

        return {
            "expression": expr,
            "family": "gap_mining",
            "template_id": f"gap_{pattern_id}",
            "fields": [field],
            "params": {
                "gap_field": field,
                "pattern": pattern_id,
                "group": group,
                "long_window": long_window,
            },
        }

    @property
    def gap_count(self) -> int:
        return len(self._gap_fields)

    def stats(self) -> dict:
        return {
            "portfolio_fields": len(self._portfolio_fields),
            "total_available": sum(len(v) for v in self._all_fields.values()),
            "gap_fields": len(self._gap_fields),
            "gap_categories": len(self._gap_by_category),
            "generated": self._stats["generated"],
            "tried_combos": len(self._tried_combos),
        }
