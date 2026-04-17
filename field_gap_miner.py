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
    # Simple standalone — like Luca's +158 revenue alpha
    ("gap_standalone_rank", "rank(ts_rank({F} / (cap + 0.001), {long_window}))"),
    ("gap_standalone_zscore", "rank(ts_zscore({F}, {short_window}))"),
    ("gap_standalone_delta", "rank(ts_rank(ts_delta({F}, {mid_window}) / (abs(ts_delay({F}, {mid_window})) + 0.001), {long_window}))"),
    ("gap_standalone_smooth", "ts_mean(rank(ts_rank({F} / (cap + 0.001), {long_window})), {smooth_window})"),
    
    # Group-relative — like the +198 fn_ alpha
    ("gap_group_rank", "group_rank(ts_rank({F} / (cap + 0.001), {long_window}), {G})"),
    ("gap_group_zscore", "group_rank(ts_zscore({F}, {mid_window}), {G})"),
    
    # With reversion component — like Griff's +28 alphas
    ("gap_plus_reversion", "rank(ts_rank({F} / (cap + 0.001), {long_window})) + rank(-ts_mean(returns, {reversion_window}))"),
    ("gap_times_reversion", "rank(ts_backfill({F}, 60)) * rank(-returns)"),
    ("gap_group_plus_rev", "group_rank(ts_rank({F} / (cap + 0.001), {long_window}), {G}) + rank(-ts_mean(returns, {reversion_window}))"),
    
    # Cross-field correlation — like the +16 ts_corr(est_dividend_ps, capex) alpha
    ("gap_cross_corr", "rank(-ts_corr(rank({F}), rank({F2}), {long_window}))"),
    
    # Backfill for sparse fields (ravenpack, events, news)
    ("gap_backfill_rank", "rank(ts_backfill({F}, 60))"),
    ("gap_backfill_reversion", "rank(ts_decay_linear(ts_backfill({F}, 60), {smooth_window})) * rank(-returns)"),
]

# Fields that need ts_backfill() wrapping (sparse/event data)
SPARSE_FIELD_PREFIXES = (
    'rp_css_', 'rp_ess_', 'rp_nip_', 'pv13_', 'nws12_', 'nws18_',
    'scl12_', 'scl15_', 'snt_', 'snt1_',
    'implied_volatility_', 'historical_volatility_', 'pcr_',
    'news_', 'rel_ret_',
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
        """Extract all fields used in submitted alphas."""
        self._portfolio_fields = set()
        if self.storage is None:
            return
        try:
            rows = self.storage.get_submitted_candidate_rows(limit=300)
            for row in rows:
                expr = row.get("canonical_expression", "")
                self._portfolio_fields.update(extract_fields_from_expr(expr))
        except Exception as exc:
            print(f"[GAP_MINER] Failed to load portfolio fields: {exc}")

        # Also add commonly saturated fields even if not in DB
        # (manual submissions with null expressions)
        self._portfolio_fields.update({
            'returns', 'close', 'cap', 'adv20', 'volume', 'vwap', 'open',
            'high', 'low', 'sharesout',  # price_volume basics always correlated
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

        # Priority categories — these have proven alpha potential
        priority_order = [
            "analyst_estimates",  # est_ebitda, est_revenue etc
            "fundamental",       # gross_profit, working_capital etc
            "fn_financial",      # 5000+ fn_ fields, only 2 used
            "supply_chain",      # rel_ret_sup, pv13_* etc
            "options",           # IV tenors not yet tried
            "news_data",         # sentiment fields
            "social_sentiment",  # scl15_*, snt_ fields
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
                   and f.lower() not in {'industry', 'subindustry', 'sector', 'market'}]
            if gap:
                self._gap_by_category[category] = gap
                all_gap.extend(gap)

        # Add remaining categories not in priority list
        for category, fields in self._all_fields.items():
            if category in priority_order:
                continue
            gap = [f for f in fields if f.lower() not in self._portfolio_fields
                   and f.lower() not in {'industry', 'subindustry', 'sector', 'market'}]
            if gap:
                self._gap_by_category[category] = gap
                all_gap.extend(gap)

        self._gap_fields = all_gap
        self._field_index = 0

        print(f"[GAP_MINER] Found {len(self._gap_fields)} untouched fields across {len(self._gap_by_category)} categories")
        for cat, fields in list(self._gap_by_category.items())[:8]:
            print(f"  {cat}: {len(fields)} gap fields (e.g. {', '.join(fields[:3])})")

    def _next_field(self) -> Optional[str]:
        """Get next gap field, rotating systematically."""
        if not self._gap_fields:
            return None
        field = self._gap_fields[self._field_index % len(self._gap_fields)]
        self._field_index += 1
        return field

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
        # For sparse fields, prefer backfill patterns
        if self._needs_backfill(field) or self._needs_vec_avg(field):
            eligible_patterns = [p for p in GAP_PATTERNS if 'backfill' in p[0] or 'reversion' in p[0]]
            if not eligible_patterns:
                eligible_patterns = GAP_PATTERNS
        else:
            eligible_patterns = GAP_PATTERNS

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
