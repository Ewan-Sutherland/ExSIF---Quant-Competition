"""
v7.0: Dynamic dataset loader — reads available fields from the team datasets Excel file.

Each teammate gets the same Excel listing all accessible WQ dataset fields.
This module parses it once at startup and exposes categorized field lists
that templates.py, generator.py, and llm_generator.py consume.

If the Excel is missing, falls back to a built-in minimal field set so the
bot can still run (just with fewer exploration options).
"""
from __future__ import annotations

import os
import logging
from pathlib import Path
from functools import lru_cache
from typing import Any

logger = logging.getLogger(__name__)

# ── Location of the datasets file(s) ───────────────────────────────
# The bot loads datasets in this priority order:
#   1. PERSONAL_DATASETS_FILE (your full personal dataset — if you have it)
#   2. DATASETS_FILE (team dataset — shared baseline)
#
# If you have the personal file, it REPLACES the team file (since it's a superset).
# If you only have the team file, that's used.
# If neither exists, built-in fallbacks are used.
_BASE_DIR = Path(__file__).resolve().parent
_DEFAULT_TEAM_PATH = _BASE_DIR / "data" / "wq_all_datasets.xlsx"
_DEFAULT_PERSONAL_PATH = _BASE_DIR / "data" / "wq_personal_datasets.xlsx"

DATASETS_FILE = Path(os.getenv("DATASETS_FILE", str(_DEFAULT_TEAM_PATH)))
PERSONAL_DATASETS_FILE = Path(os.getenv("PERSONAL_DATASETS_FILE", str(_DEFAULT_PERSONAL_PATH)))


# ── Sheet-to-category mapping ──────────────────────────────────────
# Supports BOTH Excel formats:
#   Team Excel: "Dataset_1", "Dataset_2", etc.
#   Personal Excel: "Analyst Estimates", "Fundamental Data", etc.

SHEET_CATEGORY_MAP = {
    # Team Excel sheet names
    "Dataset_1":  "analyst_estimates",
    "Dataset_2":  "fn_financial",
    "Dataset_3":  "fundamental",
    "Dataset_4":  "derivative_scores",
    "Dataset_5":  "risk_beta",
    "Dataset_6":  "news_data",
    "Dataset_7":  "news_events",
    "Dataset_8":  "hist_vol",
    "Dataset_9":  "options",
    "Dataset_10": "price_volume",
    "Dataset_11": "supply_chain",
    "Dataset_12": "universe_membership",
    "Dataset_13": "vector_data",
    "Dataset_14": "social_sentiment",
    # Personal Excel sheet names (human-readable)
    "Analyst Estimates":     "analyst_estimates",
    "Fundamental Data":      "fundamental",
    "Report Footnotes":      "fn_financial",
    "Analysts Factor Model": "model77",          # 3,241 fields — YOUR exclusive data
    "Fundamental Scores":    "derivative_scores",
    "Risk Metrics":          "risk_beta",
    "Ravenpack News":        "news_events",
    "US News Data":          "news_data",
    "Options Analytics":     "options",
    "Volatility Data":       "hist_vol",
    "Price Volume":          "price_volume",
    "Relationship Data":     "supply_chain",
    "Universe Dataset":      "universe_membership",
    "Research Sentiment":    "research_sentiment",  # snt1_d1_* fields — YOUR exclusive data
    "SM Sentiment":          "social_sentiment",
    "Social Media Data":     "vector_data",
}


# ── Built-in fallback fields (minimal set if Excel missing) ────────
_FALLBACK_FIELDS: dict[str, list[str]] = {
    "price_volume": ["adv20", "cap", "close", "high", "low", "open", "returns", "volume", "vwap", "sharesout", "split", "dividend"],
    "fundamental": ["assets", "assets_curr", "bookvalue_ps", "capex", "cash", "cash_st", "cashflow", "cashflow_op", "cogs", "current_ratio", "debt", "debt_lt", "debt_st", "ebit", "ebitda", "employee", "enterprise_value", "eps", "equity", "income", "operating_income", "sales", "revenue"],
    "analyst_estimates": ["snt1_d1_earningssurprise", "snt1_d1_netearningsrevision", "snt1_d1_dynamicfocusrank", "consensus_analyst_rating", "snt1_d1_earningsrevision", "snt1_d1_stockrank"],
    "derivative_scores": ["analyst_revision_rank_derivative", "cashflow_efficiency_rank_derivative", "composite_factor_score_derivative", "earnings_certainty_rank_derivative", "growth_potential_rank_derivative", "relative_valuation_rank_derivative"],
    "risk_beta": ["beta_last_30_days_spy", "beta_last_60_days_spy", "beta_last_90_days_spy", "beta_last_120_days_spy", "beta_last_360_days_spy"],
    "options": ["implied_volatility_call_30", "implied_volatility_call_60", "implied_volatility_call_90", "implied_volatility_call_120", "implied_volatility_call_180", "implied_volatility_put_30", "implied_volatility_put_60", "implied_volatility_put_90", "implied_volatility_put_120", "implied_volatility_put_180", "historical_volatility_30", "historical_volatility_60", "historical_volatility_90", "historical_volatility_120"],
    "news_data": ["scl12_buzz", "scl12_sentiment", "snt_social_value"],
    "fn_financial": [],
    "hist_vol": [],
    "supply_chain": [],
    "news_events": [],
    "vector_data": [],
    "social_sentiment": [],
    "universe_membership": [],
}


@lru_cache(maxsize=1)
def load_datasets() -> dict[str, list[dict[str, Any]]]:
    """Load all dataset sheets from the best available Excel file.

    Priority: personal dataset (5,903 fields) > team dataset (2,496 fields) > fallback.

    Handles two Excel formats:
      - Team format: header at row 0, column named "Field"
      - Personal format: header at row 4, column named "id" or "field"
    """
    # Try personal first, then team
    chosen_file = None
    if PERSONAL_DATASETS_FILE.exists():
        chosen_file = PERSONAL_DATASETS_FILE
        logger.info(f"[DATASETS] Using PERSONAL dataset: {PERSONAL_DATASETS_FILE.name}")
    elif DATASETS_FILE.exists():
        chosen_file = DATASETS_FILE
        logger.info(f"[DATASETS] Using TEAM dataset: {DATASETS_FILE.name}")
    else:
        logger.warning(f"[DATASETS] No Excel found — using fallback fields")
        return {}

    try:
        import pandas as pd
        xls = pd.ExcelFile(chosen_file)
        result = {}
        total = 0

        for sheet_name in xls.sheet_names:
            category = SHEET_CATEGORY_MAP.get(sheet_name, sheet_name.lower().replace(" ", "_"))

            # Try team format first (header at row 0, "Field" column)
            try:
                df = pd.read_excel(xls, sheet_name)
                if "Field" in df.columns:
                    rows = df.to_dict("records")
                    if category in result:
                        result[category].extend(rows)
                    else:
                        result[category] = rows
                    total += len(rows)
                    continue
            except Exception:
                pass

            # Try personal format (header at row 4, "id" or "field" column)
            try:
                df = pd.read_excel(xls, sheet_name, header=4)
                field_col = None
                for col in df.columns:
                    if str(col).strip().lower() in ("id", "field"):
                        field_col = col
                        break
                if field_col is not None:
                    # Normalize to team format (rename to "Field" for consistency)
                    rename_map = {field_col: "Field"}
                    if "description" in df.columns:
                        rename_map["description"] = "Description"
                    df = df.rename(columns=rename_map)
                    df = df.dropna(subset=["Field"])
                    rows = df.to_dict("records")
                    if category in result:
                        result[category].extend(rows)
                    else:
                        result[category] = rows
                    total += len(rows)
                    continue
            except Exception:
                pass

            logger.warning(f"[DATASETS] Could not parse sheet '{sheet_name}'")

        logger.info(f"[DATASETS] Loaded {total} fields across {len(result)} categories from {chosen_file.name}")
        return result
    except Exception as e:
        logger.warning(f"[DATASETS] Failed to load Excel: {e} — using fallback fields")
        return {}


@lru_cache(maxsize=1)
def get_all_field_names() -> dict[str, list[str]]:
    """Return dict[category] -> list of field name strings."""
    datasets = load_datasets()
    if not datasets:
        return dict(_FALLBACK_FIELDS)

    result = {}
    for category, rows in datasets.items():
        fields = [str(r.get("Field", "")).strip() for r in rows if r.get("Field")]
        result[category] = fields
    return result


@lru_cache(maxsize=1)
def get_all_valid_fields() -> set[str]:
    """Flat set of every known field name — used for LLM expression validation."""
    all_fields = set()
    for fields in get_all_field_names().values():
        all_fields.update(fields)
    # Always include group fields
    all_fields.update({"industry", "subindustry", "sector", "market", "exchange"})
    return all_fields


def expression_uses_valid_fields(expr: str) -> bool:
    """Check if an expression only uses fields available in this bot's dataset.

    v7.1: Used by combiner and evolver to filter out near-passers from
    teammates' datasets that use fields this bot doesn't have.
    Returns True if all fields are valid (or check fails), False if invalid fields found.
    """
    import re
    try:
        valid = get_all_valid_fields()
        tokens = set(re.findall(r'[a-z][a-z0-9_]+', expr.lower()))
        operators = {
            'rank', 'group_rank', 'ts_mean', 'ts_std_dev', 'ts_zscore', 'ts_rank',
            'ts_delta', 'ts_decay_linear', 'ts_corr', 'ts_sum', 'ts_backfill',
            'ts_regression', 'ts_step', 'ts_delay', 'ts_scale', 'ts_arg_min',
            'ts_arg_max', 'ts_covariance', 'ts_product', 'ts_count_nans',
            'ts_quantile', 'ts_av_diff', 'trade_when', 'if_else',
            'abs', 'log', 'sign', 'max', 'min', 'power', 'sqrt',
            'is_nan', 'bucket', 'densify', 'winsorize', 'normalize',
            'group_neutralize', 'group_zscore', 'group_scale', 'group_backfill',
            'group_mean', 'scale', 'quantile', 'zscore',
            'vec_avg', 'vec_sum', 'signed_power', 'inverse', 'reverse', 'hump',
            'kth_element', 'range', 'true', 'false', 'rettype',
            'industry', 'subindustry', 'sector', 'market', 'exchange',
        }
        field_tokens = tokens - operators
        valid_lower = {f.lower() for f in valid}
        missing = [t for t in field_tokens if t not in valid_lower and len(t) > 3]
        return len(missing) == 0
    except Exception:
        return True  # If check fails, allow it


# ── Convenience accessors for specific field categories ────────────
# These replace the hardcoded lists in templates.py

def get_fundamental_fields() -> list[str]:
    """Core fundamental fields (price_volume + fundamental basics)."""
    names = get_all_field_names()
    core = ["cap", "assets", "sales", "income", "cash"]
    available = set(names.get("fundamental", []) + names.get("price_volume", []))
    return [f for f in core if f in available] or core


def get_deep_fundamental_fields() -> list[str]:
    """Detailed fundamental fields for value/quality templates."""
    names = get_all_field_names()
    candidates = [
        "cashflow_op", "ebit", "ebitda", "enterprise_value",
        "bookvalue_ps", "debt", "equity", "current_ratio",
        "eps", "capex", "cashflow", "cogs", "cash_st",
        "operating_income", "retained_earnings", "working_capital",
        "inventory_turnover", "rd_expense", "revenue",
    ]
    available = set(names.get("fundamental", []))
    result = [f for f in candidates if f in available]
    return result or candidates[:13]  # fallback to original set


def get_analyst_fields() -> list[str]:
    """Curated analyst fields for template {analyst_field} placeholder.

    Keep this TIGHT — only fields proven to work in existing template structures.
    The curated core is ALWAYS included. The personal dataset's Research Sentiment
    sheet adds snt1_d1_* fields that the team dataset doesn't have.
    """
    # Core proven set — ALWAYS included
    curated = [
        "snt1_d1_earningssurprise", "snt1_d1_netearningsrevision",
        "snt1_d1_dynamicfocusrank", "consensus_analyst_rating",
        "snt1_d1_earningsrevision", "snt1_d1_stockrank",
    ]
    # Small expansion from the Excel: forward estimate fields
    expansion_prefixes = ["est_eps", "est_cashflow_op", "est_capex", "est_ptp", "est_fcf"]
    names = get_all_field_names()
    available = names.get("analyst_estimates", [])
    result = list(curated)
    for prefix in expansion_prefixes:
        matches = [a for a in available if a.startswith(prefix)]
        if matches:
            result.append(matches[0])

    # Personal dataset bonus: Research Sentiment has extra snt1_d1_* fields
    for f in names.get("research_sentiment", []):
        if f.startswith("snt1_d1_") and f not in result:
            result.append(f)
    return result


def get_sentiment_fields() -> list[str]:
    """Curated sentiment fields for template {sentiment_field} placeholder.

    EXCLUDES vec fields (scl12_*vec) — those need vec_avg()/vec_sum() operators
    which most templates don't use. Vec fields are available to the LLM.
    """
    # Core proven scalar sentiment fields only
    curated = ["scl12_buzz", "scl12_sentiment", "snt_social_value"]
    # Small expansion: other scalar sentiment fields (NOT vectors)
    scalar_prefixes = ["scl12_buzz", "scl12_sentiment", "snt_social", "snt_buzz", "snt_value"]
    vec_suffixes = ["vec", "typevec", "sentvec", "buzzvec"]

    names = get_all_field_names()
    result = list(curated)
    for cat in ["news_data", "social_sentiment", "vector_data"]:
        for f in names.get(cat, []):
            # Skip vector fields — they need vec_avg/vec_sum operators
            if any(f.endswith(s) for s in vec_suffixes):
                continue
            if any(f.startswith(p) for p in scalar_prefixes) and f not in result:
                result.append(f)
    return result


def get_fscore_fields() -> list[str]:
    names = get_all_field_names()
    all_fund = names.get("fundamental", []) + names.get("derivative_scores", [])
    result = [f for f in all_fund if "fscore" in f.lower()]
    if not result:
        result = [
            "fscore_bfl_value", "fscore_bfl_momentum", "fscore_bfl_quality",
            "fscore_bfl_growth", "fscore_bfl_profitability", "fscore_bfl_total",
        ]
    return result


def get_derivative_fields() -> list[str]:
    names = get_all_field_names()
    result = [f for f in names.get("derivative_scores", []) if "derivative" in f.lower()]
    if not result:
        result = _FALLBACK_FIELDS["derivative_scores"]
    return result


def get_options_windows() -> list[int]:
    """Extract available options windows from the options dataset fields."""
    names = get_all_field_names()
    windows = set()
    import re
    for f in names.get("options", []):
        m = re.search(r"_(\d+)$", f)
        if m:
            windows.add(int(m.group(1)))
    # Filter to reasonable windows for IV/HV
    reasonable = sorted(w for w in windows if 10 <= w <= 360)
    return reasonable or [30, 60, 90, 120, 180]


def get_pcr_windows() -> list[int]:
    names = get_all_field_names()
    windows = set()
    import re
    for f in names.get("options", []):
        if "pcr_" in f:
            m = re.search(r"_(\d+)$", f)
            if m:
                windows.add(int(m.group(1)))
    return sorted(windows) or [10, 30, 60, 90, 120, 180, 270]


def get_fn_financial_fields() -> list[str]:
    """fn_* financial statement fields — proven portfolio-additive."""
    return get_all_field_names().get("fn_financial", [])


def get_news_fields() -> list[str]:
    """All news/ravenpack fields."""
    names = get_all_field_names()
    return names.get("news_data", []) + names.get("news_events", [])


def get_news_event_fields() -> list[str]:
    """Pure nws18_* event fields (not rp_* ravenpack)."""
    names = get_all_field_names()
    return [f for f in names.get("news_events", []) if f.startswith("nws18_")]


def get_rp_underused_fields() -> list[str]:
    """RavenPack category fields that templates rarely touch."""
    names = get_all_field_names()
    all_rp = [f for f in names.get("news_events", []) if f.startswith("rp_")]
    # Exclude the handful the LLM already overuses
    common = {"rp_ess_earnings", "rp_ess_mna", "rp_nip_earnings", "rp_css_credit", "rp_css_legal"}
    return [f for f in all_rp if f not in common]


def get_supply_chain_fields() -> list[str]:
    return get_all_field_names().get("supply_chain", [])


def get_hist_vol_fields() -> list[str]:
    return get_all_field_names().get("hist_vol", [])


def get_risk_beta_fields() -> list[str]:
    return get_all_field_names().get("risk_beta", [])


def get_vector_fields() -> list[str]:
    return get_all_field_names().get("vector_data", [])


# ── Model77 fields (from Dataset_1 analyst_estimates) ──────────────
# These are the pre-computed academic anomaly fields.
# They live in Dataset_1 alongside raw analyst estimates.

MODEL77_KEYWORDS = [
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
    "implied_minus_realized_volatility", "out_of_money_put_call",
    "visibility_ratio", "treynor_ratio", "cash_burn_rate",
    "capex_to_total_assets", "capex_to_depreciation",
]


def get_model77_fields() -> list[str]:
    """Model77 pre-computed anomaly fields.

    In the personal dataset these are in the 'model77' category (3,241 fields).
    In the team dataset they're scattered across analyst_estimates (or missing).
    """
    names = get_all_field_names()

    # Direct category (personal dataset has "Analysts Factor Model" → "model77")
    if "model77" in names and names["model77"]:
        return names["model77"]

    # Fallback: search analyst_estimates for model77-like fields
    ae_fields = names.get("analyst_estimates", [])
    model77 = []
    for f in ae_fields:
        if any(kw in f.lower() for kw in MODEL77_KEYWORDS):
            model77.append(f)
    return model77


def print_dataset_summary():
    """Print a summary of loaded datasets — useful for debugging."""
    names = get_all_field_names()
    total = sum(len(v) for v in names.values())
    print(f"\n{'='*60}")
    print(f"  DATASET SUMMARY — {total} total fields")
    print(f"{'='*60}")
    for cat, fields in sorted(names.items(), key=lambda x: -len(x[1])):
        print(f"  {cat:30s} {len(fields):5d} fields")
        if fields:
            print(f"    e.g.: {', '.join(fields[:3])}")
    print(f"{'='*60}\n")


# ── Family blocking based on available data ────────────────────────
# Families that require specific data categories to function.
# If the category is empty/missing, the family CANNOT produce valid expressions.

FAMILY_REQUIRED_CATEGORIES = {
    "model77_anomaly": ["model77"],
    "model77_combo": ["model77"],
    # Note: relationship uses rel_ret_* from supply_chain — available to all via team dataset
    # Note: earnings_momentum uses snt1_d1_* — available on WQ platform even if not in team Excel
}


def get_blocked_families() -> set[str]:
    """Return families that this bot cannot use due to missing data.
    
    A family is blocked if ALL its required data categories are empty.
    """
    names = get_all_field_names()
    blocked = set()
    for family, required_cats in FAMILY_REQUIRED_CATEGORIES.items():
        has_any = any(len(names.get(cat, [])) > 0 for cat in required_cats)
        if not has_any:
            blocked.add(family)
    return blocked
