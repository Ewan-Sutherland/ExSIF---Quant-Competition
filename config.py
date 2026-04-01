from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

# Base paths
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

DB_PATH = DATA_DIR / "bot.db"

# Storage backend: "sqlite" or "supabase"
# v6.0.1: Default to supabase for team distribution — everyone shares same data
STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "supabase")

# Supabase credentials (only needed if STORAGE_BACKEND = "supabase")
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY", "")

# Environment / API
BRAIN_USERNAME = os.getenv("BRAIN_USERNAME", "")
BRAIN_PASSWORD = os.getenv("BRAIN_PASSWORD", "")

# Scheduler / runtime
MAX_CONCURRENT_SIMS = 3
POLL_INTERVAL_SECONDS = 20
SESSION_REFRESH_MINUTES = 150
SIM_TIMEOUT_MINUTES = 45

# Default simulation settings
DEFAULT_REGION = "USA"
DEFAULT_DELAY = 1
DEFAULT_UNIVERSES = ["TOP3000", "TOP1000", "TOP500", "TOP200", "TOPSP500"]
DEFAULT_NEUTRALIZATIONS = ["SUBINDUSTRY", "INDUSTRY", "SECTOR", "MARKET", "NONE"]
DEFAULT_DECAYS = [2, 4, 6, 8, 10, 12]
DEFAULT_TRUNCATIONS = [0.03, 0.05, 0.08, 0.10]

DEFAULT_INSTRUMENT_TYPE = "EQUITY"
DEFAULT_VISUALIZATION = False

DEFAULT_PASTEURIZATION = "ON"
DEFAULT_UNIT_HANDLING = "VERIFY"
DEFAULT_NAN_HANDLING = "OFF"
DEFAULT_MAX_STOCK_WEIGHT = 0.10
DEFAULT_LANGUAGE = "FASTEXPR"

# Evaluation thresholds
MIN_SHARPE = 1.25
MIN_FITNESS = 1.00
MAX_TURNOVER = 0.70

# Near-passer / refinement settings
NEAR_PASSER_MIN_SHARPE = 1.35
NEAR_PASSER_MIN_FITNESS = 0.75
NEAR_PASSER_MAX_TURNOVER = 0.75
REFINEMENT_PROBABILITY = 0.40  # v6.0: was 0.65 — logs show 50% of sims wasted on refinement

MIN_REFINEMENT_SHARPE = 1.20
FRONTIER_MIN_SHARPE = 1.25
FRONTIER_MIN_FITNESS = 0.65
FRONTIER_ALT_MIN_SHARPE = 1.45
FRONTIER_ALT_MIN_FITNESS = 0.68

# Frontier templates worth exploiting more aggressively
STRONG_TEMPLATES = {"cs_02", "pvc_04", "vol_03", "cond_01", "va_02", "mr_02", "pvc_03", "fs_04", "fs_05", "fs_06", "opt_03", "opt_04"}
ELITE_TEMPLATES = {"mr_04", "vol_03", "va_02", "cond_01"}

# Diversity / anti-self-correlation
DIVERSITY_LOOKBACK_RUNS = 120
MAX_RECENT_TEMPLATE_COUNT = 20
RELAXED_TEMPLATE_COUNT = 14
RELAXED_TEMPLATE_MIN_AVG_SHARPE = 1.30
RELAXED_TEMPLATE_MIN_AVG_FITNESS = 0.70
DIVERSITY_EXPLORATION_PROBABILITY = 0.12
MAX_REFINEMENT_ATTEMPTS_PER_BASE = 5   # v6.0: was 10 — logs show 10+ attempts never crack fitness
MAX_CORE_SIGNAL_EXHAUSTIONS = 2  # v6.0: was 3 — same core through different candidates wastes sims
MAX_FAMILY_TEMPLATE_EXHAUSTIONS = 2  # v6.0: was 3 — m7c_03 exhausted 12 times in overnight run
MAX_REFINEMENT_PER_CORE = 15  # v6.0: hard cap on total refinement sims per core signal across ALL candidates
MAX_SUBMISSIONS_PER_CORE = 3  # v6.0.1: allow up to 3 variants of same core before blocking (WQ accepts different post-processing)
LOCAL_REFINEMENT_HISTORY = 10
LOCAL_REFINEMENT_MAX_SIMILARITY = 0.90

# Template scoring / pruning
TEMPLATE_SCORE_LOOKBACK_RUNS = 180
MIN_TEMPLATE_OBS_FOR_PRUNE = 6

HARD_PRUNE_MAX_AVG_SHARPE = 0.20
HARD_PRUNE_MAX_AVG_FITNESS = 0.10

SOFT_PRUNE_MAX_AVG_SHARPE = 0.60
SOFT_PRUNE_MAX_AVG_FITNESS = 0.35

TEMPLATE_EXPLORATION_PROBABILITY = 0.10
SOFT_PRUNE_REFINEMENT_PROBABILITY = 0.35

# Submission behaviour
# v6.2: Smart submission pipeline — checks self-corr + before-after via API,
# refines settings with Optuna, picks best variant.
# AUTO_SUBMIT=True  → submits best variant directly to WQ
# AUTO_SUBMIT=False → stages best variant in ready_alphas table for manual submission
AUTO_SUBMIT = False

# v6.2: Number of Optuna settings variants to try per eligible alpha
OPTIMIZE_VARIANTS = 8

# v6.2: IQC competition ID for before-and-after-performance endpoint
IQC_COMPETITION_ID = os.getenv("IQC_COMPETITION_ID", "IQC2026S1")

# Submission diversity / self-correlation avoidance
# Structural similarity threshold: if candidate > this vs any submitted alpha, flag as correlated
# This is a PROXY for PnL correlation — structural sim 0.50+ typically means PnL corr > 0.70
SUBMISSION_MAX_SIMILARITY = 0.45
# Boost factor for families NOT yet represented in submissions
UNSUBMITTED_FAMILY_BOOST = 1.60

# Logging / reporting
REPORT_EVERY_N_COMPLETIONS = 25

# Family/template weighting
DEFAULT_FAMILY_ORDER = [
    # v5.9: NEW — pre-computed academic anomalies (HIGHEST PRIORITY — 168 untapped fields)
    "model77_anomaly",
    "model77_combo",
    "expanded_fundamental",
    "relationship",
    "risk_beta",
    "analyst_estimates",
    "wq_proven",
    # v5.8: Multi-factor combinations
    "combo_factor",
    # v5.7: Signal classes
    "fundamental_value",
    "quality_trend",
    "fundamental_scores",
    "earnings_momentum",
    "options_vol",
    "news_sentiment",
    "vol_regime",
    "size_value",
    # Existing — medium priority
    "cross_sectional",
    "liquidity_scaled",
    "conditional",
    "analyst_sentiment",
    "volume_flow",
    "price_vol_corr",
    "vol_adjusted",
    "volatility",
    "intraday",
    "fundamental",
    "mean_reversion",
    "momentum",
]

FAMILY_BASE_WEIGHTS = {
    # v5.9.1: Weights updated based on 1240-sim analysis
    # model77_combo is the goldmine — near-passers at S=1.47
    "model77_anomaly": 0.10,    # DEAD — 135 sims, best S=1.05. Standalone fields don't work.
    "model77_combo": 4.00,      # ALIVE — near-passers at S=1.47/F=0.91. HIGHEST priority.
    "expanded_fundamental": 0.80,  # Weak — 95 sims, best S=1.03
    "relationship": 0.60,       # Weak — 68 sims, best S=1.29 but can't pass fitness
    "risk_beta": 0.10,          # DEAD — 64 sims, best S=0.60. Kill it.
    "analyst_estimates": 1.50,  # ae_01 works (20 eligible), ae_03 dead (96 sims, 0 eligible)
    "wq_proven": 1.50,          # wp_05 works (23 eligible), wp_02 works (4 eligible)
    # v5.8: Multi-factor combinations (Griff's S=2.15 strategy)
    "combo_factor": 2.00,
    # v5.8: SATURATED — 9 submissions are returns mean reversion variants
    "mean_reversion": 0.05,
    "cross_sectional": 0.05,
    "liquidity_scaled": 0.08,
    "conditional": 0.10,
    "vol_adjusted": 0.08,
    # v5.7: Signal classes
    "fundamental_value": 1.00,
    "quality_trend": 1.00,
    "fundamental_scores": 1.00,
    "earnings_momentum": 1.20,
    "options_vol": 1.00,
    "news_sentiment": 1.00,
    "vol_regime": 0.60,
    "size_value": 0.60,
    # MEDIUM — legacy
    "volume_flow": 0.20,
    "price_vol_corr": 0.20,
    "analyst_sentiment": 0.30,
    "volatility": 0.15,
    "intraday": 0.15,
    "fundamental": 0.10,
    "momentum": 0.05,
}

TEMPLATE_BASE_WEIGHTS = {
    # v5.8: Multi-factor combination templates — HIGHEST PRIORITY
    "cf_01": 1.90,      # -rank(close/field) + -rank(returns) — direct Griff pattern
    "cf_02": 1.90,      # -rank(ts_zscore(field)) + -rank(vwap rev) — direct Griff pattern
    "cf_03": 1.80,      # rank(field/assets) + -rank(vwap rev) — Griff sales/assets pattern
    "cf_04": 1.70,      # rank(field/cap) + -rank(returns)
    "cf_05": 1.50,      # fundamental + vol_regime
    "cf_06": 1.70,      # earnings momentum + reversion
    "cf_07": 1.65,      # earnings rank + vwap reversion
    "cf_08": 1.60,      # options IV ratio + reversion
    "cf_09": 1.55,      # fscore + reversion
    "cf_10": 1.40,      # sentiment + fundamental
    # v5.7: New signal class templates
    "fv_05": 1.40,      # ts_rank fundamental ratio, 252 days — from WQ seminar
    "fv_06": 1.35,      # group_rank ts_rank — WQ seminar variant
    "qt_04": 1.30,
    "qt_06": 1.25,
    "em_01": 1.40,      # net earnings revision — direct analyst signal
    "em_07": 1.35,
    "opt_05": 1.40,     # PCR timing — directly from WQ researcher
    "opt_06": 1.35,     # IV momentum
    "opt_07": 1.30,
    "ns_01": 1.20,
    "ns_03": 1.20,
    "vr_01": 1.30,      # WQ researcher said "works really well!"
    "vr_03": 1.20,
    "fs_07": 1.30,      # derivative fields × adv20
    "fs_08": 1.25,
    # Existing
    "fs_04": 1.40,
    "fs_05": 1.30,
    "fs_06": 1.25,
    "opt_03": 1.20,
    "opt_04": 1.15,
    # Legacy — heavily reduced (saturated signal classes)
    "cs_02": 0.30,      # Was 0.80 — 3 submissions from this template
    "pvc_04": 0.60,
    "vol_03": 0.40,
    "mr_02": 0.20,
    "cond_01": 0.20,    # Already submitted
    "mr_04": 0.15,      # 2 submissions already
    "pvc_03": 0.40,
    "mr_01": 0.20,
}

PREFERRED_TEMPLATE_BOOSTS = {
    "mr_04": 1.35,
    "vol_03": 1.40,
    "va_02": 1.20,
    "mr_01": 1.15,
    "cond_01": 1.10,
    "fs_04": 1.50,      # v5.5: liquidity-weighted fscore — unlocks highest fitness signal
    "fs_05": 1.45,      # v5.5: adv20-weighted fscore
    "opt_03": 1.30,     # v5.5: under-tested, different data category
    "opt_04": 1.25,     # v5.5: under-tested, different data category
}

TEMPLATE_WEIGHT_PENALTIES = {
    "va_01": 0.35,
    "vol_02": 0.30,
    "cond_03": 0.40,
    # v5.5: penalize original fscore templates — consistently fail CONCENTRATED_WEIGHT
    "fs_01": 0.40,      # Unweighted fscore delta — concentrates weight
    "fs_02": 0.30,      # Unweighted fscore zscore — 34x identical failures in v5.4
    "fs_03": 0.40,      # Unweighted group_rank fscore — same issue
    # v5.9.1: HARD PRUNE — templates proven dead from 1240-sim analysis
    "ae_03": 0.01,       # 96 sims, 0 eligible — -ts_corr(est_ptp, est_fcf) doesn't work
    "wp_06": 0.01,       # 18 sims, 0 eligible — -ts_rank(fn_liab_fair_val_l1_a) doesn't work
    "wp_03": 0.05,       # 18 sims, 0 eligible — ts_regression complex expression
    # v5.9.1: Standalone model77 templates — all dead (135 sims, best S=1.05)
    "m77_01": 0.05,
    "m77_02": 0.05,
    "m77_03": 0.05,
    "m77_04": 0.05,
    "m77_05": 0.05,
    "m77_06": 0.05,
    "m77_07": 0.05,
    "m77_08": 0.05,
    "m77_09": 0.05,
}

# v5.9.1: Boost model77_combo templates (near-passers at S=1.47)
PREFERRED_TEMPLATE_BOOSTS.update({
    "m7c_03": 1.50,      # v6.0: reduced — near-passers but never cracks fitness, Optuna will handle
    "m7c_02": 1.50,      # combo with -rank(returns)
    "m7c_01": 1.40,
    "m7c_04": 1.40,
    # v6.0: Multiplicative combos — data-driven weights from overnight run
    "m7c_05": 0.01,      # DEAD: avg S=0.14 in 7 sims
    "m7c_06": 3.00,      # BEST NEW: avg S=0.81, best F=1.39 — multiplicative × vol_regime
    "m7c_07": 0.01,      # DEAD: avg S=-0.01 in 7 sims
    "m7c_08": 0.01,      # DEAD: avg S=-0.17 in 2 sims
    # v6.0: Q-theory — didn't work standalone, keep triple combo alive
    "m7c_09": 0.01,      # DEAD: avg S=0.14 standalone q-theory
    "m7c_10": 1.20,      # ALIVE: avg S=0.59, best S=1.01 — q-theory + price reversion
    "m7c_11": 0.01,      # DEAD: avg S=0.09 group_rank q-theory
    # v6.0: Quality × value — low sharpe but interesting fitness profile
    "m7c_12": 0.80,      # KEEP LOW: avg S=0.20 but best F=1.21 — unusual
    "m7c_13": 1.00,      # KEEP: only 2 sims, avg S=0.56
    # v6.0: 3-signal composites
    "m7c_14": 1.20,      # KEEP: avg S=0.42, best F=0.84
    "m7c_15": 0.60,      # SOFT PRUNE: avg S=0.43, 12 sims, never close
    # v6.1: RAW MULTIPLICATIVE rank(A * B) — research says this form outperforms
    "m7c_16": 2.50,      # model77 × price reversion — highest priority new form
    "m7c_17": 2.50,      # model77 × vwap intraday
    "m7c_18": 2.50,      # model77 × vwap reversion smoothed
    "m7c_19": 2.00,      # model77 × IV skew — cross-dataset interaction
    "m7c_20": 2.00,      # model77 × earnings revision
    "m7c_21": 2.20,      # multiplicative pair + additive third (GP/A)
    "m7c_22": 2.20,      # multiplicative pair + additive third (investment)
    "m7c_23": 2.50,      # group_rank of model77 × reversion — industry relative
    "m7c_24": 2.50,      # group_rank of model77 × vwap — subindustry relative
    "m7c_25": 2.30,      # ts_rank temporal × reversion — best for quarterly data
    "m7c_26": 2.30,      # group_rank of ts_rank temporal × reversion
    "cf_11": 2.50,       # fundamental/cap × price reversion raw mult
    "cf_12": 2.50,       # fundamental zscore × vwap raw mult
    "cf_13": 2.50,       # fundamental/cap × vwap reversion raw mult
    "cf_14": 2.50,       # group_rank fundamental × reversion — industry
    "cf_15": 2.50,       # group_rank fundamental × reversion — subindustry
    # v6.0: Relationship — decayed customer momentum underwhelming
    "rel_09": 0.80,      # KEEP LOW: avg S=0.60 in 2 sims — needs more data
    "rel_10": 0.01,      # DEAD: avg S=-1.09 — wrong sign!
    "rel_11": 0.40,      # WEAK: avg S=0.08
    # v6.0: Expanded fundamental research templates — all failed
    "ef_15": 0.01,       # DEAD: S=-0.56
    "ef_16": 0.01,       # DEAD: avg S=-0.55
    "ef_17": 0.01,       # DEAD: avg S=-0.17
    "ef_18": 0.01,       # DEAD: avg S=0.03
})

DISABLED_REFINEMENT_TEMPLATES = {"vol_02", "ae_03", "wp_06"}
SOFT_BLOCK_REFINEMENT_TEMPLATES = {"cond_03", "va_01"}
SOFT_BLOCK_REFINEMENT_PROB = 0.08

REFINEMENT_TEMPLATE_SWITCH_PROB = 0.12
REFINEMENT_ELITE_STAY_PROB = 0.90
REFINEMENT_ELITE_FITNESS_STAY_PROB = 0.96
REFINEMENT_ELITE_TURNOVER_STAY_PROB = 0.98
REFINEMENT_ELITE_SHARPE_STAY_PROB = 0.84

LIGHT_POST_PROCESS_SMOOTH_PROB = 0.34
FRESH_FORCE_SMOOTH_PROB = 0.74
FRESH_RAW_RANK_PROB = 0.02
PREFER_TS_MEAN_WINDOW = [3, 5, 10]

# v5.6: LLM generation
# v6.1: Bumped from 0.10 — hypothesis-first prompting should produce better expressions
LLM_GENERATION_PROBABILITY = 0.25

# v6.1: Signal combination — auto-combine near-passers from different data categories
COMBO_GENERATION_PROBABILITY = 0.10

# v6.1: Evolutionary mutation — LLM mutates top performers
EVOLVE_GENERATION_PROBABILITY = 0.10

# ── v5.7: Signal-class settings profiles ─────────────────────────────
# Each signal class has preferred settings based on WQ researcher recommendations.
# 85% of sims use these profiles, 15% explore full setting space.
SIGNAL_CLASS_SETTINGS = {
    # Fundamental — quarterly data, long lookback, subindustry neutral
    "fundamental_value": {"universes": ["TOP3000"], "neutralizations": ["SUBINDUSTRY"], "decays": [0, 2, 4], "truncations": [0.05, 0.08]},
    "quality_trend": {"universes": ["TOP3000"], "neutralizations": ["SUBINDUSTRY"], "decays": [0, 2, 4], "truncations": [0.05, 0.08]},
    "size_value": {"universes": ["TOP3000"], "neutralizations": ["SUBINDUSTRY"], "decays": [0, 2, 4], "truncations": [0.05, 0.08]},
    # Fundamental scores — mid universe, some smoothing
    "fundamental_scores": {"universes": ["TOP1000", "TOP500"], "neutralizations": ["SUBINDUSTRY", "INDUSTRY"], "decays": [4, 6, 8], "truncations": [0.05]},
    # Earnings momentum — daily data, broad universe
    "earnings_momentum": {"universes": ["TOP3000", "TOP1000"], "neutralizations": ["NONE", "INDUSTRY"], "decays": [2, 4, 6], "truncations": [0.05, 0.08]},
    # v5.8: Options — added TOP500/TOPSP500 to fix LOW_SUB_UNIVERSE_SHARPE failures
    "options_vol": {"universes": ["TOP3000", "TOP500", "TOPSP500"], "neutralizations": ["INDUSTRY", "NONE"], "decays": [0, 2, 4], "truncations": [0.03, 0.05]},
    # News/sentiment — liquid universe, minimal neutral
    "news_sentiment": {"universes": ["TOP1000", "TOP500"], "neutralizations": ["NONE", "MARKET"], "decays": [2, 4, 6], "truncations": [0.05, 0.08]},
    # Vol regime — market neutral
    "vol_regime": {"universes": ["TOP3000", "TOP1000"], "neutralizations": ["MARKET", "NONE"], "decays": [4, 6, 8], "truncations": [0.05, 0.08]},
    # v5.8: Multi-factor combinations — broad settings from Griff's proven results
    "combo_factor": {"universes": ["TOP3000", "TOP1000"], "neutralizations": ["MARKET", "SUBINDUSTRY", "SECTOR"], "decays": [4, 6, 8, 10], "truncations": [0.05, 0.08, 0.10]},
    # v5.9: NEW families
    "model77_anomaly": {"universes": ["TOP3000", "TOP1000"], "neutralizations": ["INDUSTRY", "SUBINDUSTRY", "MARKET"], "decays": [2, 4, 6, 8], "truncations": [0.05, 0.08]},
    "model77_combo": {"universes": ["TOP3000", "TOP1000"], "neutralizations": ["INDUSTRY", "SUBINDUSTRY", "MARKET"], "decays": [4, 6, 8], "truncations": [0.05, 0.08]},
    "relationship": {"universes": ["TOP3000", "TOP1000"], "neutralizations": ["SUBINDUSTRY", "INDUSTRY"], "decays": [2, 4, 6], "truncations": [0.05, 0.08]},
    "risk_beta": {"universes": ["TOP3000", "TOP1000"], "neutralizations": ["MARKET", "INDUSTRY"], "decays": [4, 6, 8, 10], "truncations": [0.05, 0.08]},
    "expanded_fundamental": {"universes": ["TOP3000", "TOP1000", "TOP200"], "neutralizations": ["INDUSTRY", "SUBINDUSTRY"], "decays": [2, 4, 6], "truncations": [0.05, 0.08]},
    "analyst_estimates": {"universes": ["TOP3000", "TOP1000"], "neutralizations": ["INDUSTRY", "SUBINDUSTRY"], "decays": [2, 4, 6], "truncations": [0.05, 0.08]},
    "wq_proven": {"universes": ["TOP3000", "TOP1000", "TOP200"], "neutralizations": ["INDUSTRY", "SUBINDUSTRY", "SECTOR", "MARKET"], "decays": [0, 2, 4, 6, 8, 10], "truncations": [0.05, 0.08, 0.10]},
}

# v5.7: Minimum exploration guarantee per family
MIN_EXPLORATION_PER_FAMILY = 25

# v5.9: LLM rate limit cooldown (seconds between calls)
LLM_COOLDOWN_SECONDS = 60  # v6.1: Free tier — 10 RPM, 250 RPD. Groq fallback handles overflow
LLM_AST_RETRY_MAX = 1      # v6.1: retry failed expressions once with error feedback
