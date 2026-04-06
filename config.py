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
REFINEMENT_PROBABILITY = 0.60  # v6.2.1: was 0.40 — queue growing faster than processing (178 unconsumed)

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
MAX_REFINEMENT_PER_CORE = 5  # v6.2.1: was 5 — faster exhaustion, queue was growing unbounded
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
OPTIMIZE_VARIANTS = 5

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
    # v6.2.1: Untapped data categories
    "vector_data",
    "model_data",
    "event_driven",
]

FAMILY_BASE_WEIGHTS = {
    # v6.2.1: Weights based on PORTFOLIO-ADDITIVE potential, not individual Sharpe.
    # The portfolio is saturated with price_returns and model77 — every new alpha in those
    # categories hurts score even if it passes individual checks. Options vol is the proven
    # portfolio-additive signal (all 4 overnight winners used IV/parkinson ratio).
    "model77_anomaly": 0.10,    # DEAD — standalone fields don't work
    "model77_combo": 0.80,      # v6.2.1: was 4.00 — 82 sims, 0% submit, hurts portfolio score
    "expanded_fundamental": 0.40,  # v6.2.1: was 0.80 — saturated data category
    "relationship": 0.10,       # v6.2.1: 60 sims, S=0.10, 0% submit — DEAD, wasting sims
    "risk_beta": 0.10,          # v6.2.1: 12 sims, S=-0.05, 0% submit — DEAD
    "analyst_estimates": 1.50,  # ae_01 works but saturating
    "wq_proven": 1.50,          # wp_05 works (23 eligible), wp_02 works (4 eligible)
    # v5.8: Multi-factor combinations
    "combo_factor": 1.50,       # v6.2.1: was 2.00 — many are model77 variants underneath
    # SATURATED — 9+ submissions are returns mean reversion variants
    "mean_reversion": 0.05,
    "cross_sectional": 0.05,
    "liquidity_scaled": 0.08,
    "conditional": 0.10,
    "vol_adjusted": 0.08,
    # v6.2.1: PORTFOLIO-ADDITIVE — boost genuinely different data categories
    "fundamental_value": 0.50,  # v6.2.1: was 1.00 — saturated with fundamentals
    "quality_trend": 0.60,
    "fundamental_scores": 0.10, # v6.2.1: 11 sims, S=-0.07 — DEAD
    "earnings_momentum": 1.80,  # v6.2.1: was 1.20 — some signal, underexplored standalone
    "options_vol": 3.50,        # v6.2.1: was 1.00 — PROVEN portfolio-additive (all 4 winners)
    "news_sentiment": 0.50,     # v6.2.1: 8 sims, S=-0.01 — weak, reduce exploration
    "vol_regime": 0.60,
    "size_value": 0.40,
    # MEDIUM — legacy
    "volume_flow": 0.20,
    "price_vol_corr": 0.20,
    "analyst_sentiment": 2.00,  # v6.2.1: was 0.30 — works in combos (snt1_d1 signals)
    "volatility": 0.15,
    "intraday": 0.05,           # v6.2.1: 14 sims, S=-0.66 — DEAD
    "fundamental": 0.10,
    "momentum": 0.05,
    # v6.2.1: UNTAPPED DATA — virtually zero correlation with existing portfolio
    "vector_data": 3.00,    # vec_* operators on social/news vectors — proven S=1.94
    "model_data": 0.01,     # v6.2.1: ALL mdf_*, mdl175_* fields DEAD on this account
    "event_driven": 0.01,   # v6.2.1: ALL fnd6_*, fam_* fields DEAD on this account
    # v6.2.1: NEW FAMILIES — 10 completely untapped data categories
    "supply_chain": 4.00,       # pv13_* — 14 sims S=0.34, still exploring, academic backing
    "ravenpack_cat": 3.50,      # rp_ess_* — 14 sims, rp_04 averaging S=1.07, PROMISING
    "options_analytics": 0.10,  # v6.2.1: 8 sims, S=-0.15 — DEAD
    "hist_vol": 3.00,           # v6.2.1: 14 sims, S=0.62, F=0.75 — WORKING, boost
    "fscore": 0.10,             # v6.2.1: 10 sims, S=-0.28 — DEAD
    "risk_metrics": 0.10,       # v6.2.1: 15 sims, S=0.13 — barely alive, suppress
    "intraday_pattern": 0.50,   # v6.2.1: 8 sims, S=0.17 — weak but not dead
    "analyst_deep": 0.10,       # v6.2.1: 6 sims, S=-0.22 — DEAD
    "social_scalar": 0.10,      # v6.2.1: 1 sim, S=-0.55 — looks dead
    "wild_combos": 0.10,        # v6.2.1: 7 sims, S=-0.23 — DEAD
    "tutorial_proven": 3.00,    # v6.2.1: expressions from WQ official tutorial
    "high_sharpe": 5.00,        # v6.2.1: research-proven S>2.0 patterns — HIGHEST PRIORITY
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
    # v6.2.1: wp_05 core (operating_income/cap) has 3+ submissions — always blocked by CORE_OVERLAP
    "wp_05": 0.01,
    # v6.2.1: HARD PRUNE from 600+ sim analysis
    "llm_rela": 0.01,    # 23 sims, S=-0.05, hard_prune — relationship family LLM waste
    "llm_cros": 0.01,    # 8 sims, S=-0.31, hard_prune — cross-sectional LLM waste
    "llm_llm_": 0.05,    # 11 sims, S=0.06, hard_prune — generic LLM waste
    "rel_01": 0.01,       # rank(rel_ret_cust) — S=-0.64
    "rel_02": 0.01,       # -rank(rel_ret_comp) — S=-0.20
    "fs_06": 0.01,        # composite_factor_score_derivative — S=-0.08
    "fs_08": 0.01,        # analyst_revision * cap — S=-0.33
}

# v5.9.1: Boost model77_combo templates (near-passers at S=1.47)
PREFERRED_TEMPLATE_BOOSTS.update({
    "m7c_03": 1.50,      # v6.0: reduced — near-passers but never cracks fitness, Optuna will handle
    "m7c_02": 1.50,      # combo with -rank(returns)
    "m7c_01": 1.40,
    "m7c_04": 1.40,
    # v6.2.1: Boost promising templates from latest 600+ sim data
    "rp_04": 2.50,       # ravenpack insider × vwap reversion — S=1.07 across 4 sims, VERY promising
    "hv_02": 2.00,       # vol regime conditional — S=0.86, F=1.26, best fitness in bot
    "combo_3s": 2.00,    # 3-signal combos — S=1.15, F=1.29 average
    "combo_2s": 1.80,    # 2-signal combos — S=1.41, 12% submit rate
    "hs_01": 2.00,       # -ts_zscore(EV/EBITDA) — S=1.06, getting close
    "hs_10": 1.80,       # price/book zscore — S=0.90 early
    "tut_09": 1.50,      # OEY ts_rank — S=0.88, F=0.71 first sim
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
    # v6.2.1: PORTFOLIO-ADDITIVE templates — highest priority new templates
    # Options vol (the proven winner — all 4 overnight +score alphas used IV/parkinson)
    "opt_10": 3.00,      # IV/parkinson standalone — the core winning signal
    "opt_11": 3.00,      # IV/parkinson group_rank
    "opt_12": 3.50,      # IV/parkinson × sentiment — the +48 pattern
    "opt_13": 3.50,      # IV/parkinson × fundamentals cross-category
    "opt_14": 2.50,      # Options term structure (long-short IV spread)
    "opt_15": 2.50,      # Options term structure × liquidity
    "opt_16": 2.00,      # PCR mean reversion
    # News sentiment (untapped, likely uncorrelated)
    "ns_09": 2.50,       # News × reversion
    "ns_10": 2.50,       # RavenPack earnings × reversion
    "ns_11": 2.50,       # Sentiment × liquidity × reversion
    "ns_12": 2.00,       # Buzz acceleration × sentiment
    # Risk beta cross-category
    "rb_08": 2.00,       # Beta × fundamentals
    "rb_09": 2.00,       # Beta + analyst estimates
    "rb_10": 1.80,       # Unsystematic risk × profitability
    "rb_11": 2.20,       # Beta × options vol (two additive categories!)
    # Intraday cross-category
    "iday_06": 2.00,     # Candle body × sentiment
    "iday_07": 1.80,     # Range zscore × liquidity
    "iday_08": 1.80,     # Industry-relative close position
    # Analyst sentiment cross-category
    "ans_04": 2.50,      # Sentiment × reversion (proven in +46 resim)
    "ans_05": 2.50,      # Earnings surprise × value
    "ans_06": 2.50,      # Analyst rating × vwap reversion
    # v6.2.1: UNTAPPED DATA templates — highest priority exploration
    "vec_01": 3.50,      # Proven buzz pattern (S=1.94)
    "vec_02": 3.00,      # Sentiment vector
    "vec_03": 3.00,      # Buzz count × reversion
    "vec_04": 2.50,      # News significance
    "vec_05": 3.00,      # News × reversion cross-category
    "vec_06": 3.00,      # Buzz × sentiment interaction
    "vec_07": 2.50,      # Buzz IR
    "vec_08": 3.00,      # scl15 sentiment × reversion
    "mdf_01": 2.50,      # Piotroski score
    "mdf_02": 3.00,      # Piotroski × reversion
    "mdf_03": 2.50,      # Operating earnings yield
    "mdf_04": 2.50,      # OEY group_rank
    "mdf_05": 3.50,      # Proven eg3 pattern (S=1.59)
    "mdf_06": 2.50,      # R&D intensity
    "mdf_07": 2.00,      # Model P/B
    "mdf_08": 2.50,      # Gross margin from mdl175
    "evt_01": 3.50,      # Proven forward EPS pattern (S=2.03)
    "evt_02": 3.00,      # Forward EPS / price
    "evt_03": 3.00,      # EPS revision freshness
    "evt_04": 2.50,      # Earnings surprise pct
    "evt_05": 3.00,      # Surprise × reversion
    "evt_06": 2.50,      # ROE rank × reversion
    "evt_07": 3.00,      # Event timing × fundamental
    # Multi-period (structurally different — short+long agreement)
    "fv_08": 2.00,       # Multi-period fundamental 22×252
    "fv_09": 2.00,       # Multi-period fundamental 60×252
    # Academic anomalies (proven in research)
    "ef_19": 2.50,       # Accrual anomaly — Sloan 1996
    "ef_20": 2.50,       # Balance sheet accrual
    "ef_21": 2.50,       # Retained earnings reversion — proven S=1.55
    "ef_22": 2.50,       # Investment anomaly — Titman et al
    # Pasteurize-wrapped + news conditional
    "vec_09": 2.50,      # Pasteurize buzz
    "vec_10": 2.50,      # Pasteurize news × reversion
    "vec_11": 3.50,      # News-conditional regime — proven S=1.84
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
# v6.2.1: Bumped to 0.30 — LLM now has portfolio-additive focus prompt
LLM_GENERATION_PROBABILITY = 0.30

# v6.2.1: Signal combination — DOUBLED from 0.10 — combos with additive bias are the
# most likely path to positive score changes
COMBO_GENERATION_PROBABILITY = 0.20

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
    # v6.2.1: Options — higher decay proven by +37 score winner (TOP3000/MARKET/d10)
    "options_vol": {"universes": ["TOP3000", "TOP1000"], "neutralizations": ["MARKET", "INDUSTRY", "NONE"], "decays": [6, 8, 10], "truncations": [0.05, 0.08]},
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
    # v6.2.1: Added missing settings profiles
    "analyst_sentiment": {"universes": ["TOP3000", "TOP1000"], "neutralizations": ["MARKET", "INDUSTRY", "NONE"], "decays": [4, 6, 8], "truncations": [0.05, 0.08]},
    "intraday": {"universes": ["TOP3000", "TOP1000"], "neutralizations": ["MARKET", "INDUSTRY"], "decays": [4, 6, 8], "truncations": [0.05, 0.08]},
    # v6.2.1: New untapped data families
    "vector_data": {"universes": ["TOP3000", "TOP1000"], "neutralizations": ["SUBINDUSTRY", "MARKET"], "decays": [4, 6, 8, 10], "truncations": [0.05, 0.08]},
    "model_data": {"universes": ["TOP3000", "TOP1000"], "neutralizations": ["MARKET", "INDUSTRY", "SUBINDUSTRY"], "decays": [4, 6, 8], "truncations": [0.05, 0.08]},
    "event_driven": {"universes": ["TOP3000", "TOP1000"], "neutralizations": ["INDUSTRY", "SUBINDUSTRY"], "decays": [2, 4, 6], "truncations": [0.05, 0.08]},
    # v6.2.1: 10 NEW untapped data families
    "supply_chain": {"universes": ["TOP3000", "TOP1000"], "neutralizations": ["SUBINDUSTRY", "INDUSTRY"], "decays": [3, 5, 8], "truncations": [0.01, 0.08]},
    "ravenpack_cat": {"universes": ["TOP3000", "TOP1000"], "neutralizations": ["SUBINDUSTRY", "MARKET", "INDUSTRY"], "decays": [6, 8, 10], "truncations": [0.05, 0.08]},
    "options_analytics": {"universes": ["TOP3000", "TOP1000"], "neutralizations": ["MARKET", "SECTOR", "INDUSTRY"], "decays": [6, 8, 10], "truncations": [0.05, 0.08]},
    "hist_vol": {"universes": ["TOP3000", "TOP1000"], "neutralizations": ["MARKET", "SECTOR"], "decays": [6, 8, 10], "truncations": [0.05, 0.08]},
    "fscore": {"universes": ["TOP3000", "TOP1000"], "neutralizations": ["INDUSTRY", "SUBINDUSTRY", "MARKET"], "decays": [8, 10], "truncations": [0.08]},
    "risk_metrics": {"universes": ["TOP3000", "TOP1000"], "neutralizations": ["MARKET", "INDUSTRY", "SUBINDUSTRY"], "decays": [6, 8, 10], "truncations": [0.01, 0.05]},
    "intraday_pattern": {"universes": ["TOP3000", "TOP200"], "neutralizations": ["SECTOR", "MARKET"], "decays": [4, 6], "truncations": [0.08, 0.10]},
    "analyst_deep": {"universes": ["TOP3000", "TOP1000"], "neutralizations": ["INDUSTRY", "SUBINDUSTRY"], "decays": [6, 8], "truncations": [0.08]},
    "social_scalar": {"universes": ["TOP3000", "TOP1000"], "neutralizations": ["SUBINDUSTRY", "MARKET"], "decays": [4, 6, 8], "truncations": [0.08]},
    "wild_combos": {"universes": ["TOP3000", "TOP1000"], "neutralizations": ["MARKET", "INDUSTRY", "SUBINDUSTRY"], "decays": [6, 8, 10], "truncations": [0.05, 0.08]},
    "tutorial_proven": {"universes": ["TOP3000", "TOP1000", "TOP500"], "neutralizations": ["MARKET", "INDUSTRY", "SECTOR"], "decays": [0, 4, 6, 8], "truncations": [0.01, 0.05, 0.08]},
    "high_sharpe": {"universes": ["TOP3000", "TOP1000", "TOP500", "TOP200"], "neutralizations": ["SUBINDUSTRY", "INDUSTRY", "MARKET"], "decays": [0, 2, 4, 6, 8], "truncations": [0.01, 0.05, 0.08]},
}

# v5.7: Minimum exploration guarantee per family
MIN_EXPLORATION_PER_FAMILY = 25

# v5.9: LLM rate limit cooldown (seconds between calls)
LLM_COOLDOWN_SECONDS = 30  # v6.2.1: Reduced from 120s — key rotation handles per-key rate limits (60s each)
LLM_AST_RETRY_MAX = 1      # v6.1: retry failed expressions once with error feedback
