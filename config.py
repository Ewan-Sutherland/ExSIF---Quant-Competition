from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

# Base paths
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

DB_PATH = DATA_DIR / "bot.db"

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
DEFAULT_UNIVERSES = ["TOP3000", "TOPSP500"]
DEFAULT_NEUTRALIZATIONS = ["SUBINDUSTRY", "INDUSTRY", "MARKET", "SECTOR"]
DEFAULT_DECAYS = [2, 4, 6, 8, 10, 12]
DEFAULT_TRUNCATIONS = [0.05, 0.08, 0.10]

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
REFINEMENT_PROBABILITY = 0.65

MIN_REFINEMENT_SHARPE = 1.20
FRONTIER_MIN_SHARPE = 1.25
FRONTIER_MIN_FITNESS = 0.65
FRONTIER_ALT_MIN_SHARPE = 1.45
FRONTIER_ALT_MIN_FITNESS = 0.68

# Frontier templates worth exploiting more aggressively
STRONG_TEMPLATES = {"cs_02", "pvc_04", "vol_03", "cond_01", "va_02", "mr_02", "pvc_03"}
ELITE_TEMPLATES = {"mr_04", "vol_03", "va_02", "cond_01"}

# Diversity / anti-self-correlation
DIVERSITY_LOOKBACK_RUNS = 120
MAX_RECENT_TEMPLATE_COUNT = 20
RELAXED_TEMPLATE_COUNT = 14
RELAXED_TEMPLATE_MIN_AVG_SHARPE = 1.30
RELAXED_TEMPLATE_MIN_AVG_FITNESS = 0.70
DIVERSITY_EXPLORATION_PROBABILITY = 0.12
MAX_REFINEMENT_ATTEMPTS_PER_BASE = 7
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
AUTO_SUBMIT = True

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
    "mean_reversion",
    "volume_flow",
    "conditional",
    "vol_adjusted",
    "price_vol_corr",
    "volatility",
    "intraday",
    "cross_sectional",
    "fundamental_value",
    "analyst_sentiment",
    "options_vol",
    "fundamental",
    "momentum",
]

FAMILY_BASE_WEIGHTS = {
    "mean_reversion": 0.60,      # Already submitted, reduce exploration
    "volume_flow": 0.80,         # vol_03 still near-passing
    "conditional": 1.00,         # cond_01 just passed! Keep exploring
    "vol_adjusted": 0.70,        # va_02 near-passing but exhausting refinement
    "price_vol_corr": 1.40,      # pvc_04 Sharpe 1.69 near-passer — high priority
    "volatility": 0.50,          # Only 2 templates left after removing locked ones
    "intraday": 0.60,            # Weak so far but under-explored
    "cross_sectional": 1.50,     # cs_02 PASSED! Best new family — highest priority
    "fundamental_value": 0.80,   # Moderate signal, needs more exploration
    "analyst_sentiment": 0.50,   # Weak signal so far
    "options_vol": 0.70,         # Had field errors, now fixed
    "fundamental": 0.20,         # Consistently weak
    "momentum": 0.05,            # Dead
}

TEMPLATE_BASE_WEIGHTS = {
    "cs_02": 1.50,      # avg_sharpe=1.279, PRODUCED ELIGIBLE — top priority
    "pvc_04": 1.35,     # Sharpe 1.690 near-passer
    "vol_03": 1.20,     # avg_sharpe=1.060, consistent near-passer
    "mr_02": 1.15,      # avg_sharpe=1.063
    "cond_01": 1.10,    # PRODUCED ELIGIBLE
    "va_02": 1.05,      # avg_sharpe=0.945
    "mr_04": 0.90,      # Already submitted family
    "pvc_03": 1.00,     # avg_sharpe=0.820, decent
    "mr_01": 0.80,
}

PREFERRED_TEMPLATE_BOOSTS = {
    "mr_04": 1.35,
    "vol_03": 1.40,
    "va_02": 1.20,
    "mr_01": 1.15,
    "cond_01": 1.10,
}

TEMPLATE_WEIGHT_PENALTIES = {
    "va_01": 0.35,
    "vol_02": 0.30,
    "cond_03": 0.40,
}

DISABLED_REFINEMENT_TEMPLATES = {"vol_02"}
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
