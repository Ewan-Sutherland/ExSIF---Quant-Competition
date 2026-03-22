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
DEFAULT_UNIVERSES = ["TOP3000"]
DEFAULT_NEUTRALIZATIONS = ["SUBINDUSTRY", "INDUSTRY"]
DEFAULT_DECAYS = [2, 4, 6]
DEFAULT_TRUNCATIONS = [0.08]

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
NEAR_PASSER_MIN_SHARPE = 1.55
NEAR_PASSER_MIN_FITNESS = 0.82
NEAR_PASSER_MAX_TURNOVER = 0.72
REFINEMENT_PROBABILITY = 0.45

# Submission behaviour
AUTO_SUBMIT = False

# Logging / reporting
REPORT_EVERY_N_COMPLETIONS = 50