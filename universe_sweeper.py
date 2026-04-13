"""
v6.2.1: Universe Sweeper — automatically test eligible alphas on all universes.

When an alpha passes on TOP3000/MARKET/decay=4, this module queues the same
expression for testing on TOP1000, TOP500, TOP200, TOPSP500 etc with multiple
settings variants per universe. The tutorial confirms that different universes
produce uncorrelated submissions.

Swept alphas go through the FULL normal pipeline:
  eligible check → resim → score change → self-correlation → ready_alphas
Nothing is auto-submitted.

Integration:
  - bot.py calls sweeper.queue_sweep() after staging/submitting an eligible alpha
  - bot.tick() calls sweeper.try_sweep() to submit one sweep sim per tick
  - Sweep results go through the normal evaluation pipeline
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any


# ALL universes to sweep — bidirectional (if alpha passes on TOP200, test TOP3000 too)
SWEEP_UNIVERSES = ["TOP3000", "TOP1000", "TOP500", "TOP200", "TOPSP500"]

# Per-universe settings variants to try
# Each universe gets 2 variants: (neutralization, decay) pairs
# Smaller universes need tighter neutralization + higher decay for turnover control
UNIVERSE_VARIANTS = {
    "TOP3000": [
        {"neutralization": "NONE", "decay": 0},
        {"neutralization": "MARKET", "decay": 4},
        {"neutralization": "SUBINDUSTRY", "decay": 6},
        {"neutralization": "MARKET", "decay": 10},
    ],
    "TOP1000": [
        {"neutralization": "NONE", "decay": 4},
        {"neutralization": "MARKET", "decay": 6},
        {"neutralization": "SUBINDUSTRY", "decay": 8},
        {"neutralization": "INDUSTRY", "decay": 10},
    ],
    "TOP500": [
        {"neutralization": "NONE", "decay": 6},
        {"neutralization": "SUBINDUSTRY", "decay": 6},
        {"neutralization": "INDUSTRY", "decay": 8},
        {"neutralization": "SUBINDUSTRY", "decay": 10},
    ],
    "TOP200": [
        {"neutralization": "SUBINDUSTRY", "decay": 8},
        {"neutralization": "INDUSTRY", "decay": 10},
        {"neutralization": "SUBINDUSTRY", "decay": 12},
    ],
    "TOPSP500": [
        {"neutralization": "NONE", "decay": 0},
        {"neutralization": "MARKET", "decay": 6},
        {"neutralization": "SUBINDUSTRY", "decay": 8},
        {"neutralization": "INDUSTRY", "decay": 10},
    ],
}


@dataclass
class SweepJob:
    """A pending sweep: same expression on a different universe + settings."""
    expression: str
    settings: dict[str, Any]  # full settings dict ready to submit
    family: str
    template_id: str
    source_alpha_id: str
    created_at: float = field(default_factory=time.time)


class UniverseSweeper:
    """Manages universe sweep queue for eligible alphas."""

    # v7.2: Sweep budget — 50 per window, resets every 6 hours for overnight runs
    SWEEP_BUDGET_PER_WINDOW = 50
    SWEEP_WINDOW_SECONDS = 6 * 3600  # 6 hours

    def __init__(self, storage, client):
        import time
        self.storage = storage
        self.client = client
        self._queue: list[SweepJob] = []
        self._swept: set[str] = set()  # "expr_key:universe:neut:decay" → already done
        self._max_queue = 500
        self._sweep_count = 0
        self._sweep_window_start = time.time()

    def _make_key(self, expr: str, universe: str, neut: str, decay: int) -> str:
        # v7.1: Use hash for compact storage — prevents Supabase JSON truncation
        # with 500+ swept pairs (~40KB as full strings → ~8KB as hashes)
        import hashlib
        raw = f"{expr}:{universe}:{neut}:{decay}"
        return hashlib.md5(raw.encode()).hexdigest()[:12]

    def queue_sweep(
        self,
        expression: str,
        settings: dict[str, Any],
        family: str = "",
        template_id: str = "",
        alpha_id: str = "",
    ) -> int:
        """
        Queue an eligible alpha for testing on all untested universe+settings combos.
        Returns number of sweep jobs queued.
        """
        # v7.0: Validate expression fields against this bot's dataset
        # Prevents sweeping expressions that use fields from other teammates' datasets
        try:
            from datasets import get_all_valid_fields
            valid = get_all_valid_fields()
            # Extract field-like tokens from expression (lowercase words without parens)
            import re
            tokens = set(re.findall(r'[a-z][a-z0-9_]+', expression.lower()))
            # Known operators/keywords to skip
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
                'kth_element', 'range', 'true', 'false',
                'industry', 'subindustry', 'sector', 'market', 'exchange',
                # v7.2: keyword args + logical ops that appear as tokens
                'rettype', 'lag', 'std', 'not', 'and', 'or', 'filter',
                'days_from_last_change', 'last_diff_value',
                'multiply', 'add', 'subtract', 'divide',
            }
            field_tokens = tokens - operators
            # Check for fields not in our valid set
            valid_lower = {f.lower() for f in valid}
            missing = [t for t in field_tokens if t not in valid_lower and len(t) > 3]
            if missing:
                print(f"[SWEEP_FIELD_BLOCK] expression uses fields not in dataset: {missing[:3]} — skipping sweep")
                return 0
        except Exception:
            pass  # If validation fails, allow sweep (better to try than block everything)

        original_universe = settings.get("universe", "TOP3000")
        original_neut = settings.get("neutralization", "MARKET")
        original_decay = int(settings.get("decay", 4))

        # Mark original settings as swept
        orig_key = self._make_key(expression, original_universe, original_neut, original_decay)
        self._swept.add(orig_key)

        queued = 0

        for universe in SWEEP_UNIVERSES:
            variants = UNIVERSE_VARIANTS.get(universe, [])

            for variant in variants:
                neut = variant["neutralization"]
                decay = variant["decay"]

                # Skip if this is the exact original settings
                if universe == original_universe and neut == original_neut and decay == original_decay:
                    continue

                sweep_key = self._make_key(expression, universe, neut, decay)
                if sweep_key in self._swept:
                    continue

                if len(self._queue) >= self._max_queue:
                    break

                # Build full settings
                sweep_settings = {
                    "region": settings.get("region", "USA"),
                    "universe": universe,
                    "delay": int(settings.get("delay", 1)),
                    "decay": decay,
                    "neutralization": neut,
                    "truncation": float(settings.get("truncation", 0.08)),
                }

                job = SweepJob(
                    expression=expression,
                    settings=sweep_settings,
                    family=family,
                    template_id=template_id,
                    source_alpha_id=alpha_id,
                )
                self._queue.append(job)
                self._swept.add(sweep_key)
                queued += 1

        if queued:
            print(
                f"[SWEEP_QUEUED] {queued} universe+settings sweeps for "
                f"{template_id or 'alpha'} (original={original_universe}/{original_neut}/d{original_decay})"
            )

        return queued

    def try_sweep(self) -> dict[str, Any] | None:
        """
        Pop one sweep job and return it as a candidate dict for simulation.
        Returns None if queue empty or budget exhausted.
        """
        if not self._queue:
            return None

        # v7.2: Sweep budget — reset every 6 hours for overnight runs
        import time
        if time.time() - self._sweep_window_start >= self.SWEEP_WINDOW_SECONDS:
            self._sweep_count = 0
            self._sweep_window_start = time.time()
            print(f"[SWEEP_RESET] Budget reset — {len(self._queue)} sweeps in queue")

        if self._sweep_count >= self.SWEEP_BUDGET_PER_WINDOW:
            if self._sweep_count == self.SWEEP_BUDGET_PER_WINDOW:
                print(f"[SWEEP_BUDGET] {self._sweep_count} sweeps done — pausing sweeps until next window, {len(self._queue)} remaining in queue")
                self._sweep_count += 1  # prevent repeat message
            return None

        job = self._queue.pop(0)

        return {
            "expression": job.expression,
            "settings": job.settings,
            "family": job.family,
            "template_id": f"sweep_{job.template_id}",
            "is_sweep": True,
            "source_alpha_id": job.source_alpha_id,
        }

    def count_sweep(self) -> None:
        """Increment sweep count — call only when a sweep is actually submitted."""
        self._sweep_count += 1

    @property
    def pending(self) -> int:
        return len(self._queue)

    @property
    def total_sweeps(self) -> int:
        return self._sweep_count

    def load_already_swept(self, submitted_alphas: list[dict]) -> None:
        """
        On startup, mark expression:universe:neut:decay combos that have already been
        submitted so we don't re-sweep them.
        """
        for alpha in submitted_alphas:
            # RPC returns canonical_expression, not expression
            expr = alpha.get("canonical_expression", "") or alpha.get("expression", "")
            settings_json = alpha.get("settings_json", "{}")
            if isinstance(settings_json, str):
                try:
                    settings = json.loads(settings_json)
                except (json.JSONDecodeError, TypeError):
                    settings = {}
            else:
                settings = settings_json or {}

            if not expr or not settings:
                continue

            universe = settings.get("universe", "TOP3000")
            neut = settings.get("neutralization", "MARKET")
            decay = int(settings.get("decay", 4))
            self._swept.add(self._make_key(expr, universe, neut, decay))

        print(f"[SWEEP] Loaded {len(self._swept)} already-swept expression:settings pairs")
