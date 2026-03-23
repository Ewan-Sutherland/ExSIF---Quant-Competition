from __future__ import annotations
import json
import random
from typing import Any

import config
from canonicalize import canonicalize_expression, hash_candidate
from models import Candidate, SimulationSettings
from templates import FUNDAMENTAL_FIELDS, SAFE_PARAM_RANGES, TEMPLATE_LIBRARY


BASE_FAMILY_WEIGHTS = {
    "mean_reversion": 4.8,
    "momentum": 0.01,
    "volume_flow": 1.8,
    "vol_adjusted": 0.6,
    "fundamental": 0.01,
    "conditional": 1.9,
}


class AlphaGenerator:
    def __init__(self, seed: int | None = None):
        self.rng = random.Random(seed)

    # ============================
    # PUBLIC
    # ============================

    def generate_candidate(self, family_bias=None, template_bias=None):
        family = self._sample_family(family_bias)
        template = self._sample_template(family, template_bias)

        params = self._sample_params(template["expression"])
        expr, fields = self._render(template["expression"], params)

        expr = self._post_process(expr)

        settings = self._sample_settings(family)
        canon = canonicalize_expression(expr)
        h = hash_candidate(canon, settings.to_dict())

        return Candidate.create(
            expression=expr,
            canonical_expression=canon,
            expression_hash=h,
            template_id=template["template_id"],
            family=family,
            fields=fields,
            params=params,
            settings=settings,
        )

    def mutate_candidate(self, row):
        family = row["family"]
        template_id = row["template_id"]

        template = next(
            (t for t in TEMPLATE_LIBRARY[family] if t["template_id"] == template_id),
            None,
        )
        if template is None:
            return self.generate_candidate()

        params = json.loads(row["params_json"])
        settings = json.loads(row["settings_json"])

        params = self._mutate_params(params, template["expression"])
        settings = self._mutate_settings(settings)

        expr, fields = self._render(template["expression"], params)
        expr = self._post_process(expr, light=True)

        sim = SimulationSettings(**settings)
        canon = canonicalize_expression(expr)
        h = hash_candidate(canon, sim.to_dict())

        return Candidate.create(
            expression=expr,
            canonical_expression=canon,
            expression_hash=h,
            template_id=template_id,
            family=family,
            fields=fields,
            params=params,
            settings=sim,
        )

    # ============================
    # SAMPLING
    # ============================

    def _sample_family(self, bias):
        fams = list(TEMPLATE_LIBRARY.keys())
        weights = []

        for f in fams:
            w = BASE_FAMILY_WEIGHTS.get(f, 1.0)
            if bias:
                w *= bias.get(f, 1.0)
            weights.append(max(w, 0.001))

        return self.rng.choices(fams, weights=weights, k=1)[0]

    def _sample_template(self, family, bias):
        templates = TEMPLATE_LIBRARY[family]

        if not bias:
            return self.rng.choice(templates)

        weights = [
            max(bias.get(t["template_id"], 1.0), 0.001) for t in templates
        ]
        return self.rng.choices(templates, weights=weights, k=1)[0]

    # ============================
    # POST PROCESSING (FIXED)
    # ============================

    def _post_process(self, expr, light=False):
        # Light smoothing only
        if self.rng.random() < (0.2 if not light else 0.1):
            if not expr.startswith("ts_mean"):
                w = self.rng.choice([3, 5, 10])
                expr = f"ts_mean(rank({expr}), {w})"

        # Optional outer rank
        if self.rng.random() < 0.1:
            if not expr.startswith("rank("):
                expr = f"rank({expr})"

        return expr

    # ============================
    # PARAMS
    # ============================

    def _grid(self, key):
        desired = [3, 5, 10, 20, 40, 60]
        allowed = SAFE_PARAM_RANGES.get(key, desired)
        return [x for x in desired if x in allowed] or list(allowed)

    def _sample_params(self, template):
        p = {}
        if "{n}" in template:
            p["n"] = self.rng.choice(self._grid("n"))
        if "{m}" in template:
            p["m"] = self.rng.choice(self._grid("m"))
        if "{field}" in template:
            p["field"] = self.rng.choice(FUNDAMENTAL_FIELDS)
        return p

    def _mutate_params(self, params, template):
        out = dict(params)
        grid = self._grid("n")

        if "n" in out:
            out["n"] = self._mutate(out["n"], grid)

        if "m" in out:
            out["m"] = self._mutate(out["m"], grid)

        if "field" in out and self.rng.random() < 0.1:
            out["field"] = self.rng.choice(FUNDAMENTAL_FIELDS)

        return out

    def _mutate(self, val, grid):
        if val not in grid:
            return self.rng.choice(grid)

        if self.rng.random() < 0.4:
            return val

        i = grid.index(val)
        choices = []
        if i > 0:
            choices.append(grid[i - 1])
        if i < len(grid) - 1:
            choices.append(grid[i + 1])

        return self.rng.choice(choices or [val])

    # ============================
    # SETTINGS
    # ============================

    def _mutate_settings(self, s):
        out = dict(s)

        if "decay" in out:
            out["decay"] = self._mutate(out["decay"], config.DEFAULT_DECAYS)

        if "neutralization" in out and self.rng.random() < 0.2:
            out["neutralization"] = self.rng.choice(config.DEFAULT_NEUTRALIZATIONS)

        return out

    def _sample_settings(self, family):
        return SimulationSettings(
            region=config.DEFAULT_REGION,
            universe=self.rng.choice(config.DEFAULT_UNIVERSES),
            delay=config.DEFAULT_DELAY,
            decay=self.rng.choice(config.DEFAULT_DECAYS),
            neutralization=self.rng.choice(config.DEFAULT_NEUTRALIZATIONS),
            truncation=self.rng.choice(config.DEFAULT_TRUNCATIONS),
            pasteurization=config.DEFAULT_PASTEURIZATION,
            unit_handling=config.DEFAULT_UNIT_HANDLING,
            nan_handling=config.DEFAULT_NAN_HANDLING,
            max_stock_weight=config.DEFAULT_MAX_STOCK_WEIGHT,
            language=config.DEFAULT_LANGUAGE,
        )

    # ============================
    # RENDER
    # ============================

    def _render(self, template, params):
        expr = template.format(**params)
        fields = []

        for k in ["field"]:
            if k in params:
                fields.append(params[k])

        for f in ["close", "returns", "volume"]:
            if f in expr and f not in fields:
                fields.append(f)

        return expr, fields