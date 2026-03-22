from __future__ import annotations

import json
import random
from typing import Any

import config
from canonicalize import canonicalize_expression, hash_candidate
from models import Candidate, SimulationSettings
from templates import FUNDAMENTAL_FIELDS, SAFE_PARAM_RANGES, TEMPLATE_LIBRARY


FAMILY_WEIGHTS = {
    "mean_reversion": 3.0,
    "momentum": 1.0,
    "volume_flow": 3.0,
    "vol_adjusted": 2.0,
    "fundamental": 0.4,
    "conditional": 3.5,
}


class AlphaGenerator:
    def __init__(self, seed: int | None = None):
        self.rng = random.Random(seed)

    def generate_candidate(self) -> Candidate:
        family = self._sample_family_weighted()
        template_entry = self.rng.choice(TEMPLATE_LIBRARY[family])

        params = self._sample_params(template_entry["expression"])
        expression, fields_used = self._render_expression(
            template_entry["expression"],
            params,
        )

        settings = self._sample_settings()
        canonical_expression = canonicalize_expression(expression)
        expression_hash = hash_candidate(canonical_expression, settings.to_dict())

        return Candidate.create(
            expression=expression,
            canonical_expression=canonical_expression,
            expression_hash=expression_hash,
            template_id=template_entry["template_id"],
            family=family,
            fields=fields_used,
            params=params,
            settings=settings,
        )

    def mutate_candidate(self, base_row) -> Candidate:
        family = base_row["family"]
        template_id = base_row["template_id"]

        template_entry = None
        for entry in TEMPLATE_LIBRARY[family]:
            if entry["template_id"] == template_id:
                template_entry = entry
                break

        if template_entry is None:
            return self.generate_candidate()

        params = json.loads(base_row["params_json"])
        settings_dict = json.loads(base_row["settings_json"])

        mutated_params = self._mutate_params(params, template_entry["expression"])
        mutated_settings = self._mutate_settings(settings_dict)

        expression, fields_used = self._render_expression(
            template_entry["expression"],
            mutated_params,
        )

        settings = SimulationSettings(**mutated_settings)
        canonical_expression = canonicalize_expression(expression)
        expression_hash = hash_candidate(canonical_expression, settings.to_dict())

        return Candidate.create(
            expression=expression,
            canonical_expression=canonical_expression,
            expression_hash=expression_hash,
            template_id=template_entry["template_id"],
            family=family,
            fields=fields_used,
            params=mutated_params,
            settings=settings,
        )

    def _sample_family_weighted(self) -> str:
        families = list(TEMPLATE_LIBRARY.keys())
        weights = [FAMILY_WEIGHTS.get(f, 1.0) for f in families]
        return self.rng.choices(families, weights=weights, k=1)[0]

    def _sample_params(self, template: str) -> dict[str, Any]:
        params: dict[str, Any] = {}

        if "{n}" in template:
            params["n"] = self.rng.choice(SAFE_PARAM_RANGES["n"])

        if "{m}" in template:
            params["m"] = self.rng.choice(SAFE_PARAM_RANGES["m"])

        if "{field}" in template:
            params["field"] = self.rng.choice(FUNDAMENTAL_FIELDS)

        return params

    def _mutate_params(self, params: dict[str, Any], template: str) -> dict[str, Any]:
        mutated = dict(params)

        if "{n}" in template and "n" in mutated:
            mutated["n"] = self._mutate_from_grid(
                current=mutated["n"],
                grid=SAFE_PARAM_RANGES["n"],
            )

        if "{m}" in template and "m" in mutated:
            mutated["m"] = self._mutate_from_grid(
                current=mutated["m"],
                grid=SAFE_PARAM_RANGES["m"],
            )

        if "{field}" in template and "field" in mutated:
            if self.rng.random() < 0.25:
                mutated["field"] = self.rng.choice(FUNDAMENTAL_FIELDS)

        return mutated

    def _mutate_settings(self, settings: dict[str, Any]) -> dict[str, Any]:
        mutated = dict(settings)

        if "decay" in mutated:
            mutated["decay"] = self._mutate_from_grid(
                current=mutated["decay"],
                grid=config.DEFAULT_DECAYS,
            )

        if "neutralization" in mutated and self.rng.random() < 0.35:
            mutated["neutralization"] = self.rng.choice(config.DEFAULT_NEUTRALIZATIONS)

        if "truncation" in mutated and self.rng.random() < 0.20:
            mutated["truncation"] = self.rng.choice(config.DEFAULT_TRUNCATIONS)

        return mutated

    def _mutate_from_grid(self, current: Any, grid: list[Any]) -> Any:
        if current not in grid:
            return self.rng.choice(grid)

        idx = grid.index(current)
        candidates = [current]

        if idx > 0:
            candidates.append(grid[idx - 1])
        if idx < len(grid) - 1:
            candidates.append(grid[idx + 1])

        return self.rng.choice(candidates)

    def _render_expression(
        self,
        template: str,
        params: dict[str, Any],
    ) -> tuple[str, list[str]]:
        expression = template.format(**params)

        fields_used: list[str] = []
        for field_name in ("field",):
            if field_name in params:
                fields_used.append(str(params[field_name]))

        for implicit_field in ("close", "returns", "volume"):
            if implicit_field in expression and implicit_field not in fields_used:
                fields_used.append(implicit_field)

        return expression, fields_used

    def _sample_settings(self) -> SimulationSettings:
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