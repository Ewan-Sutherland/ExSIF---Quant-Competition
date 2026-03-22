from __future__ import annotations

import random
from typing import Any

import config
from canonicalize import canonicalize_expression, hash_candidate
from models import Candidate, SimulationSettings
from templates import FUNDAMENTAL_FIELDS, SAFE_PARAM_RANGES, TEMPLATE_LIBRARY


class AlphaGenerator:
    def __init__(self, seed: int | None = None):
        self.rng = random.Random(seed)

    def generate_candidate(self) -> Candidate:
        family = self._sample_family()
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

    def _sample_family(self) -> str:
        return self.rng.choice(list(TEMPLATE_LIBRARY.keys()))

    def _sample_params(self, template: str) -> dict[str, Any]:
        params: dict[str, Any] = {}

        if "{n}" in template:
            params["n"] = self.rng.choice(SAFE_PARAM_RANGES["n"])

        if "{m}" in template:
            params["m"] = self.rng.choice(SAFE_PARAM_RANGES["m"])

        if "{field}" in template:
            params["field"] = self.rng.choice(FUNDAMENTAL_FIELDS)

        return params

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

        # add common implicit fields if present in rendered expression
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