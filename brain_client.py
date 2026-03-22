from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import requests

import config


class BrainAPIError(Exception):
    pass


class BrainAuthError(BrainAPIError):
    pass


class BrainClient:
    """
    Configurable WorldQuant Brain client.

    Notes:
    - submit_simulation() is intentionally non-blocking.
    - polling is handled separately by the bot/scheduler.
    - this version fixes the settings payload shape:
        * adds instrumentType
        * adds visualization
        * maps snake_case -> Brain camelCase
        * removes maxStockWeight from submitted payload
    """

    def __init__(
        self,
        username: str,
        password: str,
        base_url: str,
        login_path: str = "/authentication",
        simulation_path: str = "/simulations",
        timeout_seconds: int = 30,
    ):
        self.username = username
        self.password = password
        self.base_url = base_url.rstrip("/")
        self.login_path = login_path
        self.simulation_path = simulation_path
        self.timeout_seconds = timeout_seconds

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

        self.last_auth_time: Optional[datetime] = None

    def login(self) -> None:
        if not self.username or not self.password:
            raise BrainAuthError(
                "Missing BRAIN_USERNAME or BRAIN_PASSWORD environment variables."
            )

        url = f"{self.base_url}{self.login_path}"

        response = self.session.post(
            url,
            auth=(self.username, self.password),
            timeout=self.timeout_seconds,
        )

        if response.status_code not in (200, 201, 204):
            raise BrainAuthError(
                f"Login failed with status {response.status_code}: {response.text}"
            )

        self.last_auth_time = datetime.now(timezone.utc)

    def ensure_session(self) -> None:
        if self.last_auth_time is None:
            self.login()
            return

        age = datetime.now(timezone.utc) - self.last_auth_time
        if age > timedelta(minutes=config.SESSION_REFRESH_MINUTES):
            self.login()

    def submit_simulation(self, expression: str, settings: dict[str, Any]) -> str:
        """
        Submit a simulation and return a simulation id or progress URL/id.
        Does not block for completion.
        """
        self.ensure_session()

        url = f"{self.base_url}{self.simulation_path}"
        payload = self._build_simulation_payload(expression, settings)

        response = self.session.post(
            url,
            json=payload,
            timeout=self.timeout_seconds,
        )

        if response.status_code == 401:
            self.login()
            response = self.session.post(
                url,
                json=payload,
                timeout=self.timeout_seconds,
            )

        if response.status_code not in (200, 201, 202):
            raise BrainAPIError(
                f"Simulation submit failed with status {response.status_code}: {response.text}"
            )

        sim_id = self._extract_simulation_id(response)
        if not sim_id:
            raise BrainAPIError(
                "Simulation submission succeeded but no simulation id could be extracted."
            )

        return sim_id

    def poll_simulation(self, sim_id: str) -> dict[str, Any]:
        """
        Poll a simulation by id/url and return a normalized result dict.

        Returned dict always includes:
        - status
        - raw
        """
        self.ensure_session()

        url = self._simulation_status_url(sim_id)
        response = self.session.get(url, timeout=self.timeout_seconds)

        if response.status_code == 401:
            self.login()
            response = self.session.get(url, timeout=self.timeout_seconds)

        if response.status_code != 200:
            raise BrainAPIError(
                f"Polling failed for sim_id={sim_id} with status {response.status_code}: {response.text}"
            )

        raw = self._parse_json(response)
        status = self._extract_status(raw)

        result = {
            "status": status,
            "raw": raw,
        }

        if status == "completed":
            alpha_id = raw.get("alpha")
            result["alpha_id"] = alpha_id

            if alpha_id:
                try:
                    alpha_data = self.get_alpha(alpha_id)
                    result["alpha_data"] = alpha_data
                    result.update(self._extract_metrics_from_alpha(alpha_data))
                    result["checks_passed"] = self._infer_checks_passed_from_alpha(alpha_data)
                except Exception as exc:
                    result["alpha_fetch_error"] = str(exc)
                    result.update(
                        {
                            "sharpe": None,
                            "fitness": None,
                            "turnover": None,
                            "returns": None,
                            "margin": None,
                            "drawdown": None,
                            "checks_passed": True,
                        }
                    )
            else:
                result.update(
                    {
                        "sharpe": None,
                        "fitness": None,
                        "turnover": None,
                        "returns": None,
                        "margin": None,
                        "drawdown": None,
                        "checks_passed": True,
                    }
                )

        elif status == "failed":
            result["error_message"] = self._extract_error(raw)

        return result

    def submit_alpha(self, alpha_id: str) -> dict[str, Any]:
        """
        Optional final alpha submission.
        Adjust endpoint later if needed.
        """
        self.ensure_session()

        url = f"{self.base_url}/alphas/{alpha_id}/submit"
        response = self.session.post(url, timeout=self.timeout_seconds)

        if response.status_code == 401:
            self.login()
            response = self.session.post(url, timeout=self.timeout_seconds)

        if response.status_code not in (200, 201, 202):
            raise BrainAPIError(
                f"Alpha submission failed with status {response.status_code}: {response.text}"
            )

        return self._parse_json(response)

    def wait_for_completion(
        self,
        sim_id: str,
        poll_interval_seconds: int = 10,
        timeout_minutes: int = 45,
    ) -> dict[str, Any]:
        """
        Debug helper only. Production bot should not use this because it blocks.
        """
        deadline = time.time() + timeout_minutes * 60

        while time.time() < deadline:
            result = self.poll_simulation(sim_id)
            if result["status"] in {"completed", "failed", "timed_out"}:
                return result
            time.sleep(poll_interval_seconds)

        return {
            "status": "timed_out",
            "raw": {},
            "error_message": f"Polling exceeded {timeout_minutes} minutes.",
        }

    def _build_simulation_payload(
        self,
        expression: str,
        settings: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Build Brain-compatible simulation payload.

        Internal settings use Pythonic snake_case.
        Brain API expects camelCase and specific required keys.
        """
        api_settings = {
            "instrumentType": config.DEFAULT_INSTRUMENT_TYPE,
            "region": settings["region"],
            "universe": settings["universe"],
            "delay": settings["delay"],
            "decay": settings["decay"],
            "neutralization": settings["neutralization"],
            "truncation": settings["truncation"],
            "pasteurization": settings["pasteurization"],
            "unitHandling": settings["unit_handling"],
            "nanHandling": settings["nan_handling"],
            "language": settings["language"],
            "visualization": config.DEFAULT_VISUALIZATION,
        }

        return {
            "type": "REGULAR",
            "settings": api_settings,
            "regular": expression,
        }

    def _simulation_status_url(self, sim_id: str) -> str:
        """
        If submit returns a full URL in Location, use it directly.
        Otherwise treat it as an id under /simulations/{id}
        """
        if sim_id.startswith("http://") or sim_id.startswith("https://"):
            return sim_id
        return f"{self.base_url}{self.simulation_path}/{sim_id}"

    def _extract_simulation_id(self, response: requests.Response) -> Optional[str]:
        """
        Try common return patterns:
        - Location header
        - id / simulation_id / progress_id in JSON
        """
        location = response.headers.get("Location")
        if location:
            return location

        raw = self._parse_json(response)

        for key in ("id", "simulation_id", "progress_id"):
            if key in raw and raw[key]:
                return str(raw[key])

        return None

    def _extract_status(self, raw: dict[str, Any]) -> str:
        """
        Normalize possible platform statuses to:
        submitted / running / completed / failed / timed_out
        """
        candidates = [
            raw.get("status"),
            raw.get("state"),
            raw.get("simulation", {}).get("status")
            if isinstance(raw.get("simulation"), dict)
            else None,
        ]

        status = None
        for item in candidates:
            if item:
                status = str(item).lower()
                break

        if status is None:
            return "running"

        if status in {"queued", "submitted", "pending"}:
            return "submitted"
        if status in {"running", "processing", "in_progress"}:
            return "running"
        if status in {"completed", "complete", "done", "success"}:
            return "completed"
        if status in {"warning"}:
            return "completed"  # treat warnings as completed runs
        if status in {"failed", "error"}:
            return "failed"
        if status in {"timed_out", "timeout"}:
            return "timed_out"

        return status

    def _extract_metrics(self, raw: dict[str, Any]) -> dict[str, Any]:
        """
        Pull likely metrics out of the response payload.
        Adjust later once exact response schema is confirmed.
        """
        source = raw
        if isinstance(raw.get("result"), dict):
            source = raw["result"]

        return {
            "sharpe": self._get_nested_value(source, ["sharpe"]),
            "fitness": self._get_nested_value(source, ["fitness"]),
            "turnover": self._get_nested_value(source, ["turnover"]),
            "returns": self._get_nested_value(source, ["returns"]),
            "margin": self._get_nested_value(source, ["margin"]),
            "drawdown": self._get_nested_value(source, ["drawdown"]),
            "checks_passed": self._infer_checks_passed(raw),
        }

    def _infer_checks_passed(self, raw: dict[str, Any]) -> bool:
        checks = raw.get("checks")
        if isinstance(checks, list):
            dict_checks = [item for item in checks if isinstance(item, dict)]
            if dict_checks:
                return all(bool(item.get("passed", False)) for item in dict_checks)

        if isinstance(raw.get("is"), dict):
            maybe = raw["is"].get("stats_pass")
            if maybe is not None:
                return bool(maybe)

        return True

    def _extract_error(self, raw: dict[str, Any]) -> str:
        for key in ("error", "message", "detail"):
            if key in raw and raw[key]:
                return str(raw[key])
        return "Unknown simulation error"

    def get_alpha(self, alpha_id: str) -> dict[str, Any]:
        """
        Fetch alpha details after a simulation completes.
        This is where performance stats are likely to live.
        """
        self.ensure_session()

        url = f"{self.base_url}/alphas/{alpha_id}"
        response = self.session.get(url, timeout=self.timeout_seconds)

        if response.status_code == 401:
            self.login()
            response = self.session.get(url, timeout=self.timeout_seconds)

        if response.status_code != 200:
            raise BrainAPIError(
                f"Fetching alpha failed with status {response.status_code}: {response.text}"
            )

        return self._parse_json(response)

    def _extract_metrics_from_alpha(self, alpha_data: dict[str, Any]) -> dict[str, Any]:
        """
        Extract metrics from alpha details payload.

        We try several likely locations because response shapes can vary.
        """
        candidates = [
            alpha_data,
            alpha_data.get("is") if isinstance(alpha_data.get("is"), dict) else None,
            alpha_data.get("inSample") if isinstance(alpha_data.get("inSample"), dict) else None,
            alpha_data.get("in_sample") if isinstance(alpha_data.get("in_sample"), dict) else None,
            alpha_data.get("metrics") if isinstance(alpha_data.get("metrics"), dict) else None,
        ]

        source = {}
        for candidate in candidates:
            if isinstance(candidate, dict):
                source.update(candidate)

        return {
            "sharpe": self._coalesce_metric(source, ["sharpe", "sharpeRatio"]),
            "fitness": self._coalesce_metric(source, ["fitness"]),
            "turnover": self._coalesce_metric(source, ["turnover"]),
            "returns": self._coalesce_metric(source, ["returns", "return"]),
            "margin": self._coalesce_metric(source, ["margin"]),
            "drawdown": self._coalesce_metric(source, ["drawdown", "maxDrawdown"]),
        }

    def _infer_checks_passed_from_alpha(self, alpha_data: dict[str, Any]) -> bool:
        is_block = alpha_data.get("is")
        if not isinstance(is_block, dict):
            return True

        checks = is_block.get("checks")
        if not isinstance(checks, list):
            return True

        for check in checks:
            if not isinstance(check, dict):
                continue

            name = str(check.get("name", "")).upper()
            result = str(check.get("result", "")).upper()

            if result == "FAIL":
                return False

            # allow pending self-correlation to continue for now
            if result == "PENDING" and name != "SELF_CORRELATION":
                return False

        return True

    @staticmethod
    def _coalesce_metric(source: dict[str, Any], keys: list[str]) -> Any:
        for key in keys:
            if key in source and source[key] is not None:
                return source[key]
        return None

    @staticmethod
    def _parse_json(response: requests.Response) -> dict[str, Any]:
        try:
            data = response.json()
            if isinstance(data, dict):
                return data
            return {"data": data}
        except ValueError:
            return {"text": response.text}

    @staticmethod
    def _get_nested_value(source: dict[str, Any], path: list[str]) -> Any:
        cur: Any = source
        for key in path:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(key)
        return cur