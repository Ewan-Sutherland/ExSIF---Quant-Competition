"""
Supabase storage backend for the Alpha Bot.
Drop-in replacement for the SQLite Storage class — same method signatures.

Requires:
    SUPABASE_URL and SUPABASE_ANON_KEY in environment or passed to constructor.
    Tables and RPC functions created via supabase_schema.sql.
"""
from __future__ import annotations

import json
import os
import logging
from datetime import datetime, timezone
from typing import Optional, Any
from contextlib import contextmanager

import requests

from models import Candidate, Run, Metrics

logger = logging.getLogger(__name__)


def dt_to_str(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return value.isoformat()


class Storage:
    """Supabase-backed storage with the same interface as the SQLite version."""

    def __init__(self, db_path: str | None = None, supabase_url: str | None = None, supabase_key: str | None = None, owner: str | None = None):
        """
        Args:
            db_path: Ignored (kept for backward compatibility with config.DB_PATH).
            supabase_url: Supabase project URL. Falls back to SUPABASE_URL env var.
            supabase_key: Supabase anon/publishable key. Falls back to SUPABASE_ANON_KEY env var.
            owner: Username tag for all inserts (e.g. BRAIN_USERNAME).
        """
        self.url = (supabase_url or os.environ.get("SUPABASE_URL", "")).rstrip("/")
        self.key = supabase_key or os.environ.get("SUPABASE_ANON_KEY", "")
        self.owner = owner or os.environ.get("BRAIN_USERNAME", "unknown")
        if not self.url or not self.key:
            raise ValueError("SUPABASE_URL and SUPABASE_ANON_KEY must be set")
        self.base = f"{self.url}/rest/v1"
        self.headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        }

    # ── HTTP helpers ──────────────────────────────────────────────────

    def _get(self, table: str, params: dict | None = None) -> list[dict]:
        r = requests.get(f"{self.base}/{table}", headers=self.headers, params=params or {})
        if r.status_code not in (200, 206):
            logger.warning(f"GET {table} failed: {r.status_code} {r.text[:300]}")
            return []
        return r.json()

    def _post(self, table: str, data: dict, upsert: bool = False) -> dict | None:
        headers = dict(self.headers)
        if upsert:
            headers["Prefer"] = "resolution=merge-duplicates,return=representation"
        r = requests.post(f"{self.base}/{table}", headers=headers, json=data)
        if r.status_code not in (200, 201):
            logger.warning(f"POST {table} failed: {r.status_code} {r.text[:300]}")
            return None
        result = r.json()
        return result[0] if isinstance(result, list) and result else result

    def _patch(self, table: str, match: dict, data: dict) -> dict | None:
        params = {k: f"eq.{v}" for k, v in match.items()}
        r = requests.patch(f"{self.base}/{table}", headers=self.headers, params=params, json=data)
        if r.status_code not in (200, 204):
            logger.warning(f"PATCH {table} failed: {r.status_code} {r.text[:300]}")
            return None
        result = r.json() if r.text else None
        return result[0] if isinstance(result, list) and result else result

    def _delete(self, table: str, match: dict) -> bool:
        params = {k: f"eq.{v}" for k, v in match.items()}
        r = requests.delete(f"{self.base}/{table}", headers=self.headers, params=params)
        return r.status_code in (200, 204)

    def _rpc(self, function: str, params: dict | None = None) -> list[dict]:
        r = requests.post(
            f"{self.base}/rpc/{function}",
            headers=self.headers,
            json=params or {},
        )
        if r.status_code != 200:
            logger.warning(f"RPC {function} failed: {r.status_code} {r.text[:300]}")
            return []
        return r.json()

    # ── Compatibility ─────────────────────────────────────────────────

    @contextmanager
    def connect(self):
        """Compatibility shim — yields self so `with storage.connect() as conn` still works."""
        yield self

    def execute(self, sql: str, params: tuple = ()):
        """Compatibility shim for raw SQL. Only used by cleanup scripts."""
        logger.warning(f"Raw SQL not supported in Supabase mode: {sql[:100]}")
        return _EmptyResult()

    def init_db(self):
        """No-op — tables created via supabase_schema.sql."""
        pass

    def parse_dt(self, value: str) -> datetime:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    # ── Candidates ────────────────────────────────────────────────────

    def candidate_exists(self, expression_hash: str) -> bool:
        rows = self._get("candidates", {
            "expression_hash": f"eq.{expression_hash}",
            "select": "candidate_id",
        })
        return len(rows) > 0

    def insert_candidate(self, candidate: Candidate) -> None:
        self._post("candidates", {
            "candidate_id": candidate.candidate_id,
            "expression": candidate.expression,
            "canonical_expression": candidate.canonical_expression,
            "expression_hash": candidate.expression_hash,
            "template_id": candidate.template_id,
            "family": candidate.family,
            "fields_json": candidate.fields,
            "params_json": candidate.params,
            "settings_json": candidate.settings.to_dict(),
            "created_at": dt_to_str(candidate.created_at),
            "owner": self.owner,
        })

    def get_candidate_by_id(self, candidate_id: str) -> Optional[dict]:
        rows = self._get("candidates", {
            "candidate_id": f"eq.{candidate_id}",
            "select": "*",
        })
        return rows[0] if rows else None

    def get_candidate_by_hash(self, expression_hash: str) -> Optional[dict]:
        rows = self._get("candidates", {
            "expression_hash": f"eq.{expression_hash}",
            "select": "*",
        })
        return rows[0] if rows else None

    # ── Runs ──────────────────────────────────────────────────────────

    def insert_run(self, run: Run) -> None:
        self._post("runs", {
            "run_id": run.run_id,
            "candidate_id": run.candidate_id,
            "sim_id": run.sim_id,
            "alpha_id": run.alpha_id,
            "status": run.status,
            "submitted_at": dt_to_str(run.submitted_at),
            "completed_at": dt_to_str(run.completed_at),
            "error_message": run.error_message,
            "raw_result_json": run.raw_result,
            "owner": self.owner,
        })

    def update_run(
        self,
        run_id: str,
        status: str | None = None,
        sim_id: str | None = None,
        alpha_id: str | None = None,
        submitted_at: datetime | None = None,
        completed_at: datetime | None = None,
        error_message: str | None = None,
        raw_result: dict | None = None,
    ) -> None:
        data = {}
        if status is not None:
            data["status"] = status
        if sim_id is not None:
            data["sim_id"] = sim_id
        if alpha_id is not None:
            data["alpha_id"] = alpha_id
        if submitted_at is not None:
            data["submitted_at"] = dt_to_str(submitted_at)
        if completed_at is not None:
            data["completed_at"] = dt_to_str(completed_at)
        if error_message is not None:
            data["error_message"] = error_message
        if raw_result is not None:
            data["raw_result_json"] = raw_result
        if data:
            self._patch("runs", {"run_id": run_id}, data)

    def get_run_by_id(self, run_id: str) -> Optional[dict]:
        rows = self._get("runs", {
            "run_id": f"eq.{run_id}",
            "select": "*",
        })
        return rows[0] if rows else None

    def get_running_runs(self) -> list[dict]:
        return self._get("runs", {
            "status": "eq.running",
            "owner": f"eq.{self.owner}",
            "select": "*",
        })

    # ── Metrics ───────────────────────────────────────────────────────

    def insert_metrics(self, metrics: Metrics) -> None:
        self._post("metrics", {
            "run_id": metrics.run_id,
            "sharpe": metrics.sharpe,
            "fitness": metrics.fitness,
            "turnover": metrics.turnover,
            "returns": metrics.returns,
            "margin": metrics.margin,
            "drawdown": metrics.drawdown,
            "checks_passed": metrics.checks_passed,
            "submit_eligible": metrics.submit_eligible,
            "fail_reason": metrics.fail_reason,
        })

    # ── Submissions ───────────────────────────────────────────────────

    def insert_submission(
        self,
        submission_id: str,
        candidate_id: str,
        run_id: str,
        submitted_at: datetime,
        submission_status: str,
        message: str | None = None,
    ) -> None:
        self._post("submissions", {
            "submission_id": submission_id,
            "candidate_id": candidate_id,
            "run_id": run_id,
            "submitted_at": dt_to_str(submitted_at),
            "submission_status": submission_status,
            "message": message,
            "owner": self.owner,
        })

    # ── Refinement Queue ──────────────────────────────────────────────

    def add_refinement_candidate(
        self,
        candidate_id: str,
        run_id: str,
        priority: float,
        reason: str,
        created_at: datetime,
        source_stage: str = "unknown",
        base_sharpe: float | None = None,
        base_fitness: float | None = None,
        base_turnover: float | None = None,
    ) -> None:
        self._post("refinement_queue", {
            "candidate_id": candidate_id,
            "run_id": run_id,
            "priority": priority,
            "reason": reason,
            "created_at": dt_to_str(created_at),
            "consumed": False,
            "source_stage": source_stage,
            "base_sharpe": base_sharpe,
            "base_fitness": base_fitness,
            "base_turnover": base_turnover,
        }, upsert=True)

    def get_next_refinement_candidate(self) -> Optional[dict]:
        rows = self._get("refinement_queue", {
            "consumed": "eq.false",
            "select": "*",
            "order": "priority.desc",
            "limit": "1",
        })
        if not rows:
            return None

        row = rows[0]
        cid = row["candidate_id"]

        # Get the full candidate data
        cand = self.get_candidate_by_id(cid)
        if not cand:
            return None

        # Merge refinement queue data with candidate data
        merged = dict(cand)
        merged["reason"] = row.get("reason", "")
        merged["source_stage"] = row.get("source_stage", "unknown")
        merged["base_sharpe"] = row.get("base_sharpe")
        merged["base_fitness"] = row.get("base_fitness")
        merged["base_turnover"] = row.get("base_turnover")
        return merged

    def mark_refinement_consumed(self, candidate_id: str) -> None:
        self._patch("refinement_queue", {"candidate_id": candidate_id}, {"consumed": True})

    # ── Complex Queries (via RPC) ─────────────────────────────────────

    def get_recent_family_stats(self, limit: int = 500) -> list[dict]:
        return self._rpc("get_family_stats", {"run_limit": limit})

    def get_recent_template_stats(self, limit: int = 180) -> list[dict]:
        return self._rpc("get_template_stats", {"run_limit": limit})

    def get_recent_settings_stats(self, limit: int = 500) -> dict[str, list[dict]]:
        """v5.5: Return performance stats grouped by each settings dimension."""
        rows = self._rpc("get_settings_stats", {"run_limit": limit})
        # RPC returns flat rows with 'dimension' column — group by dimension
        results: dict[str, list[dict]] = {
            "universe": [], "neutralization": [], "decay": [], "truncation": [],
        }
        for row in rows:
            dim = row.get("dimension", "")
            if dim in results:
                results[dim].append(row)
        return results

    def get_submitted_candidate_rows(self, *, limit: int = 300) -> list[dict]:
        return self._rpc("get_submitted_candidates", {"row_limit": limit})

    def get_submission_eligible_candidates(self, *, limit: int = 50) -> list[dict]:
        return self._rpc("get_eligible_candidates", {"row_limit": limit})

    def get_similarity_reference_candidates(
        self, *, limit: int, min_sharpe: float, min_fitness: float,
    ) -> list[dict]:
        return self._rpc("get_reference_candidates", {
            "row_limit": limit,
            "min_s": min_sharpe,
            "min_f": min_fitness,
        })

    def get_recent_bucket_reference_candidates(self, *, limit: int) -> list[dict]:
        # Reuse reference candidates with low thresholds
        return self.get_similarity_reference_candidates(
            limit=limit, min_sharpe=0.0, min_fitness=0.0,
        )

    # ── Utility / Compatibility ───────────────────────────────────────

    def get_submitted_alphas(self, *, limit: int = 300) -> list[dict]:
        return self.get_submitted_candidate_rows(limit=limit)

    def get_refinement_report(self, limit: int = 250) -> list[dict]:
        return self._get("refinement_queue", {
            "select": "*",
            "order": "priority.desc",
            "limit": str(limit),
        })

    def register_manual_submission(self, expression_hash: str, alpha_id: str | None = None) -> bool:
        cand = self.get_candidate_by_hash(expression_hash)
        if not cand:
            return False

        cid = cand["candidate_id"]

        # Find the best completed run for this candidate
        runs = self._get("runs", {
            "candidate_id": f"eq.{cid}",
            "status": "eq.completed",
            "select": "run_id",
            "order": "completed_at.desc",
            "limit": "1",
        })
        rid = runs[0]["run_id"] if runs else "manual_run"

        from models import new_id, utc_now
        self.insert_submission(
            submission_id=new_id("sub"),
            candidate_id=cid,
            run_id=rid,
            submitted_at=utc_now(),
            submission_status="submitted",
            message="manually_registered",
        )
        return True

    def register_manual_submission_by_candidate_id(self, candidate_id: str) -> bool:
        cand = self.get_candidate_by_id(candidate_id)
        if not cand:
            return False

        runs = self._get("runs", {
            "candidate_id": f"eq.{candidate_id}",
            "status": "eq.completed",
            "select": "run_id",
            "order": "completed_at.desc",
            "limit": "1",
        })
        rid = runs[0]["run_id"] if runs else "manual_run"

        from models import new_id, utc_now
        self.insert_submission(
            submission_id=new_id("sub"),
            candidate_id=candidate_id,
            run_id=rid,
            submitted_at=utc_now(),
            submission_status="submitted",
            message="manually_registered",
        )
        return True

    # ── v6.0: Optuna warm-start data retrieval ────────────────────────

    def get_runs_for_expression(self, expression: str) -> list[dict]:
        """Get all completed runs for a specific expression with settings and metrics."""
        # Step 1: Find candidates with this exact expression
        cands = self._get("candidates", {
            "select": "candidate_id,settings_json",
            "canonical_expression": f"eq.{expression}",
            "limit": "50",
        })
        if not cands:
            return []

        results = []
        for cand in cands:
            cid = cand["candidate_id"]
            # Step 2: Find completed runs for this candidate
            runs = self._get("runs", {
                "select": "run_id",
                "candidate_id": f"eq.{cid}",
                "status": "eq.completed",
                "limit": "20",
            })
            for run in runs:
                rid = run["run_id"]
                # Step 3: Get metrics
                mets = self._get("metrics", {
                    "select": "sharpe,fitness,turnover",
                    "run_id": f"eq.{rid}",
                    "limit": "1",
                })
                if mets:
                    import json as _json
                    sj = cand.get("settings_json")
                    settings = _json.loads(sj) if isinstance(sj, str) else (sj or {})
                    results.append({
                        "run_id": rid,
                        "settings_json": settings,
                        "sharpe": mets[0].get("sharpe"),
                        "fitness": mets[0].get("fitness"),
                        "turnover": mets[0].get("turnover"),
                    })
        return results

    def get_runs_for_core_signal(self, core_signal: str) -> list[dict]:
        """Get completed runs whose expression contains the core signal."""
        # PostgREST LIKE query
        cands = self._get("candidates", {
            "select": "candidate_id,settings_json,canonical_expression",
            "canonical_expression": f"like.*{core_signal[:40]}*",
            "limit": "30",
        })
        if not cands:
            return []

        results = []
        for cand in cands:
            cid = cand["candidate_id"]
            runs = self._get("runs", {
                "select": "run_id",
                "candidate_id": f"eq.{cid}",
                "status": "eq.completed",
                "limit": "10",
            })
            for run in runs:
                rid = run["run_id"]
                mets = self._get("metrics", {
                    "select": "sharpe,fitness,turnover",
                    "run_id": f"eq.{rid}",
                    "limit": "1",
                })
                if mets and mets[0].get("fitness"):
                    import json as _json
                    sj = cand.get("settings_json")
                    settings = _json.loads(sj) if isinstance(sj, str) else (sj or {})
                    results.append({
                        "run_id": rid,
                        "settings_json": settings,
                        "sharpe": mets[0].get("sharpe"),
                        "fitness": mets[0].get("fitness"),
                        "turnover": mets[0].get("turnover"),
                    })
                    if len(results) >= 30:
                        return results
        return results

    def get_runs_for_family(self, family: str) -> list[dict]:
        """Get top completed runs from same family for cross-pollination."""
        cands = self._get("candidates", {
            "select": "candidate_id,settings_json",
            "family": f"eq.{family}",
            "limit": "50",
        })
        if not cands:
            return []

        results = []
        for cand in cands:
            cid = cand["candidate_id"]
            runs = self._get("runs", {
                "select": "run_id",
                "candidate_id": f"eq.{cid}",
                "status": "eq.completed",
                "limit": "5",
            })
            for run in runs:
                rid = run["run_id"]
                mets = self._get("metrics", {
                    "select": "sharpe,fitness,turnover",
                    "run_id": f"eq.{rid}",
                    "limit": "1",
                })
                if mets and (mets[0].get("fitness") or 0) > 0.5:
                    import json as _json
                    sj = cand.get("settings_json")
                    settings = _json.loads(sj) if isinstance(sj, str) else (sj or {})
                    results.append({
                        "run_id": rid,
                        "settings_json": settings,
                        "sharpe": mets[0].get("sharpe"),
                        "fitness": mets[0].get("fitness"),
                        "turnover": mets[0].get("turnover"),
                    })
                    if len(results) >= 30:
                        return results
        return results

    def get_concentrated_weight_failures(self, *, limit: int = 500) -> list[str]:
        """v6.0.1: Get canonical expressions that failed CONCENTRATED_WEIGHT check."""
        mets = self._get("metrics", {
            "select": "run_id",
            "fail_reason": "like.*CONCENTRATED_WEIGHT*",
            "limit": str(limit),
        })
        if not mets:
            return []

        expressions = set()
        for m in mets:
            rid = m["run_id"]
            runs = self._get("runs", {
                "select": "candidate_id",
                "run_id": f"eq.{rid}",
                "limit": "1",
            })
            if runs:
                cid = runs[0]["candidate_id"]
                cands = self._get("candidates", {
                    "select": "canonical_expression",
                    "candidate_id": f"eq.{cid}",
                    "limit": "1",
                })
                if cands and cands[0].get("canonical_expression"):
                    expressions.add(cands[0]["canonical_expression"])
        return list(expressions)

    def get_self_correlation_rejections(self, *, limit: int = 500) -> list[dict]:
        """v6.1: Get candidates that were rejected by WQ for self-correlation.
        Checks both submissions and submissions_archive tables."""
        subs = []
        # Check active submissions
        try:
            subs.extend(self._get("submissions", {
                "select": "candidate_id",
                "submission_status": "eq.rejected",
                "message": "like.*SELF_CORRELATION*",
                "limit": str(limit),
            }) or [])
        except Exception:
            pass
        # Also check archive
        try:
            subs.extend(self._get("submissions_archive", {
                "select": "candidate_id",
                "submission_status": "eq.rejected",
                "message": "like.*SELF_CORRELATION*",
                "limit": str(limit),
            }) or [])
        except Exception:
            pass

        if not subs:
            return []

        results = []
        seen = set()
        for s in subs:
            cid = s["candidate_id"]
            if cid in seen:
                continue
            seen.add(cid)
            cands = self._get("candidates", {
                "select": "canonical_expression,candidate_id",
                "candidate_id": f"eq.{cid}",
                "limit": "1",
            })
            if cands:
                results.append({
                    "canonical_expression": cands[0].get("canonical_expression", ""),
                    "candidate_id": cid,
                })
        return results

    def insert_review_queue(self, *, candidate_id, run_id, expression, core_signal,
                            family, template_id, sharpe, fitness, turnover, settings_json):
        """v6.1: Add eligible alpha to review queue for manual submission decision."""
        self._post("review_queue", {
            "candidate_id": candidate_id,
            "run_id": run_id,
            "expression": expression,
            "core_signal": core_signal,
            "family": family,
            "template_id": template_id,
            "sharpe": sharpe,
            "fitness": fitness,
            "turnover": turnover,
            "settings_json": settings_json,
            "status": "pending",
            "owner": self.owner,
        })

    def insert_ready_alpha(self, *, candidate_id, run_id, alpha_id, expression,
                           core_signal, family, template_id, sharpe, fitness,
                           turnover, score_before, score_after, score_change,
                           settings_json, variant_desc):
        """v6.2: Stage optimised alpha for manual submission."""
        self._post("ready_alphas", {
            "candidate_id": candidate_id,
            "run_id": run_id,
            "alpha_id": alpha_id,
            "expression": expression,
            "core_signal": core_signal,
            "family": family,
            "template_id": template_id,
            "sharpe": sharpe,
            "fitness": fitness,
            "turnover": turnover,
            "score_before": score_before,
            "score_after": score_after,
            "score_change": score_change,
            "settings_json": settings_json,
            "variant_desc": variant_desc,
            "status": "ready",
            "owner": self.owner,
        })


class _EmptyResult:
    """Stub for compatibility with raw SQL execute calls."""
    def fetchall(self):
        return []
    def fetchone(self):
        return None
