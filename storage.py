from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator, Optional

from models import Candidate, Metrics, Run


def dt_to_str(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return value.isoformat()


class Storage:
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        self.init_db()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def init_db(self) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS candidates (
                    candidate_id TEXT PRIMARY KEY,
                    expression TEXT NOT NULL,
                    canonical_expression TEXT NOT NULL,
                    expression_hash TEXT NOT NULL UNIQUE,
                    template_id TEXT NOT NULL,
                    family TEXT NOT NULL,
                    fields_json TEXT NOT NULL,
                    params_json TEXT NOT NULL,
                    settings_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    candidate_id TEXT NOT NULL,
                    sim_id TEXT,
                    status TEXT NOT NULL,
                    submitted_at TEXT,
                    completed_at TEXT,
                    error_message TEXT,
                    raw_result_json TEXT,
                    FOREIGN KEY(candidate_id) REFERENCES candidates(candidate_id)
                );
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS metrics (
                    run_id TEXT PRIMARY KEY,
                    sharpe REAL,
                    fitness REAL,
                    turnover REAL,
                    returns REAL,
                    margin REAL,
                    drawdown REAL,
                    checks_passed INTEGER,
                    submit_eligible INTEGER,
                    fail_reason TEXT,
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                );
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS submissions (
                    submission_id TEXT PRIMARY KEY,
                    candidate_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    submitted_at TEXT NOT NULL,
                    submission_status TEXT NOT NULL,
                    message TEXT,
                    FOREIGN KEY(candidate_id) REFERENCES candidates(candidate_id),
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                );
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS refinement_queue (
                    candidate_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    priority REAL NOT NULL,
                    reason TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    consumed INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY(candidate_id) REFERENCES candidates(candidate_id),
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                );
                """
            )

            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_candidates_family
                ON candidates(family);
                """
            )

            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_runs_status
                ON runs(status);
                """
            )

            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_runs_candidate_id
                ON runs(candidate_id);
                """
            )

            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_metrics_submit_eligible
                ON metrics(submit_eligible);
                """
            )

            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_refinement_consumed
                ON refinement_queue(consumed, priority DESC);
                """
            )

    def parse_dt(self, value: str) -> datetime:
        return datetime.fromisoformat(value)

    def candidate_exists(self, expression_hash: str) -> bool:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM candidates
                WHERE expression_hash = ?
                LIMIT 1
                """,
                (expression_hash,),
            ).fetchone()
            return row is not None

    def insert_candidate(self, candidate: Candidate) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO candidates (
                    candidate_id,
                    expression,
                    canonical_expression,
                    expression_hash,
                    template_id,
                    family,
                    fields_json,
                    params_json,
                    settings_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    candidate.candidate_id,
                    candidate.expression,
                    candidate.canonical_expression,
                    candidate.expression_hash,
                    candidate.template_id,
                    candidate.family,
                    json.dumps(candidate.fields, sort_keys=True),
                    json.dumps(candidate.params, sort_keys=True),
                    json.dumps(candidate.settings.to_dict(), sort_keys=True),
                    dt_to_str(candidate.created_at),
                ),
            )

    def insert_run(self, run: Run) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO runs (
                    run_id,
                    candidate_id,
                    sim_id,
                    status,
                    submitted_at,
                    completed_at,
                    error_message,
                    raw_result_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run.run_id,
                    run.candidate_id,
                    run.sim_id,
                    run.status,
                    dt_to_str(run.submitted_at),
                    dt_to_str(run.completed_at),
                    run.error_message,
                    json.dumps(run.raw_result, sort_keys=True) if run.raw_result is not None else None,
                ),
            )

    def update_run(
        self,
        run_id: str,
        *,
        sim_id: Optional[str] = None,
        status: Optional[str] = None,
        submitted_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
        error_message: Optional[str] = None,
        raw_result: Optional[dict[str, Any]] = None,
    ) -> None:
        fields: list[str] = []
        values: list[Any] = []

        if sim_id is not None:
            fields.append("sim_id = ?")
            values.append(sim_id)

        if status is not None:
            fields.append("status = ?")
            values.append(status)

        if submitted_at is not None:
            fields.append("submitted_at = ?")
            values.append(dt_to_str(submitted_at))

        if completed_at is not None:
            fields.append("completed_at = ?")
            values.append(dt_to_str(completed_at))

        if error_message is not None:
            fields.append("error_message = ?")
            values.append(error_message)

        if raw_result is not None:
            fields.append("raw_result_json = ?")
            values.append(json.dumps(raw_result, sort_keys=True))

        if not fields:
            return

        values.append(run_id)

        with self.connect() as conn:
            conn.execute(
                f"""
                UPDATE runs
                SET {", ".join(fields)}
                WHERE run_id = ?
                """,
                values,
            )

    def insert_metrics(self, metrics: Metrics) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO metrics (
                    run_id,
                    sharpe,
                    fitness,
                    turnover,
                    returns,
                    margin,
                    drawdown,
                    checks_passed,
                    submit_eligible,
                    fail_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    metrics.run_id,
                    metrics.sharpe,
                    metrics.fitness,
                    metrics.turnover,
                    metrics.returns,
                    metrics.margin,
                    metrics.drawdown,
                    int(metrics.checks_passed) if metrics.checks_passed is not None else None,
                    int(metrics.submit_eligible) if metrics.submit_eligible is not None else None,
                    metrics.fail_reason,
                ),
            )

    def insert_submission(
        self,
        submission_id: str,
        candidate_id: str,
        run_id: str,
        submitted_at: datetime,
        submission_status: str,
        message: Optional[str] = None,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO submissions (
                    submission_id,
                    candidate_id,
                    run_id,
                    submitted_at,
                    submission_status,
                    message
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    submission_id,
                    candidate_id,
                    run_id,
                    dt_to_str(submitted_at),
                    submission_status,
                    message,
                ),
            )

    def add_refinement_candidate(
        self,
        candidate_id: str,
        run_id: str,
        priority: float,
        reason: str,
        created_at: datetime,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO refinement_queue (
                    candidate_id,
                    run_id,
                    priority,
                    reason,
                    created_at,
                    consumed
                ) VALUES (?, ?, ?, ?, ?, 0)
                """,
                (
                    candidate_id,
                    run_id,
                    priority,
                    reason,
                    dt_to_str(created_at),
                ),
            )

    def get_next_refinement_candidate(self):
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT
                    rq.candidate_id,
                    rq.run_id,
                    rq.priority,
                    rq.reason,
                    c.template_id,
                    c.family,
                    c.params_json,
                    c.settings_json,
                    c.expression,
                    c.canonical_expression,
                    c.expression_hash
                FROM refinement_queue rq
                JOIN candidates c
                    ON rq.candidate_id = c.candidate_id
                WHERE rq.consumed = 0
                ORDER BY rq.priority DESC, rq.created_at ASC
                LIMIT 1
                """
            ).fetchone()
            return row

    def mark_refinement_consumed(self, candidate_id: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE refinement_queue
                SET consumed = 1
                WHERE candidate_id = ?
                """,
                (candidate_id,),
            )

    def get_running_runs(self) -> list[sqlite3.Row]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM runs
                WHERE status IN ('submitted', 'running')
                """
            ).fetchall()
            return rows

    def get_run_by_id(self, run_id: str) -> Optional[sqlite3.Row]:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM runs
                WHERE run_id = ?
                LIMIT 1
                """,
                (run_id,),
            ).fetchone()
            return row

    def get_candidate_by_hash(self, expression_hash: str) -> Optional[sqlite3.Row]:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM candidates
                WHERE expression_hash = ?
                LIMIT 1
                """,
                (expression_hash,),
            ).fetchone()
            return row

    def get_recent_family_stats(self, limit: int = 500) -> list[sqlite3.Row]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    c.family,
                    COUNT(*) AS n_runs,
                    AVG(m.sharpe) AS avg_sharpe,
                    AVG(m.fitness) AS avg_fitness,
                    AVG(m.turnover) AS avg_turnover,
                    AVG(CASE WHEN m.submit_eligible = 1 THEN 1.0 ELSE 0.0 END) AS submit_rate
                FROM metrics m
                JOIN runs r ON m.run_id = r.run_id
                JOIN candidates c ON r.candidate_id = c.candidate_id
                WHERE r.run_id IN (
                    SELECT run_id
                    FROM runs
                    ORDER BY submitted_at DESC
                    LIMIT ?
                )
                GROUP BY c.family
                ORDER BY n_runs DESC
                """,
                (limit,),
            ).fetchall()
            return rows