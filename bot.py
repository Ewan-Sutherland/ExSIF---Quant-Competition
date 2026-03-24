from __future__ import annotations

from datetime import timedelta

import config
from evaluator import evaluate_submission, parse_metrics
from generator import AlphaGenerator
from models import Run, new_id, utc_now
from scheduler import Scheduler
from storage import Storage
from brain_client import BrainClient, BrainAPIError


class AlphaBot:
    """
    End-of-Phase-1.5 bot:
    - keeps your current orchestration
    - keeps refinement / pruning / diversity
    - adds adaptive family weighting
    - adds adaptive template weighting
    - stays compatible with your current storage/evaluator/scheduler
    """

    def __init__(
        self,
        storage: Storage,
        client: BrainClient,
        generator: AlphaGenerator,
        scheduler: Scheduler,
    ):
        self.storage = storage
        self.client = client
        self.generator = generator
        self.scheduler = scheduler

        self.completed_runs = 0
        self.refinement_attempts_by_base: dict[str, int] = {}

    # =========================================================
    # Main loop
    # =========================================================

    def tick(self) -> None:
        self.client.ensure_session()
        self._poll_running()
        self._fill_capacity()

    # =========================================================
    # Polling
    # =========================================================

    def _poll_running(self) -> None:
        for sim_id, run_id in list(self.scheduler.active_items()):
            try:
                result = self.client.poll_simulation(sim_id)
            except BrainAPIError as exc:
                print(f"[POLL_ERROR] sim_id={sim_id} run_id={run_id} error={exc}")
                continue
            except Exception as exc:
                print(f"[POLL_UNEXPECTED] sim_id={sim_id} run_id={run_id} error={exc}")
                continue

            status = result.get("status", "running")

            if status in {"submitted", "running"}:
                if status == "running":
                    self.storage.update_run(run_id, status="running")
                continue

            self.scheduler.remove(sim_id)

            if status == "completed":
                self._handle_completed(run_id, result)

            elif status == "failed":
                error_message = result.get("error_message", "Simulation failed")
                self.storage.update_run(
                    run_id,
                    status="failed",
                    completed_at=utc_now(),
                    error_message=error_message,
                    raw_result=result,
                )
                print(f"[FAILED] run_id={run_id} sim_id={sim_id} error={error_message}")

            elif status == "timed_out":
                self.storage.update_run(
                    run_id,
                    status="timed_out",
                    completed_at=utc_now(),
                    error_message=result.get("error_message", "Timed out"),
                    raw_result=result,
                )
                print(f"[TIMED_OUT] run_id={run_id} sim_id={sim_id}")

            else:
                self.storage.update_run(
                    run_id,
                    status=status,
                    completed_at=utc_now(),
                    raw_result=result,
                )
                print(f"[UNKNOWN_TERMINAL_STATUS] run_id={run_id} sim_id={sim_id} status={status}")

    # =========================================================
    # Completion / refinement / submission
    # =========================================================

    def _handle_completed(self, run_id: str, result: dict) -> None:
        self.storage.update_run(
            run_id,
            status="completed",
            completed_at=utc_now(),
            raw_result=result,
        )

        run_row = self.storage.get_run_by_id(run_id)
        if run_row is None:
            print(f"[WARN] completed run not found in DB: run_id={run_id}")
            return

        candidate_id = run_row["candidate_id"]

        metrics = parse_metrics(run_id, result)
        self.storage.insert_metrics(metrics)

        decision = evaluate_submission(candidate_id, metrics)
        self._maybe_queue_refinement(candidate_id, run_id, metrics, decision_should_submit=decision.should_submit)

        self.completed_runs += 1

        sharpe_str = "None" if metrics.sharpe is None else f"{metrics.sharpe:.3f}"
        fitness_str = "None" if metrics.fitness is None else f"{metrics.fitness:.3f}"
        turnover_str = "None" if metrics.turnover is None else f"{metrics.turnover:.3f}"

        print(
            f"[COMPLETED] run_id={run_id} "
            f"sharpe={sharpe_str} fitness={fitness_str} turnover={turnover_str} "
            f"eligible={decision.should_submit} reason={decision.reason}"
        )

        if decision.should_submit and config.AUTO_SUBMIT:
            self._attempt_submission(candidate_id, run_id, result)

        if self.completed_runs % config.REPORT_EVERY_N_COMPLETIONS == 0:
            self._print_progress_report()

    def _maybe_queue_refinement(
        self,
        candidate_id: str,
        run_id: str,
        metrics,
        decision_should_submit: bool = False,
    ) -> None:
        sharpe = metrics.sharpe
        fitness = metrics.fitness
        turnover = metrics.turnover

        if sharpe is None or fitness is None:
            return

        min_refinement_sharpe = getattr(config, "MIN_REFINEMENT_SHARPE", 1.20)
        if sharpe < min_refinement_sharpe:
            return

        if sharpe >= config.NEAR_PASSER_MIN_SHARPE and fitness >= config.NEAR_PASSER_MIN_FITNESS:
            priority = float(sharpe) + float(fitness)

            if turnover is not None:
                priority -= max(0.0, turnover - config.NEAR_PASSER_MAX_TURNOVER)

            self.storage.add_refinement_candidate(
                candidate_id=candidate_id,
                run_id=run_id,
                priority=priority,
                reason=metrics.fail_reason or "near_passer",
                created_at=utc_now(),
            )
            print(
                f"[QUEUED_REFINEMENT] run_id={run_id} candidate_id={candidate_id} "
                f"priority={priority:.3f} reason={metrics.fail_reason}"
            )

    def _attempt_submission(self, candidate_id: str, run_id: str, result: dict) -> None:
        alpha_id = (
            result.get("alpha_id")
            or result.get("raw", {}).get("alpha_id")
            or result.get("raw", {}).get("id")
        )

        if not alpha_id:
            print(f"[SUBMIT_SKIP] run_id={run_id} reason=no_alpha_id_found")
            return

        try:
            submission_response = self.client.submit_alpha(alpha_id)
            self.storage.insert_submission(
                submission_id=new_id("sub"),
                candidate_id=candidate_id,
                run_id=run_id,
                submitted_at=utc_now(),
                submission_status="submitted",
                message=str(submission_response),
            )
            print(f"[SUBMITTED] run_id={run_id} alpha_id={alpha_id}")
        except Exception as exc:
            self.storage.insert_submission(
                submission_id=new_id("sub"),
                candidate_id=candidate_id,
                run_id=run_id,
                submitted_at=utc_now(),
                submission_status="failed",
                message=str(exc),
            )
            print(f"[SUBMIT_FAILED] run_id={run_id} alpha_id={alpha_id} error={exc}")

    # =========================================================
    # Stats / scoring maps
    # =========================================================

    def _template_stats_map(self) -> dict[str, dict]:
        rows = self.storage.get_recent_template_stats(limit=config.TEMPLATE_SCORE_LOOKBACK_RUNS)
        out: dict[str, dict] = {}
        for row in rows:
            out[row["template_id"]] = {
                "family": row["family"],
                "n_runs": row["n_runs"] or 0,
                "avg_sharpe": row["avg_sharpe"],
                "avg_fitness": row["avg_fitness"],
                "avg_turnover": row["avg_turnover"],
            }
        return out

    def _family_stats_map(self) -> dict[str, dict]:
        rows = self.storage.get_recent_family_stats(limit=config.TEMPLATE_SCORE_LOOKBACK_RUNS)
        out: dict[str, dict] = {}
        for row in rows:
            out[row["family"]] = {
                "n_runs": row["n_runs"] or 0,
                "avg_sharpe": row["avg_sharpe"],
                "avg_fitness": row["avg_fitness"],
                "avg_turnover": row["avg_turnover"],
                "submit_rate": row["submit_rate"] if "submit_rate" in row.keys() else None,
            }
        return out

    def _score_from_stats(
        self,
        avg_sharpe,
        avg_fitness,
        avg_turnover,
        n_runs: int,
    ) -> float:
        if n_runs <= 0:
            return 1.0

        if avg_sharpe is None or avg_fitness is None:
            return 1.0

        score = 1.0

        score += 0.55 * max(-1.5, min(2.5, float(avg_sharpe)))
        score += 0.35 * max(-1.5, min(2.0, float(avg_fitness)))

        if avg_turnover is not None:
            turnover = float(avg_turnover)
            if turnover > 0.75:
                score -= 0.45
            elif turnover > 0.55:
                score -= 0.20
            elif turnover < 0.08:
                score -= 0.05

        if n_runs < 6:
            score *= 0.90

        return max(0.15, min(3.00, score))

    def _family_bias_map(self) -> dict[str, float]:
        stats = self._family_stats_map()
        bias: dict[str, float] = {}

        for family in config.DEFAULT_FAMILY_ORDER if hasattr(config, "DEFAULT_FAMILY_ORDER") else []:
            if family not in stats:
                bias[family] = 1.0

        for family, row in stats.items():
            bias[family] = self._score_from_stats(
                avg_sharpe=row["avg_sharpe"],
                avg_fitness=row["avg_fitness"],
                avg_turnover=row["avg_turnover"],
                n_runs=row["n_runs"],
            )

        return bias

    def _template_bias_map(self) -> dict[str, float]:
        stats = self._template_stats_map()
        bias: dict[str, float] = {}

        for template_id, row in stats.items():
            bias[template_id] = self._score_from_stats(
                avg_sharpe=row["avg_sharpe"],
                avg_fitness=row["avg_fitness"],
                avg_turnover=row["avg_turnover"],
                n_runs=row["n_runs"],
            )

        return bias

    # =========================================================
    # Quality / pruning / diversity
    # =========================================================

    def _template_quality_class(self, template_id: str) -> str:
        stats = self._template_stats_map().get(template_id)
        if stats is None:
            return "unknown"

        n_runs = stats["n_runs"] or 0
        avg_sharpe = stats["avg_sharpe"]
        avg_fitness = stats["avg_fitness"]

        if n_runs < config.MIN_TEMPLATE_OBS_FOR_PRUNE:
            return "young"

        if (
            avg_sharpe is not None
            and avg_fitness is not None
            and avg_sharpe <= config.HARD_PRUNE_MAX_AVG_SHARPE
            and avg_fitness <= config.HARD_PRUNE_MAX_AVG_FITNESS
        ):
            return "hard_prune"

        if (
            avg_sharpe is not None
            and avg_fitness is not None
            and avg_sharpe <= config.SOFT_PRUNE_MAX_AVG_SHARPE
            and avg_fitness <= config.SOFT_PRUNE_MAX_AVG_FITNESS
        ):
            return "soft_prune"

        return "healthy"

    def _candidate_allowed_by_template_quality(self, candidate, is_refinement: bool) -> bool:
        quality = self._template_quality_class(candidate.template_id)

        if quality in {"unknown", "young", "healthy"}:
            return True

        if quality == "hard_prune":
            return False

        if quality == "soft_prune":
            if is_refinement:
                return self.generator.rng.random() < config.SOFT_PRUNE_REFINEMENT_PROBABILITY
            return self.generator.rng.random() < config.TEMPLATE_EXPLORATION_PROBABILITY

        return True

    def _candidate_allowed_by_diversity(self, candidate, is_refinement: bool) -> bool:
        stats = self.storage.get_recent_template_stats(limit=config.DIVERSITY_LOOKBACK_RUNS)
        template_stats = None
        for row in stats:
            if row["template_id"] == candidate.template_id:
                template_stats = row
                break

        if template_stats is None:
            return True

        n_runs = template_stats["n_runs"] or 0
        avg_sharpe = template_stats["avg_sharpe"]
        avg_fitness = template_stats["avg_fitness"]

        if n_runs < config.RELAXED_TEMPLATE_COUNT:
            return True

        strong_template = (
            avg_sharpe is not None
            and avg_fitness is not None
            and avg_sharpe >= config.RELAXED_TEMPLATE_MIN_AVG_SHARPE
            and avg_fitness >= config.RELAXED_TEMPLATE_MIN_AVG_FITNESS
        )

        if n_runs >= config.MAX_RECENT_TEMPLATE_COUNT:
            if strong_template and is_refinement:
                return self.generator.rng.random() < 0.30
            return self.generator.rng.random() < config.DIVERSITY_EXPLORATION_PROBABILITY

        if strong_template and is_refinement:
            return True

        penalty_prob = max(0.15, 1.0 - 0.08 * (n_runs - config.RELAXED_TEMPLATE_COUNT))
        return self.generator.rng.random() < penalty_prob

    # =========================================================
    # Candidate selection
    # =========================================================

    def _should_abandon_refinement_base(self, base_candidate_id: str) -> bool:
        attempts = self.refinement_attempts_by_base.get(base_candidate_id, 0)
        return attempts >= config.MAX_REFINEMENT_ATTEMPTS_PER_BASE

    def _fresh_candidate(self):
        family_bias = self._family_bias_map()
        template_bias = self._template_bias_map()

        for _ in range(8):
            candidate = self.generator.generate_candidate(
                family_bias=family_bias,
                template_bias=template_bias,
            )

            if self.storage.candidate_exists(candidate.expression_hash):
                continue

            if not self._candidate_allowed_by_template_quality(candidate, is_refinement=False):
                print(
                    f"[TEMPLATE_PRUNE_SKIP] template={candidate.template_id} family={candidate.family} "
                    f"expr={candidate.expression}"
                )
                continue

            if not self._candidate_allowed_by_diversity(candidate, is_refinement=False):
                print(
                    f"[DIVERSITY_SKIP] template={candidate.template_id} family={candidate.family} "
                    f"expr={candidate.expression}"
                )
                continue

            return candidate

        return None

    def _get_candidate_with_refinement_priority(self):
        for _ in range(6):
            refinement_row = None

            if self.generator.rng.random() < config.REFINEMENT_PROBABILITY:
                refinement_row = self.storage.get_next_refinement_candidate()

            if refinement_row is not None:
                base_candidate_id = refinement_row["candidate_id"]

                if self._should_abandon_refinement_base(base_candidate_id):
                    self.storage.mark_refinement_consumed(base_candidate_id)
                    self.refinement_attempts_by_base.pop(base_candidate_id, None)
                    print(
                        f"[REFINEMENT_EXHAUSTED] base_candidate_id={base_candidate_id} "
                        f"template={refinement_row['template_id']} family={refinement_row['family']}"
                    )
                    continue

                candidate = self.generator.mutate_candidate(refinement_row)
                self.refinement_attempts_by_base[base_candidate_id] = (
                    self.refinement_attempts_by_base.get(base_candidate_id, 0) + 1
                )

                print(
                    f"[REFINING] base_candidate_id={base_candidate_id} "
                    f"template={refinement_row['template_id']} family={refinement_row['family']} "
                    f"new_template={candidate.template_id} new_family={candidate.family} "
                    f"expr={candidate.expression}"
                )

                if self.storage.candidate_exists(candidate.expression_hash):
                    continue

                if not self._candidate_allowed_by_template_quality(candidate, is_refinement=True):
                    print(
                        f"[TEMPLATE_PRUNE_SKIP] template={candidate.template_id} family={candidate.family} "
                        f"expr={candidate.expression}"
                    )
                    continue

                if not self._candidate_allowed_by_diversity(candidate, is_refinement=True):
                    print(
                        f"[DIVERSITY_SKIP] template={candidate.template_id} family={candidate.family} "
                        f"expr={candidate.expression}"
                    )
                    continue

                self.storage.mark_refinement_consumed(base_candidate_id)
                self.refinement_attempts_by_base.pop(base_candidate_id, None)
                return candidate

            fresh = self._fresh_candidate()
            if fresh is not None:
                return fresh

        return None

    # =========================================================
    # Submission loop
    # =========================================================

    def _fill_capacity(self) -> None:
        attempts = 0
        max_attempts = 12

        while self.scheduler.has_capacity() and attempts < max_attempts:
            attempts += 1

            candidate = self._get_candidate_with_refinement_priority()
            if candidate is None:
                continue

            if self.storage.candidate_exists(candidate.expression_hash):
                continue

            try:
                self.storage.insert_candidate(candidate)
            except Exception as exc:
                print(f"[CANDIDATE_INSERT_ERROR] candidate_id={candidate.candidate_id} error={exc}")
                continue

            run = Run.create(candidate_id=candidate.candidate_id, status="pending")
            self.storage.insert_run(run)

            try:
                sim_id = self.client.submit_simulation(
                    candidate.expression,
                    candidate.settings.to_dict(),
                )

                now = utc_now()
                self.storage.update_run(
                    run.run_id,
                    sim_id=sim_id,
                    status="submitted",
                    submitted_at=now,
                )
                self.scheduler.add(sim_id, run.run_id)

                print(
                    f"[SUBMITTED_SIM] run_id={run.run_id} sim_id={sim_id} "
                    f"template={candidate.template_id} family={candidate.family} "
                    f"expr={candidate.expression}"
                )

            except BrainAPIError as exc:
                self.storage.update_run(
                    run.run_id,
                    status="failed",
                    completed_at=utc_now(),
                    error_message=str(exc),
                )
                print(f"[SIM_SUBMIT_ERROR] run_id={run.run_id} error={exc}")

            except Exception as exc:
                self.storage.update_run(
                    run.run_id,
                    status="failed",
                    completed_at=utc_now(),
                    error_message=str(exc),
                )
                print(f"[SIM_SUBMIT_UNEXPECTED] run_id={run.run_id} error={exc}")

    # =========================================================
    # Recovery / timeout
    # =========================================================

    def recover_running_from_storage(self) -> None:
        rows = self.storage.get_running_runs()
        recovered = 0

        for row in rows:
            sim_id = row["sim_id"]
            run_id = row["run_id"]

            if sim_id and not self.scheduler.is_running(sim_id):
                self.scheduler.add(sim_id, run_id)
                recovered += 1

        if recovered:
            print(f"[RECOVERED] restored {recovered} running simulations from storage")

    def mark_stale_runs_timed_out(self) -> None:
        cutoff = utc_now() - timedelta(minutes=config.SIM_TIMEOUT_MINUTES)
        rows = self.storage.get_running_runs()

        stale_count = 0
        for row in rows:
            submitted_at = row["submitted_at"]
            if not submitted_at:
                continue

            try:
                submitted_dt = self.storage.parse_dt(submitted_at)
            except Exception:
                continue

            if submitted_dt < cutoff:
                run_id = row["run_id"]
                sim_id = row["sim_id"]
                self.storage.update_run(
                    run_id,
                    status="timed_out",
                    completed_at=utc_now(),
                    error_message="Marked stale by timeout sweep.",
                )
                if sim_id:
                    self.scheduler.remove(sim_id)
                stale_count += 1

        if stale_count:
            print(f"[STALE_SWEEP] marked {stale_count} runs as timed_out")

    # =========================================================
    # Reporting
    # =========================================================

    def _print_progress_report(self) -> None:
        print("\n[REPORT] recent family stats")
        family_rows = self.storage.get_recent_family_stats(limit=500)

        if not family_rows:
            print("No completed family stats yet.\n")
            return

        for row in family_rows:
            family = row["family"]
            n_runs = row["n_runs"]
            avg_sharpe = row["avg_sharpe"]
            avg_fitness = row["avg_fitness"]
            avg_turnover = row["avg_turnover"]
            submit_rate = row["submit_rate"]

            def fmt(x):
                return "None" if x is None else f"{x:.3f}"

            print(
                f"family={family:<16} "
                f"n={n_runs:<4} "
                f"avg_sharpe={fmt(avg_sharpe):<8} "
                f"avg_fitness={fmt(avg_fitness):<8} "
                f"avg_turnover={fmt(avg_turnover):<8} "
                f"submit_rate={fmt(submit_rate):<8}"
            )

        print("\n[REPORT] recent template stats")
        template_rows = self.storage.get_recent_template_stats(limit=config.TEMPLATE_SCORE_LOOKBACK_RUNS)

        shown = 0
        for row in template_rows:
            template_id = row["template_id"]
            family = row["family"]
            n_runs = row["n_runs"]
            avg_sharpe = row["avg_sharpe"]
            avg_fitness = row["avg_fitness"]
            avg_turnover = row["avg_turnover"]
            quality = self._template_quality_class(template_id)

            def fmt(x):
                return "None" if x is None else f"{x:.3f}"

            print(
                f"template={template_id:<8} "
                f"family={family:<14} "
                f"n={n_runs:<4} "
                f"avg_sharpe={fmt(avg_sharpe):<8} "
                f"avg_fitness={fmt(avg_fitness):<8} "
                f"avg_turnover={fmt(avg_turnover):<8} "
                f"quality={quality}"
            )

            shown += 1
            if shown >= 12:
                break

        print()