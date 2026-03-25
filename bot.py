from __future__ import annotations

from datetime import timedelta

import config
from evaluator import evaluate_submission, parse_metrics
from generator import AlphaGenerator
from models import Run, new_id, utc_now
from scheduler import Scheduler
from storage import Storage
from similarity import SimilarityEngine
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
        self.refinement_local_history: dict[str, list[str]] = {}
        self.similarity_engine = SimilarityEngine()

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
        self._maybe_queue_refinement(candidate_id, run_id, metrics)

        self.completed_runs += 1

        sharpe_str = "None" if metrics.sharpe is None else f"{metrics.sharpe:.3f}"
        fitness_str = "None" if metrics.fitness is None else f"{metrics.fitness:.3f}"
        turnover_str = "None" if metrics.turnover is None else f"{metrics.turnover:.3f}"

        print(
            f"[COMPLETED] run_id={run_id} "
            f"sharpe={sharpe_str} fitness={fitness_str} turnover={turnover_str} "
            f"eligible={decision.should_submit} reason={decision.reason}"
        )

        # When eligible, check portfolio fit before submission
        if decision.should_submit:
            portfolio_check = self._check_submission_portfolio_fit(
                candidate_id=candidate_id,
                run_id=run_id,
                sharpe=metrics.sharpe,
            )

            if portfolio_check["fits"]:
                print(
                    f"[ELIGIBLE_DIVERSE] run_id={run_id} candidate_id={candidate_id} "
                    f"max_sim={portfolio_check['max_similarity']:.3f} "
                    f"sharpe_margin={portfolio_check.get('sharpe_margin', 'n/a')} "
                    f"READY TO SUBMIT"
                )
                if config.AUTO_SUBMIT:
                    self._attempt_submission(candidate_id, run_id, result)
            else:
                print(
                    f"[ELIGIBLE_CORRELATED] run_id={run_id} candidate_id={candidate_id} "
                    f"max_sim={portfolio_check['max_similarity']:.3f} "
                    f"vs_submitted={portfolio_check.get('ref_candidate_id', '?')} "
                    f"ref_family={portfolio_check.get('ref_family', '?')} "
                    f"reason={portfolio_check['reason']} "
                    f"SKIPPING — would likely fail self-correlation"
                )

        if self.completed_runs % config.REPORT_EVERY_N_COMPLETIONS == 0:
            self._print_progress_report()

    def _maybe_queue_refinement(self, candidate_id: str, run_id: str, metrics) -> None:
        sharpe = metrics.sharpe
        fitness = metrics.fitness
        turnover = metrics.turnover

        if sharpe is None or fitness is None:
            return

        # Don't refine alphas that already pass — that wastes sim slots
        if metrics.submit_eligible:
            return

        min_refinement_sharpe = getattr(config, "MIN_REFINEMENT_SHARPE", 1.20)
        if sharpe < min_refinement_sharpe:
            return

        source_stage = None
        priority = None

        if sharpe >= config.NEAR_PASSER_MIN_SHARPE and fitness >= config.NEAR_PASSER_MIN_FITNESS:
            priority = float(sharpe) + float(fitness)
            if turnover is not None:
                priority -= max(0.0, turnover - config.NEAR_PASSER_MAX_TURNOVER)
            source_stage = "near_passer"

        elif sharpe >= config.FRONTIER_MIN_SHARPE and fitness >= config.FRONTIER_MIN_FITNESS:
            priority = float(sharpe) + float(fitness)
            if turnover is not None:
                priority -= 0.50 * max(0.0, float(turnover) - config.NEAR_PASSER_MAX_TURNOVER)
            source_stage = "frontier"

        elif sharpe >= config.FRONTIER_ALT_MIN_SHARPE and fitness >= config.FRONTIER_ALT_MIN_FITNESS:
            priority = float(sharpe) + float(fitness)
            if turnover is not None:
                priority -= 0.70 * max(0.0, float(turnover) - config.NEAR_PASSER_MAX_TURNOVER)
            source_stage = "frontier_alt"

        if source_stage is not None:
            self.storage.add_refinement_candidate(
                candidate_id=candidate_id,
                run_id=run_id,
                priority=priority,
                reason=metrics.fail_reason or source_stage,
                created_at=utc_now(),
                source_stage=source_stage,
                base_sharpe=sharpe,
                base_fitness=fitness,
                base_turnover=turnover,
            )
            print(
                f"[QUEUED_REFINEMENT] run_id={run_id} candidate_id={candidate_id} "
                f"priority={priority:.3f} reason={metrics.fail_reason} stage={source_stage}"
            )

    def _attempt_submission(self, candidate_id: str, run_id: str, result: dict) -> None:
        alpha_id = (
            result.get("alpha_id")
            or result.get("raw", {}).get("alpha_id")
            or result.get("raw", {}).get("id")
            or result.get("raw", {}).get("alpha")
        )

        if not alpha_id:
            print(f"[SUBMIT_SKIP] run_id={run_id} reason=no_alpha_id_found raw_keys={list(result.get('raw', {}).keys())}")
            return

        # Get sim_id for PATCH approach (teammate's method)
        sim_id = None
        run_row = self.storage.get_run_by_id(run_id)
        if run_row:
            sim_id = run_row["sim_id"]

        print(f"[SUBMIT_STARTING] run_id={run_id} alpha_id={alpha_id} sim_id={sim_id}")

        try:
            submission_response = self.client.submit_alpha(alpha_id, sim_id=sim_id)
            self.storage.insert_submission(
                submission_id=new_id("sub"),
                candidate_id=candidate_id,
                run_id=run_id,
                submitted_at=utc_now(),
                submission_status="submitted",
                message=str(submission_response)[:500],
            )
            print(f"[SUBMITTED] run_id={run_id} alpha_id={alpha_id} response={str(submission_response)[:200]}")
        except Exception as exc:
            self.storage.insert_submission(
                submission_id=new_id("sub"),
                candidate_id=candidate_id,
                run_id=run_id,
                submitted_at=utc_now(),
                submission_status="failed",
                message=str(exc)[:500],
            )
            print(f"[SUBMIT_FAILED] run_id={run_id} alpha_id={alpha_id} error={exc}")

    def _check_submission_portfolio_fit(
        self,
        candidate_id: str,
        run_id: str,
        sharpe: float | None,
    ) -> dict:
        """
        Check if an eligible candidate will likely pass WQ's self-correlation test.

        WQ measures PnL Pearson correlation against ALL submitted alphas.
        Rule: correlation < 0.7, OR Sharpe >= 10% better than correlated alpha.

        Our proxy uses THREE layers:
        1. Core signal match — if the inner signal is identical, PnL correlation ~0.8+
        2. Same family — same family alphas typically correlate 0.5-0.8
        3. Structural similarity — fallback for cross-family checks
        """
        submitted_rows = self.storage.get_submitted_candidate_rows(limit=300)

        if not submitted_rows:
            return {"fits": True, "max_similarity": 0.0, "reason": "no_prior_submissions"}

        candidate_row = self.storage.get_candidate_by_id(candidate_id)
        if candidate_row is None:
            return {"fits": True, "max_similarity": 0.0, "reason": "candidate_not_found"}

        cand_expr = candidate_row["canonical_expression"]
        cand_family = candidate_row["family"]
        cand_core = self._extract_core_signal(cand_expr)

        # Check against EVERY submitted alpha (not just best match)
        for ref_row in submitted_rows:
            ref_expr = ref_row["canonical_expression"]
            ref_family = ref_row["family"]
            ref_sharpe = ref_row["sharpe"]
            ref_core = self._extract_core_signal(ref_expr)
            ref_cid = ref_row["candidate_id"]
            ref_tid = ref_row["template_id"]

            # Layer 1: Core signal match — near-certain PnL correlation > 0.7
            if cand_core and ref_core and cand_core == ref_core:
                # Same core signal — only pass if Sharpe is 10% better
                if sharpe is not None and ref_sharpe is not None and sharpe >= float(ref_sharpe) * 1.10:
                    continue  # This specific ref is ok, check others
                return {
                    "fits": False,
                    "max_similarity": 0.95,
                    "reason": f"same_core_signal '{cand_core}' vs submitted {ref_cid}",
                    "ref_candidate_id": ref_cid,
                    "ref_family": ref_family,
                    "ref_template_id": ref_tid,
                }

            # Layer 2: Same family — high risk of PnL correlation 0.5-0.8
            if cand_family == ref_family:
                # Same family but different core signal — still risky
                # Require Sharpe 10% better to be safe
                if sharpe is not None and ref_sharpe is not None and sharpe >= float(ref_sharpe) * 1.10:
                    continue
                return {
                    "fits": False,
                    "max_similarity": 0.70,
                    "reason": f"same_family '{cand_family}' — needs Sharpe >= {float(ref_sharpe) * 1.10:.2f} vs submitted {ref_cid}",
                    "ref_candidate_id": ref_cid,
                    "ref_family": ref_family,
                    "ref_template_id": ref_tid,
                }

        # Layer 3: Cross-family structural similarity check
        cand_sig = self.similarity_engine.signature_from_row(candidate_row)
        best_score = 0.0
        best_ref_row = None

        for ref_row in submitted_rows:
            ref_sig = self.similarity_engine.signature_from_row(ref_row)
            score = self.similarity_engine.pair_similarity(cand_sig, ref_sig)
            if score > best_score:
                best_score = score
                best_ref_row = ref_row

        threshold = getattr(config, "SUBMISSION_MAX_SIMILARITY", 0.45)

        if best_score >= threshold and best_ref_row is not None:
            ref_sharpe = best_ref_row["sharpe"]
            if sharpe is not None and ref_sharpe is not None and sharpe >= float(ref_sharpe) * 1.10:
                return {
                    "fits": True,
                    "max_similarity": best_score,
                    "reason": f"cross_family_sharpe_override",
                    "sharpe_margin": f"{sharpe:.3f} vs {float(ref_sharpe):.3f}",
                }
            return {
                "fits": False,
                "max_similarity": best_score,
                "reason": f"cross_family_too_similar (sim={best_score:.3f})",
                "ref_candidate_id": best_ref_row["candidate_id"],
                "ref_family": best_ref_row["family"],
                "ref_template_id": best_ref_row["template_id"],
            }

        return {
            "fits": True,
            "max_similarity": best_score,
            "reason": "diverse_enough",
        }

    def _extract_core_signal(self, expression: str) -> str:
        """
        Extract the core inner signal from an expression, stripping wrappers.

        ts_decay_linear(rank(rank(-(returns - ts_mean(returns, 5)))), 10)
        ts_mean(rank(rank(-(returns - ts_mean(returns, 5)))), 5)
        rank(-(returns - ts_mean(returns, 5)))

        All have the same core: -(returns - ts_mean(returns, 5))

        Similarly for volume_flow:
        (volume / ts_mean(volume, N)) * -returns  is the core for vol_03
        """
        import re
        expr = expression.strip()

        # Strip outer wrappers iteratively: ts_mean(..., N), ts_decay_linear(..., N), rank(...)
        changed = True
        while changed:
            changed = False

            # Strip ts_mean(X, N) or ts_decay_linear(X, N)
            for func in ["ts_mean", "ts_decay_linear"]:
                pattern = f"^{func}\\((.+),\\s*\\d+\\)$"
                m = re.match(pattern, expr)
                if m:
                    expr = m.group(1).strip()
                    changed = True

            # Strip outer rank(X)
            if expr.startswith("rank(") and expr.endswith(")"):
                inner = expr[5:-1]
                # Verify balanced parens
                depth = 0
                balanced = True
                for ch in inner:
                    if ch == "(":
                        depth += 1
                    elif ch == ")":
                        depth -= 1
                    if depth < 0:
                        balanced = False
                        break
                if balanced and depth == 0:
                    expr = inner.strip()
                    changed = True

        return expr

    def _get_submitted_family_set(self) -> set[str]:
        """Return set of families that have been successfully submitted."""
        submitted_rows = self.storage.get_submitted_candidate_rows(limit=300)
        families = set()
        for row in submitted_rows:
            try:
                families.add(row["family"])
            except (KeyError, IndexError):
                pass
        return families

    def _get_submitted_template_set(self) -> set[str]:
        """Return set of template_ids that have been successfully submitted."""
        submitted_rows = self.storage.get_submitted_candidate_rows(limit=300)
        templates = set()
        for row in submitted_rows:
            try:
                templates.add(row["template_id"])
            except (KeyError, IndexError):
                pass
        return templates

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

        # Submission diversity: boost families NOT yet represented in submissions
        submitted_families = self._get_submitted_family_set()
        if submitted_families:
            diversity_boost = getattr(config, "UNSUBMITTED_FAMILY_BOOST", 1.60)
            for family in bias:
                if family not in submitted_families and family not in {"momentum", "fundamental"}:
                    bias[family] *= diversity_boost

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
        # Always allow strong templates for refinement
        if candidate.template_id in getattr(config, "STRONG_TEMPLATES", set()):
            if is_refinement:
                return True
            return self.generator.rng.random() < 0.92

        # Submission diversity override: if this candidate's family has no submissions yet,
        # be much more permissive — we NEED diverse family submissions
        submitted_families = self._get_submitted_family_set()
        if submitted_families and candidate.family not in submitted_families:
            # Family not yet submitted — relax diversity limits heavily
            if candidate.family not in {"momentum", "fundamental"}:
                return True

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

    def _passes_local_refinement_filter(self, base_candidate_id: str, candidate) -> bool:
        history = self.refinement_local_history.get(base_candidate_id, [])
        if candidate.expression_hash in history:
            return False

        base_row = self.storage.get_candidate_by_id(base_candidate_id)
        if base_row is not None:
            result = self.similarity_engine.max_similarity_against_rows(candidate, [base_row])
            if result.score >= config.LOCAL_REFINEMENT_MAX_SIMILARITY:
                return False

        if history:
            rows = []
            for expr_hash in history:
                row = self.storage.get_candidate_by_hash(expr_hash)
                if row is not None:
                    rows.append(row)
            if rows:
                result = self.similarity_engine.max_similarity_against_rows(candidate, rows)
                if result.score >= config.LOCAL_REFINEMENT_MAX_SIMILARITY:
                    return False

        return True

    def _remember_local_refinement(self, base_candidate_id: str, expression_hash: str) -> None:
        history = self.refinement_local_history.get(base_candidate_id, [])
        history.append(expression_hash)
        keep = getattr(config, "LOCAL_REFINEMENT_HISTORY", 6)
        self.refinement_local_history[base_candidate_id] = history[-keep:]

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
        last_base_tried = None

        for _ in range(8):
            refinement_row = None

            if self.generator.rng.random() < config.REFINEMENT_PROBABILITY:
                refinement_row = self.storage.get_next_refinement_candidate()

            if refinement_row is not None:
                base_candidate_id = refinement_row["candidate_id"]

                # Avoid hammering the same base in a single tick
                if base_candidate_id == last_base_tried:
                    fresh = self._fresh_candidate()
                    if fresh is not None:
                        return fresh
                    continue
                last_base_tried = base_candidate_id

                if self._should_abandon_refinement_base(base_candidate_id):
                    self.storage.mark_refinement_consumed(base_candidate_id)
                    self.refinement_attempts_by_base.pop(base_candidate_id, None)
                    print(
                        f"[REFINEMENT_EXHAUSTED] base_candidate_id={base_candidate_id} "
                        f"template={refinement_row['template_id']} family={refinement_row['family']}"
                    )
                    continue

                metrics_hint = {
                    "sharpe": refinement_row["base_sharpe"],
                    "fitness": refinement_row["base_fitness"],
                    "turnover": refinement_row["base_turnover"],
                }
                candidate = self.generator.mutate_candidate(refinement_row, metrics_hint=metrics_hint)
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

                if not self._passes_local_refinement_filter(base_candidate_id, candidate):
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

                # Don't consume base — let it be retried until attempts exhausted
                self._remember_local_refinement(base_candidate_id, candidate.expression_hash)
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

        # Submission portfolio status
        submitted_families = self._get_submitted_family_set()
        submitted_templates = self._get_submitted_template_set()
        eligible_rows = self.storage.get_submission_eligible_candidates(limit=50)

        print(f"\n[REPORT] submission portfolio")
        print(f"  submitted_families={sorted(submitted_families) if submitted_families else 'none'}")
        print(f"  submitted_templates={sorted(submitted_templates) if submitted_templates else 'none'}")
        print(f"  eligible_not_yet_submitted={len(eligible_rows)}")

        # Show which families need eligible alphas for diversity
        all_productive = {"mean_reversion", "volume_flow", "conditional", "vol_adjusted"}
        missing = all_productive - submitted_families
        if missing:
            print(f"  diversity_gaps={sorted(missing)} — boost these for portfolio diversity")

        print()