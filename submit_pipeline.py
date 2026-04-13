"""
v7.0: Automated submission pipeline.

Each teammate runs this at their scheduled window (2x daily, 12h apart).
Processes only their own ready_alphas, re-checks scores after each submission,
never submits negative.

Schedule (configured via SUBMIT_SCHEDULE in config.py):
    Owner 1: 05:00, 17:00
    Owner 2: 06:00, 18:00
    Owner 3: 07:00, 19:00
    Owner 4: 08:00, 20:00

Can also be triggered manually: python submit_pipeline.py
"""
from __future__ import annotations

import json
import time
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


class SubmitPipeline:
    """Automated alpha submission with score re-checking."""

    # Delay between submissions (seconds) — let WQ update portfolio state
    DELAY_BETWEEN_SUBMISSIONS = 10

    # Delay after re-checking scores (seconds) — API rate limiting
    DELAY_BETWEEN_CHECKS = 3

    def __init__(self, storage, client, config):
        self.storage = storage
        self.client = client
        self.config = config
        self.owner = storage.owner
        self.MIN_SCORE_TO_SUBMIT = getattr(config, "SUBMIT_MIN_SCORE", 3)

    def run(self) -> dict[str, Any]:
        """
        Run the full submission pipeline.

        Returns summary dict with counts.
        """
        print(f"\n{'='*60}")
        print(f"  SUBMISSION PIPELINE — {self.owner}")
        print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
        print(f"{'='*60}\n")

        # Step 0: Retry unverified alphas — promote to 'ready' if score available
        self._retry_unverified()

        # Step 1: Load all ready alphas for this owner
        alphas = self._load_ready_alphas()
        if not alphas:
            print("  No ready alphas to submit.")
            return {"submitted": 0, "rejected": 0, "total": 0}

        print(f"  Loaded {len(alphas)} ready alphas\n")

        # Step 2: Re-check ALL scores (landscape may have shifted)
        print("  Re-checking scores for all alphas...")
        alphas = self._recheck_scores(alphas)

        # Step 3: Drop negatives
        positive = [a for a in alphas if a.get("live_score") is not None and a["live_score"] >= self.MIN_SCORE_TO_SUBMIT]
        negative = [a for a in alphas if a.get("live_score") is not None and a["live_score"] < 0]
        unknown = [a for a in alphas if a.get("live_score") is None]

        # v7.2: Retry unknowns once after a pause — API can be intermittent
        if unknown:
            print(f"\n  {len(unknown)} unknown scores — retrying after 10s pause...")
            time.sleep(10)
            still_unknown = []
            for a in unknown:
                alpha_id = a.get("alpha_id")
                if not alpha_id:
                    still_unknown.append(a)
                    continue
                try:
                    perf = self.client.check_before_after_performance(
                        alpha_id, competition_id=self.config.IQC_COMPETITION_ID,
                    )
                    score = perf.get("_score_change")
                    if score is not None:
                        a["live_score"] = score
                        direction = "+" if score > 0 else ""
                        print(f"    ✅ {a.get('family','')}/{a.get('template_id','')} "
                              f"S={a.get('sharpe',0):.2f} → score: {direction}{score}")
                        if score >= self.MIN_SCORE_TO_SUBMIT:
                            positive.append(a)
                        elif score < 0:
                            negative.append(a)
                    else:
                        still_unknown.append(a)
                except Exception:
                    still_unknown.append(a)
                time.sleep(3)
            unknown = still_unknown

        print(f"\n  After re-check: {len(positive)} positive, {len(negative)} negative, {len(unknown)} unknown")

        for a in negative:
            self._mark_status(a, "rejected", f"negative_score={a['live_score']}")

        # Step 4: Greedy loop — submit highest, re-check all, repeat
        submitted = 0
        rejected = len(negative)
        remaining = list(positive)

        while remaining:
            # Sort by score descending
            remaining.sort(key=lambda a: a.get("live_score", 0) or 0, reverse=True)
            best = remaining[0]

            print(f"\n  ── Submitting #{submitted+1}: score +{best['live_score']:.0f} "
                  f"S={best['sharpe']:.2f} F={best['fitness']:.2f} "
                  f"family={best.get('family','')} core={best.get('core_signal','')[:50]}")

            success = self._submit_alpha(best)
            remaining = [a for a in remaining if a["id"] != best["id"]]

            if success:
                submitted += 1
                self._mark_status(best, "submitted", f"score={best['live_score']:+.0f}")

                if remaining:
                    # Re-check ALL remaining after submission
                    print(f"\n  Re-checking {len(remaining)} remaining alphas...")
                    remaining = self._recheck_scores(remaining)

                    # Drop anything that went negative or unknown
                    for a in remaining:
                        if a.get("live_score") is not None and a["live_score"] < 0:
                            self._mark_status(a, "rejected", f"went_negative={a['live_score']}")
                            rejected += 1

                    remaining = [a for a in remaining
                                 if a.get("live_score") is not None
                                 and a["live_score"] >= self.MIN_SCORE_TO_SUBMIT]
                    print(f"  {len(remaining)} still positive")
            else:
                self._mark_status(best, "rejected", "submit_failed")
                rejected += 1

        # Summary
        print(f"\n{'='*60}")
        print(f"  SUBMISSION COMPLETE")
        print(f"  Submitted: {submitted}")
        print(f"  Rejected:  {rejected}")
        print(f"  Skipped:   {len(unknown)} (unknown score)")
        print(f"{'='*60}\n")

        return {"submitted": submitted, "rejected": rejected, "skipped": len(unknown), "total": len(alphas)}

    # ── Data loading ──────────────────────────────────────────────

    def _load_ready_alphas(self) -> list[dict]:
        """Load ready alphas for this owner only."""
        try:
            rows = self.storage._get("ready_alphas", {
                "owner": f"eq.{self.owner}",
                "status": "eq.ready",
                "select": "*",
                "order": "score_change.desc.nullslast",
            })
            return rows or []
        except Exception as e:
            print(f"  ERROR loading ready_alphas: {e}")
            return []

    def _load_unverified_alphas(self) -> list[dict]:
        """Load unverified alphas for this owner."""
        try:
            rows = self.storage._get("ready_alphas", {
                "owner": f"eq.{self.owner}",
                "status": "eq.unverified",
                "select": "*",
                "order": "sharpe.desc",
            })
            return rows or []
        except Exception as e:
            print(f"  ERROR loading unverified alphas: {e}")
            return []

    def _retry_unverified(self) -> None:
        """
        v7.1: Retry unverified alphas — check self-corr and before-after score.
        Promote to 'ready' if self-corr passes, reject if self-corr fails or score negative.
        """
        unverified = self._load_unverified_alphas()
        if not unverified:
            return

        print(f"  Retrying {len(unverified)} unverified alphas...")
        promoted = 0
        rejected = 0

        for a in unverified:
            alpha_id = a.get("alpha_id")
            if not alpha_id:
                continue

            # Step 1: Check self-correlation first
            try:
                check = self.client.check_alpha(alpha_id)
            except Exception:
                print(f"    ⏳ API timeout S={a.get('sharpe',0):.2f}")
                time.sleep(self.DELAY_BETWEEN_CHECKS)
                continue

            if check.get("_passed") is False:
                self._mark_status(a, "rejected",
                    f"self_corr_fail: corr={check.get('_self_correlation')} with={check.get('_correlated_with')}")
                rejected += 1
                print(f"    ❌ SELF-CORR FAILED S={a.get('sharpe',0):.2f} corr={check.get('_self_correlation')}")
                time.sleep(self.DELAY_BETWEEN_CHECKS)
                continue

            if check.get("_passed") is None:
                print(f"    ⏳ Self-corr still pending S={a.get('sharpe',0):.2f}")
                time.sleep(self.DELAY_BETWEEN_CHECKS)
                continue

            # Step 2: Self-corr passed — check before-after score
            score = None
            for attempt in range(3):
                try:
                    perf = self.client.check_before_after_performance(
                        alpha_id, competition_id=self.config.IQC_COMPETITION_ID,
                    )
                    score = perf.get("_score_change")
                    if score is not None:
                        break
                except Exception:
                    pass
                if attempt < 2:
                    time.sleep(self.DELAY_BETWEEN_CHECKS)

            if score is not None and score >= self.MIN_SCORE_TO_SUBMIT:
                self._mark_status(a, "ready", f"retry_promoted: score={score:+.0f}")
                promoted += 1
                print(f"    ✅ PROMOTED S={a.get('sharpe',0):.2f} score={score:+.0f} → ready")
            elif score is not None and score < 0:
                self._mark_status(a, "rejected", f"retry_negative: score={score:+.0f}")
                rejected += 1
                print(f"    ❌ REJECTED S={a.get('sharpe',0):.2f} score={score:+.0f}")
            else:
                # Self-corr passed but score unavailable — promote anyway
                # The main pipeline will re-check score before submitting
                self._mark_status(a, "ready", f"self_corr_passed_score_unknown")
                promoted += 1
                print(f"    ✅ PROMOTED (self-corr passed, score pending) S={a.get('sharpe',0):.2f} → ready")

            time.sleep(self.DELAY_BETWEEN_CHECKS)

        if promoted or rejected:
            print(f"  Unverified retry: {promoted} promoted, {rejected} rejected, "
                  f"{len(unverified) - promoted - rejected} still pending\n")

    # ── Score checking ────────────────────────────────────────────

    def _recheck_scores(self, alphas: list[dict]) -> list[dict]:
        """Re-check before-after scores for all alphas. Updates live_score field."""
        for i, a in enumerate(alphas):
            alpha_id = a.get("alpha_id")
            if not alpha_id:
                a["live_score"] = None
                continue

            # Retry up to 3 times
            score = None
            for attempt in range(3):
                try:
                    perf = self.client.check_before_after_performance(
                        alpha_id, competition_id=self.config.IQC_COMPETITION_ID,
                    )
                    score = perf.get("_score_change")
                    if score is not None:
                        break
                except Exception:
                    pass
                if attempt < 2:
                    time.sleep(self.DELAY_BETWEEN_CHECKS)

            a["live_score"] = score
            direction = "+" if score and score > 0 else "" if score else "?"
            print(f"    [{i+1}/{len(alphas)}] {a.get('family','')}/{a.get('template_id','')} "
                  f"S={a.get('sharpe',0):.2f} → score: {direction}{score if score is not None else 'unknown'}")

            time.sleep(self.DELAY_BETWEEN_CHECKS)

        return alphas

    def _recheck_single(self, alpha: dict) -> dict | None:
        """Re-check a single alpha's score."""
        alpha_id = alpha.get("alpha_id")
        if not alpha_id:
            return None

        for attempt in range(3):
            try:
                perf = self.client.check_before_after_performance(
                    alpha_id, competition_id=self.config.IQC_COMPETITION_ID,
                )
                score = perf.get("_score_change")
                if score is not None:
                    alpha["live_score"] = score
                    return alpha
            except Exception:
                pass
            if attempt < 2:
                time.sleep(self.DELAY_BETWEEN_CHECKS)

        alpha["live_score"] = None
        return alpha

    # ── Grouping ──────────────────────────────────────────────────

    def _group_by_core(self, alphas: list[dict]) -> list[tuple[str, list[dict]]]:
        """Group alphas by core_signal, sorted by best score in group (descending)."""
        groups: dict[str, list[dict]] = {}
        for a in alphas:
            core = a.get("core_signal") or a.get("expression", "")[:80]
            groups.setdefault(core, []).append(a)

        # Sort groups by the best live_score in each group
        sorted_groups = sorted(
            groups.items(),
            key=lambda x: max((a.get("live_score") or 0) for a in x[1]),
            reverse=True,
        )
        return sorted_groups

    # ── Submission ────────────────────────────────────────────────

    def _submit_alpha(self, alpha: dict) -> bool:
        """Submit a single alpha to WQ. Returns True if accepted."""
        alpha_id = alpha.get("alpha_id")
        if not alpha_id:
            print(f"     ❌ No alpha_id — cannot submit")
            return False

        print(f"     Submitting alpha_id={alpha_id}...")
        time.sleep(self.DELAY_BETWEEN_SUBMISSIONS)

        try:
            result = self.client.submit_alpha(alpha_id)
            accepted = result.get("_accepted")

            if accepted is True:
                print(f"     ✅ ACCEPTED — score +{alpha.get('live_score', '?')}")
                # Log to submissions table
                try:
                    from models import new_id, utc_now
                    self.storage.insert_submission(
                        submission_id=new_id("sub"),
                        candidate_id=alpha.get("candidate_id", ""),
                        run_id=alpha.get("run_id", ""),
                        submitted_at=utc_now(),
                        submission_status="confirmed",
                        message=f"auto_pipeline: score={alpha.get('live_score', '?')} "
                                f"S={alpha.get('sharpe', 0):.2f} F={alpha.get('fitness', 0):.2f}",
                    )
                except Exception:
                    pass
                return True
            else:
                fail = result.get("_fail_reason", "unknown")
                corr = result.get("_self_correlation", "")
                print(f"     ❌ Rejected: {fail} (corr={corr})")
                return False

        except Exception as exc:
            print(f"     ❌ Submit error: {exc}")
            return False

    # ── Status management ─────────────────────────────────────────

    def _mark_status(self, alpha: dict, status: str, notes: str = "") -> None:
        """Update the status of a ready_alpha row."""
        # Try 'id' first (Supabase dashboard-created tables), fallback to candidate_id
        match_key = None
        match_val = None
        for key in ["id", "candidate_id"]:
            if alpha.get(key) is not None:
                match_key = key
                match_val = str(alpha[key])
                break
        if match_key is None:
            return
        try:
            self.storage._patch("ready_alphas", {match_key: match_val}, {
                "status": status,
                "notes": notes[:500],
            })
        except Exception:
            pass


# ── Scheduling logic ──────────────────────────────────────────────

def get_submit_schedule(owner: str) -> list[int]:
    """Return submission hours (UTC) for this owner.

    Returns empty list if owner not in schedule (= disabled).
    Configure in config.py SUBMIT_SCHEDULE.
    """
    import config
    schedule = getattr(config, "SUBMIT_SCHEDULE", {})
    return schedule.get(owner, [])


def should_submit_now(owner: str) -> bool:
    """Check if current UTC hour matches this owner's submission window.
    Returns False if owner has no schedule (= disabled)."""
    hours = get_submit_schedule(owner)
    if not hours:
        return False
    now = datetime.now(timezone.utc)
    return now.hour in hours


# ── Standalone entry point ────────────────────────────────────────

if __name__ == "__main__":
    """Run submission pipeline directly: python submit_pipeline.py"""
    import config
    from brain_client import BrainClient
    from storage_factory import get_storage

    storage = get_storage()
    client = BrainClient(
        username=config.BRAIN_USERNAME,
        password=config.BRAIN_PASSWORD,
        base_url="https://api.worldquantbrain.com",
    )

    pipeline = SubmitPipeline(storage, client, config)
    result = pipeline.run()
    print(f"\nResult: {result}")


# ═══════════════════════════════════════════════════════════════
# v7.2: Teammate score checking
# ═══════════════════════════════════════════════════════════════

class TeammateScoreChecker:
    """
    After your own submission pipeline runs, check scores for
    teammates' ready alphas. Does NOT submit — just checks scores
    and updates them in Supabase so you can submit manually.
    """

    MAX_TEAMMATE_CHECKS = 999  # Check all teammate alphas
    DELAY_BETWEEN_CHECKS = 4  # seconds between API calls

    def __init__(self, storage, client, config):
        self.storage = storage
        self.client = client
        self.config = config

    def run(self, teammate_owners: list[str]) -> dict:
        """Check scores for teammates' ready alphas by re-simulating through own credentials."""
        import time
        import json
        from datetime import datetime, timezone

        total_checked = 0
        total_positive = 0
        total_negative = 0
        total_unknown = 0

        for owner in teammate_owners:
            print(f"\n{'='*60}")
            print(f"  TEAMMATE SCORE CHECK — {owner}")
            print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
            print(f"{'='*60}\n")

            # Load their ready + unverified alphas (sorted by sharpe desc)
            alphas = self._load_teammate_alphas(owner)
            if not alphas:
                print(f"  No ready/unverified alphas for {owner}")
                continue

            # Only check top N to avoid burning too many sim slots
            alphas = alphas[:self.MAX_TEAMMATE_CHECKS]
            print(f"  Checking {len(alphas)} alphas (by sharpe desc)\n")

            for i, a in enumerate(alphas):
                expr = a.get("expression", "")
                family = a.get("family", "")
                sharpe = a.get("sharpe", 0)
                settings_raw = a.get("settings_json", "{}")

                if not expr:
                    print(f"    [{i+1}/{len(alphas)}] No expression — skipping")
                    total_unknown += 1
                    continue

                # Parse settings
                if isinstance(settings_raw, str):
                    try:
                        settings = json.loads(settings_raw)
                    except (json.JSONDecodeError, TypeError):
                        settings = {}
                else:
                    settings = settings_raw or {}

                if not settings:
                    print(f"    [{i+1}/{len(alphas)}] No settings — skipping")
                    total_unknown += 1
                    continue

                # Re-simulate through OWN account to get own alpha_id
                try:
                    sim_id = self.client.submit_simulation(expr, settings)
                except Exception as exc:
                    if "429" in str(exc) or "CONCURRENT" in str(exc).upper():
                        print(f"    [{i+1}/{len(alphas)}] ⚠️ Rate limited — stopping teammate check")
                        break
                    print(f"    [{i+1}/{len(alphas)}] ⚠️ Sim failed: {exc}")
                    total_unknown += 1
                    continue

                # Poll until complete (max 90s)
                result = None
                for poll in range(18):
                    time.sleep(5)
                    try:
                        result = self.client.poll_simulation(sim_id)
                        if result.get("status") in ("completed", "failed", "timed_out"):
                            break
                    except Exception:
                        pass

                if not result or result.get("status") != "completed":
                    print(f"    [{i+1}/{len(alphas)}] ⚠️ {family} S={sharpe:.2f} — sim timed out")
                    total_unknown += 1
                    continue

                # Extract alpha_id from OUR simulation
                alpha_id = None
                try:
                    alpha_url = result.get("alpha", "")
                    if alpha_url:
                        alpha_id = alpha_url.rstrip("/").split("/")[-1]
                except Exception:
                    pass

                if not alpha_id:
                    print(f"    [{i+1}/{len(alphas)}] ⚠️ {family} S={sharpe:.2f} — no alpha_id in result")
                    total_unknown += 1
                    continue

                # Now check before-after with OUR alpha_id
                score = None
                for attempt in range(2):
                    try:
                        perf = self.client.check_before_after_performance(
                            alpha_id,
                            competition_id=self.config.IQC_COMPETITION_ID,
                        )
                        score = perf.get("_score_change")
                        score_before = perf.get("_score_before")
                        score_after = perf.get("_score_after")
                        if score is not None:
                            break
                    except Exception:
                        pass
                    if attempt < 1:
                        time.sleep(3)

                if score is not None:
                    direction = "📈" if score > 0 else "📉" if score < 0 else "➡️"
                    print(f"    [{i+1}/{len(alphas)}] {direction} {family} S={sharpe:.2f} → "
                          f"score: {score:+.0f}")

                    a["score_change"] = score
                    # Update in Supabase
                    try:
                        self.storage._patch("ready_alphas", {"id": a["id"]}, {
                            "score_before": score_before,
                            "score_after": score_after,
                            "score_change": score,
                            "status": "ready" if score >= 0 else "rejected",
                        })
                    except Exception:
                        pass

                    if score > 0:
                        total_positive += 1
                    elif score < 0:
                        total_negative += 1
                    total_checked += 1
                else:
                    print(f"    [{i+1}/{len(alphas)}] ❓ {family} S={sharpe:.2f} → score unavailable")
                    total_unknown += 1

            # Print summary for this teammate
            pos_alphas = [a for a in alphas if a.get("score_change") is not None and a["score_change"] > 0]
            if pos_alphas:
                print(f"\n  ✅ POSITIVE ALPHAS FOR {owner} (submit these manually):")
                for a in sorted(pos_alphas, key=lambda x: x.get("score_change", 0), reverse=True):
                    print(f"    score={a.get('score_change', '?'):+.0f} "
                          f"S={a.get('sharpe', 0):.2f} {a.get('family', '')}")

        print(f"\n{'='*60}")
        print(f"  TEAMMATE CHECK COMPLETE")
        print(f"  Checked: {total_checked}  Positive: {total_positive}  "
              f"Negative: {total_negative}  Unknown: {total_unknown}")
        print(f"{'='*60}\n")

        return {
            "checked": total_checked,
            "positive": total_positive,
            "negative": total_negative,
            "unknown": total_unknown,
        }

    def _load_teammate_alphas(self, owner: str) -> list[dict]:
        """Load ready + unverified alphas for a teammate."""
        try:
            rows = self.storage._get("ready_alphas", {
                "owner": f"eq.{owner}",
                "status": "in.(ready,unverified)",
                "select": "*",
                "order": "sharpe.desc",
            })
            return rows or []
        except Exception as e:
            print(f"  ERROR loading alphas for {owner}: {e}")
            return []
