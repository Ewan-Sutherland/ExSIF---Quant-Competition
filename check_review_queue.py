"""
Check self-correlation for review_queue entries WITHOUT submitting.

Usage:
    python check_review_queue.py          # Check all pending
    python check_review_queue.py --limit 5  # Check first 5 pending

Reads review_queue from Supabase, runs GET /alphas/{id}/check for each,
updates status to 'corr_pass' or 'corr_fail' in Supabase.
"""
import os
import sys
import time
import argparse

from dotenv import load_dotenv
load_dotenv()

from brain_client import BrainClient
from storage_supabase import Storage


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50, help="Max entries to check")
    args = parser.parse_args()

    storage = Storage(
        supabase_url=os.getenv("SUPABASE_URL"),
        supabase_key=os.getenv("SUPABASE_ANON_KEY"),
        owner=os.getenv("BRAIN_USERNAME"),
    )

    client = BrainClient()

    # Fetch pending review queue entries
    rows = storage._get("review_queue", {
        "status": "eq.pending",
        "select": "*",
        "order": "sharpe.desc",
        "limit": str(args.limit),
    })

    if not rows:
        print("No pending entries in review_queue.")
        return

    print(f"Found {len(rows)} pending entries. Checking self-correlation...\n")

    passed = 0
    failed = 0
    errors = 0

    for i, row in enumerate(rows, 1):
        rq_id = row.get("id")
        candidate_id = row.get("candidate_id", "?")
        expression = row.get("expression", "?")[:80]
        sharpe = row.get("sharpe", 0)
        fitness = row.get("fitness", 0)
        family = row.get("family", "?")

        # Get the alpha_id from the runs table
        run_id = row.get("run_id", "")
        run_rows = storage._get("runs", {
            "run_id": f"eq.{run_id}",
            "select": "alpha_id",
        })

        alpha_id = None
        if run_rows:
            alpha_id = run_rows[0].get("alpha_id")

        if not alpha_id:
            print(f"[{i}/{len(rows)}] ⚠️  SKIP — no alpha_id found for run_id={run_id}")
            errors += 1
            continue

        print(f"[{i}/{len(rows)}] Checking alpha_id={alpha_id} S={sharpe:.2f} F={fitness:.2f} family={family}")
        print(f"         expr={expression}")

        result = client.check_alpha(alpha_id)

        if result["_passed"] is True:
            print(f"         ✅ PASSED self-correlation (corr={result['_self_correlation']})")

            # Also check merged performance impact
            perf = client.check_before_after_performance(alpha_id)
            if perf["_error"]:
                print(f"         ⚠️  Performance check failed: {perf['_error']}")
                perf_note = "perf_check_failed"
            elif perf["_change"] is not None:
                direction = "📈" if perf["_change"] > 0 else "📉" if perf["_change"] < 0 else "➡️"
                print(f"         {direction} Score: {perf['_before_score']} → {perf['_after_score']} (change: {perf['_change']:+.0f})")
                perf_note = f"before={perf['_before_score']} after={perf['_after_score']} change={perf['_change']:+.0f}"
            else:
                perf_note = "perf_data_unavailable"

            print()
            passed += 1
            # Update status in Supabase
            storage._patch("review_queue", {"id": rq_id}, {
                "status": "corr_pass",
                "notes": f"Self-corr OK (corr={result['_self_correlation']}). {perf_note}",
            })

        elif result["_passed"] is False:
            corr_val = result["_self_correlation"]
            corr_with = result["_correlated_with"]
            reason = result["_fail_reason"]
            print(f"         ❌ FAILED — {reason} (corr={corr_val}, with={corr_with})\n")
            failed += 1
            storage._patch("review_queue", {"id": rq_id}, {
                "status": "corr_fail",
                "notes": f"Self-corr FAIL: {reason}. corr={corr_val} with={corr_with}",
            })

        else:
            print(f"         ⚠️  UNKNOWN — {result['_fail_reason']}\n")
            errors += 1

        # Small delay between checks to avoid rate limiting
        time.sleep(2)

    print(f"\n{'='*60}")
    print(f"Results: {passed} passed, {failed} failed, {errors} errors")
    print(f"\nIn Supabase review_queue:")
    print(f"  'corr_pass' with positive change → SUBMIT on BRAIN website")
    print(f"  'corr_pass' with negative change → SKIP (hurts merged score)")
    print(f"  'corr_fail' → don't bother checking")


if __name__ == "__main__":
    main()
