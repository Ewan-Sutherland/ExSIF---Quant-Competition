"""
Manage manually submitted alphas in the bot's database.

Usage:
    python register_submission.py --list-submitted
    python register_submission.py --list-eligible
    python register_submission.py --candidate-id cand_XXXXX
"""
from __future__ import annotations
import argparse
import config
from storage import Storage


def main():
    parser = argparse.ArgumentParser(description="Manage submission records")
    parser.add_argument("--candidate-id", type=str, help="Register a candidate as submitted")
    parser.add_argument("--hash", type=str, help="Register by expression hash")
    parser.add_argument("--list-eligible", action="store_true", help="List all eligible candidates")
    parser.add_argument("--list-submitted", action="store_true", help="List all registered submissions")
    args = parser.parse_args()

    storage = Storage(config.DB_PATH)

    if args.list_eligible:
        rows = storage.get_submission_eligible_candidates(limit=50)
        if not rows:
            print("No eligible candidates found.")
            return
        print(f"\n{'candidate_id':<46} {'template':<10} {'family':<16} {'sharpe':<8} {'fitness':<8}")
        print("-" * 120)
        for row in rows:
            s = f"{row['sharpe']:.3f}" if row['sharpe'] else "?"
            f = f"{row['fitness']:.3f}" if row['fitness'] else "?"
            print(f"{row['candidate_id']:<46} {row['template_id']:<10} {row['family']:<16} {s:<8} {f:<8}")
            print(f"  expr: {row['canonical_expression'][:100]}")
        return

    if args.list_submitted:
        rows = storage.get_submitted_candidate_rows(limit=50)
        if not rows:
            print("No submitted candidates found.")
            return
        print(f"\n{'candidate_id':<46} {'template':<10} {'family':<16} {'sharpe':<8} {'fitness':<8}")
        print("-" * 120)
        for row in rows:
            s = f"{row['sharpe']:.3f}" if row['sharpe'] else "?"
            f = f"{row['fitness']:.3f}" if row['fitness'] else "?"
            print(f"{row['candidate_id']:<46} {row['template_id']:<10} {row['family']:<16} {s:<8} {f:<8}")
            print(f"  expr: {row['canonical_expression'][:100]}")
        return

    if args.candidate_id:
        ok = storage.register_manual_submission_by_candidate_id(args.candidate_id)
        if ok:
            print(f"[OK] Registered submission for candidate_id={args.candidate_id}")
        else:
            print(f"[FAIL] Could not find completed run for candidate_id={args.candidate_id}")
        return

    if args.hash:
        ok = storage.register_manual_submission(args.hash)
        if ok:
            print(f"[OK] Registered submission for hash={args.hash}")
        else:
            print(f"[FAIL] Could not find candidate with hash={args.hash}")
        return

    parser.print_help()


if __name__ == "__main__":
    main()
