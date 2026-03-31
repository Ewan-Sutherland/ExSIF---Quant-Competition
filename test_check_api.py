"""
Test the check and before-after-performance API endpoints.

Usage:
    python test_check_api.py ALPHA_ID

Use an alpha_id from one of your submitted alphas (find on BRAIN website URL).
Example: python test_check_api.py pwz70dpx
"""
import sys
import json
from dotenv import load_dotenv
load_dotenv()

from brain_client import BrainClient
import config


def main():
    if len(sys.argv) < 2:
        print("Usage: python test_check_api.py ALPHA_ID")
        print("Get alpha_id from the BRAIN website URL of a submitted alpha")
        sys.exit(1)

    alpha_id = sys.argv[1]
    client = BrainClient()

    # Test 1: Self-correlation check
    print(f"{'='*60}")
    print(f"Testing self-correlation check for alpha_id={alpha_id}")
    print(f"Endpoint: GET /alphas/{alpha_id}/check")
    print(f"{'='*60}\n")

    result = client.check_alpha(alpha_id)
    print(f"\nResult:")
    print(f"  passed: {result['_passed']}")
    print(f"  self_correlation: {result['_self_correlation']}")
    print(f"  correlated_with: {result['_correlated_with']}")
    print(f"  fail_reason: {result['_fail_reason']}")
    print(f"  checks: {json.dumps(result['_checks'], indent=2)[:500]}")

    # Test 2: Before-after performance
    comp_id = config.IQC_COMPETITION_ID
    print(f"\n{'='*60}")
    print(f"Testing before-after performance for alpha_id={alpha_id}")
    print(f"Endpoint: GET /competitions/{comp_id}/alphas/{alpha_id}/before-and-after-performance")
    print(f"{'='*60}\n")

    perf = client.check_before_after_performance(alpha_id, competition_id=comp_id)
    print(f"\nResult:")
    print(f"  before_score: {perf['_before_score']}")
    print(f"  after_score: {perf['_after_score']}")
    print(f"  change: {perf['_change']}")
    print(f"  error: {perf['_error']}")
    if "_raw" in perf and perf["_raw"]:
        print(f"  raw response: {json.dumps(perf['_raw'], indent=2)[:500]}")

    print(f"\n{'='*60}")
    if result["_passed"] is not None and perf["_error"] is None:
        print("✅ Both endpoints working! Safe to run overnight.")
    else:
        print("⚠️ Check the output above for issues before running overnight.")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
