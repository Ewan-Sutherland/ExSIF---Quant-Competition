from evaluator import parse_metrics, evaluate_submission

result = {
    "status": "completed",
    "sharpe": 1.7,
    "fitness": 1.2,
    "turnover": 0.4,
    "returns": 0.11,
    "margin": 0.02,
    "drawdown": 0.08,
    "checks_passed": True,
}

metrics = parse_metrics("run_123", result)
decision = evaluate_submission("cand_123", metrics)

print(metrics)
print(decision)