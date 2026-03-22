from __future__ import annotations

TEMPLATE_LIBRARY: dict[str, list[dict[str, str]]] = {
    "mean_reversion": [
        {
            "template_id": "mr_01",
            "expression": "rank(ts_mean(close, {n}) - close)",
        },
        {
            "template_id": "mr_02",
            "expression": "rank(-ts_delta(close, {n}))",
        },
        {
            "template_id": "mr_03",
            "expression": "rank(-(close / ts_mean(close, {n}) - 1))",
        },
        {
            "template_id": "mr_04",
            "expression": "rank(-(returns - ts_mean(returns, {n})))",
        },
    ],
    "momentum": [
        {
            "template_id": "mom_01",
            "expression": "rank(ts_delta(close, {n}))",
        },
        {
            "template_id": "mom_02",
            "expression": "rank(ts_rank(close, {n}))",
        },
        {
            "template_id": "mom_03",
            "expression": "rank(ts_mean(returns, {n}))",
        },
    ],
    "volume_flow": [
        {
            "template_id": "vol_01",
            "expression": "rank(volume / ts_mean(volume, {n}))",
        },
        {
            "template_id": "vol_02",
            "expression": "rank(ts_delta(volume, {n}) * returns)",
        },
        {
            "template_id": "vol_03",
            "expression": "rank((volume / ts_mean(volume, {n})) * -returns)",
        },
    ],
    "vol_adjusted": [
        {
            "template_id": "va_01",
            "expression": "rank(ts_delta(close, {n}) / ts_std_dev(returns, {m}))",
        },
        {
            "template_id": "va_02",
            "expression": "rank((ts_mean(close, {n}) - close) / ts_std_dev(returns, {m}))",
        },
    ],
    "fundamental": [
        {
            "template_id": "fund_01",
            "expression": "rank({field})",
        },
        {
            "template_id": "fund_02",
            "expression": "rank(ts_delta({field}, {n}))",
        },
        {
            "template_id": "fund_03",
            "expression": "rank(({field} - ts_mean({field}, {n})))",
        },
    ],
    "conditional": [
        {
            "template_id": "cond_01",
            "expression": "trade_when(volume > ts_mean(volume, {n}), rank(-returns), -1)",
        },
        {
            "template_id": "cond_02",
            "expression": "trade_when(abs(returns) > ts_std_dev(returns, {n}), rank(-returns), -1)",
        },
        {
            "template_id": "cond_03",
            "expression": "trade_when(volume > ts_mean(volume, {n}), rank(ts_delta(close, {m})), -1)",
        },
    ],
}

FUNDAMENTAL_FIELDS = [
    "cap",
    "assets",
    "sales",
    "income",
    "cash",
]

SAFE_PARAM_RANGES = {
    "n": [5, 10, 20, 40, 60],
    "m": [10, 20, 60],
}