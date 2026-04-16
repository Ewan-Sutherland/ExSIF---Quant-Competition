# AlphaBot v7.2.1 — Changelog

Released: 2026-04-16. Fixes observed overnight failures from v7.2 run on 2026-04-15/16.

## Fixes

### 1. `KeyError: 'fresh_est_field'` in refinement (generator.py)
Root cause: `_mutate_params_for_mode` had handlers for `{field}`, `{deep_field}`, `{analyst_field}`, `{sentiment_field}`, `{fscore_field}`, `{model77_field}`, `{news_event_field}`, `{rp_field}` — but missing handlers for `{fresh_fund_field}`, `{fn_field}`, `{fresh_est_field}`, `{deriv_field}`, `{beta_field}`.

When refinement switched templates within a family (e.g. `corr_pipeline/cp_02 → cp_01`) where sister templates used different `fresh_*` placeholders than the parent, `template.format(**params)` threw `KeyError`. Test2 bot hit this 16× overnight as `[MAIN_LOOP_ERROR] 'fresh_est_field'`.

Fix: added handlers for all 5 missing placeholders, matching the same fill-if-missing + small-prob-mutate pattern used elsewhere. Falls back to general pools (`FUNDAMENTAL_FIELDS` / `ANALYST_FIELDS`) if the fresh pool is ever empty, so `template.format()` can never throw.

### 2. Stuck sims holding scheduler slots forever (bot.py)
Root cause: `mark_stale_runs_timed_out()` was only called at startup. Ewan's bot had a single stuck sim (`drc_08 / deriv_rank_composites`) from ~08:20 UTC that was never cleaned up; combined with LLM exhaustion this caused a 70-minute idle hang.

Fix: added periodic call (every 20 ticks ≈ 5 min) inside `tick()`. Stale sims now get released and scheduler slots freed without a restart.

### 3. Idle-when-no-work fallback (bot.py)
Root cause: existing `_check_stall()` is keyed on `_sims_since_last_eligible`, which doesn't advance when the bot isn't completing sims at all. So total idleness (0 active sims + 0 generation happening) was invisible to the stall detector.

Fix: new `_check_idle()` method tracks consecutive ticks with 0 active sims. Escalating recovery:
- 5 idle ticks: reset category rotation counter
- 15 idle ticks: clear `family_template_exhausted`
- 30 idle ticks: clear `core_signal_exhausted` (deep nuclear reset)

Logs `[IDLE_DETECTED]` and `[IDLE_RECOVERY]` so you can see it firing.

### 4. Epoch-aware saturation penalty (generator.py + bot.py)
Root cause: Thompson sampler learns from Sharpe/fitness feedback but NOT from self-correlation failures. 29/43 `[OPTIMIZE_START]`s overnight failed self-corr at 0.72–0.97 correlation with existing submissions — the sampler had no signal that these families were saturated in the team's submission pool.

Fix: per-epoch per-family self-corr failure tracker. Weight multipliers applied in `_sample_family`:
- 1 fail in current epoch → weight × 0.70
- 2 fails → weight × 0.35 (logged once as `[SATURATION]`)
- 3+ fails → weight × 0.10 (logged once — near-zero but keeps exploration escape hatch)

Counter resets on epoch advance so the 12-hour rotation still covers all categories. State is persisted in `bot_state.epoch_state.epoch_corr_fails` so restarts mid-epoch don't lose learning.

Expected impact: 30–50 fewer wasted sims per bot per night on saturated families.

## Not changed

- LLM key exhaustion handling (left for later per instruction)
- "High-turnover rescue" was considered then **dropped** — Sharpe 15+ T=1.0 alphas are overfitting, not real signal. The bot is correct to block them at turnover ≥ 0.7.
- Shared team state (`score_negative_cores`, `_swept`, `core_signal_exhausted`) was considered then **skipped** — impact was estimated at ~5% sim savings vs. the Supabase schema-migration risk of breaking warm-start. Saved for a separate patch when there's time to test the migration.

## Files changed

- `generator.py`: added epoch_corr_fails tracker, 5 missing param handlers, penalty logic in `_sample_family`, `record_corr_fail` public method, persistence in `get_epoch_state`/`restore_epoch_state`
- `bot.py`: added `_check_idle()`, idle counters in `__init__`, periodic stale-sim sweep in `tick()`, `record_corr_fail` notification in `OPTIMIZE_CORR_FAIL`

No schema changes. Drop-in replacement for v7.2.
