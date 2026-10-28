# Design & trade-off note

**Flow:** `events.csv` → header/schema check → row validation → dedupe vs. store → insert `events_clean` / `rejects` → recompute sessions for affected users → publish `sessions` → export + reconcile. One SQLite transaction per input file.

**Key decisions**
- *Recompute sessions per affected user from all stored events* instead of incremental patching. Simple, provably correct for late/out-of-order data (a late event can merge two sessions or extend one). Cost: work proportional to the touched users' full history.
- *Whole-file atomicity.* Any failure rolls back; retry = run the same command again. Run status is recorded in `pipeline_runs` (FAILED row written after rollback). A hard crash leaves a RUNNING row, which the next run of that file marks ABANDONED.
- *Idempotency by key + lineage:* same event_id from same file+line = `replayed` no-op; rejects for a file are re-derived each run. Output is identical on rerun (verified by test).
- *Strict boundary:* gap == timeout stays in the session (documented, tested at ±1 µs).
- *Integer microsecond timestamps* for ordering/gaps; normalised UTC text for output (fixed width, sorts lexically).
- *Expected answers kept apart:* generator writes labels to `labels/`, never in the input file.
- *Independent verification:* reconciliation uses SQL window functions, not the Python algorithm, so a bug in one is unlikely to hide in the other.

**Limitations / next steps**
- `session_id` depends on the first event, so a late event earlier than it changes the id (downstream joins on session_id must tolerate this).
- Re-delivering an *edited* file under the same name: events moved to different lines are reported as duplicates (first-wins), not updates.
- SQLite is single-writer; for scale use a warehouse with MERGE and partition by user/day, and bound recomputation to a late-data window.
- Whole-file failures on undecodable bytes / CSV parser errors (not per-row).
- Optimisation candidates: skip rewriting sessions whose content is unchanged; replace the per-event `UPDATE session_id` with a bulk join.

**Performance (100k valid events = 108k input rows, sandbox, two runs):** about 23k–32k rows/s end to end (~3.5–4.6 s). Comparing batch sizes: in one run batched lookups/inserts were ~1.3x faster than row-by-row (4.62 s vs 6.17 s); in a second run the difference was within noise (3.34–3.66 s vs 3.67 s). So batching helps a little but is not the bottleneck. The measured hotspot is the sessionize stage (~1.4–2.0 s of the total), dominated by one `UPDATE events_clean SET session_id` per event. The reasonable optimisation is to bulk-assign `session_id` (e.g. a single joined UPDATE, or rewriting only sessions whose content changed); it is identified but not implemented. See `output/benchmark_results.txt` (reproduce with `python scripts/benchmark.py 100000`).
