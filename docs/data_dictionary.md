# Data dictionary

Naming: snake_case tables/columns; all timestamps ISO-8601 UTC, fixed width `YYYY-MM-DDTHH:MM:SS.ffffffZ`.

## Source: `events.csv` (one row = one event)
| column | type | null? | notes |
|---|---|---|---|
| event_id | text | no | unique event key (business key) |
| user_id | text | no | must exist in `users.csv` / `dim_users` |
| event_type | text | no | one of page_view, click, search, add_to_cart, purchase, login, logout (case-insensitive) |
| event_ts | text | no | ISO-8601; `Z`/offset accepted, naive treated as UTC |
| page_url | text | yes | optional |
Extra columns are ignored (and reported); missing required columns fail the run.

## Source: `users.csv`
`user_id` (PK, required), `country` (optional).

## Target tables (SQLite)
**dim_users**(user_id PK, country)

**events_clean** – grain: one accepted event. PK `event_id`. FK-like: `user_id`→dim_users, `session_id`→sessions.
Derived: `event_ts` (normalised UTC), `event_epoch_us` (int µs, ordering/gap maths), `session_id`.
Lineage: `source_file`, `source_line`, `run_id`.

**sessions** – grain: one session. PK `session_id` = `s_` + sha1(`user_id|first_event_id`)[:16].
`session_start_ts/end_ts` = min/max event time, `duration_seconds`, `event_count`, first/last event id and type, `run_id` (run that last computed it).

**rejects** – grain: one rejected input row. `run_id, source_file, source_line, reason, detail, raw_record` (JSON of the original row).
Reasons: malformed_record, missing_required_field, missing_timestamp, invalid_timestamp, timestamp_out_of_range, invalid_event_type, invalid_reference_key, duplicate_event, conflicting_duplicate_event.

**pipeline_runs** – one row per run: `run_id, source_file, file_sha256, status (RUNNING|SUCCEEDED|FAILED|ABANDONED), started_at, ended_at, input_rows, accepted_new, replayed, rejected, users_affected, sessions_written, error`.
Invariant: `input_rows = accepted_new + replayed + rejected`.

## Business rules
1. Per user, order by (event time, event_id). New session when gap to previous event **> 1800 s** (strict; exactly 1800 s stays in session).
2. First occurrence wins for a duplicate `event_id`; later identical copies → `duplicate_event`, differing payload → `conflicting_duplicate_event`.
3. No maximum session length (a user active with gaps < 30 min forever has one session).
