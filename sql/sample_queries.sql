-- Run with: sqlite3 output/sample/sessions.db < sql/sample_queries.sql
-- 1. Sessions per user and average session length
SELECT user_id, COUNT(*) AS sessions, ROUND(AVG(duration_seconds),1) AS avg_duration_s, SUM(event_count) AS events
FROM sessions GROUP BY user_id ORDER BY user_id;
-- 2. Source-to-target reconciliation per run
SELECT run_id, status, input_rows, accepted_new, replayed, rejected,
       input_rows = accepted_new + replayed + rejected AS reconciles
FROM pipeline_runs ORDER BY started_at;
-- 3. Reject breakdown, traceable to the source line
SELECT reason, COUNT(*) AS n, MIN(source_file || ':' || source_line) AS example FROM rejects GROUP BY reason;
-- 4. Events that belong to a given session
SELECT e.event_id, e.event_ts, e.event_type FROM events_clean e
JOIN sessions s USING (session_id) WHERE s.user_id = 'u00001' ORDER BY e.event_epoch_us LIMIT 10;
-- 5. Gap rule check: no session may contain a gap > 1800s (expect 0 rows)
SELECT session_id FROM (
  SELECT session_id, event_epoch_us - LAG(event_epoch_us) OVER (PARTITION BY session_id ORDER BY event_epoch_us, event_id) AS gap
  FROM events_clean) WHERE gap > 1800 * 1000000;
