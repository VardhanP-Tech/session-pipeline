"""Exports, reconciliation checks and ground-truth comparison."""
from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timezone

from .config import Config


def export_table(conn, query: str, path: str) -> int:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    cur = conn.execute(query)
    cols = [d[0] for d in cur.description]
    n = 0
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(cols)
        for row in cur:
            w.writerow(list(row)); n += 1
    return n


def reconcile(conn, cfg: Config) -> dict:
    """Independent SQL verification of the published output (does not reuse the Python algorithm)."""
    one = lambda q, *a: conn.execute(q, a).fetchone()[0]
    checks = []

    def add(name, passed, detail):
        checks.append({"check": name, "passed": bool(passed), "detail": detail})

    n_events = one("SELECT COUNT(*) FROM events_clean")
    add("event_id_unique", n_events == one("SELECT COUNT(DISTINCT event_id) FROM events_clean"),
        f"{n_events} events")
    n_sess_events = one("SELECT COALESCE(SUM(event_count),0) FROM sessions")
    add("sum(session.event_count) == count(events_clean)", n_sess_events == n_events,
        f"{n_sess_events} vs {n_events}")
    add("every_event_has_a_session", one("SELECT COUNT(*) FROM events_clean WHERE session_id IS NULL") == 0,
        "events with NULL session_id = %d" % one("SELECT COUNT(*) FROM events_clean WHERE session_id IS NULL"))
    bad = one("""SELECT COUNT(*) FROM sessions s LEFT JOIN
                 (SELECT session_id, COUNT(*) c, MIN(event_ts) lo, MAX(event_ts) hi FROM events_clean GROUP BY session_id) e
                 ON e.session_id = s.session_id
                 WHERE e.c IS NOT s.event_count OR e.lo IS NOT s.session_start_ts OR e.hi IS NOT s.session_end_ts""")
    add("session_count_and_bounds_match_events", bad == 0, f"mismatching sessions = {bad}")
    gap_us = cfg.session_gap_seconds * 1_000_000
    viol = one("""SELECT COUNT(*) FROM (
                    SELECT session_id sid,
                           LAG(session_id) OVER (PARTITION BY user_id ORDER BY event_epoch_us, event_id) psid,
                           event_epoch_us - LAG(event_epoch_us) OVER (PARTITION BY user_id ORDER BY event_epoch_us, event_id) gap
                    FROM events_clean)
                  WHERE psid IS NOT NULL AND ((sid = psid AND gap > ?) OR (sid <> psid AND gap <= ?))""",
               gap_us, gap_us)
    add("gap_rule_holds_(window-function check)", viol == 0, f"violations = {viol}")
    if one("SELECT COUNT(*) FROM dim_users"):
        orphans = one("SELECT COUNT(*) FROM events_clean e LEFT JOIN dim_users u USING(user_id) WHERE u.user_id IS NULL")
        add("referential_integrity_events->dim_users", orphans == 0, f"orphans = {orphans}")
    bad_runs = [r["run_id"] for r in conn.execute(
        "SELECT run_id FROM pipeline_runs WHERE status='SUCCEEDED' AND "
        "input_rows IS NOT accepted_new + replayed + rejected")]
    add("per_run_source_to_target_counts", not bad_runs, f"unreconciled runs = {bad_runs}")
    by_reason = {r[0]: r[1] for r in conn.execute("SELECT reason, COUNT(*) FROM rejects GROUP BY reason ORDER BY reason")}
    return {"generated_at": datetime.now(timezone.utc).isoformat(), "passed": all(c["passed"] for c in checks),
            "events": n_events, "sessions": one("SELECT COUNT(*) FROM sessions"),
            "rejects_by_reason": by_reason, "checks": checks}


def compare_to_labels(conn, labels_dir: str) -> dict:
    """Compare pipeline output against generator ground truth (kept in a separate directory)."""
    with open(os.path.join(labels_dir, "expected_sessions.csv")) as fh:
        exp = {(r["user_id"], r["session_start_ts"], r["session_end_ts"], int(r["event_count"]))
               for r in csv.DictReader(fh)}
    got = {(r[0], r[1], r[2], r[3]) for r in conn.execute(
        "SELECT user_id, session_start_ts, session_end_ts, event_count FROM sessions")}
    out = {"expected_sessions": len(exp), "actual_sessions": len(got),
           "missing": sorted(exp - got)[:5], "unexpected": sorted(got - exp)[:5],
           "sessions_match": exp == got}
    rpath = os.path.join(labels_dir, "expected_rejects_by_reason.csv")
    if os.path.exists(rpath):
        with open(rpath) as fh:
            er = {r["reason"]: int(r["count"]) for r in csv.DictReader(fh)}
        gr = {r[0]: r[1] for r in conn.execute("SELECT reason, COUNT(*) FROM rejects GROUP BY reason")}
        out.update(expected_rejects=er, actual_rejects=gr, rejects_match=er == gr)
    return out


def write_report(report: dict, out_dir: str, labels: dict | None = None) -> None:
    os.makedirs(out_dir, exist_ok=True)
    if labels:
        report = {**report, "ground_truth_comparison": labels}
    with open(os.path.join(out_dir, "reconciliation_report.json"), "w") as fh:
        json.dump(report, fh, indent=2)
    lines = ["# Reconciliation report", "",
             f"Overall: **{'PASS' if report['passed'] else 'FAIL'}**  |  events={report['events']}  "
             f"sessions={report['sessions']}", "", "| check | result | detail |", "|---|---|---|"]
    lines += [f"| {c['check']} | {'PASS' if c['passed'] else 'FAIL'} | {c['detail']} |" for c in report["checks"]]
    lines += ["", "## Rejects by reason", "", "| reason | count |", "|---|---|"]
    lines += [f"| {k} | {v} |" for k, v in report["rejects_by_reason"].items()]
    if labels:
        lines += ["", "## Ground-truth comparison (generator labels)", "",
                  f"- sessions match expected: **{labels['sessions_match']}** "
                  f"({labels['actual_sessions']} actual / {labels['expected_sessions']} expected)"]
        if "rejects_match" in labels:
            lines.append(f"- rejects-by-reason match expected: **{labels['rejects_match']}**")
    with open(os.path.join(out_dir, "reconciliation_report.md"), "w") as fh:
        fh.write("\n".join(lines) + "\n")
