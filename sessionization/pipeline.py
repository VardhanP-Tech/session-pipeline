"""Ingest -> validate -> dedupe -> publish -> sessionize, as ONE atomic transaction per file.

Idempotency / recovery model
  * events_clean.event_id is the primary key; a record is "replayed" (no-op) when the same
    event_id with identical payload already came from the same source_file + line.
  * Rejects for a source_file are deleted and re-derived on every run of that file.
  * Sessions are recomputed from scratch for every user touched by the file, using ALL stored
    events, so late-arriving / out-of-order data merges or splits sessions correctly.
  * Everything happens inside BEGIN..COMMIT: a failure leaves the previous state intact and the
    run can simply be retried (no partial loads).
"""
from __future__ import annotations

import csv
import hashlib
import itertools
import json
import logging
import os
import time
import uuid
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional

from . import validation as V
from .config import KNOWN_COLUMNS, REQUIRED_COLUMNS, Config
from .sessionize import EventRow, sessionize
from .store import chunked, connect, init_schema

log = logging.getLogger("sessionization")


class PipelineError(Exception):
    pass


@dataclass
class RunResult:
    run_id: str
    source_file: str
    status: str = "RUNNING"
    started_at: str = ""
    ended_at: str = ""
    duration_seconds: float = 0.0
    file_sha256: str = ""
    input_rows: int = 0
    accepted_new: int = 0
    replayed: int = 0
    rejected: int = 0
    rejects_by_reason: dict = field(default_factory=dict)
    users_affected: int = 0
    sessions_written: int = 0
    unexpected_columns: list = field(default_factory=list)
    stage_seconds: dict = field(default_factory=dict)
    error: Optional[str] = None


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%S_") + uuid.uuid4().hex[:6]


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


# --------------------------------------------------------------------------- public API
def run_pipeline(input_path: str, cfg: Optional[Config] = None, users_path: Optional[str] = None,
                 run_id: Optional[str] = None, metrics_dir: Optional[str] = None,
                 fault_injector: Optional[Callable[[int], None]] = None) -> RunResult:
    """Process one events file. Raises on failure AFTER recording status=FAILED in pipeline_runs.

    fault_injector(rows_processed) is a test hook called after every batch.
    """
    cfg = cfg or Config()
    source_file = os.path.basename(input_path)
    res = RunResult(run_id=run_id or new_run_id(), source_file=source_file, started_at=_now())
    t_start = time.perf_counter()
    conn = connect(cfg.db_path)
    init_schema(conn)
    try:
        conn.execute("UPDATE pipeline_runs SET status='ABANDONED', error='superseded by later run' "
                     "WHERE status='RUNNING' AND source_file=?", (source_file,))
        conn.execute("INSERT INTO pipeline_runs(run_id, source_file, status, started_at) VALUES (?,?,?,?)",
                     (res.run_id, source_file, "RUNNING", res.started_at))
        log.info("run_id=%s start source_file=%s", res.run_id, source_file)
        try:
            conn.execute("BEGIN")
            _execute(conn, input_path, source_file, users_path, cfg, res, fault_injector)
            res.status = "SUCCEEDED"
            res.ended_at = _now()
            _update_run(conn, res)          # inside the same transaction -> atomic with the data
            conn.execute("COMMIT")
        except BaseException as exc:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            res.status, res.ended_at = "FAILED", _now()
            res.error = f"{type(exc).__name__}: {exc}"
            _update_run(conn, res)
            log.error("run_id=%s FAILED: %s", res.run_id, res.error)
            raise
        finally:
            res.duration_seconds = round(time.perf_counter() - t_start, 4)
            if metrics_dir:
                os.makedirs(metrics_dir, exist_ok=True)
                with open(os.path.join(metrics_dir, f"{res.run_id}.json"), "w") as fh:
                    json.dump(asdict(res), fh, indent=2)
        log.info("run_id=%s SUCCEEDED in=%d new=%d replayed=%d rejected=%d sessions=%d (%.2fs)",
                 res.run_id, res.input_rows, res.accepted_new, res.replayed, res.rejected,
                 res.sessions_written, res.duration_seconds)
        return res
    finally:
        conn.close()


# --------------------------------------------------------------------------- internals
def _update_run(conn, r: RunResult) -> None:
    conn.execute(
        "UPDATE pipeline_runs SET file_sha256=?, status=?, ended_at=?, input_rows=?, accepted_new=?, "
        "replayed=?, rejected=?, users_affected=?, sessions_written=?, error=? WHERE run_id=?",
        (r.file_sha256, r.status, r.ended_at, r.input_rows, r.accepted_new, r.replayed, r.rejected,
         r.users_affected, r.sessions_written, r.error, r.run_id))


def _load_users(conn, users_path: str) -> None:
    with open(users_path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        cols = [c.strip().lower() for c in (reader.fieldnames or [])]
        if "user_id" not in cols:
            raise V.SchemaError(f"{users_path}: missing required column user_id")
        reader.fieldnames = cols
        rows = [(r["user_id"].strip(), (r.get("country") or "").strip() or None)
                for r in reader if (r.get("user_id") or "").strip()]
    conn.executemany("INSERT OR REPLACE INTO dim_users(user_id, country) VALUES (?,?)", rows)
    log.info("loaded %d users from %s", len(rows), users_path)


def _execute(conn, input_path, source_file, users_path, cfg, res: RunResult, fault_injector):
    t0 = time.perf_counter()
    res.file_sha256 = _sha256(input_path)
    if users_path:
        _load_users(conn, users_path)
    known = {r[0] for r in conn.execute("SELECT user_id FROM dim_users")} or None
    if known is None:
        log.warning("dim_users is empty: referential check skipped")
    conn.execute("DELETE FROM rejects WHERE source_file=?", (source_file,))  # re-derived below
    reasons: Counter = Counter()
    affected: set = set()

    with open(input_path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        header = reader.fieldnames
        if header is None:
            log.warning("empty input file (no header): nothing to do")
        else:
            header = [h.strip().lower() for h in header]
            if len(set(header)) != len(header):
                raise V.SchemaError(f"duplicate column names in header: {header}")
            missing = [c for c in REQUIRED_COLUMNS if c not in header]
            if missing:
                raise V.SchemaError(f"missing required columns: {missing}")
            res.unexpected_columns = sorted(set(header) - KNOWN_COLUMNS)
            if res.unexpected_columns:
                log.warning("unexpected columns ignored: %s", res.unexpected_columns)
            reader.fieldnames = header
            batch = []
            for row in reader:
                res.input_rows += 1
                batch.append((reader.line_num, row))
                if len(batch) >= cfg.batch_size:
                    _process_batch(conn, batch, header, cfg, known, source_file, res, reasons, affected)
                    batch = []
                    if fault_injector:
                        fault_injector(res.input_rows)
            if batch:
                _process_batch(conn, batch, header, cfg, known, source_file, res, reasons, affected)
                if fault_injector:
                    fault_injector(res.input_rows)
    res.stage_seconds["ingest_validate_load"] = round(time.perf_counter() - t0, 4)

    t1 = time.perf_counter()
    res.users_affected = len(affected)
    res.sessions_written = _rebuild_sessions(conn, sorted(affected), cfg, res.run_id)
    res.stage_seconds["sessionize"] = round(time.perf_counter() - t1, 4)

    res.rejected = sum(reasons.values())
    res.rejects_by_reason = dict(sorted(reasons.items()))
    # source-to-target reconciliation guard: every input row must be accounted for
    if res.input_rows != res.accepted_new + res.replayed + res.rejected:
        raise PipelineError(
            f"row accounting mismatch: in={res.input_rows} new={res.accepted_new} "
            f"replayed={res.replayed} rejected={res.rejected}")


def _raw(row: dict) -> str:
    """JSON of the original row; surplus CSV fields (stored under key None) are labelled."""
    if None in row:
        row = {**{k: v for k, v in row.items() if k is not None}, "_extra_fields": row[None]}
    return json.dumps(row, default=str, ensure_ascii=False)


def _process_batch(conn, batch, header, cfg, known, source_file, res, reasons, affected):
    rejects, valid = [], []
    for line_no, row in batch:
        ev, reason, detail = V.validate_row(row, header, cfg, known)
        if reason:
            rejects.append((res.run_id, source_file, line_no, reason, detail,
                            _raw(row)))
            reasons[reason] += 1
        else:
            valid.append((line_no, ev, row))

    ids = [ev.event_id for _, ev, _ in valid]
    existing = {}
    if ids:
        q = ("SELECT event_id, user_id, event_type, event_epoch_us, source_file, source_line "
             f"FROM events_clean WHERE event_id IN ({','.join('?' * len(ids))})")
        existing = {r[0]: tuple(r[1:]) for r in conn.execute(q, ids)}

    new_rows = []
    for line_no, ev, row in valid:
        payload = (ev.user_id, ev.event_type, ev.event_epoch_us)
        prior = existing.get(ev.event_id)
        if prior is None:
            existing[ev.event_id] = payload + (source_file, line_no)   # visible to later rows in batch
            new_rows.append((ev.event_id, ev.user_id, ev.event_type, ev.event_ts, ev.event_epoch_us,
                             ev.page_url, source_file, line_no, res.run_id))
            affected.add(ev.user_id)
            res.accepted_new += 1
        elif prior[:3] == payload and prior[3] == source_file and prior[4] == line_no:
            res.replayed += 1                                           # same record seen again
        else:
            reason = V.DUPLICATE if prior[:3] == payload else V.CONFLICT
            detail = f"event_id already loaded from {prior[3]}:{prior[4]}"
            rejects.append((res.run_id, source_file, line_no, reason, detail,
                            _raw(row)))
            reasons[reason] += 1

    if new_rows:
        conn.executemany("INSERT INTO events_clean(event_id,user_id,event_type,event_ts,event_epoch_us,"
                         "page_url,source_file,source_line,run_id) VALUES (?,?,?,?,?,?,?,?,?)", new_rows)
    if rejects:
        conn.executemany("INSERT INTO rejects(run_id,source_file,source_line,reason,detail,raw_record) "
                         "VALUES (?,?,?,?,?,?)", rejects)


def _rebuild_sessions(conn, users: list, cfg: Config, run_id: str) -> int:
    """Recompute sessions for the given users from all stored events (idempotent)."""
    written = 0
    for chunk in chunked(users, cfg.batch_size):
        marks = ",".join("?" * len(chunk))
        rows = conn.execute(
            f"SELECT event_id, user_id, event_type, event_epoch_us FROM events_clean "
            f"WHERE user_id IN ({marks}) ORDER BY user_id, event_epoch_us, event_id", chunk).fetchall()
        sessions, assignments = sessionize((EventRow(*r) for r in rows), cfg.session_gap_seconds)
        conn.execute(f"DELETE FROM sessions WHERE user_id IN ({marks})", chunk)
        conn.executemany(
            "INSERT INTO sessions(session_id,user_id,session_start_ts,session_end_ts,duration_seconds,"
            "event_count,first_event_id,last_event_id,first_event_type,last_event_type,run_id) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            [(s.session_id, s.user_id, V.format_epoch_us(s.start_us), V.format_epoch_us(s.end_us),
              (s.end_us - s.start_us) / 1e6, s.event_count, s.first_event_id, s.last_event_id,
              s.first_event_type, s.last_event_type, run_id) for s in sessions])
        conn.executemany("UPDATE events_clean SET session_id=? WHERE event_id=?",
                         [(sid, eid) for eid, sid in assignments])
        written += len(sessions)
    return written
