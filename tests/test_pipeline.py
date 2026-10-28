import json
import os

import pytest

from sessionization import reporting
from sessionization.pipeline import run_pipeline
from sessionization.store import connect
from sessionization.validation import SchemaError
from conftest import HEADER, ev


def sessions(db):
    return db("SELECT user_id, session_start_ts, event_count FROM sessions ORDER BY user_id, session_start_ts")


def test_happy_path(cfg, users_csv, write_events, db):
    f = write_events("a.csv", [ev(1, "u1", "2026-01-01T10:00:00Z"), ev(2, "u1", "2026-01-01T10:10:00Z"),
                               ev(3, "u1", "2026-01-01T12:00:00Z"), ev(4, "u2", "2026-01-01T10:00:00Z")])
    r = run_pipeline(f, cfg, users_csv)
    assert (r.status, r.input_rows, r.accepted_new, r.rejected, r.sessions_written) == ("SUCCEEDED", 4, 4, 0, 3)
    assert [x[2] for x in sessions(db)] == [2, 1, 1]
    assert db("SELECT status, input_rows FROM pipeline_runs") == [("SUCCEEDED", 4)]


def test_empty_header_only_and_zero_byte_files(cfg, users_csv, write_events, tmp_path, db):
    r = run_pipeline(write_events("h.csv", []), cfg, users_csv)
    assert (r.status, r.input_rows, r.sessions_written) == ("SUCCEEDED", 0, 0)
    z = tmp_path / "zero.csv"; z.write_text("")
    r = run_pipeline(str(z), cfg, users_csv)
    assert (r.status, r.input_rows) == ("SUCCEEDED", 0)
    assert sessions(db) == []


def test_malformed_and_null_rows_are_quarantined_with_source_line(cfg, users_csv, write_events, db):
    f = write_events("m.csv", [ev(1, "u1", "2026-01-01T10:00:00Z"),
                               ["e2", "u1", "click", "2026-01-01T10:01:00Z", "/p", "EXTRA"],   # too many
                               ["e3", "u1", "click"],                                           # too few
                               ["e4", "", "click", "2026-01-01T10:02:00Z", "/p"],               # null user
                               ["e5", "u1", "click", "", "/p"],                                 # null ts
                               ["e6", "u1", "click", "garbage", "/p"],
                               ["e7", "ghost", "click", "2026-01-01T10:03:00Z", "/p"],          # bad ref
                               ["e8", "u1", "swipe", "2026-01-01T10:03:00Z", "/p"]])
    r = run_pipeline(f, cfg, users_csv)
    assert r.accepted_new == 1 and r.rejected == 7
    assert r.rejects_by_reason == {"malformed_record": 2, "missing_required_field": 1, "missing_timestamp": 1,
                                   "invalid_timestamp": 1, "invalid_reference_key": 1, "invalid_event_type": 1}
    lines = [x[0] for x in db("SELECT source_line FROM rejects ORDER BY source_line")]
    assert lines == [3, 4, 5, 6, 7, 8, 9]          # header is line 1


def test_in_file_duplicates_and_conflicting_duplicates(cfg, users_csv, write_events, db):
    f = write_events("d.csv", [ev(1, "u1", "2026-01-01T10:00:00Z"), ev(1, "u1", "2026-01-01T10:00:00Z"),
                               ev(1, "u1", "2026-01-01T11:00:00Z"), ev(2, "u1", "2026-01-01T10:05:00Z")])
    r = run_pipeline(f, cfg, users_csv)
    assert r.accepted_new == 2
    assert r.rejects_by_reason == {"conflicting_duplicate_event": 1, "duplicate_event": 1}
    assert sessions(db) == [("u1", "2026-01-01T10:00:00.000000Z", 2)]


def test_cross_file_duplicate_is_rejected_not_double_counted(cfg, users_csv, write_events, db):
    run_pipeline(write_events("a.csv", [ev(1, "u1", "2026-01-01T10:00:00Z")]), cfg, users_csv)
    r = run_pipeline(write_events("b.csv", [ev(1, "u1", "2026-01-01T10:00:00Z")]), cfg, users_csv)
    assert r.rejects_by_reason == {"duplicate_event": 1}
    assert db("SELECT COUNT(*) FROM events_clean") == [(1,)]


def test_boundary_values_through_pipeline(cfg, users_csv, write_events, db):
    f = write_events("b.csv", [ev(1, "u1", "2026-01-01T10:00:00Z"), ev(2, "u1", "2026-01-01T10:30:00Z"),     # == 1800s
                               ev(3, "u1", "2026-01-01T11:00:01Z")])                                         # 1801s later
    run_pipeline(f, cfg, users_csv)
    assert [x[2] for x in sessions(db)] == [2, 1]


def test_out_of_order_within_file(cfg, users_csv, write_events, db):
    f = write_events("o.csv", [ev(3, "u1", "2026-01-01T12:00:00Z"), ev(2, "u1", "2026-01-01T10:10:00Z"),
                               ev(1, "u1", "2026-01-01T10:00:00Z")])
    run_pipeline(f, cfg, users_csv)
    assert [x[2] for x in sessions(db)] == [2, 1]


def test_late_arriving_event_in_later_file_merges_sessions(cfg, users_csv, write_events, db):
    run_pipeline(write_events("a.csv", [ev(1, "u1", "2026-01-01T10:00:00Z"),
                                        ev(2, "u1", "2026-01-01T11:00:00Z")]), cfg, users_csv)
    assert [x[2] for x in sessions(db)] == [1, 1]
    run_pipeline(write_events("late.csv", [ev(3, "u1", "2026-01-01T10:30:00Z")]), cfg, users_csv)  # bridges the gap
    assert [x[2] for x in sessions(db)] == [3]
    assert reporting.reconcile(connect(cfg.db_path), cfg)["passed"]


def test_rerun_same_input_is_idempotent(cfg, users_csv, write_events, db):
    f = write_events("a.csv", [ev(1, "u1", "2026-01-01T10:00:00Z"), ev(1, "u1", "2026-01-01T10:00:00Z"),
                               ev(2, "u2", "2026-01-01T10:00:00Z"), ["x", "", "", "", ""]])
    r1 = run_pipeline(f, cfg, users_csv)
    snap = lambda: (db("SELECT * FROM events_clean ORDER BY event_id"),
                    [x[:-1] for x in db("SELECT * FROM sessions ORDER BY session_id")],   # drop run_id
                    db("SELECT source_line, reason FROM rejects ORDER BY source_line"))
    before = snap()
    r2 = run_pipeline(f, cfg, users_csv)
    after = snap()
    assert (r2.accepted_new, r2.replayed) == (0, 2)
    assert r2.rejects_by_reason == r1.rejects_by_reason
    assert [r[:-1] for r in before[0]] == [r[:-1] for r in after[0]]      # same rows (run_id aside)
    assert before[1:] == after[1:]
    assert db("SELECT COUNT(*) FROM events_clean") == [(2,)]


def test_schema_change_missing_required_column_fails_run_and_loads_nothing(cfg, users_csv, write_events, db):
    f = write_events("s.csv", [["e1", "u1", "click", "/p"]], header=["event_id", "user_id", "event_type", "page_url"])
    with pytest.raises(SchemaError):
        run_pipeline(f, cfg, users_csv)
    status, err = db("SELECT status, error FROM pipeline_runs")[0]
    assert status == "FAILED" and "event_ts" in err
    assert db("SELECT COUNT(*) FROM events_clean") == [(0,)]


def test_unexpected_extra_column_is_ignored_but_reported(cfg, users_csv, write_events, db):
    f = write_events("x.csv", [ev(1, "u1", "2026-01-01T10:00:00Z") + ["chrome"]], header=HEADER + ["browser"])
    r = run_pipeline(f, cfg, users_csv)
    assert r.status == "SUCCEEDED" and r.unexpected_columns == ["browser"] and r.accepted_new == 1


def test_header_is_case_and_whitespace_insensitive(cfg, users_csv, write_events):
    f = write_events("c.csv", [ev(1, "u1", "2026-01-01T10:00:00Z")],
                     header=[" Event_ID", "USER_ID", "event_type ", "Event_TS", "page_url"])
    assert run_pipeline(f, cfg, users_csv).accepted_new == 1


def test_partial_failure_rolls_back_then_retry_succeeds(cfg, users_csv, write_events, db):
    f = write_events("p.csv", [ev(i, "u1", f"2026-01-01T10:{i:02d}:00Z") for i in range(1, 10)])

    def boom(rows_done):
        if rows_done >= 3:
            raise RuntimeError("simulated crash after first batch")

    with pytest.raises(RuntimeError):
        run_pipeline(f, cfg, users_csv, fault_injector=boom)
    assert db("SELECT COUNT(*) FROM events_clean") == [(0,)]            # nothing partially loaded
    assert db("SELECT COUNT(*) FROM sessions") == [(0,)]
    assert db("SELECT status FROM pipeline_runs") == [("FAILED",)]
    r = run_pipeline(f, cfg, users_csv)                                  # retry
    assert r.status == "SUCCEEDED" and r.accepted_new == 9
    assert sorted(x[0] for x in db("SELECT status FROM pipeline_runs")) == ["FAILED", "SUCCEEDED"]


def test_metrics_file_written(cfg, users_csv, write_events, tmp_path):
    f = write_events("a.csv", [ev(1, "u1", "2026-01-01T10:00:00Z")])
    r = run_pipeline(f, cfg, users_csv, metrics_dir=str(tmp_path / "m"))
    m = json.load(open(tmp_path / "m" / f"{r.run_id}.json"))
    assert m["status"] == "SUCCEEDED" and m["input_rows"] == 1 and m["started_at"] and m["ended_at"]


def test_end_to_end_against_generator_ground_truth(tmp_path):
    from sessionization import generator
    from sessionization.config import Config
    d = str(tmp_path / "gen")
    generator.generate(d, n_events=3000, n_users=50, seed=7)
    cfg = Config(db_path=str(tmp_path / "g.db"))
    run_pipeline(os.path.join(d, "events.csv"), cfg, os.path.join(d, "users.csv"))
    conn = connect(cfg.db_path)
    cmp = reporting.compare_to_labels(conn, os.path.join(d, "labels"))
    assert cmp["sessions_match"] and cmp["rejects_match"], cmp
    assert reporting.reconcile(conn, cfg)["passed"]


def test_generator_is_reproducible(tmp_path):
    from sessionization import generator
    generator.generate(str(tmp_path / "a"), 500, 10, seed=1)
    generator.generate(str(tmp_path / "b"), 500, 10, seed=1)
    assert open(tmp_path / "a" / "events.csv").read() == open(tmp_path / "b" / "events.csv").read()


def test_reject_raw_record_labels_extra_fields(cfg, users_csv, write_events, db):
    f = write_events("e.csv", [["e1", "u1", "click", "2026-01-01T10:00:00Z", "/p", "EXTRA"]])
    run_pipeline(f, cfg, users_csv)
    raw = json.loads(db("SELECT raw_record FROM rejects")[0][0])
    assert raw["_extra_fields"] == ["EXTRA"] and "null" not in raw
