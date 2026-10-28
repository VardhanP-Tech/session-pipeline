"""Pure-algorithm tests (no database)."""
from sessionization.sessionize import EventRow, make_session_id, sessionize

S = 1_000_000


def rows(*pairs, user="u1"):
    return [EventRow(f"e{i}", user, "click", t * S) for i, t in pairs]


def test_happy_path_two_sessions():
    sess, assign = sessionize(rows((1, 0), (2, 60), (3, 5000)), 1800)
    assert [s.event_count for s in sess] == [2, 1]
    assert len(assign) == 3


def test_boundary_gap_equal_to_timeout_stays_in_session():
    sess, _ = sessionize(rows((1, 0), (2, 1800)), 1800)
    assert len(sess) == 1


def test_boundary_gap_one_microsecond_over_splits():
    ev = [EventRow("a", "u1", "click", 0), EventRow("b", "u1", "click", 1800 * S + 1)]
    sess, _ = sessionize(ev, 1800)
    assert len(sess) == 2


def test_out_of_order_input_gives_same_result_as_sorted():
    a = sessionize(rows((1, 0), (2, 100), (3, 9000), (4, 9100)), 1800)
    b = sessionize(rows((4, 9100), (2, 100), (3, 9000), (1, 0)), 1800)
    assert a == b


def test_same_timestamp_events_tiebreak_by_event_id_deterministically():
    sess, _ = sessionize(rows((2, 10), (1, 10)), 1800)
    assert sess[0].first_event_id == "e1" and sess[0].last_event_id == "e2"


def test_users_never_mix():
    ev = [EventRow("a", "u1", "click", 0), EventRow("b", "u2", "click", 1)]
    sess, _ = sessionize(ev, 1800)
    assert sorted(s.user_id for s in sess) == ["u1", "u2"]


def test_single_event_session_has_zero_duration_and_empty_input_ok():
    sess, _ = sessionize(rows((1, 5)), 1800)
    assert sess[0].start_us == sess[0].end_us
    assert sessionize([], 1800) == ([], [])


def test_session_id_deterministic():
    assert make_session_id("u1", "e1") == make_session_id("u1", "e1") != make_session_id("u1", "e2")
