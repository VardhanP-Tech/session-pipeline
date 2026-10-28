import pytest

from sessionization import validation as V
from sessionization.config import Config

CFG = Config()
HDR = ["event_id", "user_id", "event_type", "event_ts", "page_url"]


def row(**kw):
    base = dict(event_id="e1", user_id="u1", event_type="click", event_ts="2026-01-01T00:00:00Z", page_url="/")
    base.update(kw)
    return base


@pytest.mark.parametrize("field,reason", [("event_id", V.MISSING_REQUIRED), ("user_id", V.MISSING_REQUIRED),
                                          ("event_type", V.MISSING_REQUIRED), ("event_ts", V.MISSING_TS)])
def test_nulls_rejected(field, reason):
    for empty in ("", "   "):
        _, r, _ = V.validate_row(row(**{field: empty}), HDR, CFG, None)
        assert r == reason


@pytest.mark.parametrize("ts", ["yesterday", "2026-13-45T00:00:00Z", "2026-01-01", "1700000000"])
def test_invalid_timestamps(ts):
    assert V.validate_row(row(event_ts=ts), HDR, CFG, None)[1] == V.INVALID_TS


def test_out_of_range_timestamp():
    assert V.validate_row(row(event_ts="1969-01-01T00:00:00Z"), HDR, CFG, None)[1] == V.TS_OUT_OF_RANGE


def test_timezone_normalised_to_utc_and_naive_is_utc():
    a = V.validate_row(row(event_ts="2026-01-01T05:30:00+05:30"), HDR, CFG, None)[0]
    b = V.validate_row(row(event_ts="2026-01-01T00:00:00"), HDR, CFG, None)[0]
    assert a.event_ts == b.event_ts == "2026-01-01T00:00:00.000000Z"


def test_event_type_case_insensitive_and_unknown_rejected():
    assert V.validate_row(row(event_type="CLICK"), HDR, CFG, None)[0].event_type == "click"
    assert V.validate_row(row(event_type="hover"), HDR, CFG, None)[1] == V.INVALID_EVENT_TYPE


def test_reference_key():
    assert V.validate_row(row(), HDR, CFG, {"u2"})[1] == V.INVALID_REF
    assert V.validate_row(row(), HDR, CFG, {"u1"})[1] is None


def test_malformed_extra_and_short_rows():
    assert V.validate_row({**row(), None: ["x"]}, HDR, CFG, None)[1] == V.MALFORMED
    assert V.validate_row({**row(), "page_url": None}, HDR, CFG, None)[1] == V.MALFORMED
