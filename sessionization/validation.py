"""Row-level validation and normalisation. Pure functions, no I/O."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from .config import Config

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_ONE_US = timedelta(microseconds=1)

# Reject reason codes (stable; used in reject table, metrics and tests)
MALFORMED = "malformed_record"
MISSING_REQUIRED = "missing_required_field"
MISSING_TS = "missing_timestamp"
INVALID_TS = "invalid_timestamp"
TS_OUT_OF_RANGE = "timestamp_out_of_range"
INVALID_EVENT_TYPE = "invalid_event_type"
INVALID_REF = "invalid_reference_key"
DUPLICATE = "duplicate_event"
CONFLICT = "conflicting_duplicate_event"


class SchemaError(Exception):
    """File-level problem (e.g. required column missing). Fails the whole run."""


@dataclass(frozen=True)
class CleanEvent:
    event_id: str
    user_id: str
    event_type: str
    event_ts: str          # normalised ISO-8601 UTC, fixed width, e.g. 2026-01-01T00:00:00.000000Z
    event_epoch_us: int    # microseconds since epoch (used for ordering and gap maths)
    page_url: Optional[str]


def to_epoch_us(dt: datetime) -> int:
    return (dt - EPOCH) // _ONE_US


def format_epoch_us(us: int) -> str:
    return (EPOCH + timedelta(microseconds=us)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def parse_timestamp(raw: str) -> int:
    """Parse ISO-8601 (with 'Z' or offset; naive is assumed UTC) -> epoch microseconds."""
    s = raw.strip()
    if "T" not in s and " " not in s:
        raise ValueError("timestamp must include a time component")
    if s[-1] in "Zz":
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return to_epoch_us(dt)


def validate_row(row: dict, header: list, cfg: Config, known_users: Optional[set]):
    """Return (CleanEvent, None, None) or (None, reason_code, detail)."""
    if None in row:  # csv.DictReader puts surplus fields under key None
        return None, MALFORMED, f"too many fields (expected {len(header)})"
    if any(row.get(c) is None for c in header):
        return None, MALFORMED, f"too few fields (expected {len(header)})"

    g = lambda k: (row.get(k) or "").strip()
    for col in ("event_id", "user_id", "event_type"):
        if not g(col):
            return None, MISSING_REQUIRED, f"{col} is empty"

    ts = g("event_ts")
    if not ts:
        return None, MISSING_TS, "event_ts is empty"
    try:
        us = parse_timestamp(ts)
    except (ValueError, OverflowError) as exc:
        return None, INVALID_TS, f"cannot parse {ts!r}: {exc}"
    year = (EPOCH + timedelta(microseconds=us)).year
    if not cfg.min_year <= year <= cfg.max_year:
        return None, TS_OUT_OF_RANGE, f"year {year} outside [{cfg.min_year}, {cfg.max_year}]"

    etype = g("event_type").lower()
    if etype not in cfg.allowed_event_types:
        return None, INVALID_EVENT_TYPE, f"unknown event_type {etype!r}"

    if known_users is not None and g("user_id") not in known_users:
        return None, INVALID_REF, f"user_id {g('user_id')!r} not in dim_users"

    return CleanEvent(g("event_id"), g("user_id"), etype, format_epoch_us(us), us,
                      g("page_url") or None), None, None
