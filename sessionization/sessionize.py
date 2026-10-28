"""Core sessionization algorithm. Pure and deterministic."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Iterable, NamedTuple


class EventRow(NamedTuple):
    event_id: str
    user_id: str
    event_type: str
    epoch_us: int


@dataclass(frozen=True)
class Session:
    session_id: str
    user_id: str
    start_us: int
    end_us: int
    event_count: int
    first_event_id: str
    last_event_id: str
    first_event_type: str
    last_event_type: str


def make_session_id(user_id: str, first_event_id: str) -> str:
    """Deterministic id derived from the session's first event.
    NOTE: if late data arrives that precedes the first event, the session (and id) changes."""
    return "s_" + hashlib.sha1(f"{user_id}|{first_event_id}".encode()).hexdigest()[:16]


def sessionize(events: Iterable[EventRow], gap_seconds: int):
    """Group events into sessions per user using event-time gaps.

    Rule: sort by (user_id, epoch_us, event_id); start a new session when the user
    changes or gap > gap_seconds (strictly). Out-of-order input is therefore harmless.
    Returns (sessions, assignments) where assignments is [(event_id, session_id), ...].
    """
    gap_us = gap_seconds * 1_000_000
    ordered = sorted(events, key=lambda e: (e.user_id, e.epoch_us, e.event_id))
    sessions, assignments = [], []
    cur: list = []

    def flush():
        if not cur:
            return
        first, last = cur[0], cur[-1]
        sid = make_session_id(first.user_id, first.event_id)
        sessions.append(Session(sid, first.user_id, first.epoch_us, last.epoch_us, len(cur),
                                first.event_id, last.event_id, first.event_type, last.event_type))
        assignments.extend((e.event_id, sid) for e in cur)

    for ev in ordered:
        if cur and (ev.user_id != cur[-1].user_id or ev.epoch_us - cur[-1].epoch_us > gap_us):
            flush()
            cur = []
        cur.append(ev)
    flush()
    return sessions, assignments
