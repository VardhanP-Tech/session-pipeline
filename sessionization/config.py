"""Central configuration. All tunables live here so runs are reproducible."""
from __future__ import annotations

from dataclasses import dataclass

REQUIRED_COLUMNS = ("event_id", "user_id", "event_type", "event_ts")
OPTIONAL_COLUMNS = ("page_url",)
KNOWN_COLUMNS = frozenset(REQUIRED_COLUMNS + OPTIONAL_COLUMNS)

DEFAULT_EVENT_TYPES = frozenset(
    {"page_view", "click", "search", "add_to_cart", "purchase", "login", "logout"}
)


@dataclass(frozen=True)
class Config:
    db_path: str = "output/sessions.db"
    # A new session starts when the gap to the previous event is STRICTLY greater
    # than this value. A gap exactly equal to the timeout stays in the same session.
    session_gap_seconds: int = 1800
    allowed_event_types: frozenset = DEFAULT_EVENT_TYPES
    min_year: int = 2000          # timestamps outside [min_year, max_year] are rejected
    max_year: int = 2100
    batch_size: int = 500         # rows per lookup/insert batch (<= 900: SQLite variable limit)

    def __post_init__(self):
        if self.session_gap_seconds <= 0:
            raise ValueError("session_gap_seconds must be > 0")
        if not 1 <= self.batch_size <= 900:
            raise ValueError("batch_size must be between 1 and 900")
