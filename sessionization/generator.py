"""Seeded synthetic data generator.

Writes (a) input files and (b) SEPARATE expected-answer labels computed from the clean events
before any noise is injected, so the pipeline can be verified against ground truth.
"""
from __future__ import annotations

import csv
import os
import random
from collections import Counter

from . import validation as V

EVENT_TYPES = ["page_view", "click", "search", "add_to_cart", "purchase", "login", "logout"]
BASE_EPOCH = 1_767_225_600  # 2026-01-01T00:00:00Z


def _fmt(sec: int, rng: random.Random) -> str:
    """Format an instant either as UTC 'Z' or with a +05:30 offset (same instant)."""
    if rng.random() < 0.1:
        from datetime import datetime, timedelta, timezone
        tz = timezone(timedelta(hours=5, minutes=30))
        return datetime.fromtimestamp(sec, tz).isoformat()
    return V.format_epoch_us(sec * 1_000_000).replace(".000000Z", "Z")


def generate(out_dir: str, n_events: int = 40, n_users: int = 6, seed: int = 42,
             gap_seconds: int = 1800, noise_rate: float = 0.08) -> dict:
    rng = random.Random(seed)
    users = [f"u{i:05d}" for i in range(1, n_users + 1)]
    cursor = {u: BASE_EPOCH + rng.randint(0, 30 * 86400) for u in users}
    valid, sessions = [], []   # valid: (event_id,user,type,sec); sessions: (user,start,end,count)
    n = 0
    while len(valid) < n_events:
        u = users[len(sessions) % len(users)]   # round-robin so every user gets sessions
        # next session starts strictly more than gap after previous event (boundary: exactly gap+1)
        start = cursor[u] + gap_seconds + (1 if rng.random() < 0.15 else rng.randint(1, 4 * 3600))
        ts = start
        k = rng.randint(1, 12)
        first = ts
        for i in range(k):
            if i:
                ts += gap_seconds if rng.random() < 0.05 else rng.randint(1, gap_seconds)  # boundary: == gap
            n += 1
            valid.append((f"e{n:08d}", u, rng.choice(EVENT_TYPES), ts))
        sessions.append((u, first, ts, k))
        cursor[u] = ts

    rows = [dict(event_id=e, user_id=u, event_type=t, event_ts=_fmt(s, rng),
                 page_url=f"/p/{rng.randint(1, 50)}") for e, u, t, s in valid]
    noise: list = []
    expected_rejects: Counter = Counter()
    kinds = ["duplicate_event", "missing_timestamp", "missing_required_field", "invalid_reference_key",
             "invalid_event_type", "malformed_record", "invalid_timestamp"]
    n_noise = max(len(kinds), int(len(rows) * noise_rate))
    for i in range(n_noise):
        kind = kinds[i % len(kinds)]
        n += 1
        base = dict(event_id=f"e{n:08d}", user_id=rng.choice(users), event_type="click",
                    event_ts=_fmt(BASE_EPOCH + rng.randint(0, 20 * 86400), rng), page_url="/x")
        if kind == "duplicate_event":
            noise.append(dict(rng.choice(rows)))
        elif kind == "missing_timestamp":
            base["event_ts"] = ""; noise.append(base)
        elif kind == "missing_required_field":
            base["user_id"] = ""; noise.append(base)
        elif kind == "invalid_reference_key":
            base["user_id"] = "u_ghost"; noise.append(base)
        elif kind == "invalid_event_type":
            base["event_type"] = "hover_over"; noise.append(base)
        elif kind == "invalid_timestamp":
            base["event_ts"] = "yesterday-ish"; noise.append(base)
        elif kind == "malformed_record":
            base["_extra"] = "oops"; noise.append(base)
        expected_rejects[kind] += 1

    # Out-of-order: local disorder everywhere + a few very late arrivals
    allrows = rows + noise
    allrows.sort(key=lambda r: r["event_ts"])
    for i in range(0, len(allrows), 10):
        blk = allrows[i:i + 10]; rng.shuffle(blk); allrows[i:i + 10] = blk
    for _ in range(max(1, len(allrows) // 100)):
        r = allrows.pop(rng.randrange(len(allrows))); allrows.append(r)

    os.makedirs(os.path.join(out_dir, "labels"), exist_ok=True)
    cols = ["event_id", "user_id", "event_type", "event_ts", "page_url"]
    with open(os.path.join(out_dir, "events.csv"), "w", newline="") as fh:
        w = csv.writer(fh); w.writerow(cols)
        for r in allrows:
            vals = [r.get(c, "") for c in cols]
            if "_extra" in r:
                vals.append(r["_extra"])
            w.writerow(vals)
    with open(os.path.join(out_dir, "users.csv"), "w", newline="") as fh:
        w = csv.writer(fh); w.writerow(["user_id", "country"])
        for u in users:
            w.writerow([u, rng.choice(["IN", "US", "DE", "BR"])])
    with open(os.path.join(out_dir, "labels", "expected_sessions.csv"), "w", newline="") as fh:
        w = csv.writer(fh); w.writerow(["user_id", "session_start_ts", "session_end_ts", "event_count"])
        for u, s, e, c in sorted(sessions):
            w.writerow([u, V.format_epoch_us(s * 1_000_000), V.format_epoch_us(e * 1_000_000), c])
    with open(os.path.join(out_dir, "labels", "expected_rejects_by_reason.csv"), "w", newline="") as fh:
        w = csv.writer(fh); w.writerow(["reason", "count"])
        for k, v in sorted(expected_rejects.items()):
            w.writerow([k, v])
    return {"valid_events": len(valid), "noise_rows": len(noise), "total_rows": len(allrows),
            "expected_sessions": len(sessions), "expected_rejects": dict(expected_rejects)}
