import csv
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sessionization.config import Config  # noqa: E402
from sessionization.store import connect  # noqa: E402

HEADER = ["event_id", "user_id", "event_type", "event_ts", "page_url"]
USERS = ["u1", "u2", "u3"]


@pytest.fixture
def cfg(tmp_path):
    return Config(db_path=str(tmp_path / "t.db"), batch_size=3)  # tiny batches exercise batching


@pytest.fixture
def users_csv(tmp_path):
    p = tmp_path / "users.csv"
    p.write_text("user_id,country\n" + "\n".join(f"{u},IN" for u in USERS) + "\n")
    return str(p)


@pytest.fixture
def write_events(tmp_path):
    def _w(name, rows, header=HEADER):
        p = tmp_path / name
        with open(p, "w", newline="") as fh:
            w = csv.writer(fh)
            if header is not None:
                w.writerow(header)
            w.writerows(rows)
        return str(p)
    return _w


@pytest.fixture
def db(cfg):
    def _q(sql, *args):
        c = connect(cfg.db_path)
        try:
            return [tuple(r) for r in c.execute(sql, args)]
        finally:
            c.close()
    return _q


def ev(i, user, ts, typ="page_view"):
    return [f"e{i}", user, typ, ts, "/p"]
