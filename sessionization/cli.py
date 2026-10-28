"""Command line interface:  python -m sessionization.cli {generate|run|report} ..."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import asdict

from . import generator, reporting
from .config import Config
from .pipeline import run_pipeline
from .store import connect, init_schema


def _publish(cfg: Config, out_dir: str, labels_dir: str | None) -> int:
    conn = connect(cfg.db_path)
    init_schema(conn)
    reporting.export_table(conn, "SELECT * FROM sessions ORDER BY user_id, session_start_ts",
                           os.path.join(out_dir, "sessions.csv"))
    reporting.export_table(conn, "SELECT run_id, source_file, source_line, reason, detail, raw_record "
                                 "FROM rejects ORDER BY source_file, source_line",
                           os.path.join(out_dir, "rejects.csv"))
    report = reporting.reconcile(conn, cfg)
    labels = reporting.compare_to_labels(conn, labels_dir) if labels_dir else None
    reporting.write_report(report, out_dir, labels)
    conn.close()
    print(f"reconciliation: {'PASS' if report['passed'] else 'FAIL'}  (see {out_dir}/reconciliation_report.md)")
    if labels:
        print(f"ground truth  : sessions_match={labels['sessions_match']} "
              f"rejects_match={labels.get('rejects_match')}")
    ok = report["passed"] and (labels is None or labels["sessions_match"])
    return 0 if ok else 2


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="sessionization")
    sub = p.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("generate", help="create synthetic events + users + labels")
    g.add_argument("--out-dir", default="data/sample")
    g.add_argument("--events", type=int, default=40)
    g.add_argument("--users", type=int, default=6)
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--gap-seconds", type=int, default=1800)

    for name in ("run", "report"):
        r = sub.add_parser(name)
        r.add_argument("--db", default="output/sessions.db")
        r.add_argument("--output-dir", default="output")
        r.add_argument("--gap-seconds", type=int, default=1800)
        r.add_argument("--batch-size", type=int, default=500)
        r.add_argument("--labels-dir", default=None, help="generator labels to compare against")
        if name == "run":
            r.add_argument("--input", required=True)
            r.add_argument("--users", default=None)

    a = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if a.cmd == "generate":
        print(json.dumps(generator.generate(a.out_dir, a.events, a.users, a.seed, a.gap_seconds), indent=2))
        return 0
    cfg = Config(db_path=a.db, session_gap_seconds=a.gap_seconds, batch_size=a.batch_size)
    if a.cmd == "run":
        try:
            res = run_pipeline(a.input, cfg, users_path=a.users, metrics_dir=os.path.join(a.output_dir, "runs"))
        except Exception as exc:  # already recorded as FAILED in pipeline_runs
            print(f"RUN FAILED: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(asdict(res), indent=2))
    return _publish(cfg, a.output_dir, a.labels_dir)


if __name__ == "__main__":
    sys.exit(main())
