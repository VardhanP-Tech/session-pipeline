"""Performance measurement: generate N events, run the pipeline with different batch sizes.

Usage: python scripts/benchmark.py [n_events]   (default 100000)
batch_size=1 emulates the naive row-by-row lookup/insert; 500 is the batched default.
"""
import os, sys, tempfile, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sessionization import generator
from sessionization.config import Config
from sessionization.pipeline import run_pipeline

n = int(sys.argv[1]) if len(sys.argv) > 1 else 100_000
with tempfile.TemporaryDirectory() as d:
    t = time.perf_counter()
    info = generator.generate(d, n_events=n, n_users=max(10, n // 20), seed=42)
    print(f"generated {info['total_rows']} rows in {time.perf_counter() - t:.1f}s")
    print(f"{'batch':>6} {'total_s':>8} {'rows/s':>10} {'load_s':>8} {'sessionize_s':>13}")
    for bs in (1, 100, 500, 900):
        cfg = Config(db_path=os.path.join(d, f"b{bs}.db"), batch_size=bs)
        r = run_pipeline(os.path.join(d, "events.csv"), cfg, os.path.join(d, "users.csv"))
        print(f"{bs:>6} {r.duration_seconds:>8.2f} {r.input_rows / r.duration_seconds:>10,.0f} "
              f"{r.stage_seconds['ingest_validate_load']:>8.2f} {r.stage_seconds['sessionize']:>13.2f}")
