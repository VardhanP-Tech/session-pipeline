"""Run sql/sample_queries.sql against a DB and print results (no sqlite3 CLI needed).
Usage: python scripts/run_sample_queries.py output/sample/sessions.db"""
import re, sqlite3, sys
db = sys.argv[1] if len(sys.argv) > 1 else "output/sample/sessions.db"
sql = open(__file__.replace("scripts/run_sample_queries.py", "sql/sample_queries.sql")
           if "scripts/" in __file__ else "sql/sample_queries.sql").read()
conn = sqlite3.connect(db)
for block in re.split(r";\s*\n(?=--)|;\s*$", sql.strip()):
    block = block.strip()
    if not block:
        continue
    comments = [l.lstrip("- ").strip() for l in block.splitlines() if l.startswith("--")]
    title = comments[-1] if comments else ""
    stmt = "\n".join(l for l in block.splitlines() if not l.startswith("--"))
    if not stmt.strip():
        continue
    cur = conn.execute(stmt)
    print(f"### {title}")
    print(" | ".join(d[0] for d in cur.description))
    rows = cur.fetchall()
    for r in rows:
        print(" | ".join(str(v) for v in r))
    if not rows:
        print("(0 rows)")
    print()
