"""Compare real finished runs and pending requests in T3 with what t3-wake queued.

Usage: python3 -I tests/audit_live.py [FROM_SEQ]
FROM_SEQ defaults to the first T3 event of the last 24 hours. Read-only on both databases.
Use it after running live for a while to check that the user's own threads produced no wakes.
"""
import json
import os
import sqlite3
import sys
import time
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import t3_wake as tw  # noqa: E402

cfg = tw.load_config()
path = tw.discover_db(cfg)
if not path:
    sys.exit("No T3 database found.")
t3 = tw.T3(path)
st = sqlite3.connect(f"file:{tw.STATE_DB}?mode=ro", uri=True)
base = int(sys.argv[1]) if len(sys.argv) > 1 else t3.first_sequence_since(time.time() - 86400)
user_values = set(cfg["detection"]["user_created_by"])

woken = Counter(json.loads(b).get("threadId") for (b,) in st.execute("select body_json from events"))
seen, per_kind = set(), Counter()
for seq, tid, et, at, pj in t3.conn.execute(
        "select sequence, stream_id, event_type, occurred_at, payload_json from orchestration_events "
        "where sequence > ? and event_type in ('run.updated','runtime-request.updated') order by sequence", (base,)):
    p = json.loads(pj)
    if (et == "run.updated" and p.get("status") not in tw.TERMINAL) or \
            (et != "run.updated" and p.get("status") != "pending"):
        continue
    if (p["id"], p.get("status")) in seen:
        continue
    seen.add((p["id"], p.get("status")))
    t = t3.thread(tid) or {}
    lin = t.get("lineage") or {}
    kind = "child" if lin.get("parentThreadId") else f'{t.get("createdBy")}/{t.get("creationSource")}'
    per_kind[kind] += 1
    root = lin.get("rootThreadId") or tid
    print(f'{at[5:19]}  {kind:<14} {et.split(".")[0] + ":" + p.get("status", ""):<26} '
          f'{(t.get("title") or "")[:42]:<42} wakes for thread so far: {woken.get(root, 0)}')

print("\nfinished runs / requests by thread kind:", dict(per_kind))
user_kinds = [k for k in per_kind if k.split("/")[0] in user_values]
user_wakes = sum(n for tid, n in woken.items()
                 if tid and (t3.thread(tid) or {}).get("createdBy") in user_values)
print("events in user threads:", sum(per_kind[k] for k in user_kinds),
      "| wakes for user-created threads (allowlisted or MCP-steered runs only):", user_wakes)
