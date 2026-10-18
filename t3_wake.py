#!/usr/bin/env python3
"""t3-wake: POST a webhook when a T3 Code thread launched by an outside MCP client
finishes a turn, fails, or needs input.

It tails T3's append-only event log (statev2.sqlite, opened read-only), keeps only
threads the MCP client controls (never the user's own threads), debounces bursts,
and POSTs one webhook per wake with a bearer key. Standard library only.
"""
from __future__ import annotations

import argparse
import datetime as dt
import getpass
import glob
import hashlib
import json
import os
import secrets
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

VERSION = "1.1.0"
HOME = os.path.expanduser("~")
CONFIG_DIR = os.environ.get("T3_WAKE_CONFIG_DIR") or os.path.join(HOME, ".config", "t3-wake")
STATE_DIR = os.environ.get("T3_WAKE_STATE_DIR") or os.path.join(HOME, ".local", "state", "t3-wake")
T3_USERDATA = os.environ.get("T3_WAKE_T3_USERDATA") or os.path.join(HOME, ".t3", "userdata")
T3_DB_OVERRIDE = os.environ.get("T3_WAKE_T3_DB")
USE_SYSTEMCTL = os.environ.get("T3_WAKE_NO_SYSTEMCTL") != "1"
SECRET_STORE = os.environ.get("T3_WAKE_SECRET_STORE", "auto")  # auto | file
SERVICE = "t3-wake.service"
CRED_NAME = "t3-wake-webhook"
CRED_PATH = os.path.join(CONFIG_DIR, "webhook.cred")
PLAIN_SECRET_PATH = os.path.join(CONFIG_DIR, "webhook.json")
CONFIG_PATH = os.path.join(CONFIG_DIR, "config.json")
STATE_DB = os.path.join(STATE_DIR, "state.sqlite")

DEFAULTS = {
    "poll_interval_s": 1.0,
    "quiet_window_s": 20,          # turn_finished waits this long with no new run
    "urgent_grace_s": 5,           # failed/interrupted wait this long for a restart continuation
    "batch_window_s": 2,           # events ready within this window share one POST
    "per_thread_min_interval_s": 60,
    "max_hold_s": 600,             # send turn_finished even if the thread keeps running
    "global_cap_per_hour": 30,     # POSTs per hour; extra events wait and go out together
    "retry_backoff_s": [10, 30, 120, 600, 1800, 3600],
    "retry_give_up_s": 21600,
    "late_after_s": 120,
    "stale_drop_s": 21600,         # never send a wake for something older than this
    "http_timeout_s": 10,
    "selftest_interval_s": 3600,
    "degraded_retry_s": 60,
    "db_error_grace_s": 60,
    "keep_history_days": 14,
    "batch_limit": 5000,
    "source": "t3-wake",           # "source" field of every POST body
    # How t3-wake tells who started a thread or a run. See "How filtering works" in README.md.
    "detection": {
        "user_created_by": ["user"],          # message.createdBy for text the user typed
        "mcp_created_by": ["agent"],          # thread/message.createdBy for MCP launches
        "mcp_creation_source": ["mcp"],       # thread/message.creationSource for MCP launches
        "include_t3_agents": False,           # also count launches by agents running inside T3
    },
    # Rename values in the payload's "why" and "startedBy" fields, e.g. {"mcp_client": "bot"}.
    "labels": {},
}

TERMINAL = {"completed", "failed", "cancelled", "interrupted", "rolled_back"}
ACTIVE = {"queued", "preparing", "starting", "running", "waiting"}
KNOWN_RUN_STATUSES = TERMINAL | ACTIVE
RUN_EVENT = {
    "completed": "turn_finished",
    "failed": "run_failed",
    "cancelled": "run_cancelled",
    "interrupted": "run_interrupted",
    "rolled_back": "run_rolled_back",
}
WATCHED_TYPES = ("run.created", "run.updated", "runtime-request.updated")
REQUIRED_COLUMNS = {
    "orchestration_events": {"sequence", "stream_id", "aggregate_kind", "event_type",
                             "occurred_at", "payload_json", "metadata_json"},
    "orchestration_v2_projection_threads": {"thread_id", "project_id", "title", "payload_json"},
    "orchestration_v2_projection_messages": {"message_id", "payload_json"},
    "orchestration_v2_projection_runs": {"run_id", "thread_id", "status", "payload_json"},
    "orchestration_v2_projection_runtime_requests": {"runtime_request_id", "status"},
    "orchestration_v2_projection_turn_items": {"run_id", "type", "ordinal", "payload_json"},
    "projection_projects": {"project_id", "title"},
}


# ---------------------------------------------------------------- helpers

def now_s() -> float:
    return time.time()


def iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def parse_iso(s):
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(str(s).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def short_time(ts):
    if ts is None:
        return "never"
    return dt.datetime.fromtimestamp(float(ts), dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def ago(ts):
    if ts is None:
        return "never"
    s = max(0, int(now_s() - float(ts)))
    if s < 120:
        return f"{s}s ago"
    if s < 7200:
        return f"{s // 60}m ago"
    return f"{s // 3600}h ago"


def digest(*parts) -> str:
    return hashlib.sha256("|".join(str(p) for p in parts).encode()).hexdigest()


def log(msg: str) -> None:
    # journald adds its own timestamp
    prefix = "" if os.environ.get("JOURNAL_STREAM") else iso(now_s()) + " "
    print(prefix + msg, flush=True)


def load_config() -> dict:
    cfg = dict(DEFAULTS)
    try:
        with open(CONFIG_PATH) as f:
            user = json.load(f)
        for k, v in user.items():
            if k == "detection" and isinstance(v, dict):
                cfg[k] = {**DEFAULTS["detection"], **v}
            elif k in DEFAULTS or k == "t3_db":
                cfg[k] = v
    except FileNotFoundError:
        pass
    except (OSError, ValueError) as e:
        log(f"warning: ignoring unreadable config {CONFIG_PATH}: {e}")
    return cfg


# ---------------------------------------------------------------- state db

SCHEMA = """
create table if not exists kv(key text primary key, value text);
create table if not exists lists(
  thread_id text primary key, kind text not null check(kind in ('allow','mute')),
  added_at real not null, note text);
create table if not exists thread_state(
  thread_id text primary key, klass text, owner text,
  active_json text not null default '[]', last_tf_at real);
create table if not exists run_owner(
  run_id text primary key, thread_id text, started_by text, owner text, created_at real);
create table if not exists pending(
  thread_id text primary key, kind text not null, why text, first_at real, last_at real,
  due_at real, force integer not null default 0, runs_json text not null);
create table if not exists seen(key text primary key, at real);
create table if not exists events(
  event_id text primary key, created_at real, ready_at real, thread_id text, event text,
  body_json text, status text, delivery_id text);
create index if not exists events_status on events(status, ready_at);
create table if not exists deliveries(
  delivery_id text primary key, created_at real, body_json text, status text,
  attempts integer not null default 0, next_attempt_at real, first_attempt_at real,
  last_attempt_at real, last_code integer, last_error text, is_test integer not null default 0);
create index if not exists deliveries_status on deliveries(status, next_attempt_at);
"""


def open_state(path: str = STATE_DB) -> sqlite3.Connection:
    if path != ":memory:":
        os.makedirs(os.path.dirname(path), mode=0o700, exist_ok=True)
    old = os.umask(0o077)
    try:
        conn = sqlite3.connect(path, timeout=10, isolation_level=None)
    finally:
        os.umask(old)
    conn.row_factory = sqlite3.Row
    conn.execute("pragma busy_timeout=10000")
    if path != ":memory:":
        conn.execute("pragma journal_mode=wal")
    conn.executescript(SCHEMA)
    return conn


def kv_get(conn, key, default=None):
    r = conn.execute("select value from kv where key=?", (key,)).fetchone()
    return r[0] if r and r[0] is not None else default


def kv_set(conn, key, value):
    conn.execute(
        "insert into kv(key,value) values(?,?) on conflict(key) do update set value=excluded.value",
        (key, None if value is None else str(value)))


def kv_del(conn, key):
    conn.execute("delete from kv where key=?", (key,))


class tx:
    """BEGIN IMMEDIATE ... COMMIT/ROLLBACK on an autocommit connection."""

    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        self.conn.execute("begin immediate")
        return self.conn

    def __exit__(self, et, e, tb):
        self.conn.execute("commit" if et is None else "rollback")
        return False


def cancel_queue(st) -> int:
    """Drop everything not yet sent. Used by `off` and while disabled."""
    n = st.execute("delete from pending").rowcount
    n += st.execute("update events set status='cancelled' where status='queued'").rowcount
    n += st.execute("update deliveries set status='cancelled' where status in ('pending','retry')").rowcount
    return n


# ---------------------------------------------------------------- T3 reader

def ro_connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{urllib.parse.quote(path)}?mode=ro", uri=True, timeout=5,
                           isolation_level=None)
    conn.execute("pragma query_only=1")
    return conn


def discover_db(cfg: dict) -> str | None:
    """The live T3 database: the newest *.sqlite that has the v2 orchestration tables."""
    if T3_DB_OVERRIDE:
        return T3_DB_OVERRIDE
    if cfg.get("t3_db"):
        return os.path.expanduser(cfg["t3_db"])
    best = None
    for p in glob.glob(os.path.join(T3_USERDATA, "*.sqlite")):
        try:
            if os.path.getsize(p) == 0:
                continue
            c = ro_connect(p)
            names = {r[0] for r in c.execute("select name from sqlite_master where type='table'")}
            c.close()
        except (OSError, sqlite3.Error):
            continue
        if {"orchestration_events", "orchestration_v2_projection_threads"} <= names:
            m = max(os.path.getmtime(x) for x in (p, p + "-wal") if os.path.exists(x))
            if best is None or m > best[0]:
                best = (m, p)
    return best[1] if best else None


def t3_server_version() -> str | None:
    try:
        with open(os.path.join(T3_USERDATA, "server-runtime.json")) as f:
            return json.load(f).get("serverVersion")
    except (OSError, ValueError):
        return None


class T3:
    def __init__(self, path: str):
        self.path = path
        self.conn = ro_connect(path)
        self._threads: dict = {}

    def close(self):
        try:
            self.conn.close()
        except sqlite3.Error:
            pass

    def one(self, q, a=()):
        return self.conn.execute(q, a).fetchone()

    def clear_cache(self):
        self._threads.clear()

    def head(self) -> int:
        return self.one("select max(sequence) from orchestration_events")[0] or 0

    def events(self, after: int, upto: int, limit: int):
        return self.conn.execute(
            "select sequence, stream_id, event_type, occurred_at, payload_json, metadata_json "
            "from orchestration_events where sequence > ? and sequence <= ? "
            "and aggregate_kind = 'thread' and event_type in (?,?,?) order by sequence limit ?",
            (after, upto, *WATCHED_TYPES, limit)).fetchall()

    def first_sequence_since(self, ts: float) -> int:
        r = self.one("select min(sequence) from orchestration_events where occurred_at >= ?", (iso(ts),))
        return (r[0] or self.head() + 1) - 1

    def thread(self, tid):
        if tid in self._threads:
            return self._threads[tid]
        r = self.one("select title, project_id, payload_json from orchestration_v2_projection_threads "
                     "where thread_id = ?", (tid,))
        t = None
        if r:
            t = json.loads(r[2])
            t["id"], t["title"], t["projectId"] = tid, r[0], r[1]
        self._threads[tid] = t
        return t

    def message(self, mid):
        if not mid:
            return None
        r = self.one("select payload_json from orchestration_v2_projection_messages where message_id = ?", (mid,))
        return json.loads(r[0]) if r else None

    def project_name(self, pid):
        if not pid:
            return None
        r = self.one("select title from projection_projects where project_id = ?", (pid,))
        return r[0] if r else None

    def run_error(self, run_id):
        r = self.one("select payload_json from orchestration_v2_projection_turn_items "
                     "where run_id = ? and type = 'error' order by ordinal desc limit 1", (run_id,))
        if not r:
            return None
        f = json.loads(r[0]).get("failure") or {}
        msg = f.get("message")
        return f"{f['class']}: {msg}" if f.get("class") and msg else msg

    def request_status(self, req_id):
        r = self.one("select status from orchestration_v2_projection_runtime_requests "
                     "where runtime_request_id = ?", (req_id,))
        return r[0] if r else None

    def active_runs(self):
        q = ",".join("?" * len(ACTIVE))
        return self.conn.execute(
            f"select thread_id, run_id from orchestration_v2_projection_runs where status in ({q})",
            tuple(ACTIVE)).fetchall()

    def selftest(self) -> tuple[bool, str]:
        """Check that what this watcher relies on still looks the way it did when written."""
        try:
            for table, cols in REQUIRED_COLUMNS.items():
                have = {r[1] for r in self.conn.execute(f"pragma table_info({table})")}
                if not have:
                    return False, f"T3 table {table} is missing"
                missing = cols - have
                if missing:
                    return False, f"T3 table {table} lacks columns {sorted(missing)}"
            head = self.head()
            meta = self.one("select last_sequence from orchestration_v2_projection_metadata "
                            "where projection_name = 'thread-projections'")
            if meta and meta[0] is not None and meta[0] > head + 100:
                return False, (f"T3 projections are at sequence {meta[0]} but orchestration_events stops at "
                               f"{head}; events are probably being written somewhere else")
            r = self.one("select payload_json from orchestration_events where event_type = 'run.updated' "
                         "order by sequence desc limit 1")
            if r:
                p = json.loads(r[0])
                need = {"id", "threadId", "status", "ordinal"}
                if not need <= set(p):
                    return False, f"run.updated payload lacks {sorted(need - set(p))}"
                if p["status"] not in KNOWN_RUN_STATUSES:
                    return False, f"unknown run status {p['status']!r} in run.updated"
            r = self.one("select payload_json from orchestration_v2_projection_threads "
                         "order by created_at desc limit 1")
            if r:
                p = json.loads(r[0])
                need = {"createdBy", "creationSource", "lineage"}
                if not need <= set(p):
                    return False, f"thread payload lacks {sorted(need - set(p))}"
        except (sqlite3.Error, ValueError, KeyError) as e:
            return False, f"T3 database check failed: {e}"
        return True, "ok"


# ---------------------------------------------------------------- filtering + debounce

# Internal labels. The payload uses these unless config "labels" renames them.
USER, MCP_CLIENT, AGENT, UNKNOWN = "user", "mcp_client", "agent", "unknown"
WHY_LAUNCHED, WHY_STARTED_RUN, WHY_ALLOW = "mcp_launched", "mcp_started_run", "allowlist"


def is_mcp_launch(obj, det) -> bool:
    """True when a thread or message was created by an outside MCP client.

    An agent running inside T3 that calls the MCP tools gets the same createdBy/creationSource,
    but T3 stamps senderThreadId on its first message. An outside client has no sender thread.
    """
    if obj.get("createdBy") not in det["mcp_created_by"]:
        return False
    if obj.get("creationSource") not in det["mcp_creation_source"]:
        return False
    return bool(det["include_t3_agents"]) or not obj.get("senderThreadId")


def starter_of(m, det) -> str:
    """Who sent the message that started a run."""
    if not m:
        return UNKNOWN
    if m.get("createdBy") in det["user_created_by"]:
        return USER
    if is_mcp_launch(m, det):
        return MCP_CLIENT
    return AGENT  # T3 wake, provider wake, another thread, scheduler


class Engine:
    def __init__(self, st, t3: T3, cfg: dict, live: bool = True, logger=log):
        self.st, self.t3, self.cfg, self.live, self.log = st, t3, cfg, live, logger
        self._lists = None

    # -- small state accessors
    def lists(self):
        if self._lists is None:
            self._lists = {r[0]: r[1] for r in self.st.execute("select thread_id, kind from lists")}
        return self._lists

    def reset_batch_caches(self):
        self._lists = None

    def tstate(self, tid):
        r = self.st.execute("select * from thread_state where thread_id=?", (tid,)).fetchone()
        if not r:
            return {"klass": None, "owner": None, "active": [], "last_tf_at": None}
        return {"klass": r["klass"], "owner": r["owner"], "active": json.loads(r["active_json"]),
                "last_tf_at": r["last_tf_at"]}

    def save_tstate(self, tid, **kw):
        s = self.tstate(tid)
        s.update(kw)
        self.st.execute(
            "insert into thread_state(thread_id, klass, owner, active_json, last_tf_at) values(?,?,?,?,?) "
            "on conflict(thread_id) do update set klass=excluded.klass, owner=excluded.owner, "
            "active_json=excluded.active_json, last_tf_at=excluded.last_tf_at",
            (tid, s["klass"], s["owner"], json.dumps(sorted(set(s["active"]))), s["last_tf_at"]))

    def seen(self, key):
        return self.st.execute("select 1 from seen where key=?", (key,)).fetchone() is not None

    def mark_seen(self, key, at):
        self.st.execute("insert or ignore into seen(key, at) values(?,?)", (key, at))

    def pending(self, tid):
        r = self.st.execute("select * from pending where thread_id=?", (tid,)).fetchone()
        if not r:
            return None
        p = dict(r)
        p["runs"] = json.loads(p.pop("runs_json"))
        return p

    def save_pending(self, p):
        self.st.execute(
            "insert or replace into pending(thread_id, kind, why, first_at, last_at, due_at, force, runs_json) "
            "values(?,?,?,?,?,?,?,?)",
            (p["thread_id"], p["kind"], p["why"], p["first_at"], p["last_at"], p["due_at"],
             int(p.get("force", 0)), json.dumps(p["runs"])))

    def run_owner(self, run_id, tid, user_message_id, at):
        r = self.st.execute("select started_by, owner from run_owner where run_id=?", (run_id,)).fetchone()
        if r:
            return {"started_by": r[0], "owner": r[1]}
        starter = starter_of(self.t3.message(user_message_id), self.cfg["detection"])
        owner = starter if starter in (MCP_CLIENT, USER) else self.tstate(tid)["owner"]
        self.st.execute("insert or ignore into run_owner values(?,?,?,?,?)", (run_id, tid, starter, owner, at))
        return {"started_by": starter, "owner": owner}

    # -- who controls a thread
    def classify(self, t) -> str:
        tid = t["id"]
        k = self.tstate(tid)["klass"]
        if k:
            return k
        lin = t.get("lineage") or {}
        if lin.get("parentThreadId"):
            k = "child"
        elif is_mcp_launch({**t, "senderThreadId": None}, self.cfg["detection"]):
            first = self.t3.message(tid)  # t3_thread_launch: first message id == thread id
            if first is None:
                return UNKNOWN
            k = MCP_CLIENT if is_mcp_launch(first, self.cfg["detection"]) else "other"
        else:
            k = "other"
        self.save_tstate(tid, klass=k)
        return k

    def watched(self, tid, t, owner):
        kind = self.lists().get(tid)
        if kind == "mute":
            return None
        if kind == "allow":
            return WHY_ALLOW
        if t and self.classify(t) == MCP_CLIENT:
            return WHY_LAUNCHED
        if owner == MCP_CLIENT:
            return WHY_STARTED_RUN
        return None

    def label(self, value):
        return (self.cfg.get("labels") or {}).get(value, value) if value else value

    # -- event intake
    def process(self, seq, tid, etype, occurred_at, payload_json, meta_json, track_only=False):
        payload = json.loads(payload_json)
        meta = json.loads(meta_json) if meta_json else {}
        at = parse_iso(occurred_at) or now_s()
        t = self.t3.thread(tid)
        if t is None:
            return
        lin = t.get("lineage") or {}
        root = lin.get("rootThreadId") or tid
        is_child = bool(lin.get("parentThreadId"))
        if etype == "run.created" and not is_child:
            self._run_created(tid, payload, at)
        elif etype == "run.updated" and not is_child:
            self._run_updated(tid, t, payload, at, seq, track_only)
        elif etype == "runtime-request.updated":
            self._request(tid, root, payload, meta, at, seq, track_only)

    def _run_created(self, tid, payload, at):
        rid = payload.get("id")
        if not rid:
            return
        before = self.tstate(tid)
        ro = self.run_owner(rid, tid, payload.get("userMessageId"), at)
        owner = ro["owner"]
        active = set(before["active"])
        if payload.get("status") not in TERMINAL:
            active.add(rid)
        self.save_tstate(tid, owner=owner, active=sorted(active))
        p = self.pending(tid)
        if not p:
            return
        cont = payload.get("restartContinuationOfRunId")
        if p["kind"] == "urgent" and cont and cont in {r["id"] for r in p["runs"]}:
            # T3 is restarting the interrupted run; the continuation will report when it ends.
            self.st.execute("delete from pending where thread_id=?", (tid,))
        elif p["why"] == WHY_STARTED_RUN and owner != MCP_CLIENT:
            # The user took the thread back: report the MCP client's own run now.
            p["force"], p["due_at"] = 1, at
            self.save_pending(p)

    def _run_updated(self, tid, t, payload, at, seq, track_only):
        status, rid = payload.get("status"), payload.get("id")
        if status not in TERMINAL or not rid:
            return
        s = self.tstate(tid)
        if rid in s["active"]:
            self.save_tstate(tid, active=[r for r in s["active"] if r != rid])
        if track_only:
            return
        if status == "cancelled" and not payload.get("startedAt"):
            return  # a queued message that never ran
        key = f"run|{rid}|{status}"
        if self.seen(key):
            return
        ro = self.run_owner(rid, tid, payload.get("userMessageId"), at)
        why = self.watched(tid, t, ro["owner"])
        if not why:
            return
        self.mark_seen(key, at)
        run = {"id": rid, "status": status, "ordinal": payload.get("ordinal"), "startedBy": ro["started_by"],
               "provider": payload.get("providerInstanceId"),
               "model": (payload.get("modelSelection") or {}).get("model"), "at": at, "seq": seq}
        urgent = status != "completed"
        p = self.pending(tid)
        if p is None:
            p = {"thread_id": tid, "kind": "turn", "why": why, "first_at": at, "last_at": at,
                 "due_at": None, "force": 0, "runs": []}
        p["runs"].append(run)
        p["last_at"], p["why"] = at, why
        if urgent:
            p["kind"], p["due_at"] = "urgent", at + self.cfg["urgent_grace_s"]
        elif p["kind"] == "turn" and not p["force"]:
            p["due_at"] = at + self.cfg["quiet_window_s"]
        self.save_pending(p)

    def _request(self, tid, root, payload, meta, at, seq, track_only):
        if payload.get("status") != "pending" or track_only:
            return
        req_id = payload.get("id")
        key = f"req|{req_id}"
        if not req_id or self.seen(key):
            return
        rt = self.t3.thread(root)
        why = self.watched(root, rt, self.tstate(root)["owner"])
        if not why:
            return
        self.mark_seen(key, at)
        started_by = None
        run_id = meta.get("runId")
        if run_id and root == tid:
            r = self.st.execute("select started_by from run_owner where run_id=?", (run_id,)).fetchone()
            started_by = r[0] if r else None
        body = self.event_body("needs_input", root, rt, why, [], at, seq,
                               request={"id": req_id, "kind": payload.get("kind"),
                                        "childThreadId": tid if tid != root else None})
        body["runId"], body["startedBy"] = run_id, self.label(started_by)
        self.emit(digest(root, "needs_input", req_id), root, "needs_input", body, at)

    # -- output
    def event_body(self, event, tid, t, why, runs, at, seq, request=None, still_active=False):
        t = t or {}
        b = {"event": event, "threadId": tid, "title": t.get("title"), "projectId": t.get("projectId"),
             "projectName": self.t3.project_name(t.get("projectId")), "why": self.label(why)}
        if runs:
            first, last = runs[0], runs[-1]
            b.update(runId=last["id"], runStatus=last["status"], runsCoalesced=len(runs),
                     startedBy=self.label(first["startedBy"]), provider=last["provider"], model=last["model"])
            failed = [r for r in runs if r["status"] == "failed"]
            b["lastError"] = self.t3.run_error(failed[-1]["id"]) if failed else None
        b["request"] = request
        b["threadStillActive"] = still_active
        b["occurredAt"] = iso(at)
        b["eventSequence"] = seq
        return b

    def emit(self, event_id, tid, event, body, ready_at):
        if self.st.execute("select 1 from events where event_id=?", (event_id,)).fetchone():
            return False
        self.st.execute(
            "insert into events(event_id, created_at, ready_at, thread_id, event, body_json, status) "
            "values(?,?,?,?,?,?, 'queued')",
            (event_id, now_s() if self.live else ready_at, ready_at, tid, event, json.dumps(body)))
        what = f'"{body.get("title")}"' if body.get("title") else body.get("reason", "")
        self.log(f"queued {event} {what} ({body.get('why', '-')})")
        return True

    def tick(self, now):
        """Turn due debounce entries into events."""
        for r in self.st.execute("select thread_id from pending where due_at is not null and due_at <= ? "
                                 "order by due_at", (now,)).fetchall():
            p = self.pending(r[0])
            tid = p["thread_id"]
            s = self.tstate(tid)
            still_active = False
            if p["kind"] == "turn" and not p["force"]:
                busy = set(s["active"]) - {x["id"] for x in p["runs"]}
                if busy and now - p["first_at"] < self.cfg["max_hold_s"]:
                    continue  # more work queued or woke up: wait for it
                still_active = bool(busy)
                last = s["last_tf_at"]
                gap = self.cfg["per_thread_min_interval_s"]
                if last and now < last + gap:
                    p["due_at"] = last + gap
                    self.save_pending(p)
                    continue
            runs = p["runs"]
            if p["kind"] == "urgent":
                worst = [x for x in runs if x["status"] != "completed"][-1]
                event = RUN_EVENT[worst["status"]]
            else:
                event = "turn_finished"
            t = self.t3.thread(tid)
            body = self.event_body(event, tid, t, p["why"], runs, runs[-1]["at"], runs[-1]["seq"],
                                   still_active=still_active)
            self.emit(digest(tid, event, *sorted(x["id"] + ":" + x["status"] for x in runs)), tid, event, body, now)
            self.st.execute("delete from pending where thread_id=?", (tid,))
            if event == "turn_finished":
                self.save_tstate(tid, last_tf_at=now)

    def build_deliveries(self, now, t3_version=None):
        rows = self.st.execute("select * from events where status='queued' order by ready_at").fetchall()
        if not rows or rows[0]["ready_at"] + self.cfg["batch_window_s"] > now:
            return None
        keep = []
        for ev in rows:
            body = json.loads(ev["body_json"])
            occurred = parse_iso(body.get("occurredAt")) or ev["ready_at"]
            if now - occurred > self.cfg["stale_drop_s"]:
                self.st.execute("update events set status='stale' where event_id=?", (ev["event_id"],))
                self.log(f"dropped stale {ev['event']} for {ev['thread_id']} (from {iso(occurred)})")
                continue
            if self.live and self.t3 is not None and ev["event"] == "needs_input":
                try:
                    status = self.t3.request_status(body["request"]["id"])
                except sqlite3.Error:
                    status = "pending"
                if status not in (None, "pending"):
                    self.st.execute("update events set status='resolved' where event_id=?", (ev["event_id"],))
                    self.log(f"skipped needs_input for {ev['thread_id']}: already {status}")
                    continue
            body["late"] = now - occurred > self.cfg["late_after_s"]
            keep.append((ev["event_id"], body))
        if not keep:
            return None
        sent_last_hour = self.st.execute(
            "select count(*) from deliveries where is_test=0 and status!='cancelled' and created_at > ?",
            (now - 3600,)).fetchone()[0]
        if sent_last_hour >= self.cfg["global_cap_per_hour"]:
            if kv_get(self.st, "cap_logged") != "1":
                self.log(f"hourly cap of {self.cfg['global_cap_per_hour']} wakes reached; holding events")
                kv_set(self.st, "cap_logged", "1")
            return None
        kv_del(self.st, "cap_logged")
        ids = [k for k, _ in keep]
        did = "tw_" + digest(*sorted(ids))[:24]
        body = {"version": 1, "wakeId": did, "createdAt": iso(now), "sentAt": None, "source": self.cfg["source"],
                "t3Version": t3_version, "events": [b for _, b in keep]}
        self.st.execute(
            "insert or ignore into deliveries(delivery_id, created_at, body_json, status, next_attempt_at) "
            "values(?,?,?, 'pending', ?)", (did, now, json.dumps(body), now))
        self.st.executemany("update events set status='batched', delivery_id=? where event_id=?",
                            [(did, i) for i in ids])
        return did


# ---------------------------------------------------------------- webhook

class WebhookError(Exception):
    pass


def webhook_configured() -> str | None:
    if os.path.exists(CRED_PATH):
        return "systemd-creds"
    if os.path.exists(PLAIN_SECRET_PATH):
        return "file"
    return None


def load_webhook():
    """Return {"url", "key"} or None. Never log the result."""
    if os.path.exists(CRED_PATH):
        try:
            r = subprocess.run(["systemd-creds", "--user", "decrypt", f"--name={CRED_NAME}", CRED_PATH, "-"],
                               capture_output=True, timeout=20)
        except (OSError, subprocess.TimeoutExpired) as e:
            raise WebhookError(f"could not run systemd-creds: {type(e).__name__}") from None
        if r.returncode != 0:
            raise WebhookError(f"could not decrypt {CRED_PATH} (systemd-creds exit {r.returncode})")
        raw = r.stdout
    elif os.path.exists(PLAIN_SECRET_PATH):
        st = os.stat(PLAIN_SECRET_PATH)
        if st.st_mode & 0o077:
            raise WebhookError(f"{PLAIN_SECRET_PATH} must be mode 0600")
        with open(PLAIN_SECRET_PATH, "rb") as f:
            raw = f.read()
    else:
        return None
    try:
        data = json.loads(raw)
        validate_webhook(data["url"], data["key"])
    except (ValueError, KeyError, TypeError):
        raise WebhookError("stored webhook is unreadable; run `t3-wake set-webhook` again") from None
    return data


def validate_webhook(url: str, key: str):
    u = urllib.parse.urlsplit(url)
    local = u.hostname in ("127.0.0.1", "localhost", "::1")
    if not u.hostname or not (u.scheme == "https" or (u.scheme == "http" and local)):
        raise ValueError("webhook URL must be https://…")
    if not key or any(c.isspace() for c in key):
        raise ValueError("bearer key is empty or contains whitespace")


def redact(text, wh):
    if not text or not wh:
        return text
    for s in (wh.get("url"), wh.get("key")):
        if s:
            text = text.replace(s, "<redacted>")
            text = text.replace(urllib.parse.quote(s, safe=""), "<redacted>")
    return text


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k):
        return None  # never forward the bearer key to another location


_OPENER = urllib.request.build_opener(_NoRedirect)


def post_json(url, key, body, timeout, idem):
    data = json.dumps(body, separators=(",", ":")).encode()
    req = urllib.request.Request(url, data=data, method="POST", headers={
        "Content-Type": "application/json", "Authorization": f"Bearer {key}",
        "Idempotency-Key": idem, "User-Agent": f"t3-wake/{VERSION}"})
    t0 = time.monotonic()
    try:
        with _OPENER.open(req, timeout=timeout) as resp:
            resp.read(65536)
            return resp.status, None, None, time.monotonic() - t0
    except urllib.error.HTTPError as e:
        ra = e.headers.get("Retry-After") if e.headers else None
        return e.code, f"HTTP {e.code}", ra, time.monotonic() - t0
    except Exception as e:  # network errors, timeouts
        return None, f"{type(e).__name__}: {e}", None, time.monotonic() - t0


def summarize(body) -> str:
    parts = []
    for e in body.get("events", []):
        bits = [e.get("why") or "-"]
        if (e.get("runsCoalesced") or 0) > 1:
            bits.append(f"{e['runsCoalesced']} runs")
        if e.get("startedBy"):
            bits.append(f"by {e['startedBy']}")
        if e.get("request") and e["request"].get("childThreadId"):
            bits.append("in child")
        if e.get("late"):
            bits.append("late")
        title = f' "{e["title"]}"' if e.get("title") else (f" {e['reason']}" if e.get("reason") else "")
        parts.append(f"{e['event']}{title} ({', '.join(bits)})")
    return "; ".join(parts)


class Sender:
    def __init__(self, st, cfg, logger=log, loader=load_webhook):
        self.st, self.cfg, self.log, self.loader = st, cfg, logger, loader
        self._cache = (None, None)
        self._warned_missing = False

    def webhook(self):
        path = CRED_PATH if os.path.exists(CRED_PATH) else PLAIN_SECRET_PATH
        try:
            stamp = os.stat(path).st_mtime_ns
        except OSError:
            stamp = None
        if stamp is None:
            self._cache = (None, None)
            return None
        if self._cache[0] != stamp:
            self._cache = (stamp, self.loader())
        return self._cache[1]

    def send_due(self, now, mode):
        rows = self.st.execute("select * from deliveries where status in ('pending','retry') "
                               "and next_attempt_at <= ? order by created_at", (now,)).fetchall()
        for d in rows:
            body = json.loads(d["body_json"])
            summary = summarize(body)
            if mode != "live":
                self.log(f"[dry-run] would POST wake {d['delivery_id']}: {summary}")
                self.log("[dry-run] body=" + json.dumps(body, separators=(",", ":")))
                self.st.execute("update deliveries set status='dry_run', attempts=attempts+1, last_attempt_at=? "
                                "where delivery_id=? and status in ('pending','retry')", (now, d["delivery_id"]))
                continue
            if kv_get(self.st, "webhook_auth_failed"):
                continue
            err, wh = None, None
            try:
                wh = self.webhook()
            except WebhookError as e:
                err = str(e)
            if wh is None:
                err = err or "no webhook set (run `t3-wake set-webhook`)"
                if not self._warned_missing:
                    self.log(f"cannot send wake {d['delivery_id']}: {err}")
                    self._warned_missing = True
                self._retry(d, now, None, err, None)
                continue
            self._warned_missing = False
            body["sentAt"] = iso(now_s())
            code, err, ra, secs = post_json(wh["url"], wh["key"], body, self.cfg["http_timeout_s"], d["delivery_id"])
            err = redact(err, wh)
            first = d["first_attempt_at"] or now
            if code is not None and 200 <= code < 300:
                self.st.execute("update deliveries set status='sent', attempts=attempts+1, first_attempt_at=?, "
                                "last_attempt_at=?, last_code=?, last_error=null where delivery_id=?",
                                (first, now, code, d["delivery_id"]))
                self.log(f"sent wake {d['delivery_id']}: {summary} -> HTTP {code} ({secs:.1f}s)")
            elif code in (401, 403):
                self._fail(d, now, first, code, err)
                kv_set(self.st, "webhook_auth_failed", code)
                self.log(f"webhook rejected the key (HTTP {code}); not sending until `t3-wake set-webhook`")
            elif code is not None and 300 <= code < 500 and code not in (408, 425, 429):
                self._fail(d, now, first, code, err)
                self.log(f"webhook refused wake {d['delivery_id']} (HTTP {code}); not retrying")
            else:
                self._retry(d, now, code, err, ra, first)

    def _fail(self, d, now, first, code, err):
        self.st.execute("update deliveries set status='failed', attempts=attempts+1, first_attempt_at=?, "
                        "last_attempt_at=?, last_code=?, last_error=? where delivery_id=?",
                        (first, now, code, err, d["delivery_id"]))

    def _retry(self, d, now, code, err, retry_after, first=None):
        first = first or d["first_attempt_at"] or now
        attempts = d["attempts"] + 1
        if now - first >= self.cfg["retry_give_up_s"]:
            self.st.execute("update deliveries set status='gave_up', attempts=?, first_attempt_at=?, "
                            "last_attempt_at=?, last_code=?, last_error=? where delivery_id=?",
                            (attempts, first, now, code, err, d["delivery_id"]))
            self.log(f"gave up on wake {d['delivery_id']} after {attempts} attempts: {err}")
            return
        backoff = self.cfg["retry_backoff_s"]
        delay = float(backoff[min(attempts - 1, len(backoff) - 1)])
        try:
            if retry_after:
                delay = max(delay, min(float(retry_after), 3600.0))
        except ValueError:
            pass
        self.st.execute("update deliveries set status='retry', attempts=?, first_attempt_at=?, last_attempt_at=?, "
                        "next_attempt_at=?, last_code=?, last_error=? where delivery_id=?",
                        (attempts, first, now, now + delay, code, err, d["delivery_id"]))
        if code is not None or "no webhook" not in (err or ""):
            self.log(f"wake {d['delivery_id']} failed ({err}); retry {attempts} in {int(delay)}s")


# ---------------------------------------------------------------- daemon

class Daemon:
    def __init__(self):
        self.cfg = load_config()
        self.cfg_mtime = self._cfg_mtime()
        self.st = open_state()
        self.t3: T3 | None = None
        self.engine: Engine | None = None
        self.sender = Sender(self.st, self.cfg)
        self.stop = False
        self.last_selftest = 0.0
        self.force_selftest = True
        self.db_fail_since = None
        self.last_beat = 0.0
        self.last_version_check = 0.0
        self.last_prune = 0.0

    @staticmethod
    def _cfg_mtime():
        try:
            return os.stat(CONFIG_PATH).st_mtime_ns
        except OSError:
            return None

    def _signal(self, signum, frame):
        if signum == signal.SIGHUP:
            self.force_selftest = True
            self.cfg_mtime = None
        else:
            self.stop = True

    def run(self):
        for s in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            signal.signal(s, self._signal)
        with tx(self.st):
            if kv_get(self.st, "enabled") is None:
                kv_set(self.st, "enabled", 1)
            if kv_get(self.st, "mode") is None:
                kv_set(self.st, "mode", "dry-run")
            kv_set(self.st, "daemon_started_at", now_s())
            kv_set(self.st, "daemon_pid", os.getpid())
        log(f"t3-wake {VERSION} starting: {'ON' if kv_get(self.st, 'enabled') == '1' else 'OFF'}, "
            f"mode {kv_get(self.st, 'mode')}, state {STATE_DB}")
        while not self.stop:
            busy = False
            try:
                busy = self.loop_once()
            except Exception as e:  # keep running; systemd restarts us only on a crash
                log(f"error in main loop: {type(e).__name__}: {e}")
                time.sleep(2)
            if not busy:
                end = time.monotonic() + float(self.cfg["poll_interval_s"])
                while not self.stop and time.monotonic() < end:
                    time.sleep(0.1)
        log("t3-wake stopping")

    # -- health
    def degrade(self, reason):
        with tx(self.st):
            if kv_get(self.st, "degraded") != "1":
                since = now_s()
                kv_set(self.st, "degraded", 1)
                kv_set(self.st, "degraded_since", since)
                kv_set(self.st, "degraded_reason", reason)
                log(f"DEGRADED: {reason}")
                if kv_get(self.st, "enabled") == "1":
                    self._health_event("watcher_degraded", reason, since)
            elif kv_get(self.st, "degraded_reason") != reason:
                kv_set(self.st, "degraded_reason", reason)
                log(f"still degraded: {reason}")

    def recover(self):
        with tx(self.st):
            if kv_get(self.st, "degraded") == "1":
                since = kv_get(self.st, "degraded_since")
                for k in ("degraded", "degraded_since", "degraded_reason"):
                    kv_del(self.st, k)
                log("recovered: self-test passes again")
                if kv_get(self.st, "enabled") == "1":
                    self._health_event("watcher_recovered", "self-test passes again", float(since or now_s()))

    def _health_event(self, event, reason, since):
        now = now_s()
        body = {"event": event, "threadId": None, "reason": reason, "t3Version": t3_server_version(),
                "occurredAt": iso(now), "why": "watcher_health"}
        eng = self.engine or Engine(self.st, None, self.cfg)
        eng.emit(digest(event, since), None, event, body, now)

    def connect_and_selftest(self):
        self.last_selftest = now_s()
        self.force_selftest = False
        path = discover_db(self.cfg)
        if not path or not os.path.exists(path):
            self.degrade(f"no T3 database found under {T3_USERDATA}")
            return False
        try:
            if self.t3 is None or self.t3.path != path:
                if self.t3:
                    self.t3.close()
                self.t3 = T3(path)
                self.engine = Engine(self.st, self.t3, self.cfg)
            ok, reason = self.t3.selftest()
        except sqlite3.Error as e:
            ok, reason = False, f"cannot open T3 database {path}: {e}"
            self.t3, self.engine = None, None
        if not ok:
            self.degrade(reason)
            return False
        with tx(self.st):
            old_path = kv_get(self.st, "db_path")
            head = self.t3.head()
            if kv_get(self.st, "cursor") is None or (old_path and old_path != path):
                if old_path and old_path != path:
                    log(f"T3 database moved from {old_path} to {path}; starting from its current head {head}")
                else:
                    log(f"first start: starting at T3 event {head} (no history replay)")
                kv_set(self.st, "cursor", head)
                self.st.execute("update thread_state set active_json='[]'")
                for tid, rid in self.t3.active_runs():
                    s = self.engine.tstate(tid)
                    self.engine.save_tstate(tid, active=s["active"] + [rid])
            else:
                self._reconcile_active()
            kv_set(self.st, "db_path", path)
            kv_set(self.st, "last_selftest_at", self.last_selftest)
        self.recover()
        return True

    def _reconcile_active(self):
        """Forget active runs T3 no longer considers active (e.g. missed while T3 was rewritten)."""
        live = {rid for _, rid in self.t3.active_runs()}
        for r in self.st.execute("select thread_id, active_json from thread_state where active_json != '[]'").fetchall():
            act = [x for x in json.loads(r[1]) if x in live]
            self.st.execute("update thread_state set active_json=? where thread_id=?", (json.dumps(act), r[0]))

    # -- one iteration; returns True when there is more backlog to read right away
    def loop_once(self) -> bool:
        now = now_s()
        if now - self.last_beat >= 10:
            kv_set(self.st, "daemon_heartbeat", now)
            self.last_beat = now
        if now - self.last_version_check >= 15:
            self.last_version_check = now
            m = self._cfg_mtime()
            if m != self.cfg_mtime:
                self.cfg_mtime = m
                self.cfg.clear()
                self.cfg.update(load_config())
                self.force_selftest = True
            v = t3_server_version()
            if v and v != kv_get(self.st, "t3_version"):
                log(f"T3 version is now {v}; re-running self-test")
                kv_set(self.st, "t3_version", v)
                self.force_selftest = True
        if now - self.last_prune >= 3600:
            self.last_prune = now
            self.prune(now)

        degraded = kv_get(self.st, "degraded") == "1"
        interval = self.cfg["degraded_retry_s"] if degraded else self.cfg["selftest_interval_s"]
        if self.force_selftest or self.t3 is None or now - self.last_selftest >= interval:
            self.connect_and_selftest()
            degraded = kv_get(self.st, "degraded") == "1"

        more = False
        enabled = kv_get(self.st, "enabled") == "1"
        version = kv_get(self.st, "t3_version") or t3_server_version()
        if not degraded and self.t3 is not None:
            try:
                more = self.poll(now, enabled, version)
                self.db_fail_since = None
            except sqlite3.Error as e:
                self.db_fail_since = self.db_fail_since or now
                if now - self.db_fail_since >= self.cfg["db_error_grace_s"]:
                    self.degrade(f"T3 database unreadable for {int(now - self.db_fail_since)}s: {e}")
                if self.t3:
                    self.t3.close()
                self.t3, self.engine = None, None
        elif enabled:
            with tx(self.st):
                (self.engine or Engine(self.st, None, self.cfg)).build_deliveries(now, version)
        mode = kv_get(self.st, "mode", "dry-run")
        self.sender.send_due(now_s(), mode)
        return more

    def poll(self, now, enabled, version) -> bool:
        self.t3.clear_cache()
        self.engine.reset_batch_caches()
        head = self.t3.head()
        limit = int(self.cfg["batch_limit"])
        with tx(self.st):
            cursor = int(kv_get(self.st, "cursor", head))
            if head < cursor:
                log(f"T3 event log went backwards ({cursor} -> {head}); following it")
                cursor = head
            skip_until = int(kv_get(self.st, "skip_until_seq", 0) or 0)
            rows = self.t3.events(cursor, head, limit)
            for seq, tid, etype, at, pj, mj in rows:
                try:
                    self.engine.process(seq, tid, etype, at, pj, mj,
                                        track_only=(not enabled) or seq <= skip_until)
                except (ValueError, KeyError, TypeError, AttributeError) as e:
                    log(f"skipped malformed event {seq} ({etype}): {type(e).__name__}: {e}")
            more = len(rows) >= limit
            new_cursor = rows[-1][0] if more else head
            kv_set(self.st, "cursor", new_cursor)
            if skip_until and new_cursor >= skip_until:
                kv_del(self.st, "skip_until_seq")
            if enabled:
                self.engine.tick(now)
                self.engine.build_deliveries(now, version)
            else:
                cancel_queue(self.st)
        return more

    def prune(self, now):
        keep = now - self.cfg["keep_history_days"] * 86400
        with tx(self.st):
            self.st.execute("delete from seen where at < ?", (keep,))
            self.st.execute("delete from events where created_at < ? and status != 'queued'", (keep,))
            self.st.execute("delete from deliveries where created_at < ? and status not in ('pending','retry')", (keep,))
            self.st.execute("delete from run_owner where created_at < ?", (now - 30 * 86400,))


# ---------------------------------------------------------------- CLI

def systemctl(*args, check=False):
    if not USE_SYSTEMCTL:
        return None
    try:
        return subprocess.run(["systemctl", "--user", *args], capture_output=True, text=True, check=check)
    except (OSError, subprocess.CalledProcessError):
        return None


def open_t3_quiet():
    try:
        path = discover_db(load_config())
        return T3(path) if path else None
    except sqlite3.Error:
        return None


def thread_label(t3, tid):
    t = t3.thread(tid) if t3 else None
    if not t:
        return f"{tid} (not found in T3)"
    extra = ""
    if (t.get("lineage") or {}).get("parentThreadId"):
        extra = " [child thread]"
    return f'{tid}  "{t.get("title")}"{extra}'


def cmd_status(args):
    st = open_state()
    enabled = kv_get(st, "enabled", "1") == "1"
    mode = kv_get(st, "mode", "dry-run")
    if not enabled:
        head_line = "OFF (no wakes; cursor keeps advancing)"
    elif mode == "live":
        head_line = "ON, LIVE (POSTs wakes to the webhook)"
    else:
        head_line = "ON, DRY-RUN (logs the wakes it would send; sends nothing)"
    print(f"t3-wake:    {head_line}")

    svc = "unknown"
    if USE_SYSTEMCTL:
        a = systemctl("is-active", SERVICE)
        e = systemctl("is-enabled", SERVICE)
        svc = f"{(a.stdout.strip() if a else '?')}, {(e.stdout.strip() if e else '?')} at login"
        try:
            lg = subprocess.run(["loginctl", "show-user", getpass.getuser(), "-p", "Linger", "--value"],
                                capture_output=True, text=True).stdout.strip()
            svc += f"; linger {'on' if lg == 'yes' else 'OFF (stops at logout)'}"
        except OSError:
            pass
    beat = kv_get(st, "daemon_heartbeat")
    print(f"service:    {svc}; heartbeat {ago(beat)}")

    if kv_get(st, "degraded") == "1":
        print(f"health:     DEGRADED since {short_time(kv_get(st, 'degraded_since'))}: {kv_get(st, 'degraded_reason')}")
    else:
        print(f"health:     OK (self-test {ago(kv_get(st, 'last_selftest_at'))}, T3 {kv_get(st, 't3_version', '?')})")
    auth = kv_get(st, "webhook_auth_failed")
    if auth:
        print(f"            webhook rejected the key (HTTP {auth}); fix with `t3-wake set-webhook`")

    cursor = kv_get(st, "cursor")
    t3 = open_t3_quiet()
    head = None
    if t3:
        try:
            head = t3.head()
        except sqlite3.Error:
            head = None
    lag = f", {head - int(cursor)} behind" if head is not None and cursor is not None else ""
    print(f"cursor:     {cursor or 'not started'} (T3 head {head if head is not None else '?'}{lag}; "
          f"db {kv_get(st, 'db_path', '?')})")

    d = st.execute("select * from deliveries where is_test=0 order by created_at desc limit 1").fetchone()
    if d:
        print(f"last wake:  {short_time(d['created_at'])}  {summarize(json.loads(d['body_json']))}")
        print(f"            result: {delivery_result(d)}")
    else:
        print("last wake:  none yet")
    q = st.execute("select count(*) from events where status='queued'").fetchone()[0]
    p = st.execute("select count(*) from pending").fetchone()[0]
    r = st.execute("select count(*) from deliveries where status in ('pending','retry')").fetchone()[0]
    print(f"queue:      {p} debouncing, {q} waiting to batch, {r} waiting to send/retry")

    store = webhook_configured()
    if store:
        print(f"webhook:    set {short_time(kv_get(st, 'webhook_set_at'))} (host {kv_get(st, 'webhook_host', '?')}, "
              f"stored with {store})")
    else:
        print("webhook:    not set (run `t3-wake set-webhook`)")
    tst = st.execute("select * from deliveries where is_test=1 order by created_at desc limit 1").fetchone()
    print(f"last test:  {short_time(tst['created_at']) + '  ' + delivery_result(tst) if tst else 'never'}")
    for kind, label in (("allow", "allowlist"), ("mute", "muted")):
        rows = st.execute("select thread_id from lists where kind=? order by added_at", (kind,)).fetchall()
        if not rows:
            print(f"{label + ':':<11} (none)")
        for i, row in enumerate(rows):
            print(f"{(label + ':') if i == 0 else '':<11} {thread_label(t3, row[0])}")


def delivery_result(d) -> str:
    s = d["status"]
    if s == "sent":
        return f"sent, HTTP {d['last_code']}"
    if s == "dry_run":
        return "dry-run, not sent"
    if s == "retry":
        return (f"retrying ({d['last_error']}), attempt {d['attempts']}, next {short_time(d['next_attempt_at'])}")
    if s == "pending":
        return "waiting to send"
    if s in ("failed", "gave_up"):
        return f"{s.replace('_', ' ')}: {d['last_error']}"
    return s


def cmd_on(args):
    st = open_state()
    t3 = open_t3_quiet()
    head = None
    if t3:
        try:
            head = t3.head()
        except sqlite3.Error:
            pass
    with tx(st):
        was = kv_get(st, "enabled", "1") == "1"
        kv_set(st, "enabled", 1)
        if not was and head is not None:
            # Anything that happened while off is history: never send a wake for it.
            kv_set(st, "skip_until_seq", head)
    if USE_SYSTEMCTL:
        a = systemctl("is-active", SERVICE)
        if not a or a.stdout.strip() != "active":
            systemctl("enable", "--now", SERVICE)
    mode = kv_get(st, "mode", "dry-run")
    print(f"t3-wake is ON ({'LIVE' if mode == 'live' else 'dry-run'}). "
          + ("It was already on." if was else f"Events before T3 sequence {head} are ignored, so nothing is replayed."))


def cmd_off(args):
    st = open_state()
    with tx(st):
        kv_set(st, "enabled", 0)
        n = cancel_queue(st)
    print(f"t3-wake is OFF. No wakes will be sent; {n} queued item(s) dropped. "
          "The watcher keeps its cursor, so `t3-wake on` starts from now with no replay.")


def cmd_mode(args):
    st = open_state()
    if args.cmd == "live":
        if not webhook_configured():
            sys.exit("No webhook is set. Run `t3-wake set-webhook` first.")
        with tx(st):
            kv_set(st, "mode", "live")
        print("t3-wake is now LIVE: wakes are POSTed to the webhook."
              + ("" if kv_get(st, "enabled", "1") == "1" else " (It is OFF; run `t3-wake on` to start.)"))
    else:
        with tx(st):
            kv_set(st, "mode", "dry-run")
        print("t3-wake is now in DRY-RUN: wakes are logged, not sent.")


def cmd_list(args):
    st = open_state()
    tid = args.thread_id.strip()
    t3 = open_t3_quiet()
    t = t3.thread(tid) if t3 else None
    with tx(st):
        cur = st.execute("select kind from lists where thread_id=?", (tid,)).fetchone()
        cur = cur[0] if cur else None
        if args.cmd in ("allow", "mute"):
            kind = args.cmd
            st.execute("insert or replace into lists(thread_id, kind, added_at, note) values(?,?,?,?)",
                       (tid, kind, now_s(), getattr(args, "note", None)))
        else:
            kind = "allow" if args.cmd == "unallow" else "mute"
            st.execute("delete from lists where thread_id=? and kind=?", (tid, kind))
    label = thread_label(t3, tid)
    if args.cmd == "allow":
        print(f"Allowed: {label}. Every run in it now sends a wake." + (" (Was muted; unmuted.)" if cur == "mute" else ""))
    elif args.cmd == "mute":
        print(f"Muted: {label}. Nothing in it sends a wake." + (" (Was allowed; removed.)" if cur == "allow" else ""))
    elif cur != kind:
        print(f"{tid} was not {'allowed' if kind == 'allow' else 'muted'}; nothing changed.")
    else:
        print(f"{'Removed from allowlist' if kind == 'allow' else 'Unmuted'}: {label}.")
    if t is None and args.cmd in ("allow", "mute"):
        print("Warning: no T3 thread with that id was found (it is stored anyway).")
    elif t and (t.get("lineage") or {}).get("parentThreadId") and args.cmd == "allow":
        print(f"Note: this is a child thread; wakes are reported on its root thread "
              f"{t['lineage'].get('rootThreadId')}. Allow the root instead.")


def cmd_test(args):
    st = open_state()
    cfg = load_config()
    now = now_s()
    did = "tw_test_" + secrets.token_hex(8)
    body = {"version": 1, "wakeId": did, "createdAt": iso(now), "sentAt": iso(now), "source": cfg["source"],
            "t3Version": t3_server_version(), "test": True,
            "events": [{"event": "test", "threadId": None, "title": "t3-wake test wake", "why": "manual_test",
                        "request": None, "occurredAt": iso(now), "eventSequence": None}]}
    if args.dry_run:
        print(json.dumps(body, indent=2))
        return
    try:
        wh = load_webhook()
    except WebhookError as e:
        sys.exit(str(e))
    if not wh:
        sys.exit("No webhook is set. Run `t3-wake set-webhook` first.")
    code, err, _, secs = post_json(wh["url"], wh["key"], body, cfg["http_timeout_s"], did)
    err = redact(err, wh)
    ok = code is not None and 200 <= code < 300
    with tx(st):
        st.execute("insert into deliveries(delivery_id, created_at, body_json, status, attempts, first_attempt_at, "
                   "last_attempt_at, last_code, last_error, is_test) values(?,?,?,?,1,?,?,?,?,1)",
                   (did, now, json.dumps(body), "sent" if ok else "failed", now, now, code, err))
        if ok:
            kv_del(st, "webhook_auth_failed")
    if ok:
        print(f"Test wake delivered: HTTP {code} in {secs:.1f}s (wakeId {did}).")
    else:
        sys.exit(f"Test wake failed: {err or 'no response'}")


def cmd_set_webhook(args):
    if sys.stdin.isatty():
        url = getpass.getpass("Webhook URL (input hidden): ").strip()
        key = getpass.getpass("Bearer key (input hidden): ").strip()
    else:
        lines = sys.stdin.read().splitlines()
        if len(lines) < 2:
            sys.exit("Expected two lines on stdin: URL, then key.")
        url, key = lines[0].strip(), lines[1].strip()
    try:
        validate_webhook(url, key)
    except ValueError as e:
        sys.exit(f"Not saved: {e}")
    data = json.dumps({"url": url, "key": key}).encode()
    os.makedirs(CONFIG_DIR, mode=0o700, exist_ok=True)
    os.chmod(CONFIG_DIR, 0o700)
    storage = None
    if SECRET_STORE != "file" and shutil.which("systemd-creds"):
        tmp = CRED_PATH + ".tmp"
        old = os.umask(0o077)
        try:
            r = subprocess.run(["systemd-creds", "--user", "encrypt", f"--name={CRED_NAME}", "-", tmp],
                               input=data, capture_output=True, timeout=30)
        finally:
            os.umask(old)
        if r.returncode == 0:
            os.chmod(tmp, 0o600)
            os.replace(tmp, CRED_PATH)
            storage = "systemd-creds"
            if os.path.exists(PLAIN_SECRET_PATH):
                os.remove(PLAIN_SECRET_PATH)
        else:
            if os.path.exists(tmp):
                os.remove(tmp)
            print("systemd-creds could not encrypt; storing as a 0600 file instead.")
    if storage is None:
        tmp = PLAIN_SECRET_PATH + ".tmp"
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        os.replace(tmp, PLAIN_SECRET_PATH)
        storage = "file"
        if os.path.exists(CRED_PATH):
            os.remove(CRED_PATH)
    host = urllib.parse.urlsplit(url).hostname
    st = open_state()
    with tx(st):
        kv_set(st, "webhook_host", host)
        kv_set(st, "webhook_storage", storage)
        kv_set(st, "webhook_set_at", now_s())
        kv_del(st, "webhook_auth_failed")
    where = CRED_PATH if storage == "systemd-creds" else PLAIN_SECRET_PATH
    print(f"Webhook saved for host {host} ({'encrypted with systemd-creds' if storage == 'systemd-creds' else 'mode 0600 file'}"
          f" at {where}). The URL and key were not printed or logged.")
    print("Next: `t3-wake test` sends one test wake; `t3-wake live` switches from dry-run to live.")


def cmd_clear_webhook(args):
    for p in (CRED_PATH, PLAIN_SECRET_PATH):
        if os.path.exists(p):
            os.remove(p)
    st = open_state()
    with tx(st):
        for k in ("webhook_host", "webhook_storage", "webhook_set_at", "webhook_auth_failed"):
            kv_del(st, k)
        was_live = kv_get(st, "mode") == "live"
        kv_set(st, "mode", "dry-run")
    print("Webhook removed." + (" Switched to dry-run." if was_live else ""))


def cmd_log(args):
    if args.journal:
        os.execvp("journalctl", ["journalctl", "--user", "-u", SERVICE, "-n", str(args.n), "--no-pager"])
    st = open_state()
    rows = st.execute("select * from deliveries order by created_at desc limit ?", (args.n,)).fetchall()
    if not rows:
        print("No wakes yet.")
    for d in reversed(rows):
        tag = " [test]" if d["is_test"] else ""
        print(f"{short_time(d['created_at'])}  {delivery_result(d):<28} {d['delivery_id']}{tag}  "
              f"{summarize(json.loads(d['body_json']))}")


def cmd_replay(args):
    """Run the filter over past events with throwaway state and print the wakes it would send."""
    cfg = load_config()
    path = discover_db(cfg)
    if not path:
        sys.exit("No T3 database found.")
    t3 = T3(path)
    st = open_state(":memory:")
    if os.path.exists(STATE_DB):
        real = open_state()
        st.executemany("insert into lists values(?,?,?,?)",
                       [tuple(r) for r in real.execute("select * from lists")])
    out = []
    eng = Engine(st, t3, cfg, live=False, logger=lambda m: None)
    sender = Sender(st, cfg, logger=lambda m: out.append(m) if m.startswith("[dry-run] body=") else None)
    if args.from_seq is not None:
        start = args.from_seq
    else:
        start = t3.first_sequence_since(now_s() - parse_duration(args.since))
    end = args.to_seq or t3.head()
    clock = None

    def advance(ts):
        eng.tick(ts)
        eng.build_deliveries(ts)
        sender.send_due(ts, "dry-run")

    cursor = start
    while True:
        rows = t3.events(cursor, end, 5000)
        if not rows:
            break
        for seq, tid, etype, at, pj, mj in rows:
            ts = parse_iso(at) or 0
            if clock is None or ts > clock:
                clock = ts
                advance(ts)
            eng.reset_batch_caches()
            eng.process(seq, tid, etype, at, pj, mj)
        cursor = rows[-1][0]
    if clock is not None:
        for step in range(0, int(cfg["max_hold_s"]) + 180, 5):
            advance(clock + step)
    print(f"Replayed T3 events {start + 1}..{end} ({path})")
    n_events = 0
    for line in out:
        body = json.loads(line[len("[dry-run] body="):])
        for e in body["events"]:
            n_events += 1
            when = (e.get("occurredAt") or "")[:19].replace("T", " ")
            extra = []
            if (e.get("runsCoalesced") or 0) > 1:
                extra.append(f"{e['runsCoalesced']} runs")
            if e.get("startedBy"):
                extra.append(f"startedBy={e['startedBy']}")
            if e.get("request"):
                extra.append(f"request={e['request'].get('kind')}" + (" (child)" if e['request'].get('childThreadId') else ""))
            if e.get("threadStillActive"):
                extra.append("still active")
            print(f"{when}  {e['event']:<16} {e.get('why') or '-':<17} {(e.get('title') or '')[:44]:<44}  {', '.join(extra)}")
            if args.json:
                print("    " + json.dumps(e))
    print(f"{len(out)} POST(s), {n_events} event(s)")


def parse_duration(s: str) -> float:
    s = s.strip().lower()
    unit = {"s": 1, "m": 60, "h": 3600, "d": 86400}.get(s[-1])
    return float(s[:-1]) * unit if unit else float(s)


def main(argv=None):
    ap = argparse.ArgumentParser(prog="t3-wake", description="POST a webhook when T3 threads launched over MCP finish or need input.")
    ap.add_argument("--version", action="version", version=f"t3-wake {VERSION}")
    sub = ap.add_subparsers(dest="cmd", required=True, metavar="command")
    sub.add_parser("status", help="show on/off, mode, cursor, last wake, health, allowlist")
    sub.add_parser("on", help="start sending wakes (from now on; no replay)")
    sub.add_parser("off", help="stop sending wakes; keep the cursor moving")
    sub.add_parser("live", help="switch from dry-run to really POSTing wakes")
    sub.add_parser("dry-run", help="log wakes instead of POSTing them")
    for name, hlp in (("allow", "send a wake for every run in this thread"), ("unallow", "remove from allowlist"),
                      ("mute", "never send a wake for this thread"), ("unmute", "remove from mute list")):
        p = sub.add_parser(name, help=hlp)
        p.add_argument("thread_id")
        if name in ("allow", "mute"):
            p.add_argument("--note")
    p = sub.add_parser("test", help="send one test wake to the webhook now")
    p.add_argument("--dry-run", action="store_true", help="print the test body instead of sending it")
    sub.add_parser("set-webhook", help="store the webhook URL and key (hidden prompt)")
    sub.add_parser("clear-webhook", help="delete the stored webhook and switch to dry-run")
    p = sub.add_parser("log", help="recent wakes (or --journal for the service log)")
    p.add_argument("-n", type=int, default=20)
    p.add_argument("--journal", action="store_true")
    p = sub.add_parser("replay", help="show which wakes past events would have produced (no state, no sending)")
    p.add_argument("--since", default="24h")
    p.add_argument("--from-seq", type=int)
    p.add_argument("--to-seq", type=int)
    p.add_argument("--json", action="store_true")
    sub.add_parser("run", help="run the watcher (used by the systemd service)")
    args = ap.parse_args(argv)
    handlers = {
        "status": cmd_status, "on": cmd_on, "off": cmd_off, "live": cmd_mode, "dry-run": cmd_mode,
        "allow": cmd_list, "unallow": cmd_list, "mute": cmd_list, "unmute": cmd_list,
        "test": cmd_test, "set-webhook": cmd_set_webhook, "clear-webhook": cmd_clear_webhook,
        "log": cmd_log, "replay": cmd_replay, "run": lambda a: Daemon().run(),
    }
    handlers[args.cmd](args)


if __name__ == "__main__":
    main()
