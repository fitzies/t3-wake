"""End-to-end tests: real daemon subprocess, synthetic T3 database, fake webhook receiver.

Run:  python3 -I tests/test_t3_wake.py -v
Nothing here touches the real T3 database, real state, or the real webhook.
"""
import datetime as dt
import http.server
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(os.path.dirname(HERE), "t3_wake.py")
SECRET_KEY = "sk-test-DO-NOT-LEAK-7f3a9c"
SECRET_PATH = "/hooks/routine-SECRETPATH-91b2"

T3_SCHEMA = """
create table orchestration_events(sequence integer primary key autoincrement, event_id text not null unique,
  aggregate_kind text not null, stream_id text not null, stream_version integer not null default 0,
  event_type text not null, occurred_at text not null, command_id text, causation_event_id text,
  correlation_id text, actor_kind text not null default 'server', payload_json text not null,
  metadata_json text not null, application_event_version integer not null default 2);
create table orchestration_v2_projection_threads(thread_id text primary key, project_id text not null,
  title text not null, default_provider text not null default 'x', runtime_mode text not null default 'x',
  interaction_mode text not null default 'x', active_provider_thread_id text, created_at text not null,
  updated_at text not null, archived_at text, deleted_at text, payload_json text not null, provider_instance_id text);
create table orchestration_v2_projection_messages(message_id text primary key, thread_id text not null,
  run_id text, node_id text, role text not null, streaming integer not null default 0, created_at text not null,
  updated_at text not null, payload_json text not null);
create table orchestration_v2_projection_runs(run_id text primary key, thread_id text not null, ordinal integer not null,
  provider text not null default 'claudeAgent', provider_thread_id text, status text not null, requested_at text not null,
  completed_at text, payload_json text not null, provider_instance_id text);
create table orchestration_v2_projection_runtime_requests(runtime_request_id text primary key, thread_id text not null,
  node_id text not null default 'n', provider_turn_id text, kind text not null, status text not null,
  created_at text not null, resolved_at text, payload_json text not null);
create table orchestration_v2_projection_turn_items(turn_item_id text primary key, thread_id text not null, run_id text,
  node_id text, provider_thread_id text, provider_turn_id text, parent_item_id text, ordinal integer not null,
  type text not null, status text not null, updated_at text not null, payload_json text not null);
create table orchestration_v2_projection_metadata(projection_name text primary key, schema_version integer not null,
  last_sequence integer not null, updated_at text not null);
create table projection_projects(project_id text primary key, title text not null, workspace_root text not null default '/',
  scripts_json text not null default '[]', created_at text not null default '', updated_at text not null default '',
  deleted_at text);
"""


def iso_now():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class FakeT3:
    def __init__(self, root):
        self.userdata = os.path.join(root, "userdata")
        os.makedirs(self.userdata)
        self.path = os.path.join(self.userdata, "statev2.sqlite")
        self.db = sqlite3.connect(self.path, isolation_level=None)
        self.db.execute("pragma journal_mode=wal")
        self.db.executescript(T3_SCHEMA)
        self.db.execute("insert into projection_projects(project_id, title) values('p1','demo-app')")
        self.set_version("0.1.1-nightly.test.1")
        self.n = 0
        self.ordinals = {}

    def set_version(self, v):
        with open(os.path.join(self.userdata, "server-runtime.json"), "w") as f:
            json.dump({"serverVersion": v}, f)

    def _event(self, tid, etype, payload, meta=None):
        self.n += 1
        self.db.execute("insert into orchestration_events(event_id, aggregate_kind, stream_id, event_type, occurred_at, "
                        "payload_json, metadata_json) values(?,?,?,?,?,?,?)",
                        (f"e{self.n}-{time.time_ns()}", "thread", tid, etype, iso_now(), json.dumps(payload),
                         json.dumps(meta or {})))
        seq = self.db.execute("select max(sequence) from orchestration_events").fetchone()[0]
        self.db.execute("insert or replace into orchestration_v2_projection_metadata values('thread-projections',2,?,?)",
                        (seq, iso_now()))
        return seq

    def thread(self, tid, created_by, source, title, parent=None, root=None):
        p = {"id": tid, "createdBy": created_by, "creationSource": source, "projectId": "p1", "title": title,
             "lineage": {"parentThreadId": parent, "relationshipToParent": "subagent" if parent else None,
                         "rootThreadId": root or tid}}
        self.db.execute("insert into orchestration_v2_projection_threads(thread_id, project_id, title, created_at, "
                        "updated_at, payload_json) values(?,?,?,?,?,?)", (tid, "p1", title, iso_now(), iso_now(), json.dumps(p)))
        self._event(tid, "thread.created", p)
        return tid

    def message(self, mid, tid, created_by, source, sender=None):
        p = {"id": mid, "threadId": tid, "role": "user", "createdBy": created_by, "creationSource": source, "text": "x"}
        if sender:
            p["senderThreadId"] = sender
        self.db.execute("insert into orchestration_v2_projection_messages(message_id, thread_id, role, created_at, "
                        "updated_at, payload_json) values(?,?,?,?,?,?)", (mid, tid, "user", iso_now(), iso_now(), json.dumps(p)))
        return mid

    def run_payload(self, tid, rid, status, msg, cont=None, started=True):
        p = {"id": rid, "threadId": tid, "ordinal": self.ordinals.get(rid, 1), "providerInstanceId": "claudeAgent",
             "modelSelection": {"instanceId": "claudeAgent", "model": "claude-opus-5-5"}, "userMessageId": msg,
             "status": status, "startedAt": iso_now() if started else None}
        if cont:
            p["restartContinuationOfRunId"] = cont
        return p

    def run_created(self, tid, msg, cont=None):
        self.ordinals[tid] = self.ordinals.get(tid, 0) + 1
        rid = f"run:{tid}:ordinal:{self.ordinals[tid]}"
        self.ordinals[rid] = self.ordinals[tid]
        p = self.run_payload(tid, rid, "preparing", msg, cont, started=False)
        self.db.execute("insert into orchestration_v2_projection_runs(run_id, thread_id, ordinal, status, requested_at, "
                        "payload_json) values(?,?,?,?,?,?)", (rid, tid, p["ordinal"], "preparing", iso_now(), json.dumps(p)))
        self._event(tid, "run.created", p, {"runId": rid})
        self.run_update(tid, rid, "running", msg)
        return rid

    def run_update(self, tid, rid, status, msg, error=None):
        p = self.run_payload(tid, rid, status, msg)
        self.db.execute("update orchestration_v2_projection_runs set status=?, payload_json=? where run_id=?",
                        (status, json.dumps(p), rid))
        if error:
            self.db.execute("insert into orchestration_v2_projection_turn_items(turn_item_id, thread_id, run_id, ordinal, "
                            "type, status, updated_at, payload_json) values(?,?,?,?,?,?,?,?)",
                            (f"err-{rid}", tid, rid, 99, "error", "failed", iso_now(),
                             json.dumps({"type": "error", "failure": {"class": "transport_error", "message": error}})))
        return self._event(tid, "run.updated", p, {"runId": rid})

    def run(self, tid, msg, status="completed", error=None):
        rid = self.run_created(tid, msg)
        self.run_update(tid, rid, status, msg, error)
        return rid

    def request(self, tid, req_id, status, run_id=None, kind="user_input"):
        p = {"id": req_id, "kind": kind, "status": status}
        self.db.execute("insert or replace into orchestration_v2_projection_runtime_requests(runtime_request_id, thread_id, "
                        "kind, status, created_at, payload_json) values(?,?,?,?,?,?)",
                        (req_id, tid, kind, status, iso_now(), json.dumps(p)))
        self._event(tid, "runtime-request.updated", p, {"runId": run_id})

    # convenience: thread shapes T3 writes
    def mcp_thread(self, tid, title="MCP task"):
        self.thread(tid, "agent", "mcp", title)
        self.message(tid, tid, "agent", "mcp")  # first message id == thread id, no senderThreadId
        return tid

    def user_thread(self, tid, title="User task"):
        self.thread(tid, "user", "web", title)
        self.message(f"{tid}:m0", tid, "user", "web")
        return tid


class Receiver:
    def __init__(self):
        self.requests, self.codes, self.lock = [], [], threading.Lock()
        outer = self

        class H(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                with outer.lock:
                    code = outer.codes.pop(0) if outer.codes else 200
                    outer.requests.append({"path": self.path, "headers": dict(self.headers),
                                           "body": json.loads(body), "code": code})
                self.send_response(code)
                self.end_headers()
                self.wfile.write(b"{}")

            def log_message(self, *a):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    @property
    def url(self):
        return f"http://127.0.0.1:{self.port}{SECRET_PATH}"

    def bodies(self):
        with self.lock:
            return [r["body"] for r in self.requests]

    def events(self):
        return [e for b in self.bodies() for e in b["events"]]

    def close(self):
        self.server.shutdown()


CFG = {"poll_interval_s": 0.2, "quiet_window_s": 1.0, "urgent_grace_s": 0.6, "batch_window_s": 0.3,
       "per_thread_min_interval_s": 0, "max_hold_s": 6, "retry_backoff_s": [0.5, 0.5, 1],
       "retry_give_up_s": 30, "degraded_retry_s": 0.5, "selftest_interval_s": 3600, "http_timeout_s": 3}


def wait_for(cond, timeout=10.0, step=0.1):
    end = time.time() + timeout
    while time.time() < end:
        v = cond()
        if v:
            return v
        time.sleep(step)
    return cond()


class Base(unittest.TestCase):

    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="t3-wake-test-")
        self.t3 = FakeT3(self.root)
        self.rx = Receiver()
        self.cfg_dir = os.path.join(self.root, "config")
        os.makedirs(self.cfg_dir)
        with open(os.path.join(self.cfg_dir, "config.json"), "w") as f:
            json.dump(CFG, f)
        self.env = {**os.environ, "T3_WAKE_CONFIG_DIR": self.cfg_dir,
                    "T3_WAKE_STATE_DIR": os.path.join(self.root, "state"),
                    "T3_WAKE_T3_USERDATA": self.t3.userdata, "T3_WAKE_T3_DB": self.t3.path,
                    "T3_WAKE_NO_SYSTEMCTL": "1"}
        self.env.pop("JOURNAL_STREAM", None)
        self.proc = None
        self.logs = []

    def tearDown(self):
        self.stop_daemon()
        self.rx.close()
        self.t3.db.close()
        self.assert_no_secret_anywhere()
        shutil.rmtree(self.root, ignore_errors=True)

    def set_config(self, **kw):
        with open(os.path.join(self.cfg_dir, "config.json"), "w") as f:
            json.dump({**CFG, **kw}, f)

    def cli(self, *args, input=None, check=True):
        r = subprocess.run([sys.executable, "-I", SCRIPT, *args], env=self.env, input=input,
                           capture_output=True, text=True, timeout=60)
        self.logs.append(r.stdout + r.stderr)
        if check and r.returncode != 0:
            self.fail(f"t3-wake {' '.join(args)} failed: {r.stdout}{r.stderr}")
        return r

    def start_daemon(self):
        self.logfile = open(os.path.join(self.root, f"daemon-{time.time_ns()}.log"), "w")
        self.proc = subprocess.Popen([sys.executable, "-I", SCRIPT, "run"], env=self.env,
                                     stdout=self.logfile, stderr=subprocess.STDOUT)
        self.assertTrue(wait_for(lambda: self.state("select value from kv where key='cursor'"), 10),
                        "daemon did not start")

    def stop_daemon(self, sig=signal.SIGTERM):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(sig)
            self.proc.wait(10)
        self.proc = None
        if getattr(self, "logfile", None):
            self.logfile.close()

    def daemon_log(self):
        out = ""
        for name in sorted(os.listdir(self.root)):
            if name.startswith("daemon-"):
                with open(os.path.join(self.root, name)) as f:
                    out += f.read()
        return out

    def state(self, q, a=()):
        p = os.path.join(self.root, "state", "state.sqlite")
        if not os.path.exists(p):
            return None
        c = sqlite3.connect(p, timeout=5)
        try:
            r = c.execute(q, a).fetchone()
            return r[0] if r else None
        except sqlite3.Error:
            return None
        finally:
            c.close()

    def go_live(self):
        self.cli("set-webhook", input=f"{self.rx.url}\n{SECRET_KEY}\n")
        self.cli("live")

    def settle(self, seconds=2.5):
        time.sleep(seconds)

    def assert_no_secret_anywhere(self):
        blobs = self.logs + [self.daemon_log()]
        for root, _, files in os.walk(self.root):
            for fn in files:
                if fn.endswith((".sqlite", ".sqlite-wal")) and "state" in root:
                    with open(os.path.join(root, fn), "rb") as f:
                        blobs.append(f.read().decode("latin-1"))
        for b in blobs:
            self.assertNotIn(SECRET_KEY, b, "bearer key leaked")
            self.assertNotIn("SECRETPATH", b, "webhook URL path leaked")


class Filtering(Base):
    def test_mcp_thread_wakes_once_with_expected_payload(self):
        self.go_live()
        self.start_daemon()
        tid = self.t3.mcp_thread("mcp:aaa", "EAS builds")
        self.t3.run(tid, tid)
        self.assertTrue(wait_for(lambda: len(self.rx.requests) == 1))
        self.settle()
        self.assertEqual(len(self.rx.requests), 1)
        r = self.rx.requests[0]
        self.assertEqual(r["path"], SECRET_PATH)
        self.assertEqual(r["headers"]["Authorization"], f"Bearer {SECRET_KEY}")
        self.assertEqual(r["headers"]["Idempotency-Key"], r["body"]["wakeId"])
        e = r["body"]["events"][0]
        self.assertEqual((e["event"], e["threadId"], e["why"], e["startedBy"], e["projectName"], e["title"]),
                         ("turn_finished", tid, "mcp_launched", "mcp_client", "demo-app", "EAS builds"))
        self.assertEqual(r["body"]["t3Version"], "0.1.1-nightly.test.1")

    def test_user_threads_and_in_t3_agent_launches_never_wake(self):
        self.go_live()
        self.start_daemon()
        j = self.t3.user_thread("uuid-user")
        self.t3.run(j, f"{j}:m0")
        self.t3.request(j, "req-j", "pending")
        # a thread launched by an agent inside T3: mcp/agent but first message has senderThreadId
        a = self.t3.thread("mcp:inner", "agent", "mcp", "launched by a T3 agent")
        self.t3.message(a, a, "agent", "mcp", sender="uuid-user")
        self.t3.run(a, a)
        self.t3.run(j, self.t3.message("m-mob", j, "user", "mobile"))
        self.settle(3)
        self.assertEqual(self.rx.requests, [])

    def test_children_do_not_wake_but_their_requests_do(self):
        self.go_live()
        self.start_daemon()
        root = self.t3.mcp_thread("mcp:root")
        child = self.t3.thread("thread:delegated-task:c1", "agent", "mcp", "child", parent=root, root=root)
        self.t3.message("cm", child, "agent", "mcp", sender=root)
        self.t3.run(child, "cm")
        self.settle(2.5)
        self.assertEqual(self.rx.requests, [])
        self.t3.request(child, "req-1", "pending", kind="command")
        self.assertTrue(wait_for(lambda: self.rx.requests))
        e = self.rx.events()[0]
        self.assertEqual((e["event"], e["threadId"], e["request"]["childThreadId"], e["request"]["kind"]),
                         ("needs_input", root, child, "command"))

    def test_mcp_steering_a_user_thread_wakes_until_the_user_takes_it_back(self):
        self.go_live()
        self.start_daemon()
        j = self.t3.user_thread("uuid-j2")
        mcp_msg = self.t3.message("tm1", j, "agent", "mcp")
        self.t3.run(j, mcp_msg)
        self.assertTrue(wait_for(lambda: len(self.rx.requests) == 1))
        e = self.rx.events()[0]
        self.assertEqual((e["why"], e["startedBy"]), ("mcp_started_run", "mcp_client"))
        # T3's own follow-up wake (e.g. subagent finished) keeps the MCP client as owner
        self.t3.run(j, self.t3.message("wake1", j, "agent", "server"))
        self.assertTrue(wait_for(lambda: len(self.rx.requests) == 2))
        # the user types: that run must not wake
        self.t3.run(j, self.t3.message("jm", j, "user", "web"))
        self.t3.run(j, self.t3.message("wake2", j, "agent", "server"))
        self.settle(3)
        self.assertEqual(len(self.rx.requests), 2)

    def test_user_typing_in_an_mcp_thread_still_wakes_with_startedBy_user(self):
        self.go_live()
        self.start_daemon()
        t = self.t3.mcp_thread("mcp:u-in-mcp")
        self.t3.run(t, self.t3.message("jm", t, "user", "web"))
        self.assertTrue(wait_for(lambda: self.rx.requests))
        self.assertEqual(self.rx.events()[0]["startedBy"], "user")

    def test_allow_and_mute(self):
        self.go_live()
        self.start_daemon()
        j = self.t3.user_thread("uuid-handed")
        t = self.t3.mcp_thread("mcp:noisy")
        self.cli("allow", j)
        self.cli("mute", t)
        self.t3.run(j, f"{j}:m0")
        self.t3.run(t, t)
        self.assertTrue(wait_for(lambda: self.rx.requests))
        self.settle(2.5)
        self.assertEqual([(e["threadId"], e["why"]) for e in self.rx.events()], [(j, "allowlist")])
        st = self.cli("status").stdout
        self.assertIn("uuid-handed", st)
        self.assertIn("mcp:noisy", st)
        self.cli("unallow", j)
        self.cli("unmute", t)
        self.t3.run(j, f"{j}:m0")
        self.t3.run(t, t)
        self.assertTrue(wait_for(lambda: len(self.rx.events()) == 2))
        self.assertEqual(self.rx.events()[1]["threadId"], t)

    def test_burst_is_coalesced_into_one_wake(self):
        self.go_live()
        self.start_daemon()
        t = self.t3.mcp_thread("mcp:burst")
        self.t3.run(t, t)
        for i in range(2):  # background-monitor wakes right after
            time.sleep(0.3)
            self.t3.run(t, self.t3.message(f"pw{i}", t, "agent", "provider"))
        self.assertTrue(wait_for(lambda: self.rx.requests))
        self.settle(2.5)
        self.assertEqual(len(self.rx.requests), 1)
        self.assertEqual(self.rx.events()[0]["runsCoalesced"], 3)

    def test_failed_run_is_urgent_and_carries_the_error(self):
        self.go_live()
        self.start_daemon()
        t = self.t3.mcp_thread("mcp:fail")
        t0 = time.time()
        self.t3.run(t, t, status="failed", error="Provider turn failed.")
        self.assertTrue(wait_for(lambda: self.rx.requests))
        self.assertLess(time.time() - t0, 2.5)  # grace 0.6 + batch 0.3 + poll, no 1s quiet window
        e = self.rx.events()[0]
        self.assertEqual((e["event"], e["lastError"]), ("run_failed", "transport_error: Provider turn failed."))

    def test_interrupt_followed_by_restart_continuation_reports_only_the_continuation(self):
        self.go_live()
        self.start_daemon()
        t = self.t3.mcp_thread("mcp:restart")
        rid = self.t3.run(t, t, status="interrupted")
        cont = self.t3.run_created(t, t, cont=rid)
        self.settle(2)
        self.assertEqual(self.rx.requests, [])
        self.t3.run_update(t, cont, "completed", t)
        self.assertTrue(wait_for(lambda: self.rx.requests))
        self.assertEqual(self.rx.events()[0]["event"], "turn_finished")

    def test_request_answered_before_send_is_skipped(self):
        self.go_live()
        self.start_daemon()
        t = self.t3.mcp_thread("mcp:quick")
        self.t3.request(t, "req-q", "pending")
        self.t3.request(t, "req-q", "resolved")  # answered within the batch window
        self.settle(2)
        self.assertEqual(self.rx.requests, [])

    def test_history_before_first_start_is_not_replayed(self):
        t = self.t3.mcp_thread("mcp:old")
        self.t3.run(t, t)
        self.go_live()
        self.start_daemon()
        self.settle(2.5)
        self.assertEqual(self.rx.requests, [])


class Delivery(Base):
    def test_retries_keep_the_same_wake_id_and_duplicates_are_ignored(self):
        self.go_live()
        self.rx.codes = [500, 503, 200]
        self.start_daemon()
        t = self.t3.mcp_thread("mcp:retry")
        rid = self.t3.run(t, t)
        self.assertTrue(wait_for(lambda: len(self.rx.requests) == 3, 15))
        ids = {r["body"]["wakeId"] for r in self.rx.requests}
        keys = {r["headers"]["Idempotency-Key"] for r in self.rx.requests}
        self.assertEqual(len(ids), 1)
        self.assertEqual(ids, keys)
        self.assertEqual([r["code"] for r in self.rx.requests], [500, 503, 200])
        # T3 re-emits the same terminal update: no second wake
        self.t3.run_update(t, rid, "completed", t)
        self.settle(3)
        self.assertEqual(len(self.rx.requests), 3)
        self.assertIn("sent, HTTP 200", self.cli("status").stdout)

    def test_auth_rejection_stops_sending_until_webhook_is_reset(self):
        self.go_live()
        self.rx.codes = [401]
        self.start_daemon()
        t = self.t3.mcp_thread("mcp:auth")
        self.t3.run(t, t)
        self.assertTrue(wait_for(lambda: self.rx.requests))
        self.settle(2.5)
        self.assertEqual(len(self.rx.requests), 1)
        self.assertIn("rejected the key (HTTP 401)", self.cli("status").stdout)
        self.cli("set-webhook", input=f"{self.rx.url}\n{SECRET_KEY}\n")
        self.assertNotIn("rejected", self.cli("status").stdout)

    def test_dry_run_logs_and_sends_nothing(self):
        self.cli("set-webhook", input=f"{self.rx.url}\n{SECRET_KEY}\n")  # set, but stay in dry-run
        self.start_daemon()
        t = self.t3.mcp_thread("mcp:dry", "Dry thread")
        self.t3.run(t, t)
        self.assertTrue(wait_for(lambda: "[dry-run] would POST" in self.daemon_log()))
        self.settle(1)
        self.assertEqual(self.rx.requests, [])
        self.assertIn('turn_finished "Dry thread"', self.daemon_log())
        self.assertIn("dry-run, not sent", self.cli("status").stdout)
        self.assertIn("dry-run, not sent", self.cli("log").stdout)

    def test_hourly_cap_holds_extra_wakes(self):
        self.set_config(global_cap_per_hour=1)
        self.go_live()
        self.start_daemon()
        a, b = self.t3.mcp_thread("mcp:c1"), self.t3.mcp_thread("mcp:c2")
        self.t3.run(a, a)
        self.assertTrue(wait_for(lambda: self.rx.requests))
        self.t3.run(b, b)
        self.settle(3)
        self.assertEqual(len(self.rx.requests), 1)
        self.assertIn("1 waiting to batch", self.cli("status").stdout)

    def test_test_command_sends_one_test_wake(self):
        self.cli("set-webhook", input=f"{self.rx.url}\n{SECRET_KEY}\n")
        out = self.cli("test").stdout
        self.assertIn("Test wake delivered: HTTP 200", out)
        self.assertEqual(self.rx.events()[0]["event"], "test")
        self.assertIn("last test:", self.cli("status").stdout)

    def test_webhook_storage_is_encrypted_or_0600_and_never_shown(self):
        self.cli("set-webhook", input=f"{self.rx.url}\n{SECRET_KEY}\n")
        cred = os.path.join(self.cfg_dir, "webhook.cred")
        self.assertTrue(os.path.exists(cred), "expected systemd-creds storage")
        self.assertEqual(os.stat(cred).st_mode & 0o777, 0o600)
        with open(cred, "rb") as f:
            raw = f.read()
        self.assertNotIn(SECRET_KEY.encode(), raw)
        self.assertNotIn(b"SECRETPATH", raw)
        self.assertIn("host 127.0.0.1", self.cli("status").stdout)
        env = dict(self.env, T3_WAKE_SECRET_STORE="file")
        r = subprocess.run([sys.executable, "-I", SCRIPT, "set-webhook"], env=env, capture_output=True, text=True,
                           input=f"{self.rx.url}\n{SECRET_KEY}\n")
        self.logs.append(r.stdout + r.stderr)
        plain = os.path.join(self.cfg_dir, "webhook.json")
        self.assertEqual(os.stat(plain).st_mode & 0o777, 0o600)
        self.assertFalse(os.path.exists(cred))
        self.assertIn("Test wake delivered", self.cli("test").stdout)
        r = self.cli("set-webhook", input="ftp://nope\nkey\n", check=False)
        self.assertNotEqual(r.returncode, 0)
        self.cli("clear-webhook")
        self.assertIn("not set", self.cli("status").stdout)
        self.assertNotEqual(self.cli("live", check=False).returncode, 0)


class Lifecycle(Base):
    def test_off_drops_queued_and_on_does_not_replay(self):
        self.go_live()
        self.start_daemon()
        t = self.t3.mcp_thread("mcp:offon")
        self.t3.run(t, t)
        time.sleep(0.5)  # inside the 1s quiet window: wake is still debouncing
        self.assertIn("OFF", self.cli("off").stdout)
        self.t3.run(t, t)
        self.t3.request(t, "req-off", "pending")
        self.settle(3)
        self.assertEqual(self.rx.requests, [])
        self.assertIn("t3-wake:    OFF", self.cli("status").stdout)
        cursor_off = int(self.state("select value from kv where key='cursor'"))
        self.assertEqual(cursor_off, self.t3.db.execute("select max(sequence) from orchestration_events").fetchone()[0])
        self.cli("on")
        self.settle(2.5)
        self.assertEqual(self.rx.requests, [], "turning on replayed old events")
        self.t3.run(t, t)
        self.assertTrue(wait_for(lambda: self.rx.requests))
        self.assertEqual(len(self.rx.events()), 1)

    def test_on_after_daemon_was_down_while_off_does_not_replay(self):
        self.go_live()
        self.start_daemon()
        self.cli("off")
        self.stop_daemon()
        t = self.t3.mcp_thread("mcp:down-off")
        self.t3.run(t, t)
        self.cli("on")
        self.start_daemon()
        self.settle(3)
        self.assertEqual(self.rx.requests, [])

    def test_restart_resumes_from_cursor_and_finishes_pending_work(self):
        self.go_live()
        self.start_daemon()
        a = self.t3.mcp_thread("mcp:before")
        self.t3.run(a, a)
        self.assertTrue(wait_for(lambda: len(self.rx.requests) == 1))
        # crash mid-debounce
        b = self.t3.mcp_thread("mcp:mid")
        self.t3.run(b, b)
        time.sleep(0.5)
        self.stop_daemon(signal.SIGKILL)
        # events while the watcher is down
        c = self.t3.mcp_thread("mcp:while-down")
        self.t3.run(c, c)
        self.rx.codes = [500]  # first attempt after restart fails; then crash again mid-retry
        self.start_daemon()
        self.assertTrue(wait_for(lambda: len(self.rx.requests) == 2))
        self.stop_daemon(signal.SIGKILL)
        failed_id = self.rx.requests[1]["body"]["wakeId"]
        self.start_daemon()

        def delivered_threads():
            return sorted(e["threadId"] for r in self.rx.requests if r["code"] == 200 for e in r["body"]["events"])
        self.assertTrue(wait_for(lambda: delivered_threads() == ["mcp:before", "mcp:mid", "mcp:while-down"], 15),
                        delivered_threads())
        self.settle(2.5)
        self.assertEqual(delivered_threads(), ["mcp:before", "mcp:mid", "mcp:while-down"])
        ok_ids = [r["body"]["wakeId"] for r in self.rx.requests if r["code"] == 200]
        self.assertEqual(len(ok_ids), len(set(ok_ids)), "a wake was delivered twice")
        self.assertIn(failed_id, ok_ids, "the failed wake was not retried with the same wakeId")

    def test_schema_break_after_upgrade_sends_degraded_then_recovered(self):
        self.go_live()
        self.start_daemon()
        self.t3.db.execute("alter table orchestration_v2_projection_runs rename to runs_moved")
        self.t3.set_version("0.1.2-nightly.test.2")
        self.assertTrue(wait_for(lambda: any(e["event"] == "watcher_degraded" for e in self.rx.events()), 25))
        self.assertIn("DEGRADED", self.cli("status").stdout)
        self.t3.db.execute("alter table runs_moved rename to orchestration_v2_projection_runs")
        self.assertTrue(wait_for(lambda: any(e["event"] == "watcher_recovered" for e in self.rx.events()), 10))
        self.assertIn("health:     OK", self.cli("status").stdout)
        self.assertEqual([e["event"] for e in self.rx.events()].count("watcher_degraded"), 1)


class Config(Base):
    def test_default_source_and_labels(self):
        self.go_live()
        self.start_daemon()
        t = self.t3.mcp_thread("mcp:defaults")
        self.t3.run(t, t)
        self.assertTrue(wait_for(lambda: self.rx.requests))
        b = self.rx.bodies()[0]
        self.assertEqual(b["source"], "t3-wake")
        self.assertEqual(self.rx.requests[0]["headers"]["User-Agent"].split("/")[0], "t3-wake")

    def test_labels_rename_why_and_startedBy_values(self):
        self.set_config(source="my-host", labels={"mcp_client": "bot", "user": "me", "mcp_launched": "bot_launched",
                                                  "mcp_started_run": "bot_started_run"})
        self.go_live()
        self.start_daemon()
        t = self.t3.mcp_thread("mcp:labels")
        self.t3.run(t, t)
        self.assertTrue(wait_for(lambda: len(self.rx.events()) == 1))
        self.t3.run(t, self.t3.message("um", t, "user", "web"))
        self.assertTrue(wait_for(lambda: len(self.rx.events()) == 2))
        j = self.t3.user_thread("uuid-steered")
        self.t3.run(j, self.t3.message("bm", j, "agent", "mcp"))
        self.assertTrue(wait_for(lambda: len(self.rx.events()) == 3))
        self.t3.request(t, "req-l", "pending")
        self.assertTrue(wait_for(lambda: len(self.rx.events()) == 4))
        ev = self.rx.events()
        self.assertEqual([(e["why"], e["startedBy"]) for e in ev[:3]],
                         [("bot_launched", "bot"), ("bot_launched", "me"), ("bot_started_run", "bot")])
        self.assertEqual((ev[3]["event"], ev[3]["why"]), ("needs_input", "bot_launched"))
        self.assertEqual({b["source"] for b in self.rx.bodies()}, {"my-host"})

    def test_include_t3_agents_counts_launches_from_inside_t3(self):
        self.set_config(detection={"include_t3_agents": True})
        self.go_live()
        self.start_daemon()
        a = self.t3.thread("mcp:inner2", "agent", "mcp", "launched by a T3 agent")
        self.t3.message(a, a, "agent", "mcp", sender="uuid-parent")
        self.t3.run(a, a)
        self.assertTrue(wait_for(lambda: self.rx.requests))
        self.assertEqual(self.rx.events()[0]["why"], "mcp_launched")
        u = self.t3.user_thread("uuid-still-silent")
        self.t3.run(u, f"{u}:m0")
        self.settle(2.5)
        self.assertEqual(len(self.rx.events()), 1)


if __name__ == "__main__":
    unittest.main()
