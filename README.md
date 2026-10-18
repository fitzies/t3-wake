# t3-wake

t3-wake watches a local [T3 Code](https://github.com/pingdotgg/t3code) install and POSTs to a webhook when a thread launched by an outside MCP client finishes a turn, fails, or asks for input. Threads you start yourself in the T3 app never trigger it.

Use it when an agent drives T3 through its MCP server and would otherwise poll to find out a thread is done. The webhook can be any HTTP endpoint that accepts a JSON POST with a bearer key.

It is one Python file using only the standard library, run as a systemd user service.

## Requirements

- Linux with systemd user services.
- Python 3.9 or newer. Tested with 3.14.
- T3 Code on the same machine, with its data in `~/.t3/userdata`. Tested against `0.1.1` nightly builds, which keep state in `statev2.sqlite`.
- `systemd-creds` to encrypt the stored webhook. Without it, t3-wake stores the webhook in a 0600 file.

## Install

```
git clone https://github.com/fitzies/t3-wake ~/code/t3-wake
cd ~/code/t3-wake
./install.sh
```

`install.sh` writes the CLI wrapper `~/.local/bin/t3-wake` and the user unit `~/.config/systemd/user/t3-wake.service`. Both run `t3_wake.py` from the checkout. To upgrade, run `git pull` and `systemctl --user restart t3-wake`.

A fresh install starts in dry-run mode at the current end of T3's event log. Older events are not replayed.

User services stop when you log out unless linger is on:

```
loginctl enable-linger $USER
```

`./uninstall.sh` removes the service and CLI and keeps config and state. `./uninstall.sh --purge` deletes those too.

## Setup

```
t3-wake set-webhook    # prompts for the URL and bearer key, input hidden
t3-wake test           # sends one test wake now, even in dry-run mode
t3-wake live           # start POSTing real wakes
t3-wake status
```

When stdin is not a terminal, `set-webhook` reads two lines: the URL, then the key. The URL must be `https://`, or `http://` to localhost. t3-wake never prints or logs the URL or key. `status` shows only the host.

## On and off

```
t3-wake off        # stop sending, drop queued wakes, keep the cursor moving
t3-wake on         # resume from now, nothing from the off period is sent
t3-wake dry-run    # log the wakes it would send to the journal, send nothing
t3-wake live       # send again
```

Other commands:

```
t3-wake allow <threadId>     send a wake for every run in this thread
t3-wake unallow <threadId>
t3-wake mute <threadId>      never send a wake for this thread
t3-wake unmute <threadId>
t3-wake log [-n N]           recent wakes and their delivery results
t3-wake log --journal        the service log
t3-wake replay --since 24h   what past events would have sent, using throwaway state
t3-wake clear-webhook        delete the stored webhook and switch to dry-run
```

## How filtering works

T3 records who created each thread and each user message in two fields, `createdBy` and `creationSource`.

- Text you type in the app has `createdBy: "user"`.
- A thread or message created through T3's MCP tools has `createdBy: "agent"` and `creationSource: "mcp"`.
- An agent running inside T3 can call the same MCP tools and gets the same two values. T3 also stamps `senderThreadId`, the calling thread, on that agent's message. An outside client has no sender thread, and that is how t3-wake tells them apart.

For each finished run, t3-wake checks these rules in order:

1. Muted thread: no wake.
2. Allowlisted thread: wake for every run, with `why: "allowlist"`.
3. Thread launched by an outside MCP client: wake for every run, including runs you start by typing in it. `why` is `"mcp_launched"` and `startedBy` says who started the run.
4. Run started by an outside MCP client in any other thread, plus T3's automatic follow-ups to that run: wake with `why: "mcp_started_run"`. This stops once you type in that thread yourself.

Your own threads and threads launched by agents inside T3 send nothing. Child and subagent threads never send a wake for their own runs. A question or approval in a child is sent on its top-level thread with `request.childThreadId` set.

You can change the detection in `~/.config/t3-wake/config.json`. Copy `config.example.json` there to start. The service reloads the file within 15 seconds.

| `detection` key | Default | Meaning |
|---|---|---|
| `user_created_by` | `["user"]` | `createdBy` values for text the user typed |
| `mcp_created_by` | `["agent"]` | `createdBy` values for MCP launches |
| `mcp_creation_source` | `["mcp"]` | `creationSource` values for MCP launches |
| `include_t3_agents` | `false` | also count launches by agents inside T3 |

`labels` renames values in the payload's `why` and `startedBy` fields, for example `{"mcp_client": "mybot", "user": "alice"}`. The built-in values are `user`, `mcp_client`, `agent`, `unknown`, `mcp_launched`, `mcp_started_run` and `allowlist`. `source` sets the `source` field of each POST. The timing keys and their defaults are in `DEFAULTS` at the top of `t3_wake.py`.

## Payload

```
POST <url>
Authorization: Bearer <key>
Idempotency-Key: <wakeId>
Content-Type: application/json
```

```json
{
  "version": 1,
  "wakeId": "tw_3f9a0c1d2e4b5a6978c0d1e2",
  "createdAt": "2026-10-18T14:58:00.120Z",
  "sentAt": "2026-10-18T14:58:00.131Z",
  "source": "t3-wake",
  "t3Version": "0.1.1-nightly.20261018.3302",
  "events": [{
    "event": "turn_finished",
    "threadId": "mcp:0b6f6c1e",
    "title": "Fix the build",
    "projectId": "a1b2c3",
    "projectName": "my-app",
    "why": "mcp_launched",
    "runId": "run:mcp:0b6f6c1e:ordinal:3",
    "runStatus": "completed",
    "runsCoalesced": 1,
    "startedBy": "mcp_client",
    "provider": "claudeAgent",
    "model": "claude-opus-5-5",
    "lastError": null,
    "request": null,
    "threadStillActive": false,
    "late": false,
    "occurredAt": "2026-10-18T14:57:36.004Z",
    "eventSequence": 434980
  }]
}
```

`event` is one of:

- `turn_finished`: a run completed and the thread then stayed quiet for 20 seconds. Bursts of runs become one event, counted in `runsCoalesced`.
- `needs_input`: an approval or question is pending. Sent within about 3 seconds, and skipped if someone answers it first. `request` is `{id, kind, childThreadId}`.
- `run_failed`, `run_interrupted`, `run_cancelled`, `run_rolled_back`: sent within about 6 seconds. If T3 restarts an interrupted run, only the restarted run's result is sent.
- `watcher_degraded`, `watcher_recovered`: see Caveats.
- `test`: sent by `t3-wake test`.

The payload has no message text. The receiver should read the thread through T3's MCP server.

A 2xx response counts as delivered. Other failures retry after 10 s, 30 s, 2 m, 10 m and 30 m, then hourly, and t3-wake gives up after 6 hours. A 401 or 403 stops all sending until you run `set-webhook` again. Any other 3xx or 4xx, except 408, 425 and 429, drops that wake. t3-wake does not follow redirects, so the key only goes to the URL you set.

Retries reuse the same `wakeId`, so treat it as a dedupe key. t3-wake sends at most one `turn_finished` per thread per minute and 30 POSTs per hour. Extra events wait and go out together. Events older than 6 hours are dropped instead of sent.

## How it reads T3

T3 has no outbound webhook, so t3-wake reads its database. It opens `~/.t3/userdata/statev2.sqlite` read-only (`mode=ro` and `query_only`). It never writes to it and needs no T3 credential. If T3 starts using a different `*.sqlite` file in that folder, t3-wake switches to it and starts from that file's current end.

`orchestration_events` is an append-only log with an increasing `sequence`. t3-wake polls it once a second and stores its cursor in its own state database, `~/.local/state/t3-wake/state.sqlite`. It reads `run.updated` events with a terminal status (`completed`, `failed`, `cancelled`, `interrupted`, `rolled_back`) and `runtime-request.updated` events with status `pending`. Thread, message and run details come from the `orchestration_v2_projection_*` tables.

A restart or crash loses nothing. On start, t3-wake processes the events that arrived while it was down and resumes any wake that was waiting to retry.

## Caveats

- T3's database is internal and its schema changes often, especially on nightly builds. t3-wake depends on table names, columns and JSON fields that T3 does not document.
- t3-wake runs a self-test on start, every hour, and whenever the version in T3's `server-runtime.json` changes. It checks the tables, columns, run statuses and thread fields it relies on. If a check fails, it stops reading, `status` shows `DEGRADED` with the reason, and it sends one `watcher_degraded` event. It retries every minute and sends `watcher_recovered` once the check passes. A receiver should poll T3 itself between those two events.
- T3 does not record which outside client made an MCP call. If several outside MCP clients are connected, all their threads send wakes to the same webhook.
- `install.sh`, the service and `systemd-creds` need Linux and systemd.

## Files

| Path | What |
|---|---|
| `~/.config/t3-wake/config.json` | optional settings, see `config.example.json` |
| `~/.config/t3-wake/webhook.cred` | webhook URL and key, encrypted with `systemd-creds` |
| `~/.config/t3-wake/webhook.json` | the same as a 0600 file when `systemd-creds` is not available |
| `~/.local/state/t3-wake/state.sqlite` | cursor, allowlist, mutes, wake history |

Environment variables `T3_WAKE_CONFIG_DIR`, `T3_WAKE_STATE_DIR`, `T3_WAKE_T3_USERDATA` and `T3_WAKE_T3_DB` override these paths.

## Tests

```
python3 -I tests/test_t3_wake.py -v
```

The tests run the real daemon against a synthetic T3 database and a local fake webhook receiver, in temporary directories. They never touch your T3 data, state or webhook. They take about two minutes.

`python3 -I tests/audit_live.py` lists every finished run and pending request in T3 over the last 24 hours next to the wakes t3-wake queued. It is read-only.

## License

MIT. See [LICENSE](LICENSE).
