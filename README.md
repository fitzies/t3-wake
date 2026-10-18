# t3-wake

Get a webhook call when a [T3 Code](https://github.com/pingdotgg/t3code) thread started by your agent finishes, fails, or needs input. Threads you start yourself never trigger it.

t3-wake is for agents that drive T3 through its MCP server. The agent gets a POST as soon as there is something to read, so it doesn't have to poll.

It works with a Grok Bot routine's webhook trigger, or any HTTP endpoint that accepts a JSON POST with a bearer key.

One Python file, standard library only, running as a systemd user service.

## Requirements

- Linux with systemd
- Python 3.9 or newer
- T3 Code on the same machine, data in `~/.t3/userdata`
- `systemd-creds` to encrypt the stored webhook. Without it, t3-wake uses a 0600 file.

## Install

```sh
git clone https://github.com/fitzies/t3-wake ~/code/t3-wake
cd ~/code/t3-wake && ./install.sh
loginctl enable-linger $USER    # keep it running after logout
```

`install.sh` adds the `t3-wake` command to `~/.local/bin` and a systemd user unit. Both run from the checkout. A new install starts in dry-run mode at the current end of T3's event log, so old events are not replayed.

Upgrade with `git pull && systemctl --user restart t3-wake`. Remove with `./uninstall.sh`, or `./uninstall.sh --purge` to delete config and state too.

## Setup

```sh
t3-wake set-webhook    # paste the URL and bearer key, input is hidden
t3-wake test           # send one test wake
t3-wake live           # start sending real wakes
t3-wake status
```

For Grok Bot, create a routine with a webhook trigger and give `set-webhook` its URL and bearer key. The routine gets the thread id in the POST body and can read the thread through T3's MCP server.

The URL and key are encrypted with `systemd-creds` and never printed or logged. `status` shows only the host.

## Commands

| Command | Does |
|---|---|
| `status` | On/off, mode, cursor, last wake, health, lists |
| `off`, `on` | Stop sending, or resume from now. Nothing from the off period is sent. |
| `dry-run`, `live` | Only log wakes, or send them |
| `allow <threadId>`, `unallow <threadId>` | Wake for every run in a thread |
| `mute <threadId>`, `unmute <threadId>` | Never wake for a thread |
| `test` | Send one test wake now, even in dry-run |
| `set-webhook`, `clear-webhook` | Store or delete the webhook |
| `log [-n N] [--journal]` | Recent wakes, or the service log |
| `replay --since 24h` | Show what past events would have sent. Stores and sends nothing. |

## What triggers a wake

T3 tags every thread and message with `createdBy` and `creationSource`. Text you type has `createdBy: "user"`. Anything created through the MCP tools has `createdBy: "agent"` and `creationSource: "mcp"`. Agents running inside T3 can call the same tools, but T3 also stamps their messages with `senderThreadId`. An outside client has none, which is how t3-wake tells them apart.

For each finished run or pending question, the first matching rule wins:

1. Muted thread: no wake.
2. Allowlisted thread: wake for every run. `why` is `allowlist`.
3. Thread launched by an outside MCP client: wake for every run, including runs you start by typing in it. `why` is `mcp_launched`.
4. A run the client started in another thread, plus T3's follow-ups to it, until you type in that thread yourself. `why` is `mcp_started_run`.

Child and subagent threads never wake on their own runs. A question in a child is reported on its top-level thread.

To change detection or payload values, copy `config.example.json` to `~/.config/t3-wake/config.json`. The service reloads it within 15 seconds.

| Key | Default | Meaning |
|---|---|---|
| `detection.user_created_by` | `["user"]` | `createdBy` of text the user typed |
| `detection.mcp_created_by` | `["agent"]` | `createdBy` of MCP launches |
| `detection.mcp_creation_source` | `["mcp"]` | `creationSource` of MCP launches |
| `detection.include_t3_agents` | `false` | Also count launches by agents inside T3 |
| `labels` | `{}` | Rename `why` and `startedBy` values, e.g. `{"mcp_client": "grok"}` |
| `source` | `"t3-wake"` | `source` field of each POST |

Timing settings and their defaults are in `DEFAULTS` at the top of `t3_wake.py`.

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
  "source": "t3-wake",
  "t3Version": "0.1.1-nightly.20261018.3302",
  "events": [{
    "event": "turn_finished",
    "threadId": "mcp:0b6f6c1e",
    "title": "Fix the build",
    "why": "mcp_launched",
    "startedBy": "mcp_client",
    "runStatus": "completed",
    "occurredAt": "2026-10-18T14:57:36.004Z"
  }]
}
```

This example is shortened. A real POST also has `createdAt` and `sentAt`, and each event has `projectId`, `projectName`, `runId`, `runsCoalesced`, `provider`, `model`, `lastError`, `request`, `threadStillActive`, `late` and `eventSequence`. No message text is sent.

| `event` | Sent when |
|---|---|
| `turn_finished` | A run completed and the thread then stayed quiet for 20 s |
| `needs_input` | A question or approval is pending, within about 3 s |
| `run_failed`, `run_interrupted`, `run_cancelled`, `run_rolled_back` | Within about 6 s |
| `watcher_degraded`, `watcher_recovered` | See Caveats |
| `test` | You ran `t3-wake test` |

Any 2xx response counts as delivered. Failed POSTs retry with backoff for up to 6 hours and keep the same `wakeId`, so dedupe on it. A 401 or 403 stops sending until you run `set-webhook` again. Redirects are not followed. t3-wake sends at most one `turn_finished` per thread per minute and 30 POSTs per hour. Extra events wait and go out together.

## How it reads T3

T3 has no outbound webhook, so t3-wake reads T3's database. It opens `~/.t3/userdata/statev2.sqlite` read-only and checks `orchestration_events`, an append-only log, once a second. It keeps its position in `~/.local/state/t3-wake/state.sqlite`. After a restart or crash it picks up the events it missed. It never writes to T3's database and needs no T3 credential.

## Caveats

- T3's database schema is internal and changes often, especially on nightly builds. t3-wake relies on undocumented tables and fields. It is tested on `0.1.1` nightlies.
- A self-test runs at start, every hour, and whenever T3's version changes. If it fails, t3-wake stops reading, `status` shows `DEGRADED`, and the webhook gets one `watcher_degraded` event. It retries every minute and sends `watcher_recovered` once the test passes. Your agent should poll T3 itself in between.
- T3 does not record which outside client made an MCP call. If several outside clients are connected, all of their threads wake the same webhook.

## Tests

```sh
python3 -I tests/test_t3_wake.py -v
```

The tests run the real daemon against a fake T3 database and a local receiver in temporary folders. They take about two minutes and never touch your real data.

## License

MIT
