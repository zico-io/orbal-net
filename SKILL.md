---
name: orbal-net
description: >
  Coordinate multiple agents over a shared mission bus with the orbal-net CLI —
  rooms, direct messages, structured progress/handoff events, per-agent status, and a
  live TUI. Use when ORBAL_NET_URL / ORBAL_NET_TOKEN are set, or when agents need to
  message and track each other across processes, worktrees, or containers instead of
  fire-and-forget subagents.
---

# orbal-net — mission coordination bus

`orbal-net` is a single-binary client+server for coordinating agents within one mission.
The server is authoritative and persists everything (agents, rooms, messages, cursors) to
SQLite. Every other subcommand is a thin HTTP client. Real-time reads use SSE, so agents
push/pull without polling.

## Trigger conditions

- `ORBAL_NET_URL` and `ORBAL_NET_TOKEN` are set in the environment.
- You are one of several agents that must report progress, hand off work, or read each
  other's messages across separate processes, worktrees, or containers.
- You want a durable, observable coordination channel rather than fire-and-forget
  subagents.

## Context — before you start

Client agents authenticate entirely through three env vars:

| Var | Meaning |
|-----|---------|
| `ORBAL_NET_URL` | Base URL of the mission server, e.g. `http://127.0.0.1:54999` |
| `ORBAL_NET_TOKEN` | Bearer token (must match the server's `--token`) |
| `ORBAL_NET_AGENT` | This caller's identity/role, e.g. `orchestrator`, `worker-a` — must be unique per agent |

Start the server **once per mission** (whoever owns the mission does this — usually the
orchestrator):

```bash
orbal-net serve --token <t> [--port N] [--db <path>]   # binds 0.0.0.0; prints {"port":N}
```

Capture the printed port to build `ORBAL_NET_URL`. The server binds `0.0.0.0`, so agents
on other hosts / behind local NAT can dial in.

## Command reference

Run `orbal-net` with no args to print the live usage. Groups:

### Identity & discovery

| Command | Does |
|---------|------|
| `orbal-net whoami` | Your agent id + status |
| `orbal-net agents` | All known agents |
| `orbal-net rooms` | All rooms, members, owners |
| `orbal-net inbox` | Your pending DMs |
| `orbal-net status <active\|idle\|busy\|done>` | Set your own status |

### Rooms

| Command | Does |
|---------|------|
| `orbal-net create-room <name> [--type public]` | Create a room (auto-joins the owner) |
| `orbal-net join <room>` | Join |
| `orbal-net leave <room>` | Leave |
| `orbal-net destroy-room <room>` | Delete (owner only) |
| `orbal-net invite <room> <agent>` | Add another agent |
| `orbal-net kick <room> <agent>` | Remove an agent |

### Messaging

| Command | Does |
|---------|------|
| `orbal-net send <room> <message...>` | Broadcast to a room |
| `orbal-net dm <agent> <message...>` | Direct message |
| `orbal-net read <room> [--since <seq>]` | Read messages, **advances your cursor** |
| `orbal-net peek <room> [--since <seq>]` | Read **without** advancing (monitoring) |
| `orbal-net recv <room> [--since <id>] [--timeout <secs>] [--follow]` | SSE-backed blocking read; `--follow` streams |

### Progress protocol

Structured events, separate from chat messages:

```bash
orbal-net event <room> <kind> [--task T] [--phase P] [--step N/M] [--percent P] [--to AGENT] [--note <text...>]
#   kinds: task-start | task-done | task-error | task-abort | step | phase | blocked | handoff
orbal-net progress <room> <N/M | P%> [--task T] [--note <text...>]   # sugar for `event step`
orbal-net events <room> [--since <seq>]                              # non-consuming event read
```

Use `handoff --to <agent>` to pass work; `blocked` to signal you're stuck; `task-done` /
`task-error` to report a workstream result (e.g. after a build/test gate).

### Observability

```bash
orbal-net tui [--interval <secs>]    # live full-screen dashboard of all agents/rooms (alias: watch)
```

## Gotchas

- **One authoritative server per mission.** Don't run a second `serve`; every client
  points at the same URL/token.
- **Unique `ORBAL_NET_AGENT` per agent.** Two agents sharing an id collide on status and
  cursors.
- **Messages and events are separate streams with separate cursors.** A `send` and a
  `progress` in the same room both start at `seq: 1`. `read`/`peek`/`recv` cover messages;
  `events` covers the progress protocol. Reading one does not advance the other.
- **`read` advances your cursor; `peek` and `events` do not.** Use `peek`/`events` to
  monitor without consuming; use `read`/`recv` when you want to mark messages seen.
- **Don't poll `read` in a loop.** Use `recv --follow` (SSE push) to block until new
  messages arrive.
- **Token must match.** A client whose `ORBAL_NET_TOKEN` differs from the server's
  `--token` gets rejected.

## Verify

```bash
orbal-net whoami        # returns your ORBAL_NET_AGENT identity → env is wired
orbal-net rooms         # lists the mission room → server reachable
orbal-net send mission "ping" && orbal-net read mission   # round-trips a message
```
