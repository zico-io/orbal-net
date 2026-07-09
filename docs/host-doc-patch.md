# Host-side doc patch: orbal-net-push (wait -> recv/POST-stream)

Scope: `spawn.py`, `AGENTS.md`, `.botfile/memory/tools/orchestration.md`,
`.claude/commands/spawn-team.md` - none of these live in the orbal-net repo,
so this is a text patch for the orchestrator to apply by hand.

## 1. AGENTS.md (`/opt/botfiles/AGENTS.md`) - Orchestration protocol section

FIND (current text, confirmed from a live agent's loaded system context):

    Agents emit typed progress with `orbal-net event <room> <kind>` / `orbal-net progress
    <room> N/M` (kinds: task-start/done/error/abort, step, phase, blocked, handoff) so
    an observer can follow the fleet in `orbal-net tui` - a live room-thread drill-in plus
    a per-agent progress panel. Events are non-consuming (a separate table, they never
    advance a read cursor), so monitoring never eats a message an agent still needs. To
    monitor room *messages* the same way, use `orbal-net peek <room>` (non-consuming) - never
    `read`, which advances your cursor and eats messages agents still need.

REPLACE the last two sentences with:

    Agents emit typed progress with `orbal-net event <room> <kind>` / `orbal-net progress
    <room> N/M` (kinds: task-start/done/error/abort, step, phase, blocked, handoff) so
    an observer can follow the fleet in `orbal-net tui` - a live dashboard fed by one
    persistent server-push connection (no polling), with a room-thread drill-in plus a
    per-agent progress panel. Events are non-consuming (a separate table, they never
    advance a read cursor), so monitoring never eats a message an agent still needs. To
    monitor room *messages* the same way, use `orbal-net peek <room>` (non-consuming) - never
    `read`, which advances your cursor and eats messages agents still need. Agents block
    for their next task with `orbal-net recv <room>` (push-backed, replaces the old
    `wait`) - never a shell poll loop.

## 2. Worker/lead system-prompt template (inside spawn.py, wherever it renders
   each agent's operating instructions - the text every worker/lead receives
   verbatim, e.g. what became this session's own prompt)

FIND (the block instructing an agent to block for its next task; confirmed
verbatim from a live agent's own system prompt this mission):

    Run `orbal-net join squad-push-lead` then `orbal-net send squad-push-lead ready`. Then
    BLOCK on `orbal-net wait squad-push-lead` for each task — it returns as soon as a
    message arrives, so NEVER write a shell poll loop (no `while`/`for`/`sleep` around
    orbal-net).

REPLACE `orbal-net wait squad-push-lead` with `orbal-net recv squad-push-lead`
(same call site, same semantics - recv's default behavior is a drop-in for
wait: blocks until a message arrives or a timeout, same return shape). No
other wording change needed - "it returns as soon as a message arrives" and
"NEVER write a shell poll loop" both already describe recv's actual (now
push-driven, previously long-poll) behavior correctly.

Apply the identical `wait <room>` -> `recv <room>` substitution everywhere
else this template appears (L1 orchestrator loop, L2 lead loop, L3 worker
loop - any role whose prompt tells it to block on a room).

## 3. .botfile/memory/tools/orchestration.md

Grep this file (and spawn-team.md) for the literal strings `orbal-net wait`
and `wait <room>` and apply the same `wait` -> `recv` substitution. If either
doc explains *why* monitoring uses `peek` instead of `read` (cursor
semantics), no change needed there - that part of the contract is unchanged.
If either doc describes the TUI or fleet-observability as "polling" or
mentions an interval/refresh-cadence for `orbal-net tui`, update it to note
the dashboard is now push-driven (single `/stream` connection, no poll loop);
`--interval` on `orbal-net tui` is now a reconnect backoff, not a refresh
cadence.

## 4. .claude/commands/spawn-team.md

Same grep-and-substitute: any `orbal-net wait` reference becomes `orbal-net
recv`. If it documents the up/down lifecycle or any command examples using
`wait`, update those examples to `recv` with identical arguments.

## Net effect

Every host-side reference to `orbal-net wait <room>` becomes `orbal-net recv
<room>` (identical call site, identical default behavior - recv's no-flag
default is a byte-for-byte drop-in for the old wait's return shape). No
protocol/room/binary/env-var names change. `orbal-net tui`'s `--interval`
flag now means "reconnect backoff", not "poll cadence" - only relevant if any
doc explained tui's refresh behavior in polling terms.
