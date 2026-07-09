# orbal-net

Per-mission agent coordination in one binary: `orbal-net serve` is the
authoritative per-mission server (host-side); every other subcommand is the
thin client that agents (orchestrator + in-VM leads/workers) call. One source
of truth per mission - agents, rooms, messages, read-cursors - persisted in
SQLite so a server restart mid-mission loses nothing.

Transport is JSON-over-HTTP, one path per action, Bearer-token auth (the
server binds `0.0.0.0` so local-NAT + remote containers can dial in). The
client reads its target and identity from the environment:

```
ORBAL_NET_URL    base URL of the mission's server, e.g. http://10.0.0.4:54123
ORBAL_NET_TOKEN  the mission's bearer token
ORBAL_NET_AGENT  this caller's identity (role), e.g. lead-a / worker-a-1 / orchestrator
```

## Install

```sh
cargo install orbal-net
```

## Usage

```
orbal-net serve --token <t> [--port N] [--db <path>]   # prints {"port": N} then serves
orbal-net whoami | agents | rooms | inbox
orbal-net status <active|idle|busy|done>
orbal-net create-room <name> [--type public]
orbal-net join <room> | leave <room> | destroy-room <room>
orbal-net send <room> <message...>
orbal-net dm <agent> <message...>
orbal-net read <room> [--since <seq>]
orbal-net peek <room> [--since <seq>]   # read without advancing your cursor (monitoring)
orbal-net wait <room> [--since <seq>] [--timeout <secs>]   # blocking long-poll read
orbal-net invite <room> <agent> | kick <room> <agent>
orbal-net event <room> <kind> [--task T] [--phase P] [--step N/M] [--percent P] [--to AGENT] [--note <text...>]
  kinds: task-start | task-done | task-error | task-abort | step | phase | blocked | handoff
orbal-net progress <room> <N/M | P%> [--task T] [--note <text...>]   # sugar for `event step`
orbal-net events <room> [--since <seq>]   # non-consuming event read
orbal-net tui [--interval <secs>]   # live full-screen dashboard (alias: watch)
```

`orbal-net tui` (alias `watch`) is a live, read-only full-screen dashboard
over an existing mission server: room thread drill-in and a per-agent
progress panel, driven by the same events/progress protocol above.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
