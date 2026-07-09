//! orbal-net - per-mission agent coordination in one binary.
//!
//! `orbal-net serve` is the authoritative per-mission server (host-side); every other
//! subcommand is the thin client that agents (orchestrator + in-VM leads/workers)
//! call. One source of truth per mission: agents / rooms / messages / read-cursors,
//! persisted in SQLite so a server restart mid-mission loses nothing.
//!
//! Transport is JSON-over-HTTP, one path per action, Bearer-token auth (the server
//! binds 0.0.0.0 so local-NAT + remote containers can dial in). The client reads its
//! target and identity from the environment:
//!
//!   ORBAL_NET_URL    base URL of the mission's server, e.g. http://10.0.0.4:54123
//!   ORBAL_NET_TOKEN  the mission's bearer token
//!   ORBAL_NET_AGENT  this caller's identity (role), e.g. lead-a / worker-a-1 / orchestrator
//!
//!   orbal-net serve --token <t> [--port N] [--db <path>]   # prints {"port": N} then serves
//!   orbal-net whoami | agents | rooms | inbox
//!   orbal-net status <active|idle|busy|done>
//!   orbal-net create-room <name> [--type public]
//!   orbal-net join <room> | leave <room> | destroy-room <room>
//!   orbal-net send <room> <message...>
//!   orbal-net dm <agent> <message...>
//!   orbal-net read <room> [--since <seq>]
//!   orbal-net wait <room> [--since <seq>] [--timeout <secs>]   # deprecated, see recv
//!   orbal-net recv <room> [--since <id>] [--timeout <secs>] [--follow]   # SSE-backed
//!               blocking read; replaces wait (contract v1, mission-orbal-net-push)
//!   orbal-net invite <room> <agent> | kick <room> <agent>
//!   orbal-net event <room> <kind> [--task T] [--phase P] [--step N/M] [--percent P]
//!               [--to AGENT] [--note <text...>]            # emit a progress event
//!   orbal-net progress <room> <N/M | P%> [--task T] [--note <text...>]  # sugar for `event step`
//!   orbal-net events <room> [--since <seq>]                     # non-consuming event read

#![warn(clippy::all)]

use serde_json::{json, Value};
use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

mod server;
mod sse;
mod tui;

const USAGE: &str = "\
orbal-net <subcommand> [args]
  serve --token <t> [--port N] [--db <path>]
  whoami | agents | rooms | inbox
  status <active|idle|busy|done>
  create-room <name> [--type public]
  join <room> | leave <room> | destroy-room <room>
  send <room> <message...>
  dm <agent> <message...>
  read <room> [--since <seq>]
  peek <room> [--since <seq>]   # read without advancing your cursor (monitoring)
  wait <room> [--since <seq>] [--timeout <secs>]   # deprecated, see recv
  recv <room> [--since <id>] [--timeout <secs>] [--follow]   # SSE-backed; replaces wait
  invite <room> <agent> | kick <room> <agent>
  event <room> <kind> [--task T] [--phase P] [--step N/M] [--percent P] [--to AGENT] [--note <text...>]
    kinds: task-start | task-done | task-error | task-abort | step | phase | blocked | handoff
  progress <room> <N/M | P%> [--task T] [--note <text...>]   # sugar for `event step`
  events <room> [--since <seq>]   # non-consuming event read
  tui [--interval <secs>]   # live full-screen dashboard (alias: watch)";

fn die(msg: impl AsRef<str>) -> ! {
    eprintln!("orbal-net: {}", msg.as_ref());
    std::process::exit(1);
}

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        None => {
            eprintln!("{USAGE}");
            std::process::exit(1);
        }
        Some("serve") => server::run(&args[1..]),
        Some("tui") | Some("watch") => tui::run(&args[1..]),
        Some("recv") => recv(&args[1..]),
        Some("--selfcheck") => match server::selfcheck() {
            Ok(()) => println!("orbal-net selfcheck ok"),
            Err(e) => die(format!("selfcheck failed: {e}")),
        },
        Some(_) => client(&args),
    }
}

/// Pull `--name value` out of args, returning (value, remaining args).
fn parse_opt(args: &[String], name: &str) -> (Option<String>, Vec<String>) {
    if let Some(i) = args.iter().position(|a| a == name) {
        if i + 1 >= args.len() {
            die(format!("{name} needs a value"));
        }
        let mut rest = args.to_vec();
        let val = rest.remove(i + 1);
        rest.remove(i);
        (Some(val), rest)
    } else {
        (None, args.to_vec())
    }
}

/// Pull a bare `--name` flag out of args, returning (present, remaining args).
fn parse_flag(args: &[String], name: &str) -> (bool, Vec<String>) {
    if let Some(i) = args.iter().position(|a| a == name) {
        let mut rest = args.to_vec();
        rest.remove(i);
        (true, rest)
    } else {
        (false, args.to_vec())
    }
}

/// Read `ORBAL_NET_URL`/`ORBAL_NET_TOKEN`/`ORBAL_NET_AGENT`, or die with a usage
/// hint if any are unset.
fn env_triple() -> (String, String, String) {
    match (
        env::var("ORBAL_NET_URL").ok().filter(|s| !s.is_empty()),
        env::var("ORBAL_NET_TOKEN").ok().filter(|s| !s.is_empty()),
        env::var("ORBAL_NET_AGENT").ok().filter(|s| !s.is_empty()),
    ) {
        (Some(u), Some(t), Some(a)) => (u, t, a),
        _ => die("ORBAL_NET_URL, ORBAL_NET_TOKEN and ORBAL_NET_AGENT must all be set"),
    }
}

/// `orbal-net recv <room> [--since <id>] [--timeout <secs>] [--follow]` - SSE-backed
/// replacement for `wait` (contract v1, mission-orbal-net-push wire contract seq 8).
/// Default is a drop-in for `wait`: read frames until `: ready`; if any messages
/// arrived during backfill, print `{room,messages:[...]}` (same shape `wait`/`read`
/// returned) and exit. If none, keep reading until the first live message (print+exit)
/// or `--timeout` elapses (default 120s -> print `{room,messages:[]}`, exit).
/// `--follow` stays open and prints one JSON line per message as it arrives
/// (ignores `--timeout`; exits on signal). The server never advances any cursor but
/// the room's own consuming read cursor, so a plain `recv` behaves exactly like the
/// old `wait` from the caller's perspective.
fn recv(args: &[String]) {
    let (since, args) = parse_opt(args, "--since");
    let (timeout, args) = parse_opt(&args, "--timeout");
    let (follow, args) = parse_flag(&args, "--follow");
    if args.len() != 1 {
        die("usage: orbal-net recv <room> [--since <id>] [--timeout <secs>] [--follow]");
    }
    let room = args[0].clone();
    if let Some(s) = &since {
        if sse::parse_since(s).is_none() {
            die(format!(
                "bad --since value {s:?}; want \"<msgSeq>:<evtSeq>\""
            ));
        }
    }
    let timeout_secs: u64 = timeout
        .as_deref()
        .map(|t| {
            t.parse()
                .unwrap_or_else(|_| die(format!("bad --timeout value {t:?}")))
        })
        .unwrap_or(120);

    let (url, token, agent) = env_triple();
    let payload = json!({ "agent": agent, "mode": "recv", "room": room }).to_string();

    let (code, stream) = open_stream(&url, &token, &payload, since.as_deref())
        .unwrap_or_else(|e| die(format!("cannot reach {url} ({e})")));
    if code != 200 {
        die(format!("recv: {code} {}", read_error_detail(stream)));
    }

    // Bound each blocking read to what's left of the deadline by shrinking the shared
    // socket's read timeout every iteration; `--follow` never sets one (blocks
    // forever). `try_clone` shares the same OS socket, so this affects the reads
    // `SseReader`/`ChunkedReader` do through the moved-in `stream` below.
    let timeout_clone = stream
        .try_clone()
        .unwrap_or_else(|e| die(format!("recv: {e}")));
    let deadline = (!follow).then(|| Instant::now() + Duration::from_secs(timeout_secs));

    let mut reader = sse::SseReader::new(sse::ChunkedReader::new(stream));
    let mut backfill_msgs: Vec<Value> = Vec::new();
    let mut in_backfill = true;

    loop {
        if let Some(dl) = deadline {
            let now = Instant::now();
            if now >= dl {
                print_messages(&room, Vec::new());
                return;
            }
            timeout_clone.set_read_timeout(Some(dl - now)).ok();
        }
        match reader.next_event() {
            Ok(Some(sse::SseEvent::Comment(c))) => {
                if c == "ready" {
                    in_backfill = false;
                    if !backfill_msgs.is_empty() {
                        print_messages(&room, backfill_msgs);
                        return;
                    }
                }
                // keepalive, or a bare "ready" with nothing yet: keep reading.
            }
            Ok(Some(sse::SseEvent::Frame { event, data, .. })) if event == "message" => {
                let msg: Value = serde_json::from_str(&data).unwrap_or(Value::Null);
                if follow {
                    println!("{msg}");
                } else if in_backfill {
                    backfill_msgs.push(msg);
                } else {
                    print_messages(&room, vec![msg]);
                    return;
                }
            }
            Ok(Some(sse::SseEvent::Frame { .. })) => {} // not sent for mode=recv; ignore
            Ok(None) => die("recv: server closed the stream"),
            Err(e) if is_timeout(&e) => continue, // deadline check above handles expiry
            Err(e) => die(format!("recv: {e}")),
        }
    }
}

fn is_timeout(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    )
}

fn print_messages(room: &str, messages: Vec<Value>) {
    let out = json!({ "room": room, "messages": messages });
    println!("{}", serde_json::to_string_pretty(&out).unwrap());
}

/// Split `--note <text...>` off the end of args: everything after `--note` is
/// joined with spaces as the note text (mirrors how `send`/`dm` take a trailing
/// message), leaving the flag args before it for `parse_opt` to consume.
fn split_note(args: &[String]) -> (Vec<String>, Option<String>) {
    match args.iter().position(|a| a == "--note") {
        Some(i) => (args[..i].to_vec(), Some(args[i + 1..].join(" "))),
        None => (args.to_vec(), None),
    }
}

/// Parse a `--step N/M` value into (step_cur, step_total).
fn parse_step(s: &str) -> (i64, i64) {
    let (cur, total) = s
        .split_once('/')
        .unwrap_or_else(|| die(format!("bad --step value {s:?}; want N/M")));
    let cur: i64 = cur
        .parse()
        .unwrap_or_else(|_| die(format!("bad --step value {s:?}; want N/M")));
    let total: i64 = total
        .parse()
        .unwrap_or_else(|_| die(format!("bad --step value {s:?}; want N/M")));
    (cur, total)
}

fn client(argv: &[String]) {
    let cmd = argv[0].as_str();
    // `progress` is pure client-side sugar for `event` (kind=step): same wire
    // action, no server-side "progress" verb.
    let wire_action = if cmd == "progress" { "event" } else { cmd };
    let rest: Vec<String> = argv[1..].to_vec();

    // (action, body-fields, is_wait) — body always gets "agent" added by post().
    let mut body = serde_json::Map::new();
    let mut is_wait = false;

    match cmd {
        "whoami" | "agents" | "rooms" | "inbox" => {}
        "status" => {
            if rest.len() != 1 {
                die("usage: orbal-net status <active|idle|busy|done>");
            }
            body.insert("state".into(), json!(rest[0]));
        }
        "create-room" => {
            let (rtype, rest) = parse_opt(&rest, "--type");
            if rest.len() != 1 {
                die("usage: orbal-net create-room <name> [--type public]");
            }
            body.insert("name".into(), json!(rest[0]));
            if let Some(t) = rtype {
                body.insert("type".into(), json!(t));
            }
        }
        "join" | "leave" | "destroy-room" => {
            if rest.len() != 1 {
                die(format!("usage: orbal-net {cmd} <room>"));
            }
            body.insert("room".into(), json!(rest[0]));
        }
        "send" => {
            if rest.len() < 2 {
                die("usage: orbal-net send <room> <message...>");
            }
            body.insert("room".into(), json!(rest[0]));
            body.insert("text".into(), json!(rest[1..].join(" ")));
        }
        "dm" => {
            if rest.len() < 2 {
                die("usage: orbal-net dm <agent> <message...>");
            }
            body.insert("to".into(), json!(rest[0]));
            body.insert("text".into(), json!(rest[1..].join(" ")));
        }
        "read" | "peek" => {
            let (since, rest) = parse_opt(&rest, "--since");
            if rest.len() != 1 {
                die(format!("usage: orbal-net {cmd} <room> [--since <seq>]"));
            }
            body.insert("room".into(), json!(rest[0]));
            if let Some(s) = since {
                body.insert("since".into(), json!(s));
            }
            // peek reads without advancing the caller's cursor (monitoring).
            if cmd == "peek" {
                body.insert("peek".into(), json!(true));
            }
        }
        "wait" => {
            let (since, rest) = parse_opt(&rest, "--since");
            let (timeout, rest) = parse_opt(&rest, "--timeout");
            if rest.len() != 1 {
                die("usage: orbal-net wait <room> [--since <seq>] [--timeout <secs>]");
            }
            body.insert("room".into(), json!(rest[0]));
            if let Some(s) = since {
                body.insert("since".into(), json!(s));
            }
            if let Some(t) = &timeout {
                body.insert("timeout".into(), json!(t));
            }
            is_wait = true;
        }
        "invite" | "kick" => {
            if rest.len() != 2 {
                die(format!("usage: orbal-net {cmd} <room> <agent>"));
            }
            body.insert("room".into(), json!(rest[0]));
            body.insert("target".into(), json!(rest[1]));
        }
        "event" => {
            // --note takes the rest of argv (like send/dm), so split it off first.
            let (rest, note) = split_note(&rest);
            let (task, rest) = parse_opt(&rest, "--task");
            let (phase, rest) = parse_opt(&rest, "--phase");
            let (step, rest) = parse_opt(&rest, "--step");
            let (percent, rest) = parse_opt(&rest, "--percent");
            let (to, rest) = parse_opt(&rest, "--to");
            if rest.len() != 2 {
                die("usage: orbal-net event <room> <kind> [--task T] [--phase P] [--step N/M] [--percent P] [--to AGENT] [--note <text...>]");
            }
            body.insert("room".into(), json!(rest[0]));
            body.insert("kind".into(), json!(rest[1]));
            if let Some(t) = task {
                body.insert("task".into(), json!(t));
            }
            if let Some(p) = phase {
                body.insert("phase".into(), json!(p));
            }
            if let Some(s) = step {
                let (cur, total) = parse_step(&s);
                body.insert("step_cur".into(), json!(cur));
                body.insert("step_total".into(), json!(total));
            }
            if let Some(p) = percent {
                let p: i64 = p
                    .trim_end_matches('%')
                    .parse()
                    .unwrap_or_else(|_| die(format!("bad --percent value {p:?}")));
                body.insert("percent".into(), json!(p));
            }
            if let Some(t) = to {
                body.insert("target".into(), json!(t));
            }
            if let Some(n) = note {
                body.insert("note".into(), json!(n));
            }
        }
        "progress" => {
            let (rest, note) = split_note(&rest);
            let (task, rest) = parse_opt(&rest, "--task");
            if rest.len() != 2 {
                die("usage: orbal-net progress <room> <N/M | P%> [--task T] [--note <text...>]");
            }
            body.insert("room".into(), json!(rest[0]));
            body.insert("kind".into(), json!("step"));
            if rest[1].ends_with('%') {
                let p: i64 = rest[1]
                    .trim_end_matches('%')
                    .parse()
                    .unwrap_or_else(|_| die(format!("bad progress value {:?}", rest[1])));
                body.insert("percent".into(), json!(p));
            } else {
                let (cur, total) = parse_step(&rest[1]);
                body.insert("step_cur".into(), json!(cur));
                body.insert("step_total".into(), json!(total));
            }
            if let Some(t) = task {
                body.insert("task".into(), json!(t));
            }
            if let Some(n) = note {
                body.insert("note".into(), json!(n));
            }
        }
        "events" => {
            let (since, rest) = parse_opt(&rest, "--since");
            if rest.len() != 1 {
                die("usage: orbal-net events <room> [--since <seq>]");
            }
            body.insert("room".into(), json!(rest[0]));
            if let Some(s) = since {
                body.insert("since".into(), json!(s));
            }
        }
        other => die(format!(
            "unknown command {other:?} (see orbal-net with no args)"
        )),
    }

    let url = env::var("ORBAL_NET_URL").ok();
    let token = env::var("ORBAL_NET_TOKEN").ok();
    let agent = env::var("ORBAL_NET_AGENT").ok();
    let (url, token, agent) = match (url, token, agent) {
        (Some(u), Some(t), Some(a)) if !u.is_empty() && !t.is_empty() && !a.is_empty() => (u, t, a),
        _ => die("ORBAL_NET_URL, ORBAL_NET_TOKEN and ORBAL_NET_AGENT must all be set"),
    };
    body.insert("agent".into(), json!(agent));
    let payload = Value::Object(body).to_string();

    // `wait` long-polls: give the read a generous ceiling above the server's own
    // timeout so a normal return arrives, but a dead network still fails eventually.
    let read_timeout = if is_wait {
        Duration::from_secs(600)
    } else {
        Duration::from_secs(30)
    };

    match http_post(&url, &token, wire_action, &payload, read_timeout) {
        Ok((code, body)) => {
            let doc: Value = serde_json::from_str(&body).unwrap_or(json!({ "raw": body }));
            if code == 200 {
                println!("{}", serde_json::to_string_pretty(&doc).unwrap());
            } else {
                let detail = doc
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("(no detail)");
                die(format!("{cmd}: {code} {detail}"));
            }
        }
        Err(e) => die(format!("cannot reach {url} ({e})")),
    }
}

/// Minimal HTTP/1.1 POST over a plain TCP socket. ORBAL_NET_URL is always `http://`
/// (LAN/loopback, Bearer-token auth, no TLS), so a hand-rolled request beats
/// pulling in a full HTTP+TLS client stack for one call.
pub(crate) fn http_post(
    base: &str,
    token: &str,
    action: &str,
    body: &str,
    read_timeout: Duration,
) -> std::io::Result<(u16, String)> {
    let rest = base
        .trim()
        .trim_end_matches('/')
        .strip_prefix("http://")
        .ok_or_else(|| io_err("ORBAL_NET_URL must start with http://"))?;
    let (hostport, base_path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    let path = format!("{}/{}", base_path.trim_end_matches('/'), action);

    let mut stream = TcpStream::connect(hostport)?;
    stream.set_read_timeout(Some(read_timeout))?;
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: {hostport}\r\nAuthorization: Bearer {token}\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(req.as_bytes())?;

    let mut resp = String::new();
    stream.read_to_string(&mut resp)?;
    let code = resp
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse::<u16>().ok())
        .ok_or_else(|| io_err("malformed HTTP response"))?;
    let body = resp.split_once("\r\n\r\n").map_or("", |x| x.1).to_string();
    Ok((code, body))
}

fn io_err(msg: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg)
}

/// Open a persistent `POST /stream` SSE connection (`Connection: keep-alive`, unlike
/// `http_post`'s one-shot `close`). Returns the response status code and the raw
/// `TcpStream`, positioned right after the HTTP headers - the caller wraps it in
/// `sse::ChunkedReader`/`sse::SseReader` on 200, or reads the (non-chunked) JSON error
/// body directly on failure. `since` is sent as `Last-Event-ID`, the header the server
/// checks first for resume (contract v1, wire contract seq 8).
pub(crate) fn open_stream(
    base: &str,
    token: &str,
    body: &str,
    since: Option<&str>,
) -> std::io::Result<(u16, TcpStream)> {
    let rest = base
        .trim()
        .trim_end_matches('/')
        .strip_prefix("http://")
        .ok_or_else(|| io_err("ORBAL_NET_URL must start with http://"))?;
    let (hostport, base_path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    let path = format!("{}/stream", base_path.trim_end_matches('/'));

    let mut stream = TcpStream::connect(hostport)?;
    let mut req = format!(
        "POST {path} HTTP/1.1\r\nHost: {hostport}\r\nAuthorization: Bearer {token}\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n",
        body.len()
    );
    if let Some(id) = since {
        req.push_str(&format!("Last-Event-ID: {id}\r\n"));
    }
    req.push_str("\r\n");
    req.push_str(body);
    stream.write_all(req.as_bytes())?;

    let code = read_status_line(&mut stream)?;
    Ok((code, stream))
}

/// Read the HTTP status line + headers off `stream` one byte at a time (headers are a
/// few hundred bytes at most, so simplicity beats buffering here - and a buffered read
/// risks swallowing chunked-body bytes past the header boundary), returning the status
/// code. Leaves `stream` positioned exactly at the start of the body.
fn read_status_line(stream: &mut TcpStream) -> std::io::Result<u16> {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = stream.read(&mut byte)?;
        if n == 0 {
            return Err(io_err("connection closed before headers"));
        }
        buf.push(byte[0]);
        if buf.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8_lossy(&buf)
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse::<u16>().ok())
        .ok_or_else(|| io_err("malformed HTTP response"))
}

/// Best-effort error detail for a non-200 `/stream` response: those bodies are plain
/// (non-chunked) JSON, and since we asked for `Connection: keep-alive` the server
/// won't close the socket on its own, so a short timeout bounds the read.
pub(crate) fn read_error_detail(mut stream: TcpStream) -> String {
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok();
    let mut body = String::new();
    let _ = stream.read_to_string(&mut body);
    serde_json::from_str::<Value>(&body)
        .ok()
        .and_then(|d| d.get("error").and_then(Value::as_str).map(str::to_string))
        .unwrap_or_else(|| "(no detail)".into())
}
