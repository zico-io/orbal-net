//! comms - per-mission agent coordination in one binary.
//!
//! `comms serve` is the authoritative per-mission server (host-side); every other
//! subcommand is the thin client that agents (orchestrator + in-VM leads/workers)
//! call. One source of truth per mission: agents / rooms / messages / read-cursors,
//! persisted in SQLite so a server restart mid-mission loses nothing.
//!
//! Transport is JSON-over-HTTP, one path per action, Bearer-token auth (the server
//! binds 0.0.0.0 so local-NAT + remote containers can dial in). The client reads its
//! target and identity from the environment:
//!
//!   COMMS_URL    base URL of the mission's server, e.g. http://10.0.0.4:54123
//!   COMMS_TOKEN  the mission's bearer token
//!   COMMS_AGENT  this caller's identity (role), e.g. lead-a / worker-a-1 / orchestrator
//!
//!   comms serve --token <t> [--port N] [--db <path>]   # prints {"port": N} then serves
//!   comms whoami | agents | rooms | inbox
//!   comms status <active|idle|busy|done>
//!   comms create-room <name> [--type public]
//!   comms join <room> | leave <room> | destroy-room <room>
//!   comms send <room> <message...>
//!   comms dm <agent> <message...>
//!   comms read <room> [--since <seq>]
//!   comms wait <room> [--since <seq>] [--timeout <secs>]   # blocking long-poll read
//!   comms invite <room> <agent> | kick <room> <agent>

#![warn(clippy::all)]

use serde_json::{json, Value};
use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

mod server;

const USAGE: &str = "\
comms <subcommand> [args]
  serve --token <t> [--port N] [--db <path>]
  whoami | agents | rooms | inbox
  status <active|idle|busy|done>
  create-room <name> [--type public]
  join <room> | leave <room> | destroy-room <room>
  send <room> <message...>
  dm <agent> <message...>
  read <room> [--since <seq>]
  peek <room> [--since <seq>]   # read without advancing your cursor (monitoring)
  wait <room> [--since <seq>] [--timeout <secs>]
  invite <room> <agent> | kick <room> <agent>";

fn die(msg: impl AsRef<str>) -> ! {
    eprintln!("comms: {}", msg.as_ref());
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
        Some("--selfcheck") => match server::selfcheck() {
            Ok(()) => println!("comms selfcheck ok"),
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

fn client(argv: &[String]) {
    let cmd = argv[0].as_str();
    let rest: Vec<String> = argv[1..].to_vec();

    // (action, body-fields, is_wait) — body always gets "agent" added by post().
    let mut body = serde_json::Map::new();
    let mut is_wait = false;

    match cmd {
        "whoami" | "agents" | "rooms" | "inbox" => {}
        "status" => {
            if rest.len() != 1 {
                die("usage: comms status <active|idle|busy|done>");
            }
            body.insert("state".into(), json!(rest[0]));
        }
        "create-room" => {
            let (rtype, rest) = parse_opt(&rest, "--type");
            if rest.len() != 1 {
                die("usage: comms create-room <name> [--type public]");
            }
            body.insert("name".into(), json!(rest[0]));
            if let Some(t) = rtype {
                body.insert("type".into(), json!(t));
            }
        }
        "join" | "leave" | "destroy-room" => {
            if rest.len() != 1 {
                die(format!("usage: comms {cmd} <room>"));
            }
            body.insert("room".into(), json!(rest[0]));
        }
        "send" => {
            if rest.len() < 2 {
                die("usage: comms send <room> <message...>");
            }
            body.insert("room".into(), json!(rest[0]));
            body.insert("text".into(), json!(rest[1..].join(" ")));
        }
        "dm" => {
            if rest.len() < 2 {
                die("usage: comms dm <agent> <message...>");
            }
            body.insert("to".into(), json!(rest[0]));
            body.insert("text".into(), json!(rest[1..].join(" ")));
        }
        "read" | "peek" => {
            let (since, rest) = parse_opt(&rest, "--since");
            if rest.len() != 1 {
                die(format!("usage: comms {cmd} <room> [--since <seq>]"));
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
                die("usage: comms wait <room> [--since <seq>] [--timeout <secs>]");
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
                die(format!("usage: comms {cmd} <room> <agent>"));
            }
            body.insert("room".into(), json!(rest[0]));
            body.insert("target".into(), json!(rest[1]));
        }
        other => die(format!(
            "unknown command {other:?} (see comms with no args)"
        )),
    }

    let url = env::var("COMMS_URL").ok();
    let token = env::var("COMMS_TOKEN").ok();
    let agent = env::var("COMMS_AGENT").ok();
    let (url, token, agent) = match (url, token, agent) {
        (Some(u), Some(t), Some(a)) if !u.is_empty() && !t.is_empty() && !a.is_empty() => (u, t, a),
        _ => die("COMMS_URL, COMMS_TOKEN and COMMS_AGENT must all be set"),
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

    match http_post(&url, &token, cmd, &payload, read_timeout) {
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

/// Minimal HTTP/1.1 POST over a plain TCP socket. COMMS_URL is always `http://`
/// (LAN/loopback, Bearer-token auth, no TLS), so a hand-rolled request beats
/// pulling in a full HTTP+TLS client stack for one call.
fn http_post(
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
        .ok_or_else(|| io_err("COMMS_URL must start with http://"))?;
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
