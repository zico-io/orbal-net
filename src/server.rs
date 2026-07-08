//! The `comms serve` server: SQLite-backed store + threaded JSON-over-HTTP handler.
//!
//! One `rusqlite::Connection` behind a Mutex is the whole coordination store (the 1:1
//! analog of the previous single `threading.Lock`). A Condvar lets `wait` block until a
//! message lands instead of the client hot-polling. State lives in SQLite so a mission
//! server can crash and restart against the same `--db` file without losing history.

use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
use tiny_http::{Header, Response, Server};

pub const STATES: [&str; 4] = ["active", "idle", "busy", "done"];
const DEFAULT_WAIT_SECS: u64 = 120;

/// An error to return to the client as `{code} {"error": msg}`.
#[derive(Debug)]
struct Err {
    code: u16,
    msg: String,
}
fn err(code: u16, msg: impl Into<String>) -> Err {
    Err {
        code,
        msg: msg.into(),
    }
}
type OpResult = Result<Value, Err>;

struct State {
    db: Mutex<Connection>,
    token: String,
    // Bumped on every send/dm; `wait` parks on the Condvar and re-checks the DB when woken.
    gen: Mutex<u64>,
    cvar: Condvar,
}

pub fn run(args: &[String]) {
    let mut token = None;
    let mut port: u16 = 0;
    let mut db_path = String::from(":memory:");
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--token" => {
                token = args.get(i + 1).cloned();
                i += 2;
            }
            "--port" => {
                port = args
                    .get(i + 1)
                    .and_then(|p| p.parse().ok())
                    .unwrap_or_else(|| fail("--port needs a number"));
                i += 2;
            }
            "--db" => {
                db_path = args
                    .get(i + 1)
                    .cloned()
                    .unwrap_or_else(|| fail("--db needs a path"));
                i += 2;
            }
            other => fail(&format!("unknown serve flag {other:?}")),
        }
    }
    let token = token.unwrap_or_else(|| fail("--token is required"));

    let conn = Connection::open(&db_path).unwrap_or_else(|e| fail(&format!("open {db_path}: {e}")));
    init_schema(&conn);

    let server = Server::http(("0.0.0.0", port)).unwrap_or_else(|e| fail(&format!("bind: {e}")));
    let bound = server
        .server_addr()
        .to_ip()
        .map(|a| a.port())
        .unwrap_or(port);
    // Announce the OS-assigned port on the first stdout line so the parent learns it
    // race-free, then serve until killed.
    println!("{}", json!({ "port": bound }));
    std::io::stdout().flush().ok();

    let state = Arc::new(State {
        db: Mutex::new(conn),
        token,
        gen: Mutex::new(0),
        cvar: Condvar::new(),
    });

    // Thread-per-request: a parked `wait` holds its own thread, so it can't stall other
    // requests. ponytail: unbounded threads bounded by request rate — fine at fleet
    // scale (a handful of agents); add a pool only past dozens of concurrent waiters.
    for request in server.incoming_requests() {
        let state = Arc::clone(&state);
        std::thread::spawn(move || handle(&state, request));
    }
}

fn init_schema(conn: &Connection) {
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         CREATE TABLE IF NOT EXISTS agents(
           id TEXT PRIMARY KEY, status TEXT NOT NULL DEFAULT 'active');
         CREATE TABLE IF NOT EXISTS rooms(
           name TEXT PRIMARY KEY, owner TEXT NOT NULL, type TEXT NOT NULL DEFAULT 'public');
         CREATE TABLE IF NOT EXISTS members(
           room TEXT NOT NULL, agent TEXT NOT NULL, PRIMARY KEY(room, agent));
         CREATE TABLE IF NOT EXISTS messages(
           seq INTEGER PRIMARY KEY AUTOINCREMENT,
           room_key TEXT NOT NULL, sender TEXT NOT NULL, text TEXT NOT NULL);
         CREATE INDEX IF NOT EXISTS idx_messages_room ON messages(room_key, seq);
         CREATE TABLE IF NOT EXISTS cursors(
           agent TEXT NOT NULL, room_key TEXT NOT NULL, seq INTEGER NOT NULL,
           PRIMARY KEY(agent, room_key));",
    )
    .expect("init schema");
}

fn handle(state: &State, mut request: tiny_http::Request) {
    // Auth: constant string compare against the mission token.
    let authorized = request
        .headers()
        .iter()
        .any(|h: &Header| h.field.equiv("Authorization") && h.value.as_str() == bearer(&state.token));
    if !authorized {
        return reply(request, 401, json!({ "error": "unauthorized" }));
    }

    let action = request.url().trim_matches('/').to_string();
    let mut buf = String::new();
    request.as_reader().read_to_string(&mut buf).ok();
    let body: Value = serde_json::from_str(if buf.trim().is_empty() { "{}" } else { &buf })
        .unwrap_or(Value::Null);
    if !body.is_object() {
        return reply(request, 400, json!({ "error": "bad JSON body" }));
    }
    let agent = match body.get("agent").and_then(Value::as_str) {
        Some(a) if !a.is_empty() => a.to_string(),
        _ => return reply(request, 400, json!({ "error": "missing 'agent' (set COMMS_AGENT)" })),
    };

    let result = if action == "wait" {
        op_wait(state, &agent, &body)
    } else {
        let conn = state.db.lock().unwrap();
        touch(&conn, &agent);
        let r = dispatch(&conn, &action, &agent, &body);
        drop(conn);
        // A committed message wakes any parked waiters.
        if r.is_ok() && (action == "send" || action == "dm") {
            *state.gen.lock().unwrap() += 1;
            state.cvar.notify_all();
        }
        r
    };

    match result {
        Ok(v) => reply(request, 200, v),
        Err(e) => reply(request, e.code, json!({ "error": e.msg })),
    }
}

fn dispatch(conn: &Connection, action: &str, agent: &str, b: &Value) -> OpResult {
    match action {
        "register" => Ok(json!({ "ok": true, "agent": agent })),
        "whoami" => op_whoami(conn, agent),
        "agents" => op_agents(conn),
        "status" => op_status(conn, agent, b),
        "create-room" => op_create_room(conn, agent, b),
        "rooms" => op_rooms(conn),
        "join" => op_join(conn, agent, b),
        "leave" => op_leave(conn, agent, b),
        "destroy-room" => op_destroy_room(conn, agent, b),
        "send" => op_send(conn, agent, b),
        "dm" => op_dm(conn, agent, b),
        "read" => op_read(conn, agent, b),
        "inbox" => op_inbox(conn, agent),
        "invite" => op_invite(conn, agent, b),
        "kick" => op_kick(conn, agent, b),
        other => Result::Err(err(404, format!("unknown action {other:?}"))),
    }
}

// --- helpers ---------------------------------------------------------------

fn bearer(token: &str) -> String {
    format!("Bearer {token}")
}

fn reply(request: tiny_http::Request, code: u16, payload: Value) {
    let body = payload.to_string();
    let header = Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
    let resp = Response::from_string(body)
        .with_status_code(code)
        .with_header(header);
    request.respond(resp).ok();
}

fn touch(conn: &Connection, agent: &str) {
    conn.execute("INSERT OR IGNORE INTO agents(id) VALUES(?1)", params![agent])
        .ok();
}

/// `--name value` string field, required.
fn need<'a>(b: &'a Value, field: &str) -> Result<&'a str, Err> {
    match b.get(field).and_then(Value::as_str) {
        Some(s) if !s.is_empty() => Ok(s),
        _ => Result::Err(err(400, format!("missing field {field:?}"))),
    }
}

/// A JSON field that may arrive as a number or a numeric string (the client sends
/// `--since`/`--timeout` as strings). Returns None if absent/empty.
fn opt_i64(b: &Value, field: &str) -> Option<i64> {
    match b.get(field) {
        Some(Value::Number(n)) => n.as_i64(),
        Some(Value::String(s)) if !s.is_empty() => s.parse().ok(),
        _ => None,
    }
}

fn dm_key(a: &str, b: &str) -> String {
    if a <= b {
        format!("@{a}|{b}")
    } else {
        format!("@{b}|{a}")
    }
}

fn room_owner(conn: &Connection, name: &str) -> Result<String, Err> {
    conn.query_row("SELECT owner FROM rooms WHERE name=?1", params![name], |r| {
        r.get::<_, String>(0)
    })
    .optional()
    .expect("query room")
    .ok_or_else(|| err(404, format!("no room {name:?}")))
}

fn is_member(conn: &Connection, room: &str, agent: &str) -> bool {
    conn.query_row(
        "SELECT 1 FROM members WHERE room=?1 AND agent=?2",
        params![room, agent],
        |_| Ok(()),
    )
    .optional()
    .expect("query member")
    .is_some()
}

fn cursor(conn: &Connection, agent: &str, room_key: &str) -> i64 {
    conn.query_row(
        "SELECT seq FROM cursors WHERE agent=?1 AND room_key=?2",
        params![agent, room_key],
        |r| r.get::<_, i64>(0),
    )
    .optional()
    .expect("query cursor")
    .unwrap_or(0)
}

/// Messages in `room_key` with seq > `since`, as `[{seq, from, text}]`.
fn messages_after(conn: &Connection, room_key: &str, since: i64) -> Vec<Value> {
    let mut stmt = conn
        .prepare("SELECT seq, sender, text FROM messages WHERE room_key=?1 AND seq>?2 ORDER BY seq")
        .expect("prepare messages");
    let rows = stmt
        .query_map(params![room_key, since], |r| {
            Ok(json!({
                "seq": r.get::<_, i64>(0)?,
                "from": r.get::<_, String>(1)?,
                "text": r.get::<_, String>(2)?,
            }))
        })
        .expect("query messages");
    rows.map(|r| r.expect("row")).collect()
}

fn advance_cursor(conn: &Connection, agent: &str, room_key: &str, seq: i64) {
    conn.execute(
        "INSERT INTO cursors(agent, room_key, seq) VALUES(?1,?2,?3)
         ON CONFLICT(agent, room_key) DO UPDATE SET seq=excluded.seq",
        params![agent, room_key, seq],
    )
    .expect("advance cursor");
}

fn append(conn: &Connection, room_key: &str, sender: &str, text: &str) -> i64 {
    conn.execute(
        "INSERT INTO messages(room_key, sender, text) VALUES(?1,?2,?3)",
        params![room_key, sender, text],
    )
    .expect("insert message");
    conn.last_insert_rowid()
}

/// Room keys the agent should see in its inbox: joined rooms + its DM channels.
fn relevant_rooms(conn: &Connection, agent: &str) -> Vec<String> {
    let mut keys: Vec<String> = {
        let mut stmt = conn
            .prepare("SELECT room FROM members WHERE agent=?1")
            .unwrap();
        let rows = stmt
            .query_map(params![agent], |r| r.get::<_, String>(0))
            .unwrap();
        rows.map(|r| r.unwrap()).collect()
    };
    let mut stmt = conn
        .prepare("SELECT DISTINCT room_key FROM messages WHERE room_key LIKE '@%'")
        .unwrap();
    let dms = stmt.query_map([], |r| r.get::<_, String>(0)).unwrap();
    for k in dms.map(|r| r.unwrap()) {
        if k[1..].split('|').any(|p| p == agent) {
            keys.push(k);
        }
    }
    keys
}

// --- ops -------------------------------------------------------------------

fn op_whoami(conn: &Connection, agent: &str) -> OpResult {
    let status: String = conn
        .query_row("SELECT status FROM agents WHERE id=?1", params![agent], |r| {
            r.get(0)
        })
        .expect("whoami");
    Ok(json!({ "agent": agent, "status": status }))
}

fn op_agents(conn: &Connection) -> OpResult {
    let mut stmt = conn
        .prepare("SELECT id, status FROM agents ORDER BY id")
        .unwrap();
    let rows = stmt
        .query_map([], |r| {
            Ok(json!({ "id": r.get::<_, String>(0)?, "status": r.get::<_, String>(1)? }))
        })
        .unwrap();
    Ok(json!({ "agents": rows.map(|r| r.unwrap()).collect::<Vec<_>>() }))
}

fn op_status(conn: &Connection, agent: &str, b: &Value) -> OpResult {
    let state = need(b, "state")?;
    if !STATES.contains(&state) {
        return Result::Err(err(400, format!("bad state {state:?}; want one of {STATES:?}")));
    }
    conn.execute(
        "UPDATE agents SET status=?1 WHERE id=?2",
        params![state, agent],
    )
    .expect("status");
    Ok(json!({ "ok": true, "status": state }))
}

fn op_create_room(conn: &Connection, agent: &str, b: &Value) -> OpResult {
    let name = need(b, "name")?;
    let rtype = b.get("type").and_then(Value::as_str).unwrap_or("public");
    let existing: Option<String> = conn
        .query_row("SELECT owner FROM rooms WHERE name=?1", params![name], |r| {
            r.get(0)
        })
        .optional()
        .unwrap();
    if let Some(owner) = existing {
        if owner != agent {
            return Result::Err(err(
                409,
                format!("room {name:?} already exists (owner {owner:?})"),
            ));
        }
        return Ok(json!({ "ok": true, "room": name, "owner": owner })); // idempotent for owner
    }
    conn.execute(
        "INSERT INTO rooms(name, owner, type) VALUES(?1,?2,?3)",
        params![name, agent, rtype],
    )
    .unwrap();
    conn.execute(
        "INSERT OR IGNORE INTO members(room, agent) VALUES(?1,?2)",
        params![name, agent],
    )
    .unwrap();
    Ok(json!({ "ok": true, "room": name, "owner": agent }))
}

fn op_rooms(conn: &Connection) -> OpResult {
    let mut stmt = conn
        .prepare("SELECT name, owner, type FROM rooms ORDER BY name")
        .unwrap();
    let rooms: Vec<(String, String, String)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    let mut out = Vec::new();
    for (name, owner, rtype) in rooms {
        let mut ms = conn
            .prepare("SELECT agent FROM members WHERE room=?1 ORDER BY agent")
            .unwrap();
        let members: Vec<String> = ms
            .query_map(params![name], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        out.push(json!({ "name": name, "type": rtype, "owner": owner, "members": members }));
    }
    Ok(json!({ "rooms": out }))
}

fn op_join(conn: &Connection, agent: &str, b: &Value) -> OpResult {
    let room = need(b, "room")?;
    room_owner(conn, room)?; // 404 if missing
    conn.execute(
        "INSERT OR IGNORE INTO members(room, agent) VALUES(?1,?2)",
        params![room, agent],
    )
    .unwrap();
    Ok(json!({ "ok": true, "room": room }))
}

fn op_leave(conn: &Connection, agent: &str, b: &Value) -> OpResult {
    let room = need(b, "room")?;
    room_owner(conn, room)?;
    conn.execute(
        "DELETE FROM members WHERE room=?1 AND agent=?2",
        params![room, agent],
    )
    .unwrap();
    Ok(json!({ "ok": true, "room": room }))
}

fn op_destroy_room(conn: &Connection, agent: &str, b: &Value) -> OpResult {
    let room = need(b, "room")?;
    let owner = room_owner(conn, room)?;
    if owner != agent {
        return Result::Err(err(403, format!("not owner of {room:?} (owner {owner:?})")));
    }
    conn.execute("DELETE FROM rooms WHERE name=?1", params![room]).unwrap();
    conn.execute("DELETE FROM members WHERE room=?1", params![room]).unwrap();
    conn.execute("DELETE FROM messages WHERE room_key=?1", params![room]).unwrap();
    Ok(json!({ "ok": true, "destroyed": room }))
}

fn op_send(conn: &Connection, agent: &str, b: &Value) -> OpResult {
    let room = need(b, "room")?;
    let text = need(b, "text")?;
    room_owner(conn, room)?;
    if !is_member(conn, room, agent) {
        return Result::Err(err(403, format!("{agent:?} is not a member of {room:?}")));
    }
    let seq = append(conn, room, agent, text);
    Ok(json!({ "ok": true, "seq": seq }))
}

fn op_dm(conn: &Connection, agent: &str, b: &Value) -> OpResult {
    let to = need(b, "to")?;
    let text = need(b, "text")?;
    touch(conn, to);
    let seq = append(conn, &dm_key(agent, to), agent, text);
    Ok(json!({ "ok": true, "seq": seq }))
}

fn op_read(conn: &Connection, agent: &str, b: &Value) -> OpResult {
    let room = need(b, "room")?;
    let since = opt_i64(b, "since").unwrap_or_else(|| cursor(conn, agent, room));
    let msgs = messages_after(conn, room, since);
    if let Some(last) = msgs.last() {
        advance_cursor(conn, agent, room, last["seq"].as_i64().unwrap());
    }
    Ok(json!({ "room": room, "messages": msgs }))
}

fn op_inbox(conn: &Connection, agent: &str) -> OpResult {
    let mut out = Vec::new();
    for key in relevant_rooms(conn, agent) {
        let unread = messages_after(conn, &key, cursor(conn, agent, &key));
        if !unread.is_empty() {
            out.push(json!({ "room": key, "unread": unread.len(), "messages": unread }));
        }
    }
    Ok(json!({ "inbox": out }))
}

fn op_invite(conn: &Connection, agent: &str, b: &Value) -> OpResult {
    let room = need(b, "room")?;
    let target = need(b, "target")?;
    room_owner(conn, room)?;
    if !is_member(conn, room, agent) {
        return Result::Err(err(403, format!("{agent:?} is not a member of {room:?}")));
    }
    touch(conn, target);
    conn.execute(
        "INSERT OR IGNORE INTO members(room, agent) VALUES(?1,?2)",
        params![room, target],
    )
    .unwrap();
    Ok(json!({ "ok": true, "room": room, "invited": target }))
}

fn op_kick(conn: &Connection, agent: &str, b: &Value) -> OpResult {
    let room = need(b, "room")?;
    let target = need(b, "target")?;
    let owner = room_owner(conn, room)?;
    if owner != agent {
        return Result::Err(err(403, format!("not owner of {room:?} (owner {owner:?})")));
    }
    conn.execute(
        "DELETE FROM members WHERE room=?1 AND agent=?2",
        params![room, target],
    )
    .unwrap();
    Ok(json!({ "ok": true, "room": room, "kicked": target }))
}

/// Blocking read: return as soon as `room` has a message past the cursor, or an empty
/// list once the timeout elapses. Never holds the DB lock while parked.
fn op_wait(state: &State, agent: &str, b: &Value) -> OpResult {
    let room = need(b, "room")?.to_string();
    let secs = opt_i64(b, "timeout").filter(|t| *t > 0).unwrap_or(DEFAULT_WAIT_SECS as i64) as u64;
    let deadline = Instant::now() + Duration::from_secs(secs);
    let fixed_since = opt_i64(b, "since");

    loop {
        {
            let conn = state.db.lock().unwrap();
            touch(&conn, agent);
            let since = fixed_since.unwrap_or_else(|| cursor(&conn, agent, &room));
            let msgs = messages_after(&conn, &room, since);
            if let Some(last) = msgs.last() {
                advance_cursor(&conn, agent, &room, last["seq"].as_i64().unwrap());
                return Ok(json!({ "room": room, "messages": msgs }));
            }
        } // DB lock released before parking

        let now = Instant::now();
        if now >= deadline {
            return Ok(json!({ "room": room, "messages": [] }));
        }
        // Park until a send/dm notifies, with a 2s backstop so a missed notify costs at
        // most 2s of extra latency (never holds the DB lock, so send() can always run).
        let wait = (deadline - now).min(Duration::from_secs(2));
        let guard = state.gen.lock().unwrap();
        let _ = state.cvar.wait_timeout(guard, wait).unwrap();
    }
}

fn fail(msg: &str) -> ! {
    eprintln!("comms serve: {msg}");
    std::process::exit(1);
}

// --- self-check ------------------------------------------------------------

/// In-process integration check exercised by `comms --selfcheck` and `cargo test`.
/// Spins a real server on 127.0.0.1 and drives it through the client HTTP path.
pub fn selfcheck() -> Result<(), String> {
    use std::net::TcpStream;

    let token = "demo-token";
    let conn = Connection::open_in_memory().unwrap();
    init_schema(&conn);
    let server = Server::http(("127.0.0.1", 0)).unwrap();
    let port = server.server_addr().to_ip().unwrap().port();
    let state = Arc::new(State {
        db: Mutex::new(conn),
        token: token.into(),
        gen: Mutex::new(0),
        cvar: Condvar::new(),
    });
    {
        let state = Arc::clone(&state);
        std::thread::spawn(move || {
            for request in server.incoming_requests() {
                let state = Arc::clone(&state);
                std::thread::spawn(move || handle(&state, request));
            }
        });
    }
    let base = format!("http://127.0.0.1:{port}");

    // tiny synchronous client for the check
    let call = |action: &str, agent: &str, tok: &str, extra: Value| -> (u16, Value) {
        let mut map = extra.as_object().cloned().unwrap_or_default();
        map.insert("agent".into(), json!(agent));
        let payload = Value::Object(map).to_string();
        let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        let req = format!(
            "POST /{action} HTTP/1.1\r\nHost: {base}\r\nAuthorization: Bearer {tok}\r\n\
             Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
            payload.len()
        );
        s.write_all(req.as_bytes()).unwrap();
        let mut resp = String::new();
        s.read_to_string(&mut resp).unwrap();
        let code = resp.split_whitespace().nth(1).unwrap().parse().unwrap();
        let body = resp.splitn(2, "\r\n\r\n").nth(1).unwrap_or("");
        (code, serde_json::from_str(body).unwrap_or(Value::Null))
    };
    macro_rules! check {
        ($cond:expr, $msg:expr) => {
            if !($cond) {
                return std::result::Result::Err($msg.to_string());
            }
        };
    }

    // auth
    check!(call("rooms", "a", "wrong", json!({})).0 == 401, "bad token must 401");
    // register + room + send + read roundtrip
    check!(
        call("create-room", "alice", token, json!({"name":"r"})).1["owner"] == "alice",
        "create-room owner"
    );
    check!(call("join", "bob", token, json!({"room":"r"})).0 == 200, "join");
    check!(
        call("send", "alice", token, json!({"room":"r","text":"hi"})).1["ok"] == true,
        "send"
    );
    let (code, doc) = call("read", "bob", token, json!({"room":"r"}));
    check!(
        code == 200 && doc["messages"][0]["text"] == "hi",
        "read roundtrip"
    );
    // read advances the cursor: second read sees nothing new
    check!(
        call("read", "bob", token, json!({"room":"r"})).1["messages"]
            .as_array()
            .unwrap()
            .is_empty(),
        "cursor advance"
    );
    // inbox backlog for a fresh joiner
    check!(
        call("send", "alice", token, json!({"room":"r","text":"again"})).1["ok"] == true,
        "send2"
    );
    check!(
        call("inbox", "carol", token, json!({})).1["inbox"]
            .as_array()
            .unwrap()
            .is_empty(),
        "non-member empty inbox"
    );
    check!(call("join", "carol", token, json!({"room":"r"})).0 == 200, "carol join");
    check!(
        call("inbox", "carol", token, json!({})).1["inbox"][0]["unread"] == 2,
        "fresh joiner sees backlog"
    );
    // dm
    check!(
        call("dm", "alice", token, json!({"to":"bob","text":"psst"})).1["ok"] == true,
        "dm"
    );
    check!(
        !call("inbox", "bob", token, json!({})).1["inbox"]
            .as_array()
            .unwrap()
            .is_empty(),
        "bob sees DM"
    );
    // owner-only destroy
    check!(
        call("destroy-room", "bob", token, json!({"room":"r"})).0 == 403,
        "non-owner destroy 403"
    );
    check!(
        call("destroy-room", "alice", token, json!({"room":"r"})).1["destroyed"] == "r",
        "owner destroy"
    );
    check!(
        call("rooms", "alice", token, json!({}))
            .1["rooms"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["name"] != "r"),
        "room gone"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpStream;

    #[test]
    fn integration_selfcheck() {
        selfcheck().unwrap();
    }

    #[test]
    fn persistence_survives_reopen() {
        let path = std::env::temp_dir().join("comms_test_persist.db");
        let _ = std::fs::remove_file(&path);
        // write some state, then drop the connection
        {
            let conn = Connection::open(&path).unwrap();
            init_schema(&conn);
            op_create_room(&conn, "alice", &json!({"name":"r"})).unwrap();
            op_join(&conn, "bob", &json!({"room":"r"})).unwrap();
            op_send(&conn, "alice", &json!({"room":"r","text":"persisted"})).unwrap();
        }
        // reopen the same file: rooms, members, messages, cursors all still there
        {
            let conn = Connection::open(&path).unwrap();
            let rooms = op_rooms(&conn).unwrap();
            assert_eq!(rooms["rooms"][0]["name"], "r");
            let read = op_read(&conn, "bob", &json!({"room":"r"})).unwrap();
            assert_eq!(read["messages"][0]["text"], "persisted");
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn wait_wakes_on_send() {
        let token = "t";
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn);
        op_create_room(&conn, "alice", &json!({"name":"r"})).unwrap();
        op_join(&conn, "bob", &json!({"room":"r"})).unwrap();
        let server = Server::http(("127.0.0.1", 0)).unwrap();
        let port = server.server_addr().to_ip().unwrap().port();
        let state = Arc::new(State {
            db: Mutex::new(conn),
            token: token.into(),
            gen: Mutex::new(0),
            cvar: Condvar::new(),
        });
        {
            let state = Arc::clone(&state);
            std::thread::spawn(move || {
                for request in server.incoming_requests() {
                    let state = Arc::clone(&state);
                    std::thread::spawn(move || handle(&state, request));
                }
            });
        }
        // sender fires ~150ms after the waiter parks
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(150));
            let payload = json!({"agent":"alice","room":"r","text":"wakeup"}).to_string();
            let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
            let req = format!(
                "POST /send HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {token}\r\n\
                 Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
                payload.len()
            );
            s.write_all(req.as_bytes()).unwrap();
            let mut r = String::new();
            s.read_to_string(&mut r).ok();
        });
        // bob blocks in wait; must return the message well before the 10s timeout
        let payload = json!({"agent":"bob","room":"r","timeout":10}).to_string();
        let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        let req = format!(
            "POST /wait HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {token}\r\n\
             Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
            payload.len()
        );
        let start = Instant::now();
        s.write_all(req.as_bytes()).unwrap();
        let mut resp = String::new();
        s.read_to_string(&mut resp).unwrap();
        let body = resp.splitn(2, "\r\n\r\n").nth(1).unwrap();
        let doc: Value = serde_json::from_str(body).unwrap();
        assert_eq!(doc["messages"][0]["text"], "wakeup");
        assert!(start.elapsed() < Duration::from_secs(5), "wait should wake fast");
    }
}
