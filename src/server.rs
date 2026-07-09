//! The `orbal-net serve` server: SQLite-backed store + threaded JSON-over-HTTP handler.
//!
//! One `rusqlite::Connection` behind a Mutex is the whole coordination store (the 1:1
//! analog of the previous single `threading.Lock`). A Condvar wakes any open `/stream`
//! connection as soon as a message/event/roster change lands, so clients react on
//! arrival instead of hot-polling. State lives in SQLite so a mission server can crash
//! and restart against the same `--db` file without losing history.

use rusqlite::{params, Connection, OptionalExtension, ToSql};
use serde_json::{json, Value};
use std::io::{self, Read, Write};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tiny_http::{Header, Response, Server};

pub const STATES: [&str; 4] = ["active", "idle", "busy", "done"];
/// SSE keepalive cadence: a `: keepalive` comment every ~20s of idle, purely to keep
/// proxies/clients from timing out the connection. Never a correctness backstop —
/// delivery is notify-driven; this timer only decides when to write a no-op comment.
/// Shortened under `cfg(test)` so `stream_emits_keepalive_on_idle` doesn't take 20s.
#[cfg(not(test))]
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(20);
#[cfg(test)]
const KEEPALIVE_INTERVAL: Duration = Duration::from_millis(200);

/// The 8-kind progress-event vocabulary (contract v1). Each kind names its one
/// required field (checked in `op_event`); everything else is optional.
const EVENT_KINDS: [&str; 8] = [
    "task-start",
    "task-done",
    "task-error",
    "task-abort",
    "step",
    "phase",
    "blocked",
    "handoff",
];

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

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
    // Bumped on every send/dm/event and every roster-affecting op; a `/stream`
    // connection parks on the Condvar between frames and re-checks the DB when woken.
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

    // Thread-per-request: an open `/stream` connection holds its own thread for as long
    // as the client stays connected, so it can't stall other requests. ponytail:
    // unbounded threads bounded by concurrent subscribers — fine at mission scale
    // (single -> low-double-digit agents); past dozens of concurrent streams, swap for
    // a poll-reactor (mio/epoll) driving many connections off one or few threads.
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
           PRIMARY KEY(agent, room_key));
         CREATE TABLE IF NOT EXISTS events(
           seq        INTEGER PRIMARY KEY AUTOINCREMENT,
           room_key   TEXT NOT NULL,
           agent      TEXT NOT NULL,
           kind       TEXT NOT NULL,
           task       TEXT,
           phase      TEXT,
           step_cur   INTEGER,
           step_total INTEGER,
           percent    INTEGER,
           target     TEXT,
           note       TEXT,
           ts         INTEGER NOT NULL);
         CREATE INDEX IF NOT EXISTS idx_events_room ON events(room_key, seq);",
    )
    .expect("init schema");

    // Migration: older databases have a `messages` table with no `ts` column.
    // Additive-only (existing rows keep ts = NULL) so a pre-existing orbal-net.db
    // opens unchanged.
    let has_ts: bool = conn
        .prepare("SELECT 1 FROM pragma_table_info('messages') WHERE name='ts'")
        .and_then(|mut s| s.exists([]))
        .unwrap_or(false);
    if !has_ts {
        conn.execute_batch("ALTER TABLE messages ADD COLUMN ts INTEGER;")
            .expect("migrate messages.ts");
    }
}

fn handle(state: &State, mut request: tiny_http::Request) {
    // Auth: constant string compare against the mission token.
    let authorized = request.headers().iter().any(|h: &Header| {
        h.field.equiv("Authorization") && h.value.as_str() == bearer(&state.token)
    });
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
        _ => {
            return reply(
                request,
                400,
                json!({ "error": "missing 'agent' (set ORBAL_NET_AGENT)" }),
            )
        }
    };

    if action == "stream" {
        return handle_stream(state, request, agent, body);
    }

    let conn = state.db.lock().unwrap();
    touch(&conn, &agent);
    let r = dispatch(&conn, &action, &agent, &body);
    drop(conn);
    // A committed message/event or roster change wakes any parked `/stream` connections.
    if r.is_ok()
        && matches!(
            action.as_str(),
            "send"
                | "dm"
                | "event"
                | "status"
                | "join"
                | "leave"
                | "create-room"
                | "destroy-room"
                | "invite"
                | "kick"
        )
    {
        *state.gen.lock().unwrap() += 1;
        state.cvar.notify_all();
    }

    match r {
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
        "read" | "peek" => op_read(conn, agent, b),
        "inbox" => op_inbox(conn, agent),
        "invite" => op_invite(conn, agent, b),
        "kick" => op_kick(conn, agent, b),
        "event" => op_event(conn, agent, b),
        "events" => op_events(conn, agent, b),
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
    conn.execute(
        "INSERT OR IGNORE INTO agents(id) VALUES(?1)",
        params![agent],
    )
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
    conn.query_row(
        "SELECT owner FROM rooms WHERE name=?1",
        params![name],
        |r| r.get::<_, String>(0),
    )
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

/// Messages in `room_key` with seq > `since`, as `[{seq, from, text, ts}]`. `ts` is
/// `null` for rows written before the ts migration.
fn messages_after(conn: &Connection, room_key: &str, since: i64) -> Vec<Value> {
    let mut stmt = conn
        .prepare(
            "SELECT seq, sender, text, ts FROM messages WHERE room_key=?1 AND seq>?2 ORDER BY seq",
        )
        .expect("prepare messages");
    let rows = stmt
        .query_map(params![room_key, since], |r| {
            Ok(json!({
                "seq": r.get::<_, i64>(0)?,
                "from": r.get::<_, String>(1)?,
                "text": r.get::<_, String>(2)?,
                "ts": r.get::<_, Option<i64>>(3)?,
            }))
        })
        .expect("query messages");
    rows.map(|r| r.expect("row")).collect()
}

/// Messages across several room keys with seq > `since`, ordered by seq, each row
/// tagged with its `room`. Used by `/stream` (a monitor with no `room` filter spans
/// every relevant room + DM channel in one query).
fn messages_after_multi(conn: &Connection, room_keys: &[String], since: i64) -> Vec<Value> {
    if room_keys.is_empty() {
        return Vec::new();
    }
    let placeholders = room_keys.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT seq, room_key, sender, text, ts FROM messages \
         WHERE room_key IN ({placeholders}) AND seq>? ORDER BY seq"
    );
    let mut stmt = conn.prepare(&sql).expect("prepare messages_multi");
    let mut p: Vec<&dyn ToSql> = room_keys.iter().map(|k| k as &dyn ToSql).collect();
    p.push(&since);
    let rows = stmt
        .query_map(p.as_slice(), |r| {
            Ok(json!({
                "seq": r.get::<_, i64>(0)?,
                "room": r.get::<_, String>(1)?,
                "from": r.get::<_, String>(2)?,
                "text": r.get::<_, String>(3)?,
                "ts": r.get::<_, Option<i64>>(4)?,
            }))
        })
        .expect("query messages_multi");
    rows.map(|r| r.expect("row")).collect()
}

/// Events across several room keys with seq > `since`, ordered by seq. Mirrors
/// `op_events`' row shape but scoped to a room set instead of one room or the globe.
fn events_after_multi(conn: &Connection, room_keys: &[String], since: i64) -> Vec<Value> {
    if room_keys.is_empty() {
        return Vec::new();
    }
    let placeholders = room_keys.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT seq, room_key, agent, kind, task, phase, step_cur, step_total, percent, target, note, ts \
         FROM events WHERE room_key IN ({placeholders}) AND seq>? ORDER BY seq"
    );
    let mut stmt = conn.prepare(&sql).expect("prepare events_multi");
    let mut p: Vec<&dyn ToSql> = room_keys.iter().map(|k| k as &dyn ToSql).collect();
    p.push(&since);
    let rows = stmt
        .query_map(p.as_slice(), |r| {
            Ok(json!({
                "seq": r.get::<_, i64>(0)?,
                "room": r.get::<_, String>(1)?,
                "agent": r.get::<_, String>(2)?,
                "kind": r.get::<_, String>(3)?,
                "task": r.get::<_, Option<String>>(4)?,
                "phase": r.get::<_, Option<String>>(5)?,
                "step_cur": r.get::<_, Option<i64>>(6)?,
                "step_total": r.get::<_, Option<i64>>(7)?,
                "percent": r.get::<_, Option<i64>>(8)?,
                "target": r.get::<_, Option<String>>(9)?,
                "note": r.get::<_, Option<String>>(10)?,
                "ts": r.get::<_, i64>(11)?,
            }))
        })
        .expect("query events_multi");
    rows.map(|r| r.expect("row")).collect()
}

/// Global max `events.seq`, used only as the default pass-through evt-side of a
/// `recv` stream's composite cursor (recv never backfills or delivers events).
fn max_event_seq_global(conn: &Connection) -> i64 {
    conn.query_row("SELECT COALESCE(MAX(seq),0) FROM events", [], |r| r.get(0))
        .unwrap_or(0)
}

/// `{agents, rooms}` snapshot sent as the monitor-mode `roster` frame.
fn roster_snapshot(conn: &Connection) -> Value {
    let agents = op_agents(conn)
        .map(|v| v["agents"].clone())
        .unwrap_or_else(|_| json!([]));
    let rooms = op_rooms(conn)
        .map(|v| v["rooms"].clone())
        .unwrap_or_else(|_| json!([]));
    json!({ "agents": agents, "rooms": rooms })
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
        "INSERT INTO messages(room_key, sender, text, ts) VALUES(?1,?2,?3,?4)",
        params![room_key, sender, text, now_millis()],
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

/// Every room name in the `rooms` table (no DM channels). The scope a monitor with no
/// `room` filter uses — matches the pre-SSE poller's `/rooms` + peek-every-room, which
/// was unscoped by membership (rooms are public; DMs were never surfaced to the TUI).
fn all_room_names(conn: &Connection) -> Vec<String> {
    let mut stmt = conn.prepare("SELECT name FROM rooms").unwrap();
    let rows = stmt.query_map([], |r| r.get::<_, String>(0)).unwrap();
    rows.map(|r| r.unwrap()).collect()
}

/// Room-key scope for a `/stream` connection: the explicit `room` if given, else every
/// existing room (global, not membership-scoped — see `all_room_names`). `agent` is
/// unused here but kept for symmetry with the per-agent `relevant_rooms` this replaced
/// (a monitor watching everything, e.g. the TUI, never joins any room itself).
fn stream_scope(conn: &Connection, _agent: &str, room: &Option<String>) -> Vec<String> {
    match room {
        Some(r) => vec![r.clone()],
        None => all_room_names(conn),
    }
}

// --- ops -------------------------------------------------------------------

fn op_whoami(conn: &Connection, agent: &str) -> OpResult {
    let status: String = conn
        .query_row(
            "SELECT status FROM agents WHERE id=?1",
            params![agent],
            |r| r.get(0),
        )
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
        return Result::Err(err(
            400,
            format!("bad state {state:?}; want one of {STATES:?}"),
        ));
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
        .query_row(
            "SELECT owner FROM rooms WHERE name=?1",
            params![name],
            |r| r.get(0),
        )
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
    conn.execute("DELETE FROM rooms WHERE name=?1", params![room])
        .unwrap();
    conn.execute("DELETE FROM members WHERE room=?1", params![room])
        .unwrap();
    conn.execute("DELETE FROM messages WHERE room_key=?1", params![room])
        .unwrap();
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
    // `peek` (non-consuming) leaves the cursor put, so a monitor/human can view a room
    // without eating messages the agent still needs delivered. Plain `read` advances.
    let peek = b.get("peek").and_then(Value::as_bool).unwrap_or(false);
    if !peek {
        if let Some(last) = msgs.last() {
            advance_cursor(conn, agent, room, last["seq"].as_i64().unwrap());
        }
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

/// The field each event kind requires (contract v1 section 3); other fields stay
/// optional. `step` accepts either step_cur/step_total or percent (or both).
fn required_field(kind: &str) -> &'static str {
    match kind {
        "task-start" | "task-done" | "task-error" | "task-abort" => "task",
        "phase" => "phase",
        "blocked" => "target",
        "handoff" => "target",
        _ => "",
    }
}

fn op_event(conn: &Connection, agent: &str, b: &Value) -> OpResult {
    let room = need(b, "room")?;
    let kind = need(b, "kind")?;
    if !EVENT_KINDS.contains(&kind) {
        return Result::Err(err(
            400,
            format!("bad kind {kind:?}; want one of {EVENT_KINDS:?}"),
        ));
    }
    room_owner(conn, room)?; // 404 if the room does not exist

    let task = b.get("task").and_then(Value::as_str);
    let phase = b.get("phase").and_then(Value::as_str);
    let target = b.get("target").and_then(Value::as_str);
    let step_cur = opt_i64(b, "step_cur");
    let step_total = opt_i64(b, "step_total");
    let percent = opt_i64(b, "percent");
    let note = b.get("note").and_then(Value::as_str);

    if kind == "step" && step_cur.is_none() && step_total.is_none() && percent.is_none() {
        return Result::Err(err(
            400,
            "kind \"step\" needs step_cur/step_total or percent",
        ));
    }
    let req = required_field(kind);
    if !req.is_empty() {
        let present = match req {
            "task" => task.is_some(),
            "phase" => phase.is_some(),
            "target" => target.is_some(),
            _ => true,
        };
        if !present {
            return Result::Err(err(400, format!("kind {kind:?} needs field {req:?}")));
        }
    }

    let ts = now_millis();
    conn.execute(
        "INSERT INTO events(room_key, agent, kind, task, phase, step_cur, step_total, percent, target, note, ts)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
        params![room, agent, kind, task, phase, step_cur, step_total, percent, target, note, ts],
    )
    .expect("insert event");
    let seq = conn.last_insert_rowid();
    Ok(json!({ "ok": true, "seq": seq, "ts": ts }))
}

/// Non-consuming read of events, always by `since` against the global `events.seq`
/// (no cursor concept for events, ever). `room` narrows to one room's thread;
/// omitted, it returns across all rooms (for the progress panel's one poll/cycle).
fn op_events(conn: &Connection, _agent: &str, b: &Value) -> OpResult {
    let since = opt_i64(b, "since").unwrap_or(0);
    let room = b.get("room").and_then(Value::as_str);

    let row_to_json = |r: &rusqlite::Row| -> rusqlite::Result<Value> {
        Ok(json!({
            "seq": r.get::<_, i64>(0)?,
            "room": r.get::<_, String>(1)?,
            "agent": r.get::<_, String>(2)?,
            "kind": r.get::<_, String>(3)?,
            "task": r.get::<_, Option<String>>(4)?,
            "phase": r.get::<_, Option<String>>(5)?,
            "step_cur": r.get::<_, Option<i64>>(6)?,
            "step_total": r.get::<_, Option<i64>>(7)?,
            "percent": r.get::<_, Option<i64>>(8)?,
            "target": r.get::<_, Option<String>>(9)?,
            "note": r.get::<_, Option<String>>(10)?,
            "ts": r.get::<_, i64>(11)?,
        }))
    };
    const COLS: &str =
        "seq, room_key, agent, kind, task, phase, step_cur, step_total, percent, target, note, ts";

    let events: Vec<Value> = if let Some(room) = room {
        let mut stmt = conn
            .prepare(&format!(
                "SELECT {COLS} FROM events WHERE room_key=?1 AND seq>?2 ORDER BY seq"
            ))
            .expect("prepare events");
        stmt.query_map(params![room, since], row_to_json)
            .expect("query events")
            .map(|r| r.expect("row"))
            .collect()
    } else {
        let mut stmt = conn
            .prepare(&format!(
                "SELECT {COLS} FROM events WHERE seq>?1 ORDER BY seq"
            ))
            .expect("prepare events");
        stmt.query_map(params![since], row_to_json)
            .expect("query events")
            .map(|r| r.expect("row"))
            .collect()
    };
    Ok(json!({ "events": events }))
}

/// Parse a composite Last-Event-ID / `since` cursor `"<msgSeq>:<evtSeq>"`.
fn parse_since(s: &str) -> Option<(i64, i64)> {
    let (a, b) = s.split_once(':')?;
    Some((a.parse().ok()?, b.parse().ok()?))
}

/// Write one HTTP/1.1 chunk (hex length + CRLF + data + CRLF) and flush immediately.
/// `/stream` writes straight to the raw connection (via `Request::into_writer`)
/// instead of going through `Response`/`raw_print`: tiny_http wraps the socket in a
/// 1KB `BufWriter` and only flushes it when the response finishes, which for an SSE
/// stream that stays open indefinitely would mean frames sit unflushed. Framing and
/// flushing by hand here is exactly what a chunked `Response` would do per call, just
/// with a flush after every chunk instead of one at the end.
fn write_chunk<W: Write>(writer: &mut W, data: &[u8]) -> io::Result<()> {
    write!(writer, "{:x}\r\n", data.len())?;
    writer.write_all(data)?;
    writer.write_all(b"\r\n")?;
    writer.flush()
}

fn write_stream_headers<W: Write>(writer: &mut W) -> io::Result<()> {
    write!(
        writer,
        "HTTP/1.1 200 OK\r\n\
         Content-Type: text/event-stream\r\n\
         Cache-Control: no-cache\r\n\
         Connection: keep-alive\r\n\
         Transfer-Encoding: chunked\r\n\
         \r\n"
    )?;
    writer.flush()
}

fn write_sse_frame<W: Write>(
    writer: &mut W,
    event: &str,
    id: &str,
    data: &Value,
) -> io::Result<()> {
    write_chunk(
        writer,
        format!("event: {event}\nid: {id}\ndata: {data}\n\n").as_bytes(),
    )
}

fn write_sse_comment<W: Write>(writer: &mut W, text: &str) -> io::Result<()> {
    write_chunk(writer, format!(": {text}\n\n").as_bytes())
}

/// `POST /stream`: a persistent SSE connection, replacing the old poll-based `wait`.
/// Holds its own thread for the connection's lifetime (see the thread-per-request note
/// in `run`). `mode: "recv"` is a single-room consuming stream (advances the agent's
/// read cursor per message, like the old `wait`/`read`, and never emits events).
/// `mode: "monitor"` is non-consuming, spans a room or every relevant room + DMs, and
/// also emits `progress` (events) and `roster` (agents/rooms) frames. Resume is via a
/// composite `"<msgSeq>:<evtSeq>"` cursor (messages.seq and events.seq are independent
/// autoincrement spaces) taken from the `Last-Event-ID` header or body `since`.
fn handle_stream(state: &State, request: tiny_http::Request, agent: String, body: Value) {
    let is_monitor = match body.get("mode").and_then(Value::as_str) {
        Some("recv") => false,
        Some("monitor") => true,
        _ => {
            return reply(
                request,
                400,
                json!({ "error": "mode must be \"recv\" or \"monitor\"" }),
            )
        }
    };
    let room: Option<String> = body.get("room").and_then(Value::as_str).map(str::to_string);
    if !is_monitor && room.is_none() {
        return reply(request, 400, json!({ "error": "recv requires 'room'" }));
    }

    let since_header = request
        .headers()
        .iter()
        .find(|h| h.field.equiv("Last-Event-ID"))
        .map(|h| h.value.as_str().to_string());
    let since_body = body
        .get("since")
        .and_then(Value::as_str)
        .map(str::to_string);
    let parsed_since = since_header.or(since_body).as_deref().and_then(parse_since);

    let (mut last_msg, mut last_evt) = {
        let conn = state.db.lock().unwrap();
        touch(&conn, &agent);
        match parsed_since {
            Some(pair) => pair,
            None if !is_monitor => (
                cursor(&conn, &agent, room.as_deref().unwrap()),
                max_event_seq_global(&conn),
            ),
            None => (0, 0),
        }
    };

    let mut writer = request.into_writer();
    if write_stream_headers(&mut writer).is_err() {
        return;
    }
    let mut last_roster: Option<Value> = None;

    // Backfill (since, max] under one DB-lock snapshot, then `: ready`, then go live.
    // Holding the lock across the whole snapshot+backfill makes it atomic: nothing can
    // land between "compute the frontier" and "read up to it", so there is no
    // notify/backfill race to reason about (a notify firing mid-backfill just means the
    // live loop's first "> last-sent" query re-finds those rows, in-order, exactly once).
    {
        let conn = state.db.lock().unwrap();
        let scope = stream_scope(&conn, &agent, &room);
        for m in messages_after_multi(&conn, &scope, last_msg) {
            last_msg = m["seq"].as_i64().unwrap();
            if write_sse_frame(
                &mut writer,
                "message",
                &format!("{last_msg}:{last_evt}"),
                &m,
            )
            .is_err()
            {
                return;
            }
            if !is_monitor {
                advance_cursor(&conn, &agent, room.as_deref().unwrap(), last_msg);
            }
        }
        if is_monitor {
            for e in events_after_multi(&conn, &scope, last_evt) {
                last_evt = e["seq"].as_i64().unwrap();
                if write_sse_frame(
                    &mut writer,
                    "progress",
                    &format!("{last_msg}:{last_evt}"),
                    &e,
                )
                .is_err()
                {
                    return;
                }
            }
            let roster = roster_snapshot(&conn);
            if write_sse_frame(
                &mut writer,
                "roster",
                &format!("{last_msg}:{last_evt}"),
                &roster,
            )
            .is_err()
            {
                return;
            }
            last_roster = Some(roster);
        }
    }
    if write_sse_comment(&mut writer, "ready").is_err() {
        return;
    }

    let mut last_activity = Instant::now();
    loop {
        // Snapshot gen BEFORE reading rows: senders commit then bump gen+notify_all
        // (see `handle`), so any row landing after this snapshot also bumps gen.
        // Checking it again right before parking (below) closes the lost-wakeup window
        // between releasing the DB lock here and taking the gen lock to park — without
        // this, a notify landing in that window wakes nobody (this thread isn't parked
        // yet) and the row would sit until the next keepalive timeout instead of being
        // delivered at notify latency.
        let gen_seen = *state.gen.lock().unwrap();
        let (msgs, evts, roster_opt) = {
            let conn = state.db.lock().unwrap();
            let scope = stream_scope(&conn, &agent, &room);
            let msgs = messages_after_multi(&conn, &scope, last_msg);
            let evts = if is_monitor {
                events_after_multi(&conn, &scope, last_evt)
            } else {
                Vec::new()
            };
            let roster = if is_monitor {
                let r = roster_snapshot(&conn);
                if last_roster.as_ref() != Some(&r) {
                    Some(r)
                } else {
                    None
                }
            } else {
                None
            };
            (msgs, evts, roster)
        };

        let mut wrote = false;
        for m in &msgs {
            last_msg = m["seq"].as_i64().unwrap();
            if write_sse_frame(&mut writer, "message", &format!("{last_msg}:{last_evt}"), m)
                .is_err()
            {
                return;
            }
            if !is_monitor {
                let conn = state.db.lock().unwrap();
                advance_cursor(&conn, &agent, room.as_deref().unwrap(), last_msg);
            }
            wrote = true;
        }
        for e in &evts {
            last_evt = e["seq"].as_i64().unwrap();
            if write_sse_frame(
                &mut writer,
                "progress",
                &format!("{last_msg}:{last_evt}"),
                e,
            )
            .is_err()
            {
                return;
            }
            wrote = true;
        }
        if let Some(r) = roster_opt {
            if write_sse_frame(&mut writer, "roster", &format!("{last_msg}:{last_evt}"), &r)
                .is_err()
            {
                return;
            }
            last_roster = Some(r);
            wrote = true;
        }

        if wrote {
            last_activity = Instant::now();
            continue;
        }

        // Test-only fault injection point: lets a test deterministically force a
        // sender's commit+notify to land exactly here, between our DB-unlock (the
        // query above found nothing) and the gen-lock/park below — the lost-wakeup
        // window `gen_seen` exists to close. A no-op outside tests.
        #[cfg(test)]
        race_hook_pause_point();

        if last_activity.elapsed() >= KEEPALIVE_INTERVAL {
            if write_sse_comment(&mut writer, "keepalive").is_err() {
                return;
            }
            last_activity = Instant::now();
        }

        // Park until notified; re-query "> last-sent" on every wake (notify or
        // keepalive timeout) rather than trusting the wake reason. Not a correctness
        // backstop (delivery is notify-driven) — purely what drives the keepalive timer.
        let remaining = KEEPALIVE_INTERVAL
            .saturating_sub(last_activity.elapsed())
            .max(Duration::from_millis(100));
        let guard = state.gen.lock().unwrap();
        if *guard != gen_seen {
            // A notify landed between our DB-unlock and this gen-lock: don't park on a
            // Condvar nobody will signal again soon, loop straight back to re-query.
            continue;
        }
        let _ = state.cvar.wait_timeout(guard, remaining).unwrap();
    }
}

/// Deterministic fault-injection rendezvous for `stream_lost_wakeup_regression`: when
/// installed, `race_hook_pause_point` blocks the live loop at the exact lost-wakeup
/// window until the test signals resume, so the test can force a competing
/// commit+notify to land inside that window on every run instead of hoping OS
/// scheduling happens to hit it. `None` (the default, and always outside tests) makes
/// the pause point a no-op.
#[cfg(test)]
struct RaceHook {
    paused_tx: std::sync::mpsc::Sender<()>,
    resume_rx: Mutex<std::sync::mpsc::Receiver<()>>,
}

#[cfg(test)]
static RACE_HOOK: Mutex<Option<Arc<RaceHook>>> = Mutex::new(None);

#[cfg(test)]
fn install_race_hook() -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
    let (paused_tx, paused_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    *RACE_HOOK.lock().unwrap() = Some(Arc::new(RaceHook {
        paused_tx,
        resume_rx: Mutex::new(resume_rx),
    }));
    (paused_rx, resume_tx)
}

#[cfg(test)]
fn clear_race_hook() {
    *RACE_HOOK.lock().unwrap() = None;
}

#[cfg(test)]
fn race_hook_pause_point() {
    let hook = RACE_HOOK.lock().unwrap().clone();
    if let Some(hook) = hook {
        hook.paused_tx.send(()).ok();
        hook.resume_rx.lock().unwrap().recv().ok();
    }
}

fn fail(msg: &str) -> ! {
    eprintln!("orbal-net serve: {msg}");
    std::process::exit(1);
}

// --- self-check ------------------------------------------------------------

/// In-process integration check exercised by `orbal-net --selfcheck` and `cargo test`.
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
        let body = resp.split_once("\r\n\r\n").map_or("", |x| x.1);
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
    check!(
        call("rooms", "a", "wrong", json!({})).0 == 401,
        "bad token must 401"
    );
    // register + room + send + read roundtrip
    check!(
        call("create-room", "alice", token, json!({"name":"r"})).1["owner"] == "alice",
        "create-room owner"
    );
    check!(
        call("join", "bob", token, json!({"room":"r"})).0 == 200,
        "join"
    );
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
    check!(
        call("join", "carol", token, json!({"room":"r"})).0 == 200,
        "carol join"
    );
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
    // events: emit each kind, non-consuming read, bad-kind/missing-field rejection
    check!(
        call(
            "event",
            "alice",
            token,
            json!({"room":"r","kind":"task-start","task":"build"})
        )
        .1["ok"]
            == true,
        "event task-start"
    );
    check!(
        call(
            "event",
            "alice",
            token,
            json!({"room":"r","kind":"step","step_cur":3,"step_total":8})
        )
        .1["ok"]
            == true,
        "event step"
    );
    check!(
        call(
            "event",
            "alice",
            token,
            json!({"room":"r","kind":"phase","phase":"compiling"})
        )
        .1["ok"]
            == true,
        "event phase"
    );
    check!(
        call(
            "event",
            "alice",
            token,
            json!({"room":"r","kind":"blocked","target":"host"})
        )
        .1["ok"]
            == true,
        "event blocked"
    );
    check!(
        call(
            "event",
            "alice",
            token,
            json!({"room":"r","kind":"task-done","task":"build"})
        )
        .1["ok"]
            == true,
        "event task-done"
    );
    check!(
        call(
            "event",
            "alice",
            token,
            json!({"room":"r","kind":"nonsense"})
        )
        .0 == 400,
        "bad kind rejected"
    );
    check!(
        call(
            "event",
            "alice",
            token,
            json!({"room":"r","kind":"task-start"})
        )
        .0 == 400,
        "missing required field rejected"
    );
    let (code, doc) = call("events", "bob", token, json!({"room":"r","since":0}));
    check!(
        code == 200 && doc["events"].as_array().unwrap().len() == 5,
        "events read sees all 5"
    );
    // events read is non-consuming: same since=0 read again sees the same 5
    check!(
        call("events", "bob", token, json!({"room":"r","since":0})).1["events"]
            .as_array()
            .unwrap()
            .len()
            == 5,
        "events read is non-consuming"
    );
    // room-omitted events read spans all rooms
    check!(
        call("events", "bob", token, json!({"since":0})).1["events"]
            .as_array()
            .unwrap()
            .len()
            >= 5,
        "events read across all rooms"
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
        call("rooms", "alice", token, json!({})).1["rooms"]
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
    fn migration_adds_ts_to_old_schema_db() {
        let path = std::env::temp_dir().join("comms_test_migrate.db");
        let _ = std::fs::remove_file(&path);
        // Build a pre-migration db by hand: the old schema, no `events` table, no
        // `ts` column on `messages`, with a real row already in it.
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE agents(id TEXT PRIMARY KEY, status TEXT NOT NULL DEFAULT 'active');
                 CREATE TABLE rooms(name TEXT PRIMARY KEY, owner TEXT NOT NULL, type TEXT NOT NULL DEFAULT 'public');
                 CREATE TABLE members(room TEXT NOT NULL, agent TEXT NOT NULL, PRIMARY KEY(room, agent));
                 CREATE TABLE messages(seq INTEGER PRIMARY KEY AUTOINCREMENT,
                   room_key TEXT NOT NULL, sender TEXT NOT NULL, text TEXT NOT NULL);
                 CREATE TABLE cursors(agent TEXT NOT NULL, room_key TEXT NOT NULL, seq INTEGER NOT NULL,
                   PRIMARY KEY(agent, room_key));
                 INSERT INTO rooms(name, owner) VALUES('r', 'alice');
                 INSERT INTO members(room, agent) VALUES('r', 'alice');
                 INSERT INTO messages(room_key, sender, text) VALUES('r', 'alice', 'before migration');",
            )
            .unwrap();
        }
        // Reopening runs init_schema, which must migrate this db in place: add
        // messages.ts (existing row -> NULL) and create the events table, without
        // touching the pre-existing row's data.
        {
            let conn = Connection::open(&path).unwrap();
            init_schema(&conn);
            let read = op_read(
                &conn,
                "bob",
                &json!({"room": "r", "since": 0, "peek": true}),
            )
            .unwrap();
            assert_eq!(read["messages"][0]["text"], "before migration");
            assert!(
                read["messages"][0]["ts"].is_null(),
                "legacy row keeps ts=null"
            );

            // events table exists and a fresh event can be written and read back.
            op_join(&conn, "bob", &json!({"room": "r"})).unwrap();
            let posted = op_event(
                &conn,
                "alice",
                &json!({"room":"r","kind":"phase","phase":"migrated"}),
            )
            .unwrap();
            assert_eq!(posted["ok"], true);
            assert!(posted["ts"].as_i64().unwrap() > 0);
            let events = op_events(&conn, "bob", &json!({"room":"r","since":0})).unwrap();
            assert_eq!(events["events"][0]["phase"], "migrated");
        }
        let _ = std::fs::remove_file(&path);
    }

    /// Minimal test-only SSE client: opens `/stream`, decodes HTTP chunked framing by
    /// hand (no dependency needed for a handful of tests), and hands back parsed
    /// `(event, id, data)` frames one at a time.
    struct SseClient {
        stream: TcpStream,
        buf: Vec<u8>,
    }

    impl SseClient {
        fn connect(port: u16, token: &str, body: Value) -> Self {
            let payload = body.to_string();
            let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let req = format!(
                "POST /stream HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {token}\r\n\
                 Content-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{payload}",
                payload.len()
            );
            stream.write_all(req.as_bytes()).unwrap();
            let mut me = SseClient {
                stream,
                buf: Vec::new(),
            };
            me.skip_http_headers();
            me
        }

        fn fill(&mut self) {
            let mut chunk = [0u8; 4096];
            let n = self.stream.read(&mut chunk).expect("stream read");
            assert!(n > 0, "stream closed unexpectedly");
            self.buf.extend_from_slice(&chunk[..n]);
        }

        fn find(&self, needle: &[u8]) -> Option<usize> {
            self.buf.windows(needle.len()).position(|w| w == needle)
        }

        fn skip_http_headers(&mut self) {
            loop {
                if let Some(pos) = self.find(b"\r\n\r\n") {
                    self.buf.drain(..pos + 4);
                    return;
                }
                self.fill();
            }
        }

        /// One de-chunked HTTP chunk body as text (an SSE frame or `: comment` line).
        fn next_chunk(&mut self) -> String {
            loop {
                if let Some(pos) = self.find(b"\r\n") {
                    let len_str = std::str::from_utf8(&self.buf[..pos]).unwrap().trim();
                    let len = usize::from_str_radix(len_str, 16).expect("chunk len");
                    let start = pos + 2;
                    let end = start + len;
                    if self.buf.len() >= end + 2 {
                        let text = String::from_utf8(self.buf[start..end].to_vec()).unwrap();
                        self.buf.drain(..end + 2);
                        return text;
                    }
                }
                self.fill();
            }
        }

        /// Read chunks until `: ready`, returning every `(event, id, data)` frame seen
        /// during backfill (comments other than `ready` are swallowed).
        fn backfill(&mut self) -> Vec<(String, String, Value)> {
            let mut out = Vec::new();
            loop {
                let text = self.next_chunk();
                if text.trim() == ": ready" {
                    return out;
                }
                if text.starts_with(':') {
                    continue; // keepalive comment
                }
                out.push(Self::parse_frame(&text));
            }
        }

        /// Read live chunks until the next real frame (skipping keepalive comments).
        fn next_frame(&mut self) -> (String, String, Value) {
            loop {
                let text = self.next_chunk();
                if text.starts_with(':') {
                    continue;
                }
                return Self::parse_frame(&text);
            }
        }

        fn parse_frame(text: &str) -> (String, String, Value) {
            let mut event = String::new();
            let mut id = String::new();
            let mut data = Value::Null;
            for line in text.lines() {
                if let Some(v) = line.strip_prefix("event: ") {
                    event = v.to_string();
                } else if let Some(v) = line.strip_prefix("id: ") {
                    id = v.to_string();
                } else if let Some(v) = line.strip_prefix("data: ") {
                    data = serde_json::from_str(v).expect("frame data json");
                }
            }
            (event, id, data)
        }
    }

    fn spawn_test_server(conn: Connection, token: &str) -> (Arc<State>, u16) {
        let server = Server::http(("127.0.0.1", 0)).unwrap();
        let port = server.server_addr().to_ip().unwrap().port();
        let state = Arc::new(State {
            db: Mutex::new(conn),
            token: token.into(),
            gen: Mutex::new(0),
            cvar: Condvar::new(),
        });
        let state2 = Arc::clone(&state);
        std::thread::spawn(move || {
            for request in server.incoming_requests() {
                let state = Arc::clone(&state2);
                std::thread::spawn(move || handle(&state, request));
            }
        });
        (state, port)
    }

    fn post(port: u16, token: &str, action: &str, agent: &str, extra: Value) -> Value {
        let mut map = extra.as_object().cloned().unwrap_or_default();
        map.insert("agent".into(), json!(agent));
        let payload = Value::Object(map).to_string();
        let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        let req = format!(
            "POST /{action} HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {token}\r\n\
             Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
            payload.len()
        );
        s.write_all(req.as_bytes()).unwrap();
        let mut resp = String::new();
        s.read_to_string(&mut resp).unwrap();
        let body = resp.split_once("\r\n\r\n").map_or("", |x| x.1);
        serde_json::from_str(body).unwrap_or(Value::Null)
    }

    /// Replaces the old `wait_wakes_on_send`: a `recv` stream must see a message
    /// pushed after it connects with no polling loop, well inside notify latency.
    #[test]
    fn stream_pushes_new_message_fast() {
        let token = "t";
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn);
        op_create_room(&conn, "alice", &json!({"name":"r"})).unwrap();
        op_join(&conn, "bob", &json!({"room":"r"})).unwrap();
        let (_state, port) = spawn_test_server(conn, token);

        let mut client =
            SseClient::connect(port, token, json!({"agent":"bob","mode":"recv","room":"r"}));
        assert!(client.backfill().is_empty(), "empty room backfills nothing");

        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(150));
            post(
                port,
                token,
                "send",
                "alice",
                json!({"room":"r","text":"wakeup"}),
            );
        });

        let start = Instant::now();
        let (event, _id, data) = client.next_frame();
        assert_eq!(event, "message");
        assert_eq!(data["text"], "wakeup");
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "stream should push fast, not wait on a poll interval"
        );
    }

    /// Reconnect test (acceptance criterion): a `recv` stream that disconnects and
    /// resumes with the last `Last-Event-ID` it saw must get exactly the messages sent
    /// while it was offline — zero missed, zero duplicated.
    #[test]
    fn stream_reconnect_no_loss_no_dup() {
        let token = "t";
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn);
        op_create_room(&conn, "alice", &json!({"name":"r"})).unwrap();
        op_join(&conn, "bob", &json!({"room":"r"})).unwrap();
        let (_state, port) = spawn_test_server(conn, token);

        // 3 messages land before bob ever connects.
        for text in ["m1", "m2", "m3"] {
            post(
                port,
                token,
                "send",
                "alice",
                json!({"room":"r","text":text}),
            );
        }

        let mut client =
            SseClient::connect(port, token, json!({"agent":"bob","mode":"recv","room":"r"}));
        let backfilled = client.backfill();
        assert_eq!(backfilled.len(), 3, "sees the full pre-connect backlog");
        assert_eq!(
            backfilled
                .iter()
                .map(|(_, _, d)| d["text"].clone())
                .collect::<Vec<_>>(),
            vec![json!("m1"), json!("m2"), json!("m3")]
        );
        let last_id = backfilled.last().unwrap().1.clone();
        drop(client); // bob "goes offline"

        // 2 more messages land while bob is disconnected.
        for text in ["m4", "m5"] {
            post(
                port,
                token,
                "send",
                "alice",
                json!({"room":"r","text":text}),
            );
        }

        // Reconnect with the last Last-Event-ID: must see exactly m4, m5 (no repeat of
        // m1-m3, nothing missing).
        let mut client = SseClient::connect(
            port,
            token,
            json!({"agent":"bob","mode":"recv","room":"r","since":last_id}),
        );
        let resumed = client.backfill();
        assert_eq!(
            resumed
                .iter()
                .map(|(_, _, d)| d["text"].clone())
                .collect::<Vec<_>>(),
            vec![json!("m4"), json!("m5")],
            "reconnect backfill must be exactly the offline window, no loss or dup"
        );

        // read/inbox stay consistent with what the stream delivered (consuming mode
        // advances the cursor exactly like the old `wait`/`read`).
        let unread = post(port, token, "read", "bob", json!({"room":"r","peek":true}));
        assert!(
            unread["messages"].as_array().unwrap().is_empty(),
            "recv's cursor advance leaves nothing unread behind"
        );
    }

    /// A `monitor` stream backfills `0:0` by default (full history) and pushes
    /// `progress` (event) and `roster` frames live, without ever advancing any read
    /// cursor (peek semantics).
    #[test]
    fn stream_monitor_sees_events_and_roster_non_consuming() {
        let token = "t";
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn);
        op_create_room(&conn, "alice", &json!({"name":"r"})).unwrap();
        op_join(&conn, "bob", &json!({"room":"r"})).unwrap();
        op_send(&conn, "alice", &json!({"room":"r","text":"hi"})).unwrap();
        let (_state, port) = spawn_test_server(conn, token);

        // "watcher" never joins "r": a monitor with no room filter must still see every
        // room globally (see stream_monitor_default_scope_is_global_not_membership).
        let mut client =
            SseClient::connect(port, token, json!({"agent":"watcher","mode":"monitor"}));
        let backfilled = client.backfill();
        assert!(
            backfilled
                .iter()
                .any(|(ev, _, d)| ev == "message" && d["text"] == "hi"),
            "monitor backfills existing messages"
        );
        assert!(
            backfilled.iter().any(|(ev, _, _)| ev == "roster"),
            "monitor gets an initial roster frame"
        );

        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(150));
            post(
                port,
                token,
                "event",
                "alice",
                json!({"room":"r","kind":"phase","phase":"building"}),
            );
        });
        let (event, _id, data) = client.next_frame();
        assert_eq!(event, "progress");
        assert_eq!(data["phase"], "building");

        // monitor never advances bob's read cursor: bob's own recv/read still sees "hi".
        let unread = post(port, token, "read", "bob", json!({"room":"r","peek":true}));
        assert_eq!(unread["messages"][0]["text"], "hi");
    }

    /// Monitor's default scope (`room` omitted) is every existing room, not the
    /// requesting agent's memberships — matching the pre-SSE TUI poller, which fetched
    /// `/rooms` unscoped and peeked every one of them. The TUI's `observer` identity
    /// never joins any room (see smoke-tui.py), so member-scoping here would silently
    /// blank the whole dashboard; global-by-default is required, not just nicer. DM
    /// channels are excluded from the default (the old poller never surfaced them
    /// either) — a DM only shows up in monitor mode via an explicit `room`.
    #[test]
    fn stream_monitor_default_scope_is_global_not_membership() {
        let token = "t";
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn);
        op_create_room(&conn, "alice", &json!({"name":"r1"})).unwrap();
        op_create_room(&conn, "carol", &json!({"name":"r2"})).unwrap();
        op_send(&conn, "alice", &json!({"room":"r1","text":"m1"})).unwrap();
        op_send(&conn, "carol", &json!({"room":"r2","text":"m2"})).unwrap();
        op_dm(&conn, "alice", &json!({"to":"carol","text":"secret"})).unwrap();
        let (_state, port) = spawn_test_server(conn, token);

        // "observer" is a member of neither r1 nor r2, and no DM channel names it.
        let mut client =
            SseClient::connect(port, token, json!({"agent":"observer","mode":"monitor"}));
        let backfilled = client.backfill();
        let texts: Vec<&str> = backfilled
            .iter()
            .filter(|(ev, _, _)| ev == "message")
            .map(|(_, _, d)| d["text"].as_str().unwrap())
            .collect();
        assert!(
            texts.contains(&"m1") && texts.contains(&"m2"),
            "non-member monitor sees every room's backlog: got {texts:?}"
        );
        assert!(
            !texts.contains(&"secret"),
            "default monitor scope excludes DM channels, matching the old poller: got {texts:?}"
        );
    }

    /// Deterministic regression for the lost-wakeup: `race_hook_pause_point` lets us
    /// force a competing commit+notify to land exactly in the DB-unlock-to-gen-lock
    /// window instead of hoping OS scheduling happens to hit it (empirically, plain
    /// timing races essentially never do — the reader's window is a handful of
    /// instructions, far smaller than any sender's round trip, so it almost always
    /// wins the race to park first even under sustained concurrent load). This test
    /// fails on the pre-fix code (message arrives only after the ~200ms cfg(test)
    /// keepalive timeout) and passes on the fix (sub-tens-of-ms, notify-driven).
    #[test]
    fn stream_lost_wakeup_regression() {
        let token = "t";
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn);
        op_create_room(&conn, "alice", &json!({"name":"r"})).unwrap();
        op_join(&conn, "bob", &json!({"room":"r"})).unwrap();
        let (_state, port) = spawn_test_server(conn, token);

        let mut client =
            SseClient::connect(port, token, json!({"agent":"bob","mode":"recv","room":"r"}));
        assert!(client.backfill().is_empty());

        let (paused_rx, resume_tx) = install_race_hook();
        // The live loop's idle-path pause point fires every iteration that finds
        // nothing new; it may fire once or twice (each retried after a ~200ms
        // keepalive) before landing here with the hook installed. Once it does, the
        // stream thread is parked at exactly the lost-wakeup window.
        paused_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("stream never reached the pause point");
        post(
            port,
            token,
            "send",
            "alice",
            json!({"room":"r","text":"raced"}),
        );
        // The send above has already committed and notify_all()'d by this point,
        // strictly before the reader (still parked at the pause point) reaches its
        // gen-lock/park call.
        resume_tx.send(()).unwrap();

        let start = Instant::now();
        let (event, _id, data) = client.next_frame();
        let elapsed = start.elapsed();
        clear_race_hook();

        assert_eq!(event, "message");
        assert_eq!(data["text"], "raced");
        assert!(
            elapsed < Duration::from_millis(50),
            "elapsed {elapsed:?}: a lost wakeup falls back to the ~200ms cfg(test) keepalive \
             timeout instead of being delivered at notify latency"
        );
    }

    /// Non-deterministic companion to the above: repeated no-delay sends under
    /// sustained load must keep every inter-frame gap well under the keepalive
    /// interval. Doesn't reliably hit the exact lost-wakeup window on its own (see
    /// `stream_lost_wakeup_regression` for why), but guards against any regression
    /// that widens the race window enough for plain scheduling jitter to hit it.
    #[test]
    fn stream_repeated_no_sleep_send_never_falls_back_to_keepalive() {
        let token = "t";
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn);
        op_create_room(&conn, "alice", &json!({"name":"r"})).unwrap();
        op_join(&conn, "bob", &json!({"room":"r"})).unwrap();
        let (_state, port) = spawn_test_server(conn, token);

        let mut client =
            SseClient::connect(port, token, json!({"agent":"bob","mode":"recv","room":"r"}));
        assert!(client.backfill().is_empty());

        // A persistent sender hammering `send` back-to-back (no per-message spawn
        // overhead, which would itself dwarf the race window) gives many chances for a
        // notify to land in the tiny DB-unlock-to-gen-lock window each time the reader
        // is about to park. A single delayed send (like `stream_pushes_new_message_fast`)
        // never lands there; this does, repeatedly, over the run.
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let done2 = Arc::clone(&done);
        let sender = std::thread::spawn(move || {
            let mut i: u64 = 0;
            while !done2.load(std::sync::atomic::Ordering::Relaxed) {
                post(
                    port,
                    token,
                    "send",
                    "alice",
                    json!({"room":"r","text": format!("s{i}")}),
                );
                i += 1;
            }
        });

        let mut max_gap = Duration::ZERO;
        let mut last = Instant::now();
        for _ in 0..300 {
            let (event, _id, _data) = client.next_frame();
            assert_eq!(event, "message");
            let gap = last.elapsed();
            max_gap = max_gap.max(gap);
            last = Instant::now();
        }
        done.store(true, std::sync::atomic::Ordering::Relaxed);
        sender.join().unwrap();

        assert!(
            max_gap < Duration::from_millis(150),
            "max inter-frame gap {max_gap:?} (KEEPALIVE_INTERVAL is 200ms under cfg(test)); \
             a lost wakeup falls back to the keepalive timeout instead of notify latency"
        );
    }

    /// Backfill/live boundary race: fire sends concurrently with the stream connecting
    /// (some may land before the backfill snapshot, some after, some right on the
    /// boundary). Every message must be delivered exactly once regardless of which
    /// side of `: ready` it fell on — never missed, never duplicated.
    #[test]
    fn stream_connect_send_race_exactly_once() {
        let token = "t";
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn);
        op_create_room(&conn, "alice", &json!({"name":"r"})).unwrap();
        op_join(&conn, "bob", &json!({"room":"r"})).unwrap();
        let (_state, port) = spawn_test_server(conn, token);

        const N: i64 = 20;
        let sender = std::thread::spawn(move || {
            for i in 0..N {
                post(
                    port,
                    token,
                    "send",
                    "alice",
                    json!({"room":"r","text": format!("race{i}")}),
                );
            }
        });

        let mut client =
            SseClient::connect(port, token, json!({"agent":"bob","mode":"recv","room":"r"}));
        let mut seen = std::collections::HashSet::new();
        for (_, _, d) in client.backfill() {
            assert!(
                seen.insert(d["seq"].as_i64().unwrap()),
                "duplicate seq in backfill: {d}"
            );
        }
        while (seen.len() as i64) < N {
            let (_, _, d) = client.next_frame();
            assert!(
                seen.insert(d["seq"].as_i64().unwrap()),
                "duplicate seq live: {d}"
            );
        }
        sender.join().unwrap();

        let mut seqs: Vec<i64> = seen.into_iter().collect();
        seqs.sort();
        assert_eq!(seqs, (1..=N).collect::<Vec<_>>(), "exactly-once, no gaps");
    }

    /// Idle connections get a `: keepalive` comment on the configured cadence (proxy
    /// liveness only — never a delivery backstop; delivery correctness is covered by
    /// the notify-driven tests above).
    #[test]
    fn stream_emits_keepalive_on_idle() {
        let token = "t";
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn);
        op_create_room(&conn, "alice", &json!({"name":"r"})).unwrap();
        op_join(&conn, "bob", &json!({"room":"r"})).unwrap();
        let (_state, port) = spawn_test_server(conn, token);

        let mut client =
            SseClient::connect(port, token, json!({"agent":"bob","mode":"recv","room":"r"}));
        assert!(client.backfill().is_empty());

        let text = client.next_chunk();
        assert_eq!(
            text.trim(),
            ": keepalive",
            "idle stream must emit keepalive"
        );
    }
}
