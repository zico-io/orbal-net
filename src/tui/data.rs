//! Background poller: fills the shared `Snapshot` from the server. Owns server-health
//! inference/reconnect and the per-room read cursor used for `peek` (never touches a
//! real agent's cursor - `since` is always sent explicitly).
//!
//! Each cycle: fetch `agents` + `rooms`, then `peek` every room for messages past its
//! locally-tracked `since`. A transport failure on either `agents` or `rooms` marks the
//! server Disconnected and freezes the displayed data at its last known-good value; a
//! non-200 status still counts as Connected (the server answered).

use super::{AgentView, CallError, Client, Config, Health, MsgView, RoomView, Snapshot};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub fn run(cfg: &Config, shared: Arc<Mutex<Snapshot>>, stop: Arc<AtomicBool>) {
    let client = Client::new(cfg);
    // Room name -> next `since` to request. Lives here, not in Snapshot, so a peek
    // never re-fetches (or advances any agent's real cursor for) messages already seen.
    let mut last_seq: HashMap<String, i64> = HashMap::new();
    let mut prev_agents: Vec<AgentView> = Vec::new();
    let mut prev_rooms: Vec<RoomView> = Vec::new();

    while !stop.load(Ordering::SeqCst) {
        let agents_res = client.call("agents", Map::new());
        let rooms_res = client.call("rooms", Map::new());

        let transport_reason = match (&agents_res, &rooms_res) {
            (Err(CallError::Transport(e)), _) => Some(e.clone()),
            (_, Err(CallError::Transport(e))) => Some(e.clone()),
            _ => None,
        };

        let (health, agents, rooms) = if let Some(reason) = transport_reason {
            // Server unreachable: keep showing the last known-good data, just flag it.
            (Health::Disconnected(reason), prev_agents.clone(), prev_rooms.clone())
        } else {
            let agents = match &agents_res {
                Ok(v) => parse_agents(v),
                Err(_) => prev_agents.clone(), // non-200 status: keep previous roster
            };
            let rooms = match &rooms_res {
                Ok(v) => peek_rooms(&client, v, &mut last_seq, &prev_rooms),
                Err(_) => prev_rooms.clone(),
            };
            (Health::Connected, agents, rooms)
        };

        prev_agents = agents.clone();
        prev_rooms = rooms.clone();

        {
            let mut snap = shared.lock().unwrap();
            let connected = matches!(health, Health::Connected);
            snap.health = health;
            snap.agents = agents;
            snap.rooms = rooms;
            if connected {
                snap.last_update = Some(Instant::now());
            }
            snap.polls += 1;
        }

        sleep_responsive(cfg.interval, &stop);
    }
}

fn parse_agents(v: &Value) -> Vec<AgentView> {
    v.get("agents")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|a| {
                    let id = a.get("id").and_then(Value::as_str)?.to_string();
                    let status = a
                        .get("status")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    Some(AgentView { id, status })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Build the room list from a `rooms` response, then `peek` each room for messages past
/// its tracked `since`, carrying `total_seen`/`last` forward from `prev_rooms` (by name)
/// so a room with no new activity this cycle keeps its prior preview instead of blanking.
fn peek_rooms(
    client: &Client,
    rooms_json: &Value,
    last_seq: &mut HashMap<String, i64>,
    prev_rooms: &[RoomView],
) -> Vec<RoomView> {
    let arr = rooms_json
        .get("rooms")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut out = Vec::with_capacity(arr.len());
    for r in arr {
        let name = r.get("name").and_then(Value::as_str).unwrap_or("").to_string();
        let rtype = r.get("type").and_then(Value::as_str).unwrap_or("").to_string();
        let owner = r.get("owner").and_then(Value::as_str).unwrap_or("").to_string();
        let members = r
            .get("members")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(|m| m.as_str().map(str::to_string)).collect())
            .unwrap_or_default();

        let prev = prev_rooms.iter().find(|p| p.name == name);
        let mut total_seen = prev.map(|p| p.total_seen).unwrap_or(0);
        let mut last = prev.and_then(|p| p.last.clone());

        let since = *last_seq.get(&name).unwrap_or(&0);
        let mut fields = Map::new();
        fields.insert("room".into(), Value::String(name.clone()));
        fields.insert("since".into(), Value::from(since));
        fields.insert("peek".into(), Value::Bool(true));

        // A peek failure here just leaves this room's total_seen/last as they were -
        // the overall Health signal comes from the agents/rooms calls, not this one.
        if let Ok(resp) = client.call("read", fields) {
            if let Some(msgs) = resp.get("messages").and_then(Value::as_array) {
                if !msgs.is_empty() {
                    total_seen += msgs.len() as u64;
                    if let Some(m) = msgs.last() {
                        let seq = m.get("seq").and_then(Value::as_i64).unwrap_or(since);
                        let from = m.get("from").and_then(Value::as_str).unwrap_or("").to_string();
                        let text = m.get("text").and_then(Value::as_str).unwrap_or("").to_string();
                        last_seq.insert(name.clone(), seq);
                        last = Some(MsgView { seq, from, text });
                    }
                }
            }
        }

        out.push(RoomView { name, rtype, owner, members, total_seen, last });
    }
    out
}

/// Sleep `interval`, but in short chunks so `stop` is noticed promptly (a quit shouldn't
/// have to wait out a multi-second poll interval).
fn sleep_responsive(interval: Duration, stop: &AtomicBool) {
    let chunk = Duration::from_millis(100);
    let mut remaining = interval;
    while remaining > Duration::ZERO {
        if stop.load(Ordering::SeqCst) {
            return;
        }
        let step = remaining.min(chunk);
        std::thread::sleep(step);
        remaining = remaining.saturating_sub(step);
    }
}
