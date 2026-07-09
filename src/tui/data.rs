//! Background poller: fills the shared `Snapshot` from the server. Owns server-health
//! inference/reconnect and the per-room read cursor used for `peek` (never touches a
//! real agent's cursor - `since` is always sent explicitly).
//!
//! Each cycle: fetch `agents` + `rooms`, then `peek` every room for messages past its
//! locally-tracked `since`. A transport failure on either `agents` or `rooms` marks the
//! server Disconnected and freezes the displayed data at its last known-good value; a
//! non-200 status still counts as Connected (the server answered).

use super::{
    AgentView, CallError, Client, Config, EventView, FocusHandle, Health, MsgView, RoomThread,
    RoomView, Snapshot, ThreadItem,
};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Cap on the global event ring kept in `Snapshot.events`, so a long-running
/// observer's memory stays flat. Oldest events are dropped first.
const EVENT_RING_CAP: usize = 2000;

pub fn run(cfg: &Config, shared: Arc<Mutex<Snapshot>>, stop: Arc<AtomicBool>, focus: FocusHandle) {
    let client = Client::new(cfg);
    // Room name -> next `since` to request. Lives here, not in Snapshot, so a peek
    // never re-fetches (or advances any agent's real cursor for) messages already seen.
    let mut last_seq: HashMap<String, i64> = HashMap::new();
    let mut prev_agents: Vec<AgentView> = Vec::new();
    let mut prev_rooms: Vec<RoomView> = Vec::new();
    // Global event ring, oldest-first, capped at EVENT_RING_CAP.
    let mut events: Vec<EventView> = Vec::new();
    let mut events_since: i64 = 0;
    // Focused-room thread accumulation. Reset whenever `focus` changes room.
    let mut current_focus: Option<String> = None;
    let mut thread_msg_since: i64 = 0;
    let mut thread_evt_since: i64 = 0;
    let mut thread_msgs: Vec<MsgView> = Vec::new();
    let mut thread_evts: Vec<EventView> = Vec::new();

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
            (
                Health::Disconnected(reason),
                prev_agents.clone(),
                prev_rooms.clone(),
            )
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

        // Global event ring: one incremental call per cycle feeds the whole
        // progress panel. Tolerate a 404/non-200 (old server without /events) same
        // as a failed per-room peek - it must never flip Health on its own.
        if let Some(new_events) = poll_events(&client, None, events_since) {
            for e in new_events {
                events_since = events_since.max(e.seq);
                events.push(e);
            }
            if events.len() > EVENT_RING_CAP {
                let drop = events.len() - EVENT_RING_CAP;
                events.drain(0..drop);
            }
        }

        // Focused-room drill-in: the view requests a room via `focus`; this is the
        // only server-facing side of that feature; see mod.rs::FocusHandle.
        let want_focus = focus.lock().unwrap().clone();
        if want_focus != current_focus {
            current_focus = want_focus;
            thread_msg_since = 0;
            thread_evt_since = 0;
            thread_msgs.clear();
            thread_evts.clear();
        }
        let thread = current_focus.as_ref().map(|room| {
            if let Some(new_msgs) = poll_messages(&client, room, thread_msg_since) {
                for m in new_msgs {
                    thread_msg_since = thread_msg_since.max(m.0);
                    thread_msgs.push(m.1);
                }
            }
            if let Some(new_evts) = poll_events(&client, Some(room.as_str()), thread_evt_since) {
                for e in new_evts {
                    thread_evt_since = thread_evt_since.max(e.seq);
                    thread_evts.push(e);
                }
            }
            RoomThread {
                room: room.clone(),
                items: merge_thread(&thread_msgs, &thread_evts),
            }
        });

        {
            let mut snap = shared.lock().unwrap();
            let connected = matches!(health, Health::Connected);
            snap.health = health;
            snap.agents = agents;
            snap.rooms = rooms;
            snap.events = events.clone();
            snap.thread = thread;
            if connected {
                snap.last_update = Some(Instant::now());
            }
            snap.polls += 1;
        }

        sleep_responsive(cfg.interval, &stop);
    }
}

/// Non-consuming `/events` read. `room = None` fetches across all rooms (feeds the
/// global progress ring); `room = Some(_)` filters to one room (feeds a drill-in
/// thread). Returns `None` on any transport/status failure so the caller can just
/// leave its accumulator untouched - an old server without `/events` must not
/// affect Health, matching the existing per-room peek failure rule.
fn poll_events(client: &Client, room: Option<&str>, since: i64) -> Option<Vec<EventView>> {
    let mut fields = Map::new();
    if let Some(r) = room {
        fields.insert("room".into(), Value::String(r.to_string()));
    }
    fields.insert("since".into(), Value::from(since));
    let resp = client.call("events", fields).ok()?;
    let arr = resp.get("events").and_then(Value::as_array)?;
    Some(arr.iter().filter_map(parse_event).collect())
}

/// Non-consuming `read` (peek) of a room's full backlog past `since`, for the
/// drill-in thread. Returns `(seq, MsgView)` pairs so the caller can track its own
/// `since` cursor without threading seq through `MsgView` itself.
fn poll_messages(client: &Client, room: &str, since: i64) -> Option<Vec<(i64, MsgView)>> {
    let mut fields = Map::new();
    fields.insert("room".into(), Value::String(room.to_string()));
    fields.insert("since".into(), Value::from(since));
    fields.insert("peek".into(), Value::Bool(true));
    let resp = client.call("read", fields).ok()?;
    let arr = resp.get("messages").and_then(Value::as_array)?;
    Some(
        arr.iter()
            .filter_map(|m| {
                let seq = m.get("seq").and_then(Value::as_i64)?;
                Some((seq, parse_msg(m)))
            })
            .collect(),
    )
}

fn parse_msg(m: &Value) -> MsgView {
    MsgView {
        from: m
            .get("from")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        text: m
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        ts: m.get("ts").and_then(Value::as_i64),
    }
}

fn parse_event(e: &Value) -> Option<EventView> {
    Some(EventView {
        seq: e.get("seq").and_then(Value::as_i64)?,
        room: e
            .get("room")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        agent: e
            .get("agent")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        kind: e
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        task: e.get("task").and_then(Value::as_str).map(str::to_string),
        phase: e.get("phase").and_then(Value::as_str).map(str::to_string),
        step_cur: e.get("step_cur").and_then(Value::as_i64),
        step_total: e.get("step_total").and_then(Value::as_i64),
        percent: e.get("percent").and_then(Value::as_i64),
        target: e.get("target").and_then(Value::as_str).map(str::to_string),
        note: e.get("note").and_then(Value::as_str).map(str::to_string),
        ts: e.get("ts").and_then(Value::as_i64).unwrap_or(0),
    })
}

/// Merge a room's messages and events into one `ts`-ascending thread. Legacy
/// messages with `ts = None` sort oldest-first; at equal `ts` a message sorts
/// before an event (contract v1 tiebreak).
fn merge_thread(msgs: &[MsgView], evts: &[EventView]) -> Vec<ThreadItem> {
    let mut items: Vec<ThreadItem> = Vec::with_capacity(msgs.len() + evts.len());
    items.extend(msgs.iter().cloned().map(ThreadItem::Msg));
    items.extend(evts.iter().cloned().map(ThreadItem::Evt));
    items.sort_by_key(|item| match item {
        ThreadItem::Msg(m) => (m.ts.unwrap_or(i64::MIN), 0u8),
        ThreadItem::Evt(e) => (e.ts, 1u8),
    });
    items
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
        let name = r
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let rtype = r
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let owner = r
            .get("owner")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let members = r
            .get("members")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|m| m.as_str().map(str::to_string))
                    .collect()
            })
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
                        last_seq.insert(name.clone(), seq);
                        last = Some(parse_msg(m));
                    }
                }
            }
        }

        out.push(RoomView {
            name,
            rtype,
            owner,
            members,
            total_seen,
            last,
        });
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
