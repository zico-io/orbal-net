//! Background stream consumer: fills the shared `Snapshot` from a single persistent
//! `POST /stream` (monitor mode, all relevant rooms + DMs) instead of polling. Owns
//! server-health inference and reconnect (with a composite `since` cursor, so a
//! reconnect resumes exactly where the last connection left off - no replay, no gap).
//!
//! Everything the view needs - agents, rooms, the global progress ring, and the
//! focused room's full thread - arrives as `message`/`progress`/`roster` frames on
//! this one connection, so unlike the old poller there is no separate network call
//! for the drill-in thread: every relevant room's messages+events are buffered
//! locally (capped per room) as they arrive, and focusing a room just serves its
//! buffer - switching focus never touches the network.

use super::{
    AgentView, Config, EventView, FocusHandle, Health, MsgView, RoomThread, RoomView, Snapshot,
    ThreadItem,
};
use crate::sse::{self, SseEvent};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Cap on the global event ring kept in `Snapshot.events`, so a long-running
/// observer's memory stays flat. Oldest events are dropped first.
const EVENT_RING_CAP: usize = 2000;
/// Cap on each room's buffered thread (messages+events merged), same idea as
/// `EVENT_RING_CAP` but per room, so drill-in has bounded memory too.
const ROOM_THREAD_CAP: usize = 500;
/// How often a stalled read is unblocked to notice `stop` and re-check the deadline.
const STOP_CHECK_INTERVAL: Duration = Duration::from_millis(300);

pub fn run(cfg: &Config, shared: Arc<Mutex<Snapshot>>, stop: Arc<AtomicBool>, focus: FocusHandle) {
    let mut agents: Vec<AgentView> = Vec::new();
    let mut rooms: Vec<RoomView> = Vec::new();
    let mut events: Vec<EventView> = Vec::new();
    let mut threads: HashMap<String, Vec<ThreadItem>> = HashMap::new();
    // Composite high-water mark, carried across reconnects so resuming never replays
    // history already delivered nor misses anything committed while disconnected.
    let mut last_msg: i64 = 0;
    let mut last_evt: i64 = 0;

    while !stop.load(Ordering::SeqCst) {
        set_health(&shared, Health::Connecting);

        let since = sse::since_str(last_msg, last_evt);
        let payload = serde_json::json!({ "agent": cfg.agent, "mode": "monitor" }).to_string();
        let (code, tcp) = match crate::open_stream(&cfg.url, &cfg.token, &payload, Some(&since)) {
            Ok(pair) => pair,
            Err(e) => {
                set_health(&shared, Health::Disconnected(e.to_string()));
                sleep_responsive(cfg.interval, &stop);
                continue;
            }
        };
        if code != 200 {
            let detail = crate::read_error_detail(tcp);
            set_health(&shared, Health::Disconnected(format!("{code} {detail}")));
            sleep_responsive(cfg.interval, &stop);
            continue;
        }
        let Ok(timeout_handle) = tcp.try_clone() else {
            set_health(
                &shared,
                Health::Disconnected("could not clone stream socket".into()),
            );
            sleep_responsive(cfg.interval, &stop);
            continue;
        };
        timeout_handle
            .set_read_timeout(Some(STOP_CHECK_INTERVAL))
            .ok();

        set_health(&shared, Health::Connected);
        let mut reader = sse::SseReader::new(sse::ChunkedReader::new(tcp));

        loop {
            if stop.load(Ordering::SeqCst) {
                return;
            }
            match reader.next_event() {
                Ok(Some(SseEvent::Frame { event, id, data })) => {
                    if let Some((m, e)) = id.as_deref().and_then(sse::parse_since) {
                        last_msg = last_msg.max(m);
                        last_evt = last_evt.max(e);
                    }
                    match event.as_str() {
                        "message" => handle_message(&data, &mut rooms, &mut threads),
                        "progress" => handle_progress(&data, &mut events, &mut threads),
                        "roster" => handle_roster(&data, &mut agents, &mut rooms),
                        _ => {}
                    }
                    publish(&shared, &focus, &agents, &rooms, &events, &threads);
                }
                Ok(Some(SseEvent::Comment(_))) => {
                    // `: ready` / `: keepalive` - no data, just a liveness beat.
                    publish(&shared, &focus, &agents, &rooms, &events, &threads);
                }
                Ok(None) => break,             // server closed the connection
                Err(e) if is_timeout(&e) => {} // periodic stop-check wake, not a failure
                Err(_) => break,               // hard read error: reconnect
            }
        }

        set_health(&shared, Health::Disconnected("stream closed".into()));
        sleep_responsive(cfg.interval, &stop);
    }
}

fn is_timeout(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    )
}

fn set_health(shared: &Mutex<Snapshot>, health: Health) {
    shared.lock().unwrap().health = health;
}

/// Push the current in-memory state into the shared `Snapshot`. Cloning agents/rooms/
/// events every frame is the same trade the old poller made every cycle - fine at
/// mission-scale message rates, and simpler than diffing.
fn publish(
    shared: &Mutex<Snapshot>,
    focus: &FocusHandle,
    agents: &[AgentView],
    rooms: &[RoomView],
    events: &[EventView],
    threads: &HashMap<String, Vec<ThreadItem>>,
) {
    let want_focus = focus.lock().unwrap().clone();
    let thread = want_focus.map(|room| {
        let items = threads.get(&room).cloned().unwrap_or_default();
        RoomThread {
            room,
            items: merge_thread_items(&items),
        }
    });

    let mut snap = shared.lock().unwrap();
    snap.health = Health::Connected;
    snap.agents = agents.to_vec();
    snap.rooms = rooms.to_vec();
    snap.events = events.to_vec();
    snap.thread = thread;
    snap.last_update = Some(Instant::now());
    snap.polls += 1;
}

/// One `message` frame: `{"seq":N,"room":"...","from":"...","text":"...","ts":<ms|null>}`.
/// DM channels (`room` starting with `@`) are filtered out - the room panel has never
/// shown DMs (the old poller only ever peeked named rooms from `rooms`, never `inbox`).
fn handle_message(
    data: &str,
    rooms: &mut Vec<RoomView>,
    threads: &mut HashMap<String, Vec<ThreadItem>>,
) {
    let Ok(v) = serde_json::from_str::<Value>(data) else {
        return;
    };
    let Some(room) = v.get("room").and_then(Value::as_str) else {
        return;
    };
    if room.starts_with('@') {
        return;
    }
    let room = room.to_string();
    let msg = parse_msg(&v);

    match rooms.iter_mut().find(|r| r.name == room) {
        Some(r) => {
            r.total_seen += 1;
            r.last = Some(msg.clone());
        }
        // A message can arrive before the first `roster` frame populates this room's
        // metadata (backfill sends messages, then events, then roster). Seed a bare
        // entry now; the next roster carries its rtype/owner/members forward onto it.
        None => rooms.push(RoomView {
            name: room.clone(),
            rtype: String::new(),
            owner: String::new(),
            members: Vec::new(),
            total_seen: 1,
            last: Some(msg.clone()),
        }),
    }
    push_thread_item(threads, &room, ThreadItem::Msg(msg));
}

/// One `progress` frame: same column shape as the old `/events` read.
fn handle_progress(
    data: &str,
    events: &mut Vec<EventView>,
    threads: &mut HashMap<String, Vec<ThreadItem>>,
) {
    let Ok(v) = serde_json::from_str::<Value>(data) else {
        return;
    };
    let Some(ev) = parse_event(&v) else {
        return;
    };
    let room = ev.room.clone();
    events.push(ev.clone());
    if events.len() > EVENT_RING_CAP {
        let drop = events.len() - EVENT_RING_CAP;
        events.drain(0..drop);
    }
    push_thread_item(threads, &room, ThreadItem::Evt(ev));
}

/// One `roster` frame: `{"agents":[{id,status}...],"rooms":[{name,type,owner,members}...]}`.
/// Rebuilds the room list from scratch (so a destroyed room disappears), carrying
/// forward `total_seen`/`last` from the current list by name, same merge the old
/// poller did across polling cycles.
fn handle_roster(data: &str, agents: &mut Vec<AgentView>, rooms: &mut Vec<RoomView>) {
    let Ok(v) = serde_json::from_str::<Value>(data) else {
        return;
    };
    *agents = parse_agents(&v);

    let arr = v
        .get("rooms")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut merged = Vec::with_capacity(arr.len());
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

        let prev = rooms.iter().find(|p| p.name == name);
        let total_seen = prev.map(|p| p.total_seen).unwrap_or(0);
        let last = prev.and_then(|p| p.last.clone());
        merged.push(RoomView {
            name,
            rtype,
            owner,
            members,
            total_seen,
            last,
        });
    }
    *rooms = merged;
}

fn push_thread_item(threads: &mut HashMap<String, Vec<ThreadItem>>, room: &str, item: ThreadItem) {
    let buf = threads.entry(room.to_string()).or_default();
    buf.push(item);
    if buf.len() > ROOM_THREAD_CAP {
        let drop = buf.len() - ROOM_THREAD_CAP;
        buf.drain(0..drop);
    }
}

/// Sort a room's buffered messages+events into one `ts`-ascending thread. Legacy
/// messages with `ts = None` sort oldest-first; at equal `ts` a message sorts before
/// an event (contract v1 tiebreak) - needed because the two frame kinds arrive on
/// independent seq spaces and aren't guaranteed interleaved in `ts` order.
fn merge_thread_items(items: &[ThreadItem]) -> Vec<ThreadItem> {
    let mut v = items.to_vec();
    v.sort_by_key(|item| match item {
        ThreadItem::Msg(m) => (m.ts.unwrap_or(i64::MIN), 0u8),
        ThreadItem::Evt(e) => (e.ts, 1u8),
    });
    v
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
    e.get("seq").and_then(Value::as_i64)?; // sanity-check: a well-formed row has one
    Some(EventView {
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

/// Sleep `interval` (repurposed as the reconnect backoff - there is no poll cadence
/// anymore), but in short chunks so `stop` is noticed promptly.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn msg_data(room: &str, from: &str, text: &str, ts: i64) -> String {
        serde_json::json!({"seq": 1, "room": room, "from": from, "text": text, "ts": ts})
            .to_string()
    }

    #[test]
    fn handle_message_seeds_and_updates_room() {
        let mut rooms = Vec::new();
        let mut threads = HashMap::new();
        handle_message(&msg_data("r", "alice", "hi", 100), &mut rooms, &mut threads);
        assert_eq!(rooms.len(), 1);
        assert_eq!(rooms[0].total_seen, 1);
        assert_eq!(rooms[0].last.as_ref().unwrap().text, "hi");

        handle_message(&msg_data("r", "bob", "yo", 200), &mut rooms, &mut threads);
        assert_eq!(rooms.len(), 1, "same room, no duplicate entry");
        assert_eq!(rooms[0].total_seen, 2);
        assert_eq!(rooms[0].last.as_ref().unwrap().text, "yo");
        assert_eq!(threads.get("r").unwrap().len(), 2);
    }

    #[test]
    fn handle_message_filters_dm_channels() {
        let mut rooms = Vec::new();
        let mut threads = HashMap::new();
        handle_message(
            &msg_data("@alice|bob", "alice", "psst", 100),
            &mut rooms,
            &mut threads,
        );
        assert!(rooms.is_empty());
        assert!(threads.is_empty());
    }

    #[test]
    fn handle_roster_carries_forward_preview_and_drops_destroyed_rooms() {
        let mut rooms = Vec::new();
        let mut threads = HashMap::new();
        handle_message(&msg_data("r", "alice", "hi", 100), &mut rooms, &mut threads);

        let mut agents = Vec::new();
        let roster = serde_json::json!({
            "agents": [{"id": "alice", "status": "active"}],
            "rooms": [{"name": "r", "type": "public", "owner": "alice", "members": ["alice"]}],
        })
        .to_string();
        handle_roster(&roster, &mut agents, &mut rooms);

        assert_eq!(agents.len(), 1);
        assert_eq!(rooms.len(), 1);
        assert_eq!(rooms[0].rtype, "public");
        assert_eq!(rooms[0].owner, "alice");
        assert_eq!(rooms[0].total_seen, 1, "preview carried forward");
        assert_eq!(rooms[0].last.as_ref().unwrap().text, "hi");

        // A second roster that omits "r" (destroyed) must drop it, even though its
        // preview data is still sitting in the pre-roster state.
        let roster2 = serde_json::json!({"agents": [], "rooms": []}).to_string();
        handle_roster(&roster2, &mut agents, &mut rooms);
        assert!(rooms.is_empty());
    }

    #[test]
    fn handle_progress_feeds_global_ring_and_room_thread() {
        let mut events = Vec::new();
        let mut threads = HashMap::new();
        let data = serde_json::json!({
            "seq": 1, "room": "r", "agent": "alice", "kind": "phase",
            "phase": "building", "ts": 500,
        })
        .to_string();
        handle_progress(&data, &mut events, &mut threads);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].phase.as_deref(), Some("building"));
        assert_eq!(threads.get("r").unwrap().len(), 1);
    }

    #[test]
    fn event_ring_and_room_thread_are_capped() {
        let mut events = Vec::new();
        let mut threads = HashMap::new();
        for i in 0..(EVENT_RING_CAP + 10) {
            let data = serde_json::json!({
                "seq": i, "room": "r", "agent": "a", "kind": "step",
                "step_cur": i, "step_total": 1, "ts": i as i64,
            })
            .to_string();
            handle_progress(&data, &mut events, &mut threads);
        }
        assert_eq!(events.len(), EVENT_RING_CAP);
        assert_eq!(events[0].step_cur, Some(10), "oldest 10 dropped");
        assert_eq!(threads.get("r").unwrap().len(), ROOM_THREAD_CAP);
    }

    #[test]
    fn merge_thread_items_sorts_by_ts_with_message_before_event_tiebreak() {
        let items = vec![
            ThreadItem::Evt(EventView {
                room: "r".into(),
                agent: "a".into(),
                kind: "phase".into(),
                task: None,
                phase: Some("p".into()),
                step_cur: None,
                step_total: None,
                percent: None,
                target: None,
                note: None,
                ts: 100,
            }),
            ThreadItem::Msg(MsgView {
                from: "alice".into(),
                text: "hi".into(),
                ts: Some(100),
            }),
            ThreadItem::Msg(MsgView {
                from: "bob".into(),
                text: "legacy".into(),
                ts: None,
            }),
        ];
        let sorted = merge_thread_items(&items);
        assert!(matches!(&sorted[0], ThreadItem::Msg(m) if m.text == "legacy"));
        assert!(matches!(&sorted[1], ThreadItem::Msg(m) if m.text == "hi"));
        assert!(matches!(&sorted[2], ThreadItem::Evt(_)));
    }
}
