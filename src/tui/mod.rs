//! `comms tui` (alias `watch`) - a live, read-only full-screen dashboard over an
//! existing mission comms server. It is NOT a mission agent: it reuses the same
//! JSON-over-HTTP client as every other subcommand, reads globally (`agents` /
//! `rooms`) and previews each room with `peek` (non-cursor-advancing) so it never
//! eats messages real agents still need. The observer's own identity is filtered
//! out of the displayed roster.
//!
//! Module layout (the integration seam - keep these signatures stable):
//!   mod.rs   - this file: shared model (Snapshot etc), Config, Client, thread wiring.
//!   data.rs  - background poller: fills a shared Snapshot from the server; owns
//!              server-health inference and reconnect.  `data::run(cfg, shared, stop)`.
//!   view.rs  - terminal lifecycle + ratatui render + input; sets `stop` on quit.
//!              `view::run(cfg, shared, stop) -> io::Result<()>`.
//!
//! The two halves communicate ONLY through `Arc<Mutex<Snapshot>>` (data writes, view
//! reads) and an `Arc<AtomicBool>` stop flag. Neither half calls the other directly.

use serde_json::Value;
use std::env;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

mod data;
mod view;

/// Runtime config, resolved once from the environment (+ optional flags).
#[derive(Clone)]
pub struct Config {
    pub url: String,
    pub token: String,
    /// Observer identity (COMMS_AGENT). Reads only; filtered from the roster view.
    pub agent: String,
    /// Poll cadence for the background refresher.
    pub interval: Duration,
}

/// Server reachability, inferred from the transport result of the last poll.
/// There is no health endpoint: a successful request => Connected; a connect/timeout
/// failure => Disconnected(reason). An HTTP error status still means the server is up.
#[derive(Clone, Debug)]
pub enum Health {
    Connecting,
    Connected,
    Disconnected(String),
}

/// One agent as reported by `comms agents`.
#[derive(Clone, Debug)]
pub struct AgentView {
    pub id: String,
    pub status: String,
}

/// The latest message in a room (from `peek`).
#[derive(Clone, Debug)]
pub struct MsgView {
    pub from: String,
    pub text: String,
}

/// One room as reported by `comms rooms`, enriched with observed message activity.
#[derive(Clone, Debug)]
pub struct RoomView {
    pub name: String,
    pub rtype: String,
    pub owner: String,
    pub members: Vec<String>,
    /// Cumulative messages this TUI has observed in the room since it started.
    /// The view derives an "unread" highlight from its own remembered baseline.
    pub total_seen: u64,
    /// Latest message preview, if any.
    pub last: Option<MsgView>,
}

/// The full render model. `data` writes it under the mutex; `view` clones/reads it.
#[derive(Clone)]
pub struct Snapshot {
    pub health: Health,
    pub agents: Vec<AgentView>,
    pub rooms: Vec<RoomView>,
    /// Observer identity, so the view can filter it out of the roster.
    pub observer: String,
    /// When the last successful refresh completed.
    pub last_update: Option<Instant>,
    /// Number of completed poll cycles (successful or not) - a liveness heartbeat.
    pub polls: u64,
}

impl Snapshot {
    fn initial(observer: String) -> Self {
        Snapshot {
            health: Health::Connecting,
            agents: Vec::new(),
            rooms: Vec::new(),
            observer,
            last_update: None,
            polls: 0,
        }
    }
}

/// Outcome of a single client call: distinguish "server unreachable" (transport error
/// => Disconnected) from "server answered with an error status" (still Connected).
pub enum CallError {
    /// Could not reach the server at all (connect refused, timeout, malformed).
    Transport(String),
    /// Server answered but with a non-200 status.
    Status { code: u16, detail: String },
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallError::Transport(e) => write!(f, "{e}"),
            CallError::Status { code, detail } => write!(f, "{code} {detail}"),
        }
    }
}

/// Thin reusable client over the crate's hand-rolled `http_post`. One instance is
/// shared by the poller for all its reads.
pub struct Client {
    url: String,
    token: String,
    agent: String,
    timeout: Duration,
}

impl Client {
    pub fn new(cfg: &Config) -> Self {
        Client {
            url: cfg.url.clone(),
            token: cfg.token.clone(),
            agent: cfg.agent.clone(),
            timeout: Duration::from_secs(15),
        }
    }

    /// POST `action` with `fields` (the observer identity is injected as `agent`),
    /// returning the parsed JSON body on 200. A transport failure maps to
    /// `CallError::Transport` (=> the caller should mark the server Disconnected).
    pub fn call(
        &self,
        action: &str,
        mut fields: serde_json::Map<String, Value>,
    ) -> Result<Value, CallError> {
        fields.insert("agent".into(), Value::String(self.agent.clone()));
        let payload = Value::Object(fields).to_string();
        match crate::http_post(&self.url, &self.token, action, &payload, self.timeout) {
            Ok((200, body)) => Ok(serde_json::from_str(&body).unwrap_or(Value::Null)),
            Ok((code, body)) => {
                let detail = serde_json::from_str::<Value>(&body)
                    .ok()
                    .and_then(|d| d.get("error").and_then(Value::as_str).map(str::to_string))
                    .unwrap_or_else(|| "(no detail)".into());
                Err(CallError::Status { code, detail })
            }
            Err(e) => Err(CallError::Transport(e.to_string())),
        }
    }
}

/// Entry point for `comms tui` / `comms watch`.
pub fn run(args: &[String]) {
    let interval = parse_interval(args).unwrap_or(Duration::from_secs(1));

    let (url, token, agent) = match (
        env::var("COMMS_URL").ok().filter(|s| !s.is_empty()),
        env::var("COMMS_TOKEN").ok().filter(|s| !s.is_empty()),
        env::var("COMMS_AGENT").ok().filter(|s| !s.is_empty()),
    ) {
        (Some(u), Some(t), Some(a)) => (u, t, a),
        _ => {
            eprintln!("comms tui: COMMS_URL, COMMS_TOKEN and COMMS_AGENT must all be set");
            std::process::exit(1);
        }
    };
    let cfg = Config {
        url,
        token,
        agent,
        interval,
    };

    let shared = Arc::new(Mutex::new(Snapshot::initial(cfg.agent.clone())));
    let stop = Arc::new(AtomicBool::new(false));

    // Background poller fills `shared`; the view renders it on the main thread and
    // flips `stop` when the user quits.
    let poller = {
        let cfg = cfg.clone();
        let shared = Arc::clone(&shared);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || data::run(&cfg, shared, stop))
    };

    let result = view::run(&cfg, Arc::clone(&shared), Arc::clone(&stop));

    // Ensure the poller unwinds even if the view returned on its own.
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    let _ = poller.join();

    if let Err(e) = result {
        eprintln!("comms tui: {e}");
        std::process::exit(1);
    }
}

/// Optional `--interval <secs>` flag (fractional seconds allowed).
fn parse_interval(args: &[String]) -> Option<Duration> {
    let i = args.iter().position(|a| a == "--interval")?;
    let raw = args.get(i + 1)?;
    let secs: f64 = raw.parse().ok()?;
    if secs > 0.0 {
        Some(Duration::from_secs_f64(secs))
    } else {
        None
    }
}
