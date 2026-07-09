//! Render + interaction layer for `comms tui` (alias `watch`). Owned by worker-ui.
//!
//! Usage (launch in a herdr pane):
//! - Inside an existing mission agent pane (COMMS_URL/COMMS_TOKEN/COMMS_AGENT
//!   already set by spawn.py), just run: `comms tui`
//! - In a dedicated observer pane that is NOT a mission agent, set the three env
//!   vars yourself (any COMMS_AGENT value works - it reads only and is filtered
//!   out of the displayed roster automatically):
//!   `COMMS_URL=http://<host>:<port> COMMS_TOKEN=<token> COMMS_AGENT=observer comms tui`
//! - `--interval <secs>` (fractional allowed) overrides the poll cadence, e.g.
//!   `comms tui --interval 0.5`
//!
//! Keys: q / Ctrl-C quit; Up/k and Down/j move the room selection (or scroll the
//! thread when drilled in); Enter drills into the selected room's live thread;
//! Esc backs out of a drilled-in room, or quits from the room list.
//!
//! This file only reads `Snapshot` (cloned under the mutex, rendered off-lock) and
//! writes `stop` + `focus`. It never talks to the server directly - that is
//! data.rs's job; `focus` is the one-way channel by which drilling into a room
//! asks data.rs to start fetching that room's full thread (see mod.rs::FocusHandle).

use super::{
    AgentView, Config, EventView, FocusHandle, Health, RoomThread, RoomView, Snapshot, ThreadItem,
};
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::Frame;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Restores the terminal on drop, so early returns (via `?`) tear down cleanly.
/// Panics are handled separately: `ratatui::try_init` installs a panic hook that
/// restores the terminal before unwinding, so a crash never leaves a garbled tty.
struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        ratatui::restore();
    }
}

/// View-local state: which room is selected/scrolled to, and the per-room
/// "seen" baseline used to derive the unread highlight. None of this lives in
/// Snapshot - it belongs to the view, not the data layer.
struct ViewState {
    selected: usize,
    baseline: HashMap<String, u64>,
    /// Room currently drilled into, if any. Mirrored into the shared `FocusHandle`
    /// so data.rs knows to fetch its thread; `None` means the room list is shown.
    focused: Option<String>,
    /// How many thread items back from the live tail the view is scrolled. 0 means
    /// "follow the tail" (new items keep the view pinned to the bottom).
    thread_backscroll: usize,
    /// Item count of the focused room's thread as of the last frame, used to clamp
    /// `thread_backscroll` without draw() needing to mutate state.
    thread_len: usize,
}

impl ViewState {
    fn new() -> Self {
        ViewState {
            selected: 0,
            baseline: HashMap::new(),
            focused: None,
            thread_backscroll: 0,
            thread_len: 0,
        }
    }

    /// Called once per frame: clamps selection to the current room list, marks the
    /// currently-selected room as "seen", and refreshes the thread length used to
    /// clamp scroll input.
    fn note_frame(&mut self, snap: &Snapshot) {
        if snap.rooms.is_empty() {
            self.selected = 0;
        } else {
            if self.selected >= snap.rooms.len() {
                self.selected = snap.rooms.len() - 1;
            }
            if let Some(room) = snap.rooms.get(self.selected) {
                self.baseline.insert(room.name.clone(), room.total_seen);
            }
        }
        self.thread_len = snap.thread.as_ref().map(|t| t.items.len()).unwrap_or(0);
    }

    /// Drill into `room`: switches the view to thread mode and asks data.rs (via
    /// `focus`) to start fetching it.
    fn enter_focus(&mut self, room: String, focus: &FocusHandle) {
        *focus.lock().unwrap() = Some(room.clone());
        self.focused = Some(room);
        self.thread_backscroll = 0;
    }

    /// Back out of the drilled-in room to the room list.
    fn exit_focus(&mut self, focus: &FocusHandle) {
        *focus.lock().unwrap() = None;
        self.focused = None;
        self.thread_backscroll = 0;
    }

    fn scroll_thread_up(&mut self) {
        self.thread_backscroll =
            (self.thread_backscroll + 1).min(self.thread_len.saturating_sub(1));
    }

    fn scroll_thread_down(&mut self) {
        self.thread_backscroll = self.thread_backscroll.saturating_sub(1);
    }

    fn select_next(&mut self, snap: &Snapshot) {
        if !snap.rooms.is_empty() {
            self.selected = (self.selected + 1) % snap.rooms.len();
        }
    }

    fn select_prev(&mut self, snap: &Snapshot) {
        if !snap.rooms.is_empty() {
            self.selected = if self.selected == 0 {
                snap.rooms.len() - 1
            } else {
                self.selected - 1
            };
        }
    }

    fn is_unread(&self, room: &RoomView) -> bool {
        room.total_seen > self.baseline.get(&room.name).copied().unwrap_or(0)
    }
}

pub fn run(
    cfg: &Config,
    shared: Arc<Mutex<Snapshot>>,
    stop: Arc<AtomicBool>,
    focus: FocusHandle,
) -> io::Result<()> {
    let mut terminal = ratatui::try_init()?;
    let _guard = TerminalGuard;

    let mut state = ViewState::new();
    let tick = Duration::from_millis(150);

    loop {
        if stop.load(Ordering::SeqCst) {
            break;
        }

        let snapshot = { shared.lock().unwrap().clone() };
        state.note_frame(&snapshot);

        terminal.draw(|f| draw(f, cfg, &snapshot, &state))?;

        if event::poll(tick)? {
            match event::read()? {
                Event::Key(key) if key.kind == KeyEventKind::Press => match key.code {
                    KeyCode::Char('q') => {
                        stop.store(true, Ordering::SeqCst);
                        break;
                    }
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        stop.store(true, Ordering::SeqCst);
                        break;
                    }
                    KeyCode::Esc => {
                        if state.focused.is_some() {
                            state.exit_focus(&focus);
                        } else {
                            stop.store(true, Ordering::SeqCst);
                            break;
                        }
                    }
                    KeyCode::Enter => {
                        if state.focused.is_none() {
                            if let Some(room) = snapshot.rooms.get(state.selected) {
                                state.enter_focus(room.name.clone(), &focus);
                            }
                        }
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        if state.focused.is_some() {
                            state.scroll_thread_down();
                        } else {
                            state.select_next(&snapshot);
                        }
                    }
                    KeyCode::Up | KeyCode::Char('k') => {
                        if state.focused.is_some() {
                            state.scroll_thread_up();
                        } else {
                            state.select_prev(&snapshot);
                        }
                    }
                    _ => {}
                },
                _ => {}
            }
        }
    }

    Ok(())
}

fn draw(f: &mut Frame, cfg: &Config, snap: &Snapshot, state: &ViewState) {
    let area = f.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4),
            Constraint::Min(3),
            Constraint::Length(1),
        ])
        .split(area);

    draw_header(f, chunks[0], cfg, snap);

    if let Some(room) = &state.focused {
        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(65), Constraint::Percentage(35)])
            .split(chunks[1]);
        draw_thread(
            f,
            body[0],
            room,
            snap.thread.as_ref(),
            state.thread_backscroll,
        );
        draw_progress(f, body[1], snap);
    } else {
        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(45),
                Constraint::Percentage(25),
                Constraint::Percentage(30),
            ])
            .split(chunks[1]);
        draw_rooms(f, body[0], snap, state);
        draw_agents(f, body[1], snap);
        draw_progress(f, body[2], snap);
    }

    draw_footer(f, chunks[2], state.focused.is_some());
}

fn draw_header(f: &mut Frame, area: Rect, cfg: &Config, snap: &Snapshot) {
    let (health_text, health_style) = match &snap.health {
        Health::Connecting => (
            "CONNECTING".to_string(),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Health::Connected => (
            "CONNECTED".to_string(),
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ),
        Health::Disconnected(reason) => (
            format!("DISCONNECTED ({reason})"),
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        ),
    };

    let last_update = match snap.last_update {
        Some(t) => format!("{:.1}s ago", t.elapsed().as_secs_f64()),
        None => "never".to_string(),
    };

    let line1 = Line::from(vec![
        Span::styled("comms tui", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw("  observer: "),
        Span::styled(cfg.agent.clone(), Style::default().fg(Color::Cyan)),
        Span::raw("  "),
        Span::styled(health_text, health_style),
    ]);
    let line2 = Line::from(vec![Span::raw(format!(
        "last update: {last_update}   polls: {}   rooms: {}   agents: {}",
        snap.polls,
        snap.rooms.len(),
        snap.agents.iter().filter(|a| a.id != snap.observer).count()
    ))]);

    let header = Paragraph::new(vec![line1, line2]).block(
        Block::default()
            .borders(Borders::ALL)
            .title("mission comms"),
    );
    f.render_widget(header, area);
}

fn draw_rooms(f: &mut Frame, area: Rect, snap: &Snapshot, state: &ViewState) {
    let inner_width = area.width.saturating_sub(4) as usize;

    let items: Vec<ListItem> = snap
        .rooms
        .iter()
        .map(|room| room_item(room, inner_width, state.is_unread(room)))
        .collect();

    let mut list_state = ListState::default();
    if !snap.rooms.is_empty() {
        list_state.select(Some(state.selected));
    }

    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title("rooms"))
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    if snap.rooms.is_empty() {
        let empty = Paragraph::new(placeholder_text(snap))
            .block(Block::default().borders(Borders::ALL).title("rooms"))
            .wrap(Wrap { trim: true });
        f.render_widget(empty, area);
    } else {
        f.render_stateful_widget(list, area, &mut list_state);
    }
}

fn room_item(room: &RoomView, width: usize, unread: bool) -> ListItem<'static> {
    let base_style = if unread {
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    };

    let header = format!(
        "{}{} [{}] owner={} members={} seen={}",
        if unread { "* " } else { "  " },
        room.name,
        room.rtype,
        room.owner,
        room.members.len(),
        room.total_seen,
    );

    let preview = match &room.last {
        Some(msg) => truncate(&format!("    {}: {}", msg.from, msg.text), width),
        None => "    (no messages yet)".to_string(),
    };

    ListItem::new(vec![
        Line::from(Span::styled(header, base_style)),
        Line::from(Span::styled(preview, Style::default().fg(Color::DarkGray))),
    ])
}

fn draw_agents(f: &mut Frame, area: Rect, snap: &Snapshot) {
    let items: Vec<ListItem> = snap
        .agents
        .iter()
        .filter(|a| a.id != snap.observer)
        .map(agent_item)
        .collect();

    if items.is_empty() {
        let empty = Paragraph::new(placeholder_text(snap))
            .block(Block::default().borders(Borders::ALL).title("agents"))
            .wrap(Wrap { trim: true });
        f.render_widget(empty, area);
        return;
    }

    let list = List::new(items).block(Block::default().borders(Borders::ALL).title("agents"));
    f.render_widget(list, area);
}

fn agent_item(agent: &AgentView) -> ListItem<'static> {
    let color = match agent.status.to_lowercase().as_str() {
        "active" => Color::Green,
        "idle" => Color::Yellow,
        "busy" => Color::Cyan,
        "done" => Color::DarkGray,
        _ => Color::White,
    };
    let text = format!("{}  [{}]", agent.id, agent.status);
    ListItem::new(Line::from(Span::styled(text, Style::default().fg(color))))
}

fn draw_footer(f: &mut Frame, area: Rect, focused: bool) {
    let text = if focused {
        "q / Ctrl-C: quit    Esc: back    Up/k, Down/j: scroll thread"
    } else {
        "q / Ctrl-C: quit    Up/k, Down/j: select room    Enter: open thread"
    };
    let footer = Paragraph::new(Line::from(text)).style(Style::default().fg(Color::DarkGray));
    f.render_widget(footer, area);
}

// --- drill-in thread ---------------------------------------------------------

fn draw_thread(
    f: &mut Frame,
    area: Rect,
    room: &str,
    thread: Option<&RoomThread>,
    backscroll: usize,
) {
    let title = format!("thread: {room}");
    let block = Block::default().borders(Borders::ALL).title(title);

    // Guard the one-frame gap after switching rooms: data.rs resets its
    // accumulator on the next poll, but the snapshot may still hold the
    // previous room's thread for a frame. Never render it under the wrong title.
    let items = match thread {
        Some(t) if t.room == room => &t.items[..],
        _ => &[],
    };
    if items.is_empty() {
        let empty = Paragraph::new("(loading thread...)")
            .block(block)
            .wrap(Wrap { trim: true });
        f.render_widget(empty, area);
        return;
    }

    // backscroll=0 follows the live tail; increasing it walks the window back
    // through older items. Scroll unit is thread items, not wrapped screen lines -
    // an approximation that keeps the math simple and predictable on resize.
    let inner_height = area.height.saturating_sub(2) as usize;
    let total = items.len();
    let end = total.saturating_sub(backscroll.min(total));
    let start = end.saturating_sub(inner_height.max(1));
    let visible = &items[start..end];

    let lines: Vec<Line> = visible.iter().map(thread_item_line).collect();
    let body = Paragraph::new(lines)
        .block(block)
        .wrap(Wrap { trim: false });
    f.render_widget(body, area);
}

fn thread_item_line(item: &ThreadItem) -> Line<'static> {
    match item {
        ThreadItem::Msg(m) => Line::from(vec![
            Span::styled(
                format!("{}  ", fmt_time(m.ts)),
                Style::default().fg(Color::DarkGray),
            ),
            Span::styled(
                format!("{}: ", m.from),
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::raw(m.text.clone()),
        ]),
        ThreadItem::Evt(e) => {
            let color = event_color(&e.kind);
            let detail = event_detail(e);
            let text = if detail.is_empty() {
                e.agent.clone()
            } else {
                format!("{}: {}", e.agent, detail)
            };
            Line::from(vec![
                Span::styled(
                    format!("{}  ", fmt_time(Some(e.ts))),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled(
                    format!("[{}] ", event_tag(e)),
                    Style::default().fg(color).add_modifier(Modifier::BOLD),
                ),
                Span::raw(text),
            ])
        }
    }
}

/// Bracketed tag for an event line, e.g. `step 3/8 37%`, `blocked->host`.
fn event_tag(e: &EventView) -> String {
    match e.kind.as_str() {
        "step" => {
            let mut parts = Vec::new();
            if let (Some(c), Some(t)) = (e.step_cur, e.step_total) {
                parts.push(format!("{c}/{t}"));
            }
            if let Some(p) = e.percent {
                parts.push(format!("{p}%"));
            }
            if parts.is_empty() {
                "step".to_string()
            } else {
                format!("step {}", parts.join(" "))
            }
        }
        "blocked" => format!("blocked->{}", e.target.as_deref().unwrap_or("?")),
        "handoff" => format!("handoff->{}", e.target.as_deref().unwrap_or("?")),
        other => other.to_string(),
    }
}

/// Freeform detail after the tag: task label, phase label, note - whichever apply.
fn event_detail(e: &EventView) -> String {
    let mut parts = Vec::new();
    if let Some(t) = &e.task {
        parts.push(t.clone());
    }
    if let Some(p) = &e.phase {
        parts.push(p.clone());
    }
    if let Some(n) = &e.note {
        parts.push(n.clone());
    }
    parts.join(" - ")
}

fn event_color(kind: &str) -> Color {
    match kind {
        "task-start" => Color::Yellow,
        "task-done" => Color::Green,
        "task-error" => Color::Red,
        "task-abort" => Color::DarkGray,
        "step" => Color::Cyan,
        "phase" => Color::Blue,
        "blocked" => Color::Red,
        "handoff" => Color::Magenta,
        _ => Color::White,
    }
}

/// Server epoch millis -> `HH:MM:SS` (UTC; no timezone dep). `None`/non-positive
/// renders as a placeholder rather than crashing (legacy rows have `ts = NULL`).
fn fmt_time(ts: Option<i64>) -> String {
    match ts {
        Some(ms) if ms > 0 => {
            let secs = ms / 1000;
            let secs_of_day = secs.rem_euclid(86_400);
            let h = secs_of_day / 3600;
            let m = (secs_of_day % 3600) / 60;
            let s = secs_of_day % 60;
            format!("{h:02}:{m:02}:{s:02}")
        }
        _ => "--:--:--".to_string(),
    }
}

// --- progress panel ------------------------------------------------------------

/// Per-agent latest-wins fold of the global event ring: task state, phase, step,
/// blocked/handoff headline. Recomputed fresh each frame from `snap.events` - the
/// ring is capped (~2000) so this stays cheap.
#[derive(Default)]
struct AgentProgress {
    task_label: Option<String>,
    task_state: Option<String>,
    phase: Option<String>,
    step_cur: Option<i64>,
    step_total: Option<i64>,
    percent: Option<i64>,
    blocked: Option<String>,
    handoff: Option<String>,
    /// Room the most recent event was posted to - an agent working across
    /// several rooms (mission + squad) can otherwise look ambiguous.
    last_room: String,
}

fn fold_progress(events: &[EventView], observer: &str) -> Vec<(String, AgentProgress)> {
    let mut map: HashMap<String, AgentProgress> = HashMap::new();
    for e in events {
        if e.agent == observer {
            continue;
        }
        let p = map.entry(e.agent.clone()).or_default();
        p.last_room = e.room.clone();
        // Blocked is a headline that reflects only the *latest* event for this
        // agent - any newer event (of any kind) clears it.
        if e.kind != "blocked" {
            p.blocked = None;
        }
        match e.kind.as_str() {
            "task-start" => {
                p.task_label = e.task.clone();
                p.task_state = Some("start".to_string());
            }
            "task-done" => {
                p.task_label = e.task.clone().or_else(|| p.task_label.clone());
                p.task_state = Some("done".to_string());
            }
            "task-error" => {
                p.task_label = e.task.clone().or_else(|| p.task_label.clone());
                p.task_state = Some("error".to_string());
            }
            "task-abort" => {
                p.task_label = e.task.clone().or_else(|| p.task_label.clone());
                p.task_state = Some("abort".to_string());
            }
            "step" => {
                p.step_cur = e.step_cur;
                p.step_total = e.step_total;
                p.percent = e.percent;
            }
            "phase" => p.phase = e.phase.clone(),
            "blocked" => p.blocked = e.target.clone(),
            "handoff" => p.handoff = e.target.clone(),
            _ => {}
        }
    }
    let mut out: Vec<(String, AgentProgress)> = map.into_iter().collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn task_badge(state: &str) -> (&'static str, Color) {
    match state {
        "start" => ("running", Color::Yellow),
        "done" => ("done", Color::Green),
        "error" => ("error", Color::Red),
        "abort" => ("abort", Color::DarkGray),
        _ => ("?", Color::White),
    }
}

fn step_text(p: &AgentProgress) -> Option<String> {
    match (p.step_cur, p.step_total, p.percent) {
        (Some(c), Some(t), Some(pct)) => Some(format!("{c}/{t}  {pct}%")),
        (Some(c), Some(t), None) => Some(format!("{c}/{t}")),
        (None, None, Some(pct)) => Some(format!("{pct}%")),
        _ => None,
    }
}

fn draw_progress(f: &mut Frame, area: Rect, snap: &Snapshot) {
    let progress = fold_progress(&snap.events, &snap.observer);
    let block = Block::default().borders(Borders::ALL).title("progress");
    if progress.is_empty() {
        let empty = Paragraph::new("(no progress events yet)")
            .block(block)
            .wrap(Wrap { trim: true });
        f.render_widget(empty, area);
        return;
    }

    let mut lines: Vec<Line> = Vec::new();
    for (agent, p) in &progress {
        lines.push(Line::from(vec![
            Span::styled(agent.clone(), Style::default().add_modifier(Modifier::BOLD)),
            Span::styled(
                format!("  ({})", p.last_room),
                Style::default().fg(Color::DarkGray),
            ),
        ]));
        if let Some(state) = &p.task_state {
            let (label, color) = task_badge(state);
            let task_text = p.task_label.clone().unwrap_or_default();
            lines.push(Line::from(vec![
                Span::raw("  "),
                Span::styled(format!("[{label}]"), Style::default().fg(color)),
                Span::raw(format!(" {task_text}")),
            ]));
        }
        if let Some(phase) = &p.phase {
            lines.push(Line::from(Span::raw(format!("  phase: {phase}"))));
        }
        if let Some(step) = step_text(p) {
            lines.push(Line::from(Span::raw(format!("  {step}"))));
        }
        if let Some(target) = &p.blocked {
            lines.push(Line::from(Span::styled(
                format!("  BLOCKED on {target}"),
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            )));
        }
        if let Some(target) = &p.handoff {
            lines.push(Line::from(Span::styled(
                format!("  -> {target}"),
                Style::default().fg(Color::Magenta),
            )));
        }
    }
    let body = Paragraph::new(lines).block(block).wrap(Wrap { trim: true });
    f.render_widget(body, area);
}

fn placeholder_text(snap: &Snapshot) -> &'static str {
    match snap.health {
        Health::Connecting => "connecting...",
        _ => "(none yet)",
    }
}

fn truncate(s: &str, max: usize) -> String {
    let max = max.max(4);
    if s.chars().count() <= max {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max - 3).collect();
    out.push_str("...");
    out
}
