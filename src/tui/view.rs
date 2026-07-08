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
//! Keys: q / Esc / Ctrl-C quit; Up/k and Down/j move the room selection.
//!
//! This file only reads `Snapshot` (cloned under the mutex, rendered off-lock) and
//! writes `stop`. It never talks to the server directly - that is data.rs's job.

use super::{AgentView, Config, Health, RoomView, Snapshot};
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
}

impl ViewState {
    fn new() -> Self {
        ViewState {
            selected: 0,
            baseline: HashMap::new(),
        }
    }

    /// Called once per frame: clamps selection to the current room list and
    /// marks the currently-selected room as "seen" (its unread highlight
    /// clears because the user is looking at it right now).
    fn note_frame(&mut self, snap: &Snapshot) {
        if snap.rooms.is_empty() {
            self.selected = 0;
            return;
        }
        if self.selected >= snap.rooms.len() {
            self.selected = snap.rooms.len() - 1;
        }
        if let Some(room) = snap.rooms.get(self.selected) {
            self.baseline.insert(room.name.clone(), room.total_seen);
        }
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

pub fn run(cfg: &Config, shared: Arc<Mutex<Snapshot>>, stop: Arc<AtomicBool>) -> io::Result<()> {
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
                    KeyCode::Char('q') | KeyCode::Esc => {
                        stop.store(true, Ordering::SeqCst);
                        break;
                    }
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        stop.store(true, Ordering::SeqCst);
                        break;
                    }
                    KeyCode::Down | KeyCode::Char('j') => state.select_next(&snapshot),
                    KeyCode::Up | KeyCode::Char('k') => state.select_prev(&snapshot),
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

    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(chunks[1]);

    draw_rooms(f, body[0], snap, state);
    draw_agents(f, body[1], snap);

    draw_footer(f, chunks[2]);
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

fn draw_footer(f: &mut Frame, area: Rect) {
    let footer = Paragraph::new(Line::from(
        "q / Esc / Ctrl-C: quit    Up/k, Down/j: select room",
    ))
    .style(Style::default().fg(Color::DarkGray));
    f.render_widget(footer, area);
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
