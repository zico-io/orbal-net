//! Regression test for the recv message-loss bug (contract v2, mission-orbal-net-push):
//! a `recv` client that closes its connection after one call must never let a
//! subsequent message land on a lingering server-side consumer thread and get
//! silently consumed-and-lost. This only reproduces across SEPARATE processes/
//! connections - an in-process test that keeps one socket open across the scenario
//! cannot see it, so this spawns the actual built `orbal-net` binary as real child
//! processes against a real `orbal-net serve`.

use serde_json::Value;
use std::io::{BufRead, BufReader, Read};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_orbal-net")
}

struct Server {
    child: Child,
    port: u16,
    db: PathBuf,
}

impl Server {
    fn start(token: &str) -> Self {
        let db = std::env::temp_dir().join(format!(
            "orbal-net-recv-cycle-test-{}-{}.db",
            std::process::id(),
            token
        ));
        let _ = std::fs::remove_file(&db);
        let mut child = Command::new(bin())
            .args(["serve", "--token", token, "--port", "0", "--db"])
            .arg(&db)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn serve");
        let stdout = child.stdout.take().expect("serve stdout");
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        reader.read_line(&mut line).expect("read port line");
        let port = serde_json::from_str::<Value>(line.trim())
            .expect("parse port json")
            .get("port")
            .and_then(Value::as_u64)
            .expect("port field") as u16;
        Server { child, port, db }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.db);
        let _ = std::fs::remove_file(format!("{}-wal", self.db.display()));
        let _ = std::fs::remove_file(format!("{}-shm", self.db.display()));
    }
}

/// One `orbal-net` client invocation as its own process, returning its parsed JSON
/// stdout. Panics on a non-zero exit (the CLI itself already prints the error).
fn cli(port: u16, token: &str, agent: &str, args: &[&str]) -> Value {
    let out = Command::new(bin())
        .args(args)
        .env("ORBAL_NET_URL", format!("http://127.0.0.1:{port}"))
        .env("ORBAL_NET_TOKEN", token)
        .env("ORBAL_NET_AGENT", agent)
        .output()
        .expect("run cli");
    assert!(
        out.status.success(),
        "cli {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap_or_else(|_| {
        panic!(
            "bad json from {args:?}: {}",
            String::from_utf8_lossy(&out.stdout)
        )
    })
}

/// A fresh `recv` process (separate from any prior call) using the agent's own
/// persisted cursor - never an explicit `--since` - so this exercises exactly the
/// "next agent instance blocks for its next message" path real agents use.
fn recv_texts(port: u16, token: &str, agent: &str, room: &str, timeout_secs: &str) -> Vec<String> {
    let v = cli(port, token, agent, &["recv", room, "--timeout", timeout_secs]);
    v["messages"]
        .as_array()
        .unwrap_or_else(|| panic!("no messages array in {v}"))
        .iter()
        .map(|m| m["text"].as_str().unwrap_or("").to_string())
        .collect()
}

fn wait_ready(port: u16, token: &str) {
    for _ in 0..50 {
        let ok = Command::new(bin())
            .args(["agents"])
            .env("ORBAL_NET_URL", format!("http://127.0.0.1:{port}"))
            .env("ORBAL_NET_TOKEN", token)
            .env("ORBAL_NET_AGENT", "x")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        if ok {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("server never became ready");
}

#[test]
fn recv_cycle_no_loss_across_separate_processes() {
    let token = "recv-cycle-tok-1";
    let srv = Server::start(token);
    wait_ready(srv.port, token);

    cli(srv.port, token, "alice", &["create-room", "r"]);
    cli(srv.port, token, "bob", &["join", "r"]);

    // Cycle 1: send then an immediate recv - the ordinary backlog path.
    cli(srv.port, token, "alice", &["send", "r", "a1"]);
    assert_eq!(recv_texts(srv.port, token, "bob", "r", "5"), vec!["a1"]);

    // Cycle 2: THE regression. Nothing is connected when a2 lands - if the prior
    // `recv` process's server-side thread lingered (v1 bug), it silently consumes
    // and loses a2 into a dead socket before this fresh `recv` ever connects.
    cli(srv.port, token, "alice", &["send", "r", "a2"]);
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        recv_texts(srv.port, token, "bob", "r", "5"),
        vec!["a2"],
        "a2 must not be lost to a lingering consumer from the prior recv"
    );

    // Cycle 3: a batch of two lands while nothing is connected.
    cli(srv.port, token, "alice", &["send", "r", "a3"]);
    cli(srv.port, token, "alice", &["send", "r", "a4"]);
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        recv_texts(srv.port, token, "bob", "r", "5"),
        vec!["a3", "a4"]
    );

    // Cycle 4: repeat several more times so a lingering consumer from ANY prior
    // cycle - not just the first - would still be caught.
    for i in 0..5 {
        let text = format!("loop-{i}");
        cli(srv.port, token, "alice", &["send", "r", &text]);
        std::thread::sleep(Duration::from_millis(150));
        assert_eq!(
            recv_texts(srv.port, token, "bob", "r", "5"),
            vec![text.clone()],
            "loop {i}"
        );
    }

    // No lingering consumer ate anything meant for a plain read either.
    let after = cli(srv.port, token, "bob", &["read", "r"]);
    assert!(
        after["messages"].as_array().unwrap().is_empty(),
        "unexpected leftover messages: {after}"
    );
}

#[test]
fn recv_timeout_with_no_message_returns_empty_and_the_server_closes() {
    let token = "recv-cycle-tok-2";
    let srv = Server::start(token);
    wait_ready(srv.port, token);
    cli(srv.port, token, "alice", &["create-room", "r"]);
    cli(srv.port, token, "bob", &["join", "r"]);

    let start = std::time::Instant::now();
    let texts = recv_texts(srv.port, token, "bob", "r", "1");
    assert!(texts.is_empty());
    // The server must close on its own once the timeout elapses - not linger. A
    // generous ceiling catches "never closes" (which would otherwise only be
    // caught by the client's own backstop, tens of seconds later).
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "recv took {:?}, expected the server to close near the 1s timeout",
        start.elapsed()
    );
}

#[test]
fn recv_follow_does_not_advance_the_cursor() {
    let token = "recv-cycle-tok-3";
    let srv = Server::start(token);
    wait_ready(srv.port, token);
    cli(srv.port, token, "alice", &["create-room", "r"]);
    cli(srv.port, token, "bob", &["join", "r"]);
    cli(srv.port, token, "alice", &["send", "r", "f1"]);

    // A short-lived --follow process: it should see f1 but, being non-consuming,
    // must NOT advance bob's read cursor.
    let mut child = Command::new(bin())
        .args(["recv", "r", "--follow"])
        .env("ORBAL_NET_URL", format!("http://127.0.0.1:{}", srv.port))
        .env("ORBAL_NET_TOKEN", token)
        .env("ORBAL_NET_AGENT", "bob")
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn follow");
    let mut out = child.stdout.take().unwrap();
    let mut buf = [0u8; 256];
    let mut seen = String::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !seen.contains("f1") && std::time::Instant::now() < deadline {
        if let Ok(n) = out.read(&mut buf) {
            if n == 0 {
                break;
            }
            seen.push_str(&String::from_utf8_lossy(&buf[..n]));
        }
    }
    let _ = child.kill();
    let _ = child.wait();
    assert!(seen.contains("f1"), "follow never saw f1: {seen:?}");

    // A normal one-shot recv must still see f1 - --follow never consumed it.
    assert_eq!(recv_texts(srv.port, token, "bob", "r", "3"), vec!["f1"]);
}
