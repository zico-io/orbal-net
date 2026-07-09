#!/usr/bin/env python3
"""Smoke test for `comms tui`. Self-contained: spawns its own comms server,
seeds a mission (messages + every progress-event kind), drives the TUI through a
PTY, and asserts it renders live data, drills into a room's thread and back out,
shows the progress panel, flips CONNECTED->DISCONNECTED when the server dies,
recovers when it returns, and quits cleanly. Exits 0 iff every check passes.

    python3 comms/smoke-tui.py        # builds the release binary if missing

Traps baked in so we don't thrash on this again:
  - ratatui needs a PTY *with a winsize* or it draws an empty frame -> we set it.
  - ratatui diffs frames: a word is drawn once, so scan a CUMULATIVE buffer.
  - "DISCONNECTED" contains "CONNECTED" -> match CONNECTED with a (?<!DIS) guard.
  - the master fd races the child's exit with EIO -> tolerate it everywhere.
  - liveness via os.kill(pid, 0); reap the child exactly once, at the end.
  - ratatui's terminal backend skips re-transmitting unchanged/blank cells (uses
    cursor-forward escapes for gaps), so plain byte-stripped-of-ANSI text can lose
    a space here and there -> drill-in/progress checks compare whitespace-
    normalized text, not raw substrings with spaces baked in.
"""
import os, pty, re, select, signal, struct, subprocess, sys, tempfile, termios, time, fcntl

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.path.join(HERE, "target", "release", "comms")
PORT, TOKEN = "7799", "smoke-tui-tok"
DB = os.path.join(tempfile.gettempdir(), "comms-smoke-tui.db")
ENV = dict(os.environ, COMMS_URL=f"http://127.0.0.1:{PORT}", COMMS_TOKEN=TOKEN, COMMS_AGENT="observer")


def cli(agent, *args, check=False):
    return subprocess.run([BIN, *args], env=dict(ENV, COMMS_AGENT=agent),
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=check)


def serve():
    return subprocess.Popen([BIN, "serve", "--token", TOKEN, "--port", PORT, "--db", DB],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=ENV)


def wait_ready(deadline=3.0):
    end = time.time() + deadline
    while time.time() < end:
        if cli("observer", "agents").returncode == 0:
            return True
        time.sleep(0.1)
    return False


def strip_ansi(b):
    return re.sub(r"\x1b\[[0-9;?]*[ -/]*[@-~]", "", b.decode("utf-8", "replace"))


def norm(s):
    """Collapse/strip whitespace so a lost cursor-forward-skipped space (see the
    module docstring trap) doesn't break a substring match."""
    return re.sub(r"\s+", "", s)


def alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def main():
    if not os.path.exists(BIN):
        print("building release binary...")
        subprocess.run(["cargo", "build", "--release"], cwd=HERE, check=True)

    # clean slate: no stray servers on our token, fresh db
    subprocess.run(["pkill", "-f", f"comms serve --token {TOKEN}"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(0.3)
    for p in (DB, DB + "-wal", DB + "-shm"):
        try: os.remove(p)
        except FileNotFoundError: pass

    srv = serve()
    if not wait_ready():
        print("FAIL: server never became ready (port in use?)")
        srv.terminate()
        return 1

    # seed a mission the TUI can render
    cli("lead-a", "status", "active"); cli("lead-a", "create-room", "mission-smoke")
    cli("worker-a-1", "status", "busy"); cli("worker-a-1", "join", "mission-smoke")
    cli("lead-a", "send", "mission-smoke", "worker-a-1: write smoke-ok.txt")
    cli("worker-a-1", "send", "mission-smoke", "on it, writing the file now")

    # seed every event kind (contract v1: 8 kinds) so drill-in + progress panel
    # have real data to render. task-error/task-abort sit mid-sequence so the
    # final folded state (asserted below) still lands on task-done/phase/step/
    # handoff as expected - blocked is cleared by any later event, by design.
    cli("worker-a-1", "event", "mission-smoke", "task-start", "--task", "build", "--note", "starting build")
    cli("worker-a-1", "progress", "mission-smoke", "3/8", "--note", "compiling")
    cli("worker-a-1", "event", "mission-smoke", "phase", "--phase", "implementing")
    cli("worker-a-1", "event", "mission-smoke", "blocked", "--to", "host", "--note", "waiting on E2E box")
    cli("worker-a-1", "event", "mission-smoke", "handoff", "--to", "lead-a", "--note", "over to you")
    cli("worker-a-1", "event", "mission-smoke", "task-error", "--task", "build", "--note", "compile failed")
    cli("worker-a-1", "event", "mission-smoke", "task-abort", "--task", "build", "--note", "retrying")
    cli("worker-a-1", "event", "mission-smoke", "task-done", "--task", "build", "--note", "build finished")

    pid, fd = pty.fork()
    if pid == 0:  # child
        os.environ.update(ENV)
        os.execvp(BIN, [BIN, "tui", "--interval", "0.4"])
    # parent: give ratatui a real terminal size, else it renders nothing
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", 40, 120, 0, 0))

    cum = bytearray()

    def drain(secs):
        end = time.time() + secs
        while time.time() < end:
            r, _, _ = select.select([fd], [], [], 0.1)
            if r:
                try:
                    d = os.read(fd, 65536)
                except OSError:  # EIO: child gone
                    return
                if not d:
                    return
                cum.extend(d)

    def mark(m): print(m, file=sys.stderr, flush=True)
    mark("phase: connected"); drain(1.8);  connected_alive = alive(pid)

    mark("phase: drill-in")
    pre_enter = len(cum)
    try:
        os.write(fd, b"\r")  # Enter: drill into the (only, pre-selected) room
    except OSError:
        pass
    drain(1.2)
    post_enter = len(cum)
    drilled_alive = alive(pid)
    thread_text = norm(strip_ansi(bytes(cum[pre_enter:post_enter])))

    mark("phase: back")
    try:
        os.write(fd, b"\x1b")  # Esc: back out of the thread to the room list
    except OSError:
        pass
    drain(1.0)
    post_back = len(cum)
    back_alive = alive(pid)
    back_text = norm(strip_ansi(bytes(cum[post_enter:post_back])))

    mark("phase: kill");      srv.terminate()
    drain(2.5);  killed_alive = alive(pid)
    mark("phase: restart");   srv2 = serve(); wait_ready()
    drain(2.5);  restarted_alive = alive(pid)
    mark("phase: quit")

    try:
        os.write(fd, b"q")
    except OSError:
        pass
    # keep draining while we wait: a full PTY buffer blocks the child inside
    # draw() so it never reads 'q'. Reap non-blocking; SIGKILL as a backstop.
    reaped, end = False, time.time() + 3.0
    while time.time() < end:
        r, _, _ = select.select([fd], [], [], 0.1)
        if r:
            try: os.read(fd, 65536)
            except OSError: pass
        if os.waitpid(pid, os.WNOHANG)[0] == pid:
            reaped = True
            break
    quit_clean = reaped
    if not reaped:
        try: os.kill(pid, signal.SIGKILL)
        except OSError: pass
        try: os.waitpid(pid, 0)
        except OSError: pass
    srv2.terminate()

    text = strip_ansi(cum)
    nx = norm(text)
    conn = [m.start() for m in re.finditer(r"(?<!DIS)CONNECTED", text)]  # standalone, not DISCONNECTED
    disc = [m.start() for m in re.finditer(r"DISCONNECTED", text)]

    checks = [
        ("renders mission + room",   "mission-smoke" in text),
        ("renders agent roster",     "worker-a-1" in text and "lead-a" in text),
        ("renders message preview",  "writing the file now" in text),
        ("health shows CONNECTED",   bool(conn)),
        ("stays alive while up",     connected_alive),
        ("flips to DISCONNECTED",    bool(disc) and bool(conn) and conn[0] < disc[0]),
        ("survives server loss",     killed_alive),
        ("recovers after restart",   restarted_alive and any(c > disc[0] for c in conn) if disc else False),
        ("quits cleanly on 'q'",     quit_clean),
        # drill-in: Enter opens the room thread with messages + tagged events
        ("drill-in stays alive",         drilled_alive),
        ("drill-in shows thread title",  "thread:mission-smoke" in thread_text),
        ("drill-in shows a message",     "writingthefilenow" in thread_text),
        ("drill-in tags task-start",     "[task-start]" in thread_text),
        ("drill-in tags step w/ N/M",    "[step3/8]" in thread_text),
        ("drill-in tags phase",          "[phase]" in thread_text),
        ("drill-in tags blocked->target", "blocked->host" in thread_text),
        ("drill-in tags handoff->target", "handoff->lead-a" in thread_text),
        ("drill-in tags task-done",      "[task-done]" in thread_text),
        ("drill-in shows back-nav hint", "Esc:back" in thread_text),
        # progress panel: per-agent latest-wins fold, visible in both view modes
        ("progress panel shows agent",   "worker-a-1" in nx),
        ("progress panel shows phase",   "phase:implementing" in nx),
        ("progress panel shows step",    "3/8" in nx),
        ("progress panel shows handoff", "->lead-a" in nx),
        # Esc backs out to the room list (not a quit) - alive + room-list footer back
        ("esc backs out (stays alive)",  back_alive),
        ("esc returns to room list",     "Enter:openthread" in back_text),
    ]
    ok = all(v for _, v in checks)
    for name, v in checks:
        print(f"  {'PASS' if v else 'FAIL'}  {name}")
    print(f"\ncomms tui smoke: {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
