//! SSE client-side wire support: dechunk an HTTP/1.1 "Transfer-Encoding: chunked"
//! byte stream into the plain body, then parse that body into SSE blocks (frames or
//! bare comments). Both layers are `Read`-adapters over any blocking reader (a
//! `TcpStream` in practice), so a caller just stacks `SseReader::new(ChunkedReader::new(stream))`
//! and pulls blocks off the top - each `next_event()` call blocks until a full block
//! has arrived, exactly mirroring the underlying socket's blocking behavior.
//!
//! Contract v1 (mission-orbal-net-push, wire contract seq 8): frames carry a
//! composite `id: "<msgSeq>:<evtSeq>"` that round-trips both independent autoincrement
//! spaces in one Last-Event-ID; `: ready` / `: keepalive` are content-free comment
//! lines the client must recognize without treating them as data frames.

use std::io::{self, Read};

/// One parsed SSE block: either a named frame (`event:`/`id:`/`data:` fields) or a
/// bare comment line (`: ready`, `: keepalive`) with no fields at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SseEvent {
    Frame {
        event: String,
        id: Option<String>,
        data: String,
    },
    Comment(String),
}

/// Pull-parser over a byte stream already stripped of HTTP chunk framing. Buffers
/// partial reads internally, so it is agnostic to how the underlying `Read` chooses
/// to fragment the stream (one byte at a time or the whole block at once).
pub struct SseReader<R> {
    inner: R,
    buf: Vec<u8>,
    lines: Vec<String>,
}

impl<R: Read> SseReader<R> {
    pub fn new(inner: R) -> Self {
        SseReader {
            inner,
            buf: Vec::new(),
            lines: Vec::new(),
        }
    }

    /// Block until one complete SSE block (terminated by a blank line) is available,
    /// then return it parsed. `Ok(None)` on clean EOF between blocks; a block left
    /// partial by EOF is silently dropped (the stream ended mid-frame, nothing to
    /// deliver).
    pub fn next_event(&mut self) -> io::Result<Option<SseEvent>> {
        let mut read_buf = [0u8; 4096];
        loop {
            while let Some(nl) = self.buf.iter().position(|&b| b == b'\n') {
                let mut line: Vec<u8> = self.buf.drain(..=nl).collect();
                line.pop(); // trailing '\n'
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                let line = String::from_utf8_lossy(&line).into_owned();
                if line.is_empty() {
                    if self.lines.is_empty() {
                        continue; // stray blank line between blocks
                    }
                    let block = std::mem::take(&mut self.lines);
                    return Ok(Some(parse_block(block)));
                }
                self.lines.push(line);
            }
            let n = self.inner.read(&mut read_buf)?;
            if n == 0 {
                return Ok(None);
            }
            self.buf.extend_from_slice(&read_buf[..n]);
        }
    }
}

/// Turn a block's raw (non-empty, newline-stripped) lines into a frame or comment.
/// A block with only `:`-prefixed lines is a `Comment`; any `event:`/`id:`/`data:`
/// field makes it a `Frame` (unrecognized field names are ignored, per the SSE spec).
fn parse_block(lines: Vec<String>) -> SseEvent {
    let mut event = String::from("message");
    let mut id: Option<String> = None;
    let mut data_parts: Vec<String> = Vec::new();
    let mut comment_parts: Vec<String> = Vec::new();
    let mut has_field = false;

    for line in lines {
        if let Some(rest) = line.strip_prefix(':') {
            comment_parts.push(rest.strip_prefix(' ').unwrap_or(rest).to_string());
            continue;
        }
        has_field = true;
        let (field, value) = match line.split_once(':') {
            Some((f, v)) => (f, v.strip_prefix(' ').unwrap_or(v)),
            None => (line.as_str(), ""),
        };
        match field {
            "event" => event = value.to_string(),
            "id" => id = Some(value.to_string()),
            "data" => data_parts.push(value.to_string()),
            _ => {} // retry/other fields: not used by this protocol
        }
    }

    if !has_field {
        return SseEvent::Comment(comment_parts.join("\n"));
    }
    SseEvent::Frame {
        event,
        id,
        data: data_parts.join("\n"),
    }
}

/// Decodes an HTTP/1.1 "Transfer-Encoding: chunked" byte stream into the plain body,
/// one `Read` layer under `SseReader`. The server never sends trailers of interest;
/// they're drained and discarded.
pub struct ChunkedReader<R> {
    inner: R,
    remaining: usize,
    done: bool,
}

impl<R: Read> ChunkedReader<R> {
    pub fn new(inner: R) -> Self {
        ChunkedReader {
            inner,
            remaining: 0,
            done: false,
        }
    }

    /// Read one CRLF- or LF-terminated line (CR stripped), one byte at a time. Chunk
    /// size lines and trailers are short, so per-byte reads keep this simple without
    /// a second internal buffer to reconcile against the body bytes.
    fn read_line(&mut self) -> io::Result<String> {
        let mut line = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            let n = self.inner.read(&mut byte)?;
            if n == 0 {
                break;
            }
            if byte[0] == b'\n' {
                break;
            }
            if byte[0] != b'\r' {
                line.push(byte[0]);
            }
        }
        Ok(String::from_utf8_lossy(&line).into_owned())
    }
}

impl<R: Read> Read for ChunkedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.done {
            return Ok(0);
        }
        if self.remaining == 0 {
            let size_line = self.read_line()?;
            let size_str = size_line.split(';').next().unwrap_or("").trim();
            let size = usize::from_str_radix(size_str, 16)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad chunk size"))?;
            if size == 0 {
                self.done = true;
                loop {
                    // drain trailer headers up to the terminating blank line
                    let l = self.read_line()?;
                    if l.is_empty() {
                        break;
                    }
                }
                return Ok(0);
            }
            self.remaining = size;
        }
        let want = buf.len().min(self.remaining);
        if want == 0 {
            return Ok(0);
        }
        let n = self.inner.read(&mut buf[..want])?;
        self.remaining -= n;
        if self.remaining == 0 && n > 0 {
            let mut crlf = [0u8; 2];
            self.inner.read_exact(&mut crlf).ok();
        }
        Ok(n)
    }
}

/// Build the composite Last-Event-ID / `since` cursor "<msgSeq>:<evtSeq>".
// TODO(orbal-net-push Phase B): wired into the TUI's reconnect-with-last-id next.
#[allow(dead_code)]
pub fn since_str(msg_seq: i64, evt_seq: i64) -> String {
    format!("{msg_seq}:{evt_seq}")
}

/// Split a composite cursor "<msgSeq>:<evtSeq>" back into its two independent
/// autoincrement high-water marks. `None` if malformed.
pub fn parse_since(s: &str) -> Option<(i64, i64)> {
    let (m, e) = s.split_once(':')?;
    Some((m.parse().ok()?, e.parse().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `Read` that only ever hands back up to `chunk` bytes per call, to exercise
    /// parsers against split-across-read boundaries deterministically.
    struct SlowReader {
        data: Vec<u8>,
        pos: usize,
        chunk: usize,
    }
    impl SlowReader {
        fn new(data: impl Into<Vec<u8>>, chunk: usize) -> Self {
            SlowReader {
                data: data.into(),
                pos: 0,
                chunk,
            }
        }
    }
    impl Read for SlowReader {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let want = buf.len().min(self.chunk).min(self.data.len() - self.pos);
            buf[..want].copy_from_slice(&self.data[self.pos..self.pos + want]);
            self.pos += want;
            Ok(want)
        }
    }

    #[test]
    fn parses_message_frame() {
        let raw = "event: message\nid: 5:2\ndata: {\"seq\":5}\n\n";
        let mut r = SseReader::new(SlowReader::new(raw.as_bytes().to_vec(), 4096));
        let ev = r.next_event().unwrap().unwrap();
        assert_eq!(
            ev,
            SseEvent::Frame {
                event: "message".into(),
                id: Some("5:2".into()),
                data: "{\"seq\":5}".into(),
            }
        );
    }

    #[test]
    fn parses_ready_and_keepalive_comments() {
        let raw = ": ready\n\n: keepalive\n\n";
        let mut r = SseReader::new(SlowReader::new(raw.as_bytes().to_vec(), 4096));
        assert_eq!(
            r.next_event().unwrap().unwrap(),
            SseEvent::Comment("ready".into())
        );
        assert_eq!(
            r.next_event().unwrap().unwrap(),
            SseEvent::Comment("keepalive".into())
        );
    }

    #[test]
    fn handles_split_across_read_boundaries() {
        let raw = "event: progress\nid: 3:9\ndata: {\"kind\":\"step\"}\n\n: keepalive\n\nevent: message\nid: 4:9\ndata: {\"seq\":4}\n\n";
        for chunk in [1, 2, 3, 7, 64] {
            let mut r = SseReader::new(SlowReader::new(raw.as_bytes().to_vec(), chunk));
            assert_eq!(
                r.next_event().unwrap().unwrap(),
                SseEvent::Frame {
                    event: "progress".into(),
                    id: Some("3:9".into()),
                    data: "{\"kind\":\"step\"}".into(),
                },
                "chunk size {chunk}"
            );
            assert_eq!(
                r.next_event().unwrap().unwrap(),
                SseEvent::Comment("keepalive".into()),
                "chunk size {chunk}"
            );
            assert_eq!(
                r.next_event().unwrap().unwrap(),
                SseEvent::Frame {
                    event: "message".into(),
                    id: Some("4:9".into()),
                    data: "{\"seq\":4}".into(),
                },
                "chunk size {chunk}"
            );
            assert_eq!(r.next_event().unwrap(), None, "chunk size {chunk}");
        }
    }

    #[test]
    fn multiline_data_joins_with_newline() {
        let raw = "event: roster\ndata: line1\ndata: line2\n\n";
        let mut r = SseReader::new(SlowReader::new(raw.as_bytes().to_vec(), 5));
        assert_eq!(
            r.next_event().unwrap().unwrap(),
            SseEvent::Frame {
                event: "roster".into(),
                id: None,
                data: "line1\nline2".into(),
            }
        );
    }

    #[test]
    fn since_roundtrip() {
        assert_eq!(since_str(12, 7), "12:7");
        assert_eq!(parse_since("12:7"), Some((12, 7)));
        assert_eq!(parse_since("0:0"), Some((0, 0)));
        assert_eq!(parse_since("bad"), None);
        assert_eq!(parse_since(""), None);
    }

    #[test]
    fn chunked_decode_roundtrip() {
        let body = b"event: message\nid: 1:0\ndata: {\"seq\":1}\n\n: keepalive\n\n";
        // Encode as chunked with small, uneven chunk sizes to exercise multi-chunk decode.
        let mut encoded = Vec::new();
        for piece in body.chunks(7) {
            encoded.extend_from_slice(format!("{:x}\r\n", piece.len()).as_bytes());
            encoded.extend_from_slice(piece);
            encoded.extend_from_slice(b"\r\n");
        }
        encoded.extend_from_slice(b"0\r\n\r\n");

        for read_chunk in [1, 3, 16, 4096] {
            let src = SlowReader::new(encoded.clone(), read_chunk);
            let mut dechunked = ChunkedReader::new(src);
            let mut out = Vec::new();
            dechunked.read_to_end(&mut out).unwrap();
            assert_eq!(out, body, "read_chunk {read_chunk}");
        }
    }

    #[test]
    fn chunked_then_sse_parse_end_to_end() {
        let body = b"event: message\nid: 2:0\ndata: {\"seq\":2}\n\n: ready\n\n";
        let mut encoded = Vec::new();
        for piece in body.chunks(9) {
            encoded.extend_from_slice(format!("{:x}\r\n", piece.len()).as_bytes());
            encoded.extend_from_slice(piece);
            encoded.extend_from_slice(b"\r\n");
        }
        encoded.extend_from_slice(b"0\r\n\r\n");

        let src = SlowReader::new(encoded, 5);
        let dechunked = ChunkedReader::new(src);
        let mut r = SseReader::new(dechunked);
        assert_eq!(
            r.next_event().unwrap().unwrap(),
            SseEvent::Frame {
                event: "message".into(),
                id: Some("2:0".into()),
                data: "{\"seq\":2}".into(),
            }
        );
        assert_eq!(
            r.next_event().unwrap().unwrap(),
            SseEvent::Comment("ready".into())
        );
        assert_eq!(r.next_event().unwrap(), None);
    }
}
