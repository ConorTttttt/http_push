//! Minimal incremental Server-Sent Events parser (WHATWG HTML §9.2.6).

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SseEvent {
    pub event: String,
    pub data: String,
    pub id: Option<String>,
}

#[derive(Debug, Default)]
pub struct SseParser {
    buf: Vec<u8>,
    event: String,
    data: String,
    id: Option<String>,
    has_data: bool,
}

/// Refuse lines longer than this (protects against a malicious relay).
const MAX_LINE: usize = 1 << 20;

impl SseParser {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed bytes; returns completed events.
    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<SseEvent>, &'static str> {
        self.buf.extend_from_slice(chunk);
        let mut out = Vec::new();
        loop {
            let Some(pos) = self.buf.iter().position(|b| *b == b'\n' || *b == b'\r') else {
                if self.buf.len() > MAX_LINE {
                    return Err("SSE line too long");
                }
                break;
            };
            // A lone '\r' at the end of the buffer may be the first half of
            // "\r\n": wait for more data.
            if self.buf[pos] == b'\r' && pos + 1 == self.buf.len() {
                break;
            }
            let line = String::from_utf8_lossy(&self.buf[..pos]).into_owned();
            let skip = if self.buf[pos] == b'\r' && self.buf.get(pos + 1) == Some(&b'\n') {
                2
            } else {
                1
            };
            self.buf.drain(..pos + skip);
            if let Some(ev) = self.line(&line) {
                out.push(ev);
            }
        }
        Ok(out)
    }

    fn line(&mut self, line: &str) -> Option<SseEvent> {
        if line.is_empty() {
            if !self.has_data {
                self.event.clear();
                return None;
            }
            let ev = SseEvent {
                event: if self.event.is_empty() {
                    "message".into()
                } else {
                    std::mem::take(&mut self.event)
                },
                data: std::mem::take(&mut self.data),
                id: self.id.clone(),
            };
            self.event.clear();
            self.has_data = false;
            return Some(ev);
        }
        if line.starts_with(':') {
            return None; // comment / keep-alive
        }
        let (field, value) = match line.split_once(':') {
            Some((f, v)) => (f, v.strip_prefix(' ').unwrap_or(v)),
            None => (line, ""),
        };
        match field {
            "event" => self.event = value.to_string(),
            "data" => {
                if self.has_data {
                    self.data.push('\n');
                }
                self.data.push_str(value);
                self.has_data = true;
            }
            "id" if !value.contains('\0') => self.id = Some(value.to_string()),
            _ => {}
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_events_across_chunks() {
        let mut p = SseParser::new();
        assert!(p
            .push(b": keep-alive\n\nevent: change\nid: v1.ab.1\nda")
            .unwrap()
            .is_empty());
        let evs = p.push(b"ta: {\"a\":1}\n\n").unwrap();
        assert_eq!(
            evs,
            vec![SseEvent {
                event: "change".into(),
                data: "{\"a\":1}".into(),
                id: Some("v1.ab.1".into())
            }]
        );
    }

    #[test]
    fn handles_crlf_and_multiline_data() {
        let mut p = SseParser::new();
        let evs = p.push(b"data: a\r\ndata: b\r").unwrap();
        assert!(evs.is_empty());
        let evs = p.push(b"\n\r\n").unwrap();
        assert_eq!(evs[0].data, "a\nb");
        assert_eq!(evs[0].event, "message");
    }

    #[test]
    fn rejects_unbounded_lines() {
        let mut p = SseParser::new();
        assert!(p.push(&vec![b'x'; MAX_LINE + 1]).is_err());
    }
}
