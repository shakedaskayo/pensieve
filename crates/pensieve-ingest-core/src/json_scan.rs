//! Iterative scanner for one NDJSON object.
//!
//! `serde_json::Value` decodes nested objects by recursion. On a Tokio worker
//! (2 MiB stack) a few hundred levels — a tool payload, a trace, a minified
//! document — aborts the process (`fatal runtime error: stack overflow`)
//! instead of returning an error. Ingest only needs top-level keys and the
//! raw bytes of a few fields, so this scanner keeps an explicit depth counter
//! and never recurses.

use serde_json::{Map, Value};

/// One top-level field. Nested arrays and objects stay as raw slices.
#[derive(Debug)]
pub struct Field<'a> {
    pub key: String,
    pub value: JsonVal<'a>,
    /// Byte range of the value token inside the source line.
    pub range: std::ops::Range<usize>,
}

#[derive(Debug)]
pub enum JsonVal<'a> {
    Null,
    Bool(bool),
    /// Raw number text, borrowed from the line (`-1.5e2`).
    Number(&'a str),
    /// Unescaped string contents.
    String(String),
    /// Array or object, exactly as it appeared.
    Raw(&'a [u8]),
}

pub enum Line<'a> {
    Object(Vec<Field<'a>>),
    /// A complete JSON value that is not an object (array, literal).
    Other,
}

/// Scan one NDJSON line. Whitespace-only input is [`Line::Other`].
pub fn scan_line(line: &[u8]) -> Result<Line<'_>, String> {
    let mut c = Cur { b: line, i: 0 };
    c.skip_ws()?;
    if c.i >= c.b.len() {
        return Ok(Line::Other);
    }
    if c.b[c.i] != b'{' {
        // Validate it is some JSON value so a truncated line still errors,
        // matching the old `serde_json::from_slice` failure mode.
        let _ = c.take_value()?;
        c.skip_ws()?;
        if c.i != c.b.len() {
            return Err("trailing data after JSON value".into());
        }
        return Ok(Line::Other);
    }
    c.i += 1;
    let mut fields = Vec::new();
    loop {
        c.skip_ws()?;
        if c.i >= c.b.len() {
            return Err("unterminated object".into());
        }
        if c.b[c.i] == b'}' {
            c.i += 1;
            break;
        }
        if c.b[c.i] == b',' {
            c.i += 1;
            c.skip_ws()?;
            if c.i < c.b.len() && c.b[c.i] == b'}' {
                return Err("trailing comma".into());
            }
            continue;
        }
        if c.b[c.i] != b'"' {
            return Err("expected object key".into());
        }
        let key = c.parse_string()?;
        c.skip_ws()?;
        if c.i >= c.b.len() || c.b[c.i] != b':' {
            return Err("expected ':' after object key".into());
        }
        c.i += 1;
        let (value, range) = c.take_value()?;
        fields.push(Field { key, value, range });
    }
    c.skip_ws()?;
    if c.i != c.b.len() {
        return Err("trailing data after JSON object".into());
    }
    Ok(Line::Object(fields))
}

/// Last occurrence of `key` (JSON duplicate-key rule: last wins).
pub fn find<'a, 'b>(fields: &'b [Field<'a>], key: &str) -> Option<&'b Field<'a>> {
    fields.iter().rev().find(|f| f.key == key)
}

/// Top-level keys only, as objects of nulls. Schema evolution looks at keys;
/// it must not build a DOM of the nested payload.
pub fn records_for_schema_evolve(bytes: &[u8]) -> Result<Vec<Value>, String> {
    let mut out = Vec::new();
    for line in bytes.split(|&b| b == b'\n') {
        if line.iter().all(|b| b.is_ascii_whitespace()) {
            continue;
        }
        match scan_line(line)? {
            Line::Object(fields) => {
                let mut map = Map::new();
                for f in fields {
                    map.insert(f.key, Value::Null);
                }
                out.push(Value::Object(map));
            }
            Line::Other => out.push(Value::Null),
        }
    }
    Ok(out)
}

struct Cur<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Cur<'a> {
    fn skip_ws(&mut self) -> Result<(), String> {
        while self.i < self.b.len() && self.b[self.i].is_ascii_whitespace() {
            self.i += 1;
        }
        Ok(())
    }

    fn parse_string(&mut self) -> Result<String, String> {
        let start = self.i;
        if self.i >= self.b.len() || self.b[self.i] != b'"' {
            return Err("expected string".into());
        }
        self.i += 1;
        let mut escaped = false;
        while self.i < self.b.len() {
            let ch = self.b[self.i];
            self.i += 1;
            if escaped {
                escaped = false;
                continue;
            }
            if ch == b'\\' {
                escaped = true;
                continue;
            }
            if ch == b'"' {
                let quoted = &self.b[start..self.i];
                return serde_json::from_slice(quoted).map_err(|e| format!("bad string: {e}"));
            }
        }
        Err("unterminated string".into())
    }

    fn take_value(&mut self) -> Result<(JsonVal<'a>, std::ops::Range<usize>), String> {
        self.skip_ws()?;
        if self.i >= self.b.len() {
            return Err("expected value".into());
        }
        let start = self.i;
        let val = match self.b[self.i] {
            b'n' => {
                self.expect_lit(b"null")?;
                JsonVal::Null
            }
            b't' => {
                self.expect_lit(b"true")?;
                JsonVal::Bool(true)
            }
            b'f' => {
                self.expect_lit(b"false")?;
                JsonVal::Bool(false)
            }
            b'"' => JsonVal::String(self.parse_string()?),
            b'{' | b'[' => {
                self.skip_container()?;
                JsonVal::Raw(&self.b[start..self.i])
            }
            b'-' | b'0'..=b'9' => {
                self.skip_number()?;
                let text = std::str::from_utf8(&self.b[start..self.i])
                    .map_err(|_| "number is not utf-8".to_string())?;
                JsonVal::Number(text)
            }
            other => return Err(format!("unexpected byte {other} in JSON value")),
        };
        Ok((val, start..self.i))
    }

    fn expect_lit(&mut self, lit: &[u8]) -> Result<(), String> {
        if self.b[self.i..].starts_with(lit) {
            self.i += lit.len();
            Ok(())
        } else {
            Err(format!("expected {}", String::from_utf8_lossy(lit)))
        }
    }

    fn skip_number(&mut self) -> Result<(), String> {
        let start = self.i;
        if self.b[self.i] == b'-' {
            self.i += 1;
        }
        if self.i >= self.b.len() || !self.b[self.i].is_ascii_digit() {
            return Err("bad number".into());
        }
        while self.i < self.b.len() && self.b[self.i].is_ascii_digit() {
            self.i += 1;
        }
        if self.i < self.b.len() && self.b[self.i] == b'.' {
            self.i += 1;
            if self.i >= self.b.len() || !self.b[self.i].is_ascii_digit() {
                return Err("bad number".into());
            }
            while self.i < self.b.len() && self.b[self.i].is_ascii_digit() {
                self.i += 1;
            }
        }
        if self.i < self.b.len() && matches!(self.b[self.i], b'e' | b'E') {
            self.i += 1;
            if self.i < self.b.len() && matches!(self.b[self.i], b'+' | b'-') {
                self.i += 1;
            }
            if self.i >= self.b.len() || !self.b[self.i].is_ascii_digit() {
                return Err("bad number".into());
            }
            while self.i < self.b.len() && self.b[self.i].is_ascii_digit() {
                self.i += 1;
            }
        }
        if self.i == start {
            return Err("bad number".into());
        }
        Ok(())
    }

    /// Advance past one array or object, including its nested contents.
    /// Depth is a counter, not a call stack.
    fn skip_container(&mut self) -> Result<(), String> {
        let open = self.b[self.i];
        let close = if open == b'{' { b'}' } else { b']' };
        self.i += 1;
        let mut depth: u32 = 1;
        let mut in_string = false;
        let mut escaped = false;
        while self.i < self.b.len() {
            let ch = self.b[self.i];
            self.i += 1;
            if in_string {
                if escaped {
                    escaped = false;
                    continue;
                }
                if ch == b'\\' {
                    escaped = true;
                    continue;
                }
                if ch == b'"' {
                    in_string = false;
                }
                continue;
            }
            match ch {
                b'"' => in_string = true,
                b'{' | b'[' => {
                    depth = depth.checked_add(1).ok_or("json nesting too deep")?;
                }
                c if c == close || c == b'}' || c == b']' => {
                    // Any closer drops depth; a mismatch still terminates
                    // instead of looping, and the caller rejects bad JSON
                    // when the surrounding structure doesn't line up.
                    depth -= 1;
                    if depth == 0 {
                        if c != close {
                            return Err("mismatched bracket".into());
                        }
                        return Ok(());
                    }
                }
                _ => {}
            }
        }
        Err("unterminated array or object".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scans_top_level_without_materializing_nested_objects() {
        let line = br#"{"id":"a","n":1,"ok":true,"z":null,"props":{"a":{"a":1}},"s":"x\"y"}"#;
        let Line::Object(fields) = scan_line(line).unwrap() else {
            panic!("object");
        };
        assert_eq!(find(&fields, "id").unwrap().value_str(), "a");
        match &find(&fields, "props").unwrap().value {
            JsonVal::Raw(raw) => assert!(raw.starts_with(b"{")),
            other => panic!("expected raw object, got {other:?}"),
        }
        assert_eq!(find(&fields, "s").unwrap().value_str(), "x\"y");
        assert!(matches!(find(&fields, "z").unwrap().value, JsonVal::Null));
        assert!(matches!(
            find(&fields, "ok").unwrap().value,
            JsonVal::Bool(true)
        ));
    }

    #[test]
    fn deep_nesting_does_not_recurse() {
        let mut line = String::from(r#"{"props":"#);
        for _ in 0..4_000 {
            line.push_str(r#"{"a":"#);
        }
        line.push('1');
        for _ in 0..4_000 {
            line.push('}');
        }
        line.push('}');
        let Line::Object(fields) = scan_line(line.as_bytes()).unwrap() else {
            panic!("object");
        };
        let JsonVal::Raw(raw) = &find(&fields, "props").unwrap().value else {
            panic!("raw");
        };
        assert!(raw.starts_with(b"{"));
        assert!(raw.ends_with(b"}"));
    }

    impl<'a> Field<'a> {
        fn value_str(&self) -> &str {
            match &self.value {
                JsonVal::String(s) => s,
                _ => panic!("not a string"),
            }
        }
    }
}
