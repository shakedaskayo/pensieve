//! Populate the `at` column from well-known timestamp aliases in NDJSON.
//!
//! When the target schema has an `at` field typed `Timestamp(..)`, and an
//! incoming NDJSON record either lacks `"at"` or has `"at": null`, this module
//! finds the first available alias (`"timestamp"`, `"time_unix_nano"`,
//! `"observed_time_unix_nano"`) and injects the value as an RFC 3339 string
//! into `"at"` before the record reaches arrow-json.
//!
//! Records that already carry a non-null `"at"` are copied through byte-for-
//! byte. Lines that fail to parse as JSON are also copied through verbatim so
//! the downstream parser can own the error.

use arrow_schema::{DataType, SchemaRef};
use chrono::{DateTime, Utc};
use std::borrow::Cow;

/// Populate a missing/null `at` key in each NDJSON record from the first
/// available alias: `timestamp`, `time_unix_nano`, `observed_time_unix_nano`.
/// Implements the contract documented on `default_table_schema()`.
///
/// Returns the original bytes untouched when the schema has no Timestamp-typed
/// `at` field (pass-through; not our table shape).
pub fn populate_at_column<'a>(bytes: &'a [u8], schema: &SchemaRef) -> Cow<'a, [u8]> {
    // Only activate when the schema has an `at` field with a Timestamp type.
    let has_at_timestamp = schema
        .fields()
        .iter()
        .any(|f| f.name() == "at" && matches!(f.data_type(), DataType::Timestamp(..)));
    if !has_at_timestamp {
        return Cow::Borrowed(bytes);
    }

    // Two-pass approach:
    //
    // Pass 1 — scan every line and check whether ANY line actually needs `at`
    // injected (i.e. `at` is absent/null AND at least one alias key is
    // present). If none does, return Cow::Borrowed immediately — no allocation,
    // no output buffer. This is the common production case (records already
    // carry `at`, or no schema-supported aliases exist).
    //
    // Pass 2 — only runs when pass 1 found at least one line that needs
    // rewriting. Re-scans every line and splices `"at"` into the original
    // bytes. Nested payloads are never decoded: a recursive `serde_json::Value`
    // parse here overflows the Tokio worker stack and aborts the process.

    // ------------------------------------------------------------------
    // Pass 1: determine whether any rewriting is needed.
    // ------------------------------------------------------------------
    let any_needs_injection = bytes.split(|&b| b == b'\n').any(|line| {
        if line.iter().all(|b| b.is_ascii_whitespace()) {
            return false;
        }
        plan_injection(line).is_some()
    });

    if !any_needs_injection {
        return Cow::Borrowed(bytes);
    }

    // ------------------------------------------------------------------
    // Pass 2: rebuild the buffer, injecting `at` where needed.
    // ------------------------------------------------------------------
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len() + 64);

    for line in bytes.split(|&b| b == b'\n') {
        if line.iter().all(|b| b.is_ascii_whitespace()) {
            if !out.is_empty() {
                out.push(b'\n');
            }
            continue;
        }

        if !out.is_empty() {
            out.push(b'\n');
        }
        match plan_injection(line) {
            Some(plan) => splice_at(&mut out, line, &plan),
            None => out.extend_from_slice(line),
        }
    }

    Cow::Owned(out)
}

struct Injection {
    /// `Some` when `"at"` is present and should be replaced (it was null).
    /// `None` when the key is absent and should be inserted after `{`.
    replace: Option<std::ops::Range<usize>>,
    rfc3339: String,
}

/// `Some` when this line needs an `at` value spliced in.
fn plan_injection(line: &[u8]) -> Option<Injection> {
    let crate::json_scan::Line::Object(fields) = crate::json_scan::scan_line(line).ok()? else {
        return None;
    };
    let at = crate::json_scan::find(&fields, "at");
    let at_missing_or_null = match at {
        None => true,
        Some(f) => matches!(f.value, crate::json_scan::JsonVal::Null),
    };
    if !at_missing_or_null {
        return None;
    }
    let rfc3339 = resolve_at_from_aliases(&fields)?;
    let replace = at
        .filter(|f| matches!(f.value, crate::json_scan::JsonVal::Null))
        .map(|f| f.range.clone());
    Some(Injection { replace, rfc3339 })
}

fn splice_at(out: &mut Vec<u8>, line: &[u8], plan: &Injection) {
    let quoted = format!("\"{}\"", plan.rfc3339);
    if let Some(range) = &plan.replace {
        out.extend_from_slice(&line[..range.start]);
        out.extend_from_slice(quoted.as_bytes());
        out.extend_from_slice(&line[range.end..]);
        return;
    }
    let brace = line.iter().position(|b| *b == b'{').unwrap_or(0);
    out.extend_from_slice(&line[..=brace]);
    out.extend_from_slice(b"\"at\":");
    out.extend_from_slice(quoted.as_bytes());
    let rest = &line[brace + 1..];
    let next = rest
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .map(|i| rest[i]);
    if next != Some(b'}') {
        out.push(b',');
    }
    out.extend_from_slice(rest);
}

/// Look for `"timestamp"`, `"time_unix_nano"`, `"observed_time_unix_nano"` in
/// the record and return an RFC 3339 string, or `None` if none found / parseable.
fn resolve_at_from_aliases(fields: &[crate::json_scan::Field<'_>]) -> Option<String> {
    // Priority 1: "timestamp"
    if let Some(v) = crate::json_scan::find(fields, "timestamp") {
        if let Some(s) = json_val_to_rfc3339(&v.value, TimestampKey::Timestamp) {
            return Some(s);
        }
    }
    // Priority 2: "time_unix_nano"
    if let Some(v) = crate::json_scan::find(fields, "time_unix_nano") {
        if let Some(s) = json_val_to_rfc3339(&v.value, TimestampKey::UnixNano) {
            return Some(s);
        }
    }
    // Priority 3: "observed_time_unix_nano"
    if let Some(v) = crate::json_scan::find(fields, "observed_time_unix_nano") {
        if let Some(s) = json_val_to_rfc3339(&v.value, TimestampKey::UnixNano) {
            return Some(s);
        }
    }
    None
}

fn json_val_to_rfc3339(v: &crate::json_scan::JsonVal<'_>, key: TimestampKey) -> Option<String> {
    match v {
        crate::json_scan::JsonVal::String(s) => Some(s.clone()),
        crate::json_scan::JsonVal::Number(raw) => {
            let nanos = match key {
                TimestampKey::UnixNano => raw.parse::<i64>().ok()?,
                TimestampKey::Timestamp => numeric_timestamp_to_nanos(raw.parse::<f64>().ok()?)?,
            };
            let dt = DateTime::<Utc>::from_timestamp_nanos(nanos);
            Some(dt.to_rfc3339())
        }
        _ => None,
    }
}

#[derive(Clone, Copy)]
enum TimestampKey {
    /// The `"timestamp"` key — unit is ambiguous when it's a number.
    Timestamp,
    /// The `"time_unix_nano"` / `"observed_time_unix_nano"` keys — always nanos.
    UnixNano,
}

/// Convert a numeric `"timestamp"` value to nanoseconds using a magnitude
/// heuristic.
///
/// Thresholds (same boundaries used by OpenTelemetry / ClickHouse heuristics):
/// - `>= 10^16`  → nanoseconds
/// - `>= 10^14`  → microseconds
/// - `>= 10^11`  → milliseconds
/// - else        → seconds
fn numeric_timestamp_to_nanos(f: f64) -> Option<i64> {
    if !f.is_finite() {
        return None;
    }
    let abs = f.abs();
    let nanos_f: f64 = if abs >= 1e16 {
        // Already nanoseconds
        f
    } else if abs >= 1e14 {
        // Microseconds → nanoseconds
        f * 1_000.0
    } else if abs >= 1e11 {
        // Milliseconds → nanoseconds
        f * 1_000_000.0
    } else {
        // Seconds → nanoseconds
        f * 1_000_000_000.0
    };

    // Guard against overflow of i64 (from_timestamp_nanos takes i64).
    // i64::MAX ≈ 9.22 × 10^18; DateTime valid range is much narrower
    // (~year 1678 to ~year 2262) but from_timestamp_nanos handles the
    // mathematical range of i64 without panicking.
    if nanos_f > i64::MAX as f64 || nanos_f < i64::MIN as f64 {
        return None;
    }
    Some(nanos_f as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    fn schema_with_at() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("at", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("message", DataType::Utf8, true),
        ]))
    }

    fn schema_without_at_timestamp() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("value", DataType::Int64, true),
        ]))
    }

    // -------------------------------------------------------------------------
    // (a) `timestamp` ISO string → `at` injected with same value
    // -------------------------------------------------------------------------
    #[test]
    fn timestamp_string_injects_at() {
        let schema = schema_with_at();
        let ndjson = b"{\"timestamp\":\"2026-06-05T10:00:00Z\",\"message\":\"hi\"}\n";
        let result = populate_at_column(ndjson, &schema);
        let output = std::str::from_utf8(result.as_ref()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(output.trim()).unwrap();
        assert_eq!(
            parsed["at"],
            serde_json::json!("2026-06-05T10:00:00Z"),
            "at should equal the timestamp string verbatim"
        );
        assert_eq!(parsed["message"], serde_json::json!("hi"));
    }

    // -------------------------------------------------------------------------
    // (b) Existing non-null `at` → line unchanged (byte-identical)
    // -------------------------------------------------------------------------
    #[test]
    fn existing_at_leaves_line_unchanged() {
        let schema = schema_with_at();
        let ndjson = b"{\"at\":\"2026-01-01T00:00:00Z\",\"message\":\"existing\"}\n";
        let result = populate_at_column(ndjson, &schema);
        // Should be Cow::Borrowed (same bytes) or at least identical content
        let input_str = std::str::from_utf8(ndjson).unwrap().trim();
        let output_str = std::str::from_utf8(result.as_ref()).unwrap().trim();
        // Parse both to compare semantically (trailing newline differences are ok)
        let input_v: serde_json::Value = serde_json::from_str(input_str).unwrap();
        let output_v: serde_json::Value = serde_json::from_str(output_str).unwrap();
        assert_eq!(input_v, output_v, "line should be semantically identical");
        assert_eq!(input_v["at"], serde_json::json!("2026-01-01T00:00:00Z"));
    }

    // -------------------------------------------------------------------------
    // (c) `time_unix_nano` number → at = correct RFC 3339
    // -------------------------------------------------------------------------
    #[test]
    fn time_unix_nano_number_injects_rfc3339() {
        let schema = schema_with_at();
        // 2022-09-11T18:34:48Z in nanoseconds
        let nanos: i64 = 1662921288_000_000_000;
        let ndjson = format!("{{\"time_unix_nano\":{},\"message\":\"hello\"}}\n", nanos);
        let result = populate_at_column(ndjson.as_bytes(), &schema);
        let output = std::str::from_utf8(result.as_ref()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(output.trim()).unwrap();
        let at_str = parsed["at"].as_str().expect("at should be a string");
        // Parse the RFC3339 and verify the epoch matches
        let dt = DateTime::parse_from_rfc3339(at_str).expect("should be valid RFC3339");
        assert_eq!(dt.timestamp_nanos_opt().unwrap(), nanos);
    }

    // -------------------------------------------------------------------------
    // (d) Numeric `timestamp` in millis → correct instant via heuristic
    // -------------------------------------------------------------------------
    #[test]
    fn numeric_timestamp_millis_via_heuristic() {
        let schema = schema_with_at();
        // 2026-06-05T10:00:00Z in milliseconds
        // 2026-06-05T10:00:00Z → Unix timestamp: ~1780488000s → ~1780488000000ms
        let millis: i64 = 1780488000000_i64; // >= 10^11 and < 10^14 → millis
        let ndjson = format!("{{\"timestamp\":{},\"message\":\"millis\"}}\n", millis);
        let result = populate_at_column(ndjson.as_bytes(), &schema);
        let output = std::str::from_utf8(result.as_ref()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(output.trim()).unwrap();
        let at_str = parsed["at"].as_str().expect("at should be a string");
        let dt = DateTime::parse_from_rfc3339(at_str).expect("valid RFC3339");
        // Should be within 1 second of expected
        let expected_secs = millis / 1000;
        assert_eq!(
            dt.timestamp(),
            expected_secs,
            "seconds portion should match"
        );
    }

    // -------------------------------------------------------------------------
    // (e) No alias keys → line unchanged (at stays null/absent)
    // -------------------------------------------------------------------------
    #[test]
    fn no_alias_keys_line_unchanged() {
        let schema = schema_with_at();
        let ndjson = b"{\"message\":\"no time here\"}\n";
        let result = populate_at_column(ndjson, &schema);
        let output = std::str::from_utf8(result.as_ref()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(output.trim()).unwrap();
        // "at" should not have been injected (absent or null)
        assert!(
            parsed.get("at").is_none() || parsed["at"].is_null(),
            "at should not be injected when no alias found"
        );
        assert_eq!(parsed["message"], serde_json::json!("no time here"));
    }

    // -------------------------------------------------------------------------
    // (f) Schema WITHOUT `at` Timestamp col → bytes returned untouched (Cow::Borrowed)
    // -------------------------------------------------------------------------
    #[test]
    fn schema_without_at_timestamp_returns_borrowed() {
        let schema = schema_without_at_timestamp();
        let ndjson = b"{\"timestamp\":\"2026-06-05T10:00:00Z\",\"value\":42}\n";
        let result = populate_at_column(ndjson, &schema);
        // Should be Cow::Borrowed — exact same bytes
        assert!(
            matches!(result, Cow::Borrowed(_)),
            "should return Cow::Borrowed for schemas without Timestamp at"
        );
        assert_eq!(result.as_ref(), ndjson.as_slice());
    }

    // -------------------------------------------------------------------------
    // (g) Malformed JSON line → copied through verbatim
    // -------------------------------------------------------------------------
    #[test]
    fn deep_nested_payload_with_timestamp_does_not_overflow() {
        let schema = schema_with_at();
        let mut ndjson = String::from("{\"timestamp\":\"2026-06-05T10:00:00Z\",\"props\":");
        for _ in 0..2_000 {
            ndjson.push_str("{\"a\":");
        }
        ndjson.push('1');
        for _ in 0..2_000 {
            ndjson.push('}');
        }
        ndjson.push_str("}\n");
        let result = populate_at_column(ndjson.as_bytes(), &schema);
        let output = std::str::from_utf8(result.as_ref()).unwrap();
        assert!(output.contains("\"at\":\"2026-06-05T10:00:00Z\""));
        assert!(output.contains("\"props\":"));
    }

    #[test]
    fn malformed_json_line_copied_verbatim() {
        let schema = schema_with_at();
        let ndjson = b"not-json-at-all\n";
        let result = populate_at_column(ndjson, &schema);
        let output = std::str::from_utf8(result.as_ref()).unwrap();
        assert!(
            output.trim() == "not-json-at-all",
            "malformed JSON should be copied verbatim, got: {output}"
        );
    }

    // -------------------------------------------------------------------------
    // (h) Multi-line body mixing all cases — preserves line count and order
    // -------------------------------------------------------------------------
    #[test]
    fn multiline_preserves_count_and_order() {
        let schema = schema_with_at();
        // Line 0: has timestamp string → inject
        // Line 1: has existing at → leave alone
        // Line 2: has time_unix_nano → inject
        // Line 3: no aliases → leave alone
        // Line 4: malformed → copy verbatim
        let nanos: i64 = 1662921288_000_000_000;
        let input = format!(
            "{{\"timestamp\":\"2026-06-05T10:00:00Z\",\"message\":\"a\"}}\n\
             {{\"at\":\"2026-01-01T00:00:00Z\",\"message\":\"b\"}}\n\
             {{\"time_unix_nano\":{},\"message\":\"c\"}}\n\
             {{\"message\":\"d\"}}\n\
             bad-json\n",
            nanos
        );
        let result = populate_at_column(input.as_bytes(), &schema);
        let output = std::str::from_utf8(result.as_ref()).unwrap();
        let lines: Vec<&str> = output
            .split('\n')
            .filter(|l| !l.trim().is_empty())
            .collect();
        assert_eq!(lines.len(), 5, "line count must be preserved");

        // Line 0: at injected from timestamp
        let v0: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(v0["at"], serde_json::json!("2026-06-05T10:00:00Z"));
        assert_eq!(v0["message"], serde_json::json!("a"));

        // Line 1: at unchanged
        let v1: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(v1["at"], serde_json::json!("2026-01-01T00:00:00Z"));
        assert_eq!(v1["message"], serde_json::json!("b"));

        // Line 2: at from time_unix_nano
        let v2: serde_json::Value = serde_json::from_str(lines[2]).unwrap();
        let at2 = v2["at"].as_str().expect("at should be a string");
        let dt2 = DateTime::parse_from_rfc3339(at2).expect("valid RFC3339");
        assert_eq!(dt2.timestamp_nanos_opt().unwrap(), nanos);
        assert_eq!(v2["message"], serde_json::json!("c"));

        // Line 3: no at (or null)
        let v3: serde_json::Value = serde_json::from_str(lines[3]).unwrap();
        assert!(v3.get("at").is_none() || v3["at"].is_null());
        assert_eq!(v3["message"], serde_json::json!("d"));

        // Line 4: verbatim bad-json
        assert_eq!(lines[4].trim(), "bad-json");
    }

    // -------------------------------------------------------------------------
    // Extra: `observed_time_unix_nano` works the same as `time_unix_nano`
    // -------------------------------------------------------------------------
    #[test]
    fn observed_time_unix_nano_injects_rfc3339() {
        let schema = schema_with_at();
        let nanos: i64 = 1662921288_000_000_000;
        let ndjson = format!(
            "{{\"observed_time_unix_nano\":{},\"message\":\"otlp\"}}\n",
            nanos
        );
        let result = populate_at_column(ndjson.as_bytes(), &schema);
        let output = std::str::from_utf8(result.as_ref()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(output.trim()).unwrap();
        let at_str = parsed["at"].as_str().expect("at should be a string");
        let dt = DateTime::parse_from_rfc3339(at_str).expect("valid RFC3339");
        assert_eq!(dt.timestamp_nanos_opt().unwrap(), nanos);
    }

    // -------------------------------------------------------------------------
    // Extra: numeric timestamp in seconds (< 10^11)
    // -------------------------------------------------------------------------
    #[test]
    fn numeric_timestamp_seconds_via_heuristic() {
        let schema = schema_with_at();
        let secs: i64 = 1780488000; // < 10^11 → treated as seconds
        let ndjson = format!("{{\"timestamp\":{},\"message\":\"secs\"}}\n", secs);
        let result = populate_at_column(ndjson.as_bytes(), &schema);
        let output = std::str::from_utf8(result.as_ref()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(output.trim()).unwrap();
        let at_str = parsed["at"].as_str().expect("at should be a string");
        let dt = DateTime::parse_from_rfc3339(at_str).expect("valid RFC3339");
        assert_eq!(dt.timestamp(), secs);
    }

    // -------------------------------------------------------------------------
    // Extra: numeric `at: null` treated same as absent (injection happens)
    // -------------------------------------------------------------------------
    #[test]
    fn null_at_with_timestamp_alias_injects() {
        let schema = schema_with_at();
        let ndjson = b"{\"at\":null,\"timestamp\":\"2026-06-05T10:00:00Z\"}\n";
        let result = populate_at_column(ndjson, &schema);
        let output = std::str::from_utf8(result.as_ref()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(output.trim()).unwrap();
        assert_eq!(
            parsed["at"],
            serde_json::json!("2026-06-05T10:00:00Z"),
            "null at should be replaced"
        );
    }

    // =========================================================================
    // Fix 3: New tests (a)–(f)
    // =========================================================================

    // -------------------------------------------------------------------------
    // Fix3(a) — microseconds-range numeric timestamp
    // -------------------------------------------------------------------------
    #[test]
    fn numeric_timestamp_micros_via_heuristic() {
        let schema = schema_with_at();
        // 2024-06-05T10:03:20Z in microseconds = 1717577000 * 1_000_000
        let micros: i64 = 1717577000_000_000_i64; // >= 10^14 → microseconds
        let ndjson = format!("{{\"timestamp\":{},\"message\":\"micros\"}}\n", micros);
        let result = populate_at_column(ndjson.as_bytes(), &schema);
        let output = std::str::from_utf8(result.as_ref()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(output.trim()).unwrap();
        let at_str = parsed["at"].as_str().expect("at should be a string");
        let dt = DateTime::parse_from_rfc3339(at_str).expect("valid RFC3339");
        // microseconds / 1_000_000 = seconds
        let expected_secs = micros / 1_000_000;
        assert_eq!(
            dt.timestamp(),
            expected_secs,
            "microseconds heuristic should recover seconds"
        );
    }

    // -------------------------------------------------------------------------
    // Fix3(b) — boundary pinning for the heuristic thresholds
    // -------------------------------------------------------------------------

    /// Exactly 1e11 → milliseconds boundary (inclusive).
    #[test]
    fn boundary_1e11_is_millis() {
        let schema = schema_with_at();
        let val: i64 = 100_000_000_000_i64; // exactly 10^11
        let ndjson = format!("{{\"timestamp\":{}}}\n", val);
        let result = populate_at_column(ndjson.as_bytes(), &schema);
        let output = std::str::from_utf8(result.as_ref()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(output.trim()).unwrap();
        let at_str = parsed["at"].as_str().expect("at should be a string");
        let dt = DateTime::parse_from_rfc3339(at_str).expect("valid RFC3339");
        // 10^11 ms = 10^8 s = 100_000_000 seconds from epoch ≈ year 5138
        let expected_secs = val / 1000;
        assert_eq!(dt.timestamp(), expected_secs);
    }

    /// 1e11 - 1 = 99_999_999_999 → falls into seconds branch.
    ///
    /// 99_999_999_999 s × 10^9 ns/s ≈ 1×10^20 ns, which overflows i64::MAX
    /// (~9.2×10^18). The overflow guard in `numeric_timestamp_to_nanos` returns
    /// `None`, so no `at` is injected and the line is left unchanged. This pins
    /// the contract: values just below 10^11 that would overflow in seconds
    /// are silently passed through rather than wrapping or panicking.
    #[test]
    fn boundary_below_1e11_is_seconds_overflow_passthrough() {
        let schema = schema_with_at();
        let val: i64 = 99_999_999_999_i64; // 10^11 - 1 → seconds branch → overflow → no inject
        let ndjson = format!("{{\"timestamp\":{}}}\n", val);
        let result = populate_at_column(ndjson.as_bytes(), &schema);
        let output = std::str::from_utf8(result.as_ref()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(output.trim()).unwrap();
        // `at` should NOT be injected because the nanosecond conversion overflows i64.
        assert!(
            parsed.get("at").is_none() || parsed["at"].is_null(),
            "99_999_999_999 seconds overflows i64 nanos, so `at` must not be injected; got: {:?}",
            parsed.get("at")
        );
    }

    /// Exactly 1e14 → microseconds boundary (inclusive).
    #[test]
    fn boundary_1e14_is_micros() {
        let schema = schema_with_at();
        let val: i64 = 100_000_000_000_000_i64; // exactly 10^14
        let ndjson = format!("{{\"timestamp\":{}}}\n", val);
        let result = populate_at_column(ndjson.as_bytes(), &schema);
        let output = std::str::from_utf8(result.as_ref()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(output.trim()).unwrap();
        let at_str = parsed["at"].as_str().expect("at should be a string");
        let dt = DateTime::parse_from_rfc3339(at_str).expect("valid RFC3339");
        let expected_secs = val / 1_000_000;
        assert_eq!(
            dt.timestamp(),
            expected_secs,
            "exactly 1e14 treated as microseconds"
        );
    }

    /// Exactly 1e16 → nanoseconds boundary (inclusive).
    #[test]
    fn boundary_1e16_is_nanos() {
        let schema = schema_with_at();
        let val: i64 = 10_000_000_000_000_000_i64; // exactly 10^16
        let ndjson = format!("{{\"timestamp\":{}}}\n", val);
        let result = populate_at_column(ndjson.as_bytes(), &schema);
        let output = std::str::from_utf8(result.as_ref()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(output.trim()).unwrap();
        let at_str = parsed["at"].as_str().expect("at should be a string");
        let dt = DateTime::parse_from_rfc3339(at_str).expect("valid RFC3339");
        let expected_secs = val / 1_000_000_000;
        assert_eq!(
            dt.timestamp(),
            expected_secs,
            "exactly 1e16 treated as nanoseconds"
        );
    }

    // -------------------------------------------------------------------------
    // Fix3(c) — negative seconds timestamp → pre-1970, no panic
    // -------------------------------------------------------------------------
    #[test]
    fn negative_seconds_timestamp_pre_epoch() {
        let schema = schema_with_at();
        // -1717577000 seconds → before Unix epoch (pre-1970)
        let secs: i64 = -1717577000_i64;
        let ndjson = format!("{{\"timestamp\":{}}}\n", secs);
        let result = populate_at_column(ndjson.as_bytes(), &schema);
        let output = std::str::from_utf8(result.as_ref()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(output.trim()).unwrap();
        let at_str = parsed["at"].as_str().expect("at should be a string");
        let dt = DateTime::parse_from_rfc3339(at_str).expect("valid RFC3339");
        assert_eq!(
            dt.timestamp(),
            secs,
            "negative seconds should yield pre-epoch datetime"
        );
        assert!(
            dt.format("%Y").to_string().parse::<i32>().unwrap() < 1970,
            "year should be before 1970, got {}",
            dt.format("%Y")
        );
    }

    // -------------------------------------------------------------------------
    // Fix3(d) — float seconds with sub-second precision preserved
    // -------------------------------------------------------------------------
    #[test]
    fn float_seconds_sub_second_preserved() {
        let schema = schema_with_at();
        // 1717577000.5 → 0.5 seconds = 500_000_000 nanoseconds sub-second part
        let ndjson = b"{\"timestamp\":1717577000.5}\n";
        let result = populate_at_column(ndjson, &schema);
        let output = std::str::from_utf8(result.as_ref()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(output.trim()).unwrap();
        let at_str = parsed["at"].as_str().expect("at should be a string");
        let dt = DateTime::parse_from_rfc3339(at_str).expect("valid RFC3339");
        assert_eq!(dt.timestamp(), 1717577000, "whole seconds should match");
        // Sub-second part should be 500ms (allow ±1ms for float rounding)
        let nanos = dt.timestamp_subsec_nanos();
        assert!(
            nanos >= 499_000_000 && nanos <= 501_000_000,
            "sub-second should be ~500ms, got {} ns",
            nanos
        );
    }

    // -------------------------------------------------------------------------
    // Fix3(e) — no-op body (all lines carry `at`) → Cow::Borrowed
    // -------------------------------------------------------------------------
    #[test]
    fn all_lines_carry_at_returns_borrowed() {
        let schema = schema_with_at();
        let ndjson = b"{\"at\":\"2026-01-01T00:00:00Z\",\"message\":\"a\"}\n\
                       {\"at\":\"2026-01-02T00:00:00Z\",\"message\":\"b\"}\n";
        let result = populate_at_column(ndjson, &schema);
        assert!(
            matches!(result, Cow::Borrowed(_)),
            "body where all lines carry `at` should return Cow::Borrowed"
        );
    }

    // -------------------------------------------------------------------------
    // Fix3(f) — at absent but NO aliases → Cow::Borrowed (no injection possible)
    // -------------------------------------------------------------------------
    #[test]
    fn at_absent_no_aliases_returns_borrowed() {
        let schema = schema_with_at();
        let ndjson = b"{\"message\":\"no time\",\"level\":\"info\"}\n\
                       {\"message\":\"also no time\"}\n";
        let result = populate_at_column(ndjson, &schema);
        assert!(
            matches!(result, Cow::Borrowed(_)),
            "body where at is absent but no aliases exist should return Cow::Borrowed"
        );
    }
}
