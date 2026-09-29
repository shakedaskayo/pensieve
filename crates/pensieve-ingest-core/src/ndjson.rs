//! Shared NDJSON → Arrow RecordBatch converter.
//!
//! Wraps arrow-json's `ReaderBuilder` for all columns that Arrow's JSON
//! reader already supports, and adds:
//! - `FixedSizeList<Float32, N>` (vector columns) — upstream arrow-json rejects these.
//! - `Binary` (dynamic columns) — arrow-json's JSON reader cannot decode Binary.
//!   Binary/dynamic columns are read from the NDJSON as either a JSON string (stored
//!   as UTF-8 bytes) or any other value serialized to JSON bytes. This matches pensieve's
//!   `dynamic` column semantic: the stored value is the raw JSON representation.
//!
//! Used by every ingest frontend so vector and dynamic columns work the same way
//! from REST, Kafka, and file-drop.

use arrow_array::{ArrayRef, BinaryArray, FixedSizeListArray, Float32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::io::BufReader;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum NdjsonError {
    #[error("arrow-json: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("parse: {0}")]
    Parse(#[from] serde_json::Error),
    #[error("parse: {0}")]
    Scan(String),
    #[error("column `{column}`: expected array of {dimension} floats, got length {got}")]
    VectorDimensionMismatch {
        column: String,
        dimension: i32,
        got: usize,
    },
    #[error("column `{column}`: expected array of floats, got {got}")]
    VectorWrongType { column: String, got: String },
    #[error("column `{column}`: non-numeric element at index {index}: {value}")]
    VectorNonNumeric {
        column: String,
        index: usize,
        value: String,
    },
    #[error("column `{column}`: FixedSizeList inner type must be Float32, got {got:?}")]
    VectorUnsupportedInner { column: String, got: DataType },
}

/// Parse NDJSON bytes into a batch of `RecordBatch`es against the given schema.
///
/// Columns handled via the manual slow path (stripped from arrow-json then spliced):
/// - `FixedSizeList<Float32, N>` — vector columns
/// - `Binary` / `LargeBinary` — dynamic columns (stored as raw JSON bytes)
///
/// All other columns delegate to arrow-json's built-in reader.
pub fn parse_ndjson(bytes: &[u8], schema: SchemaRef) -> Result<Vec<RecordBatch>, NdjsonError> {
    // Populate the `at` column from timestamp aliases before parsing so that
    // time-range filters and the Discover UI can order rows correctly.
    let coerced = crate::event_time::populate_at_column(bytes, &schema);
    let bytes: &[u8] = coerced.as_ref();
    // Identify columns that arrow-json cannot handle natively.
    let mut vector_cols: Vec<(usize, String, i32)> = Vec::new();
    let mut binary_cols: Vec<(usize, String)> = Vec::new();

    for (i, f) in schema.fields().iter().enumerate() {
        match f.data_type() {
            DataType::FixedSizeList(inner, dim) => match inner.data_type() {
                DataType::Float32 => vector_cols.push((i, f.name().clone(), *dim)),
                other => {
                    return Err(NdjsonError::VectorUnsupportedInner {
                        column: f.name().clone(),
                        got: other.clone(),
                    })
                }
            },
            DataType::Binary | DataType::LargeBinary => {
                binary_cols.push((i, f.name().clone()));
            }
            _ => {}
        }
    }

    if vector_cols.is_empty() && binary_cols.is_empty() {
        // Fast path: pure arrow-json, preserves existing behavior.
        let reader = arrow_json::ReaderBuilder::new(schema).build(BufReader::new(bytes))?;
        return reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(NdjsonError::from);
    }

    // Slow path: strip unsupported columns from schema, parse the rest via
    // arrow-json, build manual arrays for the stripped columns, then splice
    // everything back.
    //
    // Important: arrow-json ignores unknown JSON fields by default, so
    // leaving the stripped fields' values in the raw NDJSON is safe.
    let is_manual = |i: usize| {
        vector_cols.iter().any(|(vi, _, _)| *vi == i) || binary_cols.iter().any(|(bi, _)| *bi == i)
    };

    let stripped_fields: Vec<Arc<Field>> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(i, _)| !is_manual(*i))
        .map(|(_, f)| f.clone())
        .collect();
    let stripped_schema = Arc::new(Schema::new(stripped_fields));
    let stripped_batches: Vec<RecordBatch> =
        arrow_json::ReaderBuilder::new(stripped_schema.clone())
            .build(BufReader::new(bytes))?
            .collect::<Result<Vec<_>, _>>()?;

    // Pull manual columns with an iterative scan. `serde_json::Value` recurses
    // on nested objects and overflows the Tokio worker stack on deep payloads
    // (tool results, traces). The scanner never builds that DOM.
    let mut objects: Vec<Vec<crate::json_scan::Field>> = Vec::new();
    for line in bytes.split(|&b| b == b'\n') {
        if line.iter().all(|b| b.is_ascii_whitespace()) {
            continue;
        }
        match crate::json_scan::scan_line(line).map_err(NdjsonError::Scan)? {
            crate::json_scan::Line::Object(fields) => objects.push(fields),
            crate::json_scan::Line::Other => objects.push(Vec::new()),
        }
    }
    let arrow_rows: usize = stripped_batches.iter().map(|b| b.num_rows()).sum();
    if arrow_rows != objects.len() {
        return Err(NdjsonError::Scan(format!(
            "row count mismatch: arrow decoded {arrow_rows} rows, scanner saw {}",
            objects.len()
        )));
    }

    // ---- Build vector arrays ----
    let mut manual_arrays: Vec<(usize, ArrayRef)> = Vec::new();

    for (pos, name, dim) in &vector_cols {
        let mut flat: Vec<f32> = Vec::with_capacity(objects.len() * *dim as usize);
        for row in &objects {
            let val =
                crate::json_scan::find(row, name).ok_or_else(|| NdjsonError::VectorWrongType {
                    column: name.clone(),
                    got: "missing".into(),
                })?;
            let raw = match &val.value {
                crate::json_scan::JsonVal::Raw(raw) if raw.first() == Some(&b'[') => *raw,
                other => {
                    return Err(NdjsonError::VectorWrongType {
                        column: name.clone(),
                        got: json_val_type_name(other).into(),
                    })
                }
            };
            let arr = parse_f32_array(raw).map_err(|e| NdjsonError::VectorWrongType {
                column: name.clone(),
                got: e,
            })?;
            if arr.len() != *dim as usize {
                return Err(NdjsonError::VectorDimensionMismatch {
                    column: name.clone(),
                    dimension: *dim,
                    got: arr.len(),
                });
            }
            for (idx, item) in arr.iter().enumerate() {
                let Some(f) = *item else {
                    return Err(NdjsonError::VectorNonNumeric {
                        column: name.clone(),
                        index: idx,
                        value: "non-numeric".into(),
                    });
                };
                flat.push(f);
            }
        }
        let values = Float32Array::from(flat);
        let inner_field = Arc::new(Field::new("item", DataType::Float32, false));
        let arr = FixedSizeListArray::new(inner_field, *dim, Arc::new(values), None);
        manual_arrays.push((*pos, Arc::new(arr) as ArrayRef));
    }

    // ---- Build binary (dynamic) arrays ----
    // For each Binary column, the JSON value is encoded as its UTF-8 JSON
    // representation (or the raw string bytes if the value is a JSON string).
    // This matches pensieve's `dynamic` semantic: the stored bytes are the JSON.
    for (pos, name) in &binary_cols {
        let mut bufs: Vec<Option<Vec<u8>>> = Vec::with_capacity(objects.len());
        for row in &objects {
            match crate::json_scan::find(row, name).map(|f| &f.value) {
                None | Some(crate::json_scan::JsonVal::Null) => bufs.push(None),
                Some(crate::json_scan::JsonVal::String(s)) => {
                    bufs.push(Some(s.as_bytes().to_vec()))
                }
                Some(crate::json_scan::JsonVal::Number(n)) => {
                    bufs.push(Some(n.as_bytes().to_vec()))
                }
                Some(crate::json_scan::JsonVal::Bool(true)) => bufs.push(Some(b"true".to_vec())),
                Some(crate::json_scan::JsonVal::Bool(false)) => bufs.push(Some(b"false".to_vec())),
                Some(crate::json_scan::JsonVal::Raw(raw)) => bufs.push(Some(raw.to_vec())),
            }
        }
        let arr = BinaryArray::from(
            bufs.iter()
                .map(|opt| opt.as_deref())
                .collect::<Vec<Option<&[u8]>>>(),
        );
        manual_arrays.push((*pos, Arc::new(arr) as ArrayRef));
    }

    // Sort by position so the splice loop below is index-stable.
    manual_arrays.sort_by_key(|(pos, _)| *pos);

    // Splice back: build full RecordBatches by merging stripped batches with
    // manual arrays. arrow-json may emit multiple batches; slice manual arrays
    // accordingly.
    let mut out = Vec::with_capacity(stripped_batches.len());
    let mut offset: usize = 0;
    for stripped in stripped_batches {
        let nrows = stripped.num_rows();
        let mut full_cols: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
        let mut stripped_idx: usize = 0;
        for (i, _) in schema.fields().iter().enumerate() {
            if let Some((_, arr)) = manual_arrays.iter().find(|(vi, _)| *vi == i) {
                full_cols.push(arr.slice(offset, nrows));
            } else {
                full_cols.push(stripped.column(stripped_idx).clone());
                stripped_idx += 1;
            }
        }
        out.push(RecordBatch::try_new(schema.clone(), full_cols)?);
        offset += nrows;
    }
    Ok(out)
}

fn json_val_type_name(v: &crate::json_scan::JsonVal<'_>) -> &'static str {
    match v {
        crate::json_scan::JsonVal::Null => "null",
        crate::json_scan::JsonVal::Bool(_) => "bool",
        crate::json_scan::JsonVal::Number(_) => "number",
        crate::json_scan::JsonVal::String(_) => "string",
        crate::json_scan::JsonVal::Raw(raw) => match raw.first() {
            Some(b'[') => "array",
            Some(b'{') => "object",
            _ => "value",
        },
    }
}

/// Flat JSON array of numbers. `None` entries are non-numeric elements so the
/// caller can report the index. Nested arrays are rejected.
fn parse_f32_array(raw: &[u8]) -> Result<Vec<Option<f32>>, String> {
    let s = std::str::from_utf8(raw).map_err(|_| "vector is not utf-8".to_string())?;
    let s = s.trim();
    if !s.starts_with('[') || !s.ends_with(']') {
        return Err("expected array of floats".into());
    }
    let inner = s[1..s.len() - 1].trim();
    if inner.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for part in inner.split(',') {
        let p = part.trim();
        if p.is_empty() {
            return Err("empty array element".into());
        }
        match p.parse::<f64>() {
            Ok(f) => out.push(Some(f as f32)),
            Err(_) => out.push(None),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod binary_column_tests {
    use super::*;
    use arrow_array::Array;
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    fn make_schema_with_binary() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("props", DataType::Binary, true),
        ]))
    }

    #[test]
    fn binary_col_from_string_value() {
        let schema = make_schema_with_binary();
        let ndjson = b"{\"id\":\"a\",\"props\":\"{\\\"x\\\":1}\"}\n";
        let batches = parse_ndjson(ndjson, schema).expect("parse ok");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let arr = batches[0]
            .column_by_name("props")
            .expect("props column exists");
        let bin = arr
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("BinaryArray");
        assert_eq!(bin.value(0), b"{\"x\":1}");
    }

    #[test]
    fn binary_col_from_object_value() {
        let schema = make_schema_with_binary();
        let ndjson = b"{\"id\":\"b\",\"props\":{\"y\":2}}\n";
        let batches = parse_ndjson(ndjson, schema).expect("parse ok");
        let arr = batches[0]
            .column_by_name("props")
            .expect("props column exists");
        let bin = arr
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("BinaryArray");
        let stored: serde_json::Value =
            serde_json::from_slice(bin.value(0)).expect("stored bytes are valid JSON");
        assert_eq!(stored["y"], serde_json::json!(2));
    }

    #[test]
    fn binary_col_deeply_nested_object_does_not_overflow() {
        let schema = make_schema_with_binary();
        let mut ndjson = String::from("{\"id\":\"a\",\"props\":");
        for _ in 0..2_000 {
            ndjson.push_str("{\"a\":");
        }
        ndjson.push('1');
        for _ in 0..2_000 {
            ndjson.push('}');
        }
        ndjson.push_str("}\n");
        let batches = parse_ndjson(ndjson.as_bytes(), schema).expect("deep payload must parse");
        let arr = batches[0].column_by_name("props").expect("props");
        let bin = arr.as_any().downcast_ref::<BinaryArray>().expect("bin");
        assert!(bin.value(0).starts_with(b"{"));
        assert!(bin.value(0).ends_with(b"}"));
    }

    #[test]
    fn binary_col_null_value() {
        let schema = make_schema_with_binary();
        let ndjson = b"{\"id\":\"c\",\"props\":null}\n";
        let batches = parse_ndjson(ndjson, schema).expect("parse ok");
        let arr = batches[0]
            .column_by_name("props")
            .expect("props column exists");
        let bin = arr
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("BinaryArray");
        assert!(bin.is_null(0));
    }
}

#[cfg(test)]
mod event_time_integration_tests {
    use super::*;
    use arrow_array::{Array, TimestampNanosecondArray};
    use arrow_schema::{Field, Schema, TimeUnit};
    use chrono::DateTime;
    use std::sync::Arc;

    /// The default table schema used by auto-created tables (mirrors `default_table_schema()`).
    fn make_default_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("at", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("label", DataType::Utf8, true),
            Field::new("body", DataType::Utf8, true),
            Field::new("props", DataType::Binary, true),
        ]))
    }

    /// A record that carries only `"timestamp"` (no explicit `"at"`) should
    /// produce a non-null `at` column whose value equals the expected instant.
    #[test]
    fn parse_ndjson_populates_at_from_timestamp_string() {
        let schema = make_default_schema();
        let ts = "2026-06-05T10:00:00Z";
        let ndjson = format!("{{\"timestamp\":\"{}\",\"body\":\"hello world\"}}\n", ts);
        let batches = parse_ndjson(ndjson.as_bytes(), schema).expect("parse ok");
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);

        let at_col = batch.column_by_name("at").expect("at column present");
        let at_arr = at_col
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("at column is TimestampNanosecondArray");

        assert!(
            !at_arr.is_null(0),
            "at should be non-null after timestamp injection"
        );

        // Verify the value matches the expected instant.
        let expected_nanos = DateTime::parse_from_rfc3339(ts)
            .expect("valid rfc3339")
            .timestamp_nanos_opt()
            .expect("in range");
        assert_eq!(
            at_arr.value(0),
            expected_nanos,
            "at nanoseconds should equal the timestamp instant"
        );
    }
}
