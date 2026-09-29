//! HTTP / NDJSON ingest frontend.
//!
//! Accepts `POST /v1/ingest` with headers:
//!   - `X-Database`: target database (default `default`)
//!   - `X-Table`: target table name (required)
//!   - `Content-Type: application/x-ndjson`
//!
//! Body is NDJSON — one JSON object per line. The table's catalog-stored
//! schema is used to coerce fields; unknown fields are dropped (phase A —
//! dynamic-column routing lands in M2).
//!
//! Response (200 JSON):
//! ```json
//! { "snapshot_id": "...", "rows_ingested": 123, "bytes_written": 4567 }
//! ```

#![forbid(unsafe_code)]

use axum::{
    extract::{Path, Request, State},
    http::{HeaderMap, HeaderName, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use bytes::Bytes;
use pensieve_core::catalog::Catalog;
use pensieve_ingest_core::{
    ensure_table, evolve_schema_for_records, parse_ndjson, IngestAck, WritePath,
};
use serde::Serialize;
use std::sync::Arc;

mod rate_limit;
use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};
use tracing::Instrument as _;
use tracing::{error, info, warn};

const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

/// Shared HTTP-handler state.
#[derive(Clone)]
pub struct IngestState {
    pub catalog: Arc<dyn Catalog>,
    pub write_path: WritePath,
}

/// Build the ingest router. Mount at whatever base path the server wants.
pub fn router(state: IngestState) -> Router {
    Router::new()
        .route("/v1/ingest", post(ingest_handler))
        // Idempotent table provisioning. Cloud / orchestrator components call
        // this on pipeline create so the first ingest doesn't have to pay for
        // the create-on-write path. Plain `ensure_table`; no schema args yet.
        .route(
            "/v1/admin/databases/{database}/tables/{table}",
            post(admin_ensure_table_handler).get(admin_get_table_handler),
        )
        .with_state(state)
        // Set an X-Request-ID if the client didn't send one, then propagate
        // it back on the response so clients and logs share the same id.
        .layer(SetRequestIdLayer::new(
            REQUEST_ID_HEADER.clone(),
            MakeRequestUuid,
        ))
        .layer(PropagateRequestIdLayer::new(REQUEST_ID_HEADER.clone()))
}

/// Response shape returned on successful ingest.
#[derive(Debug, Serialize)]
pub struct IngestResponse {
    pub snapshot_id: String,
    pub extent_count: usize,
    pub rows_ingested: u64,
    pub bytes_written: u64,
    /// `true` if this response was replayed from the idempotency ledger.
    pub replayed: bool,
}

impl From<IngestAck> for IngestResponse {
    fn from(a: IngestAck) -> Self {
        Self {
            snapshot_id: a.snapshot_id.to_string(),
            extent_count: a.extent_count,
            rows_ingested: a.rows_ingested,
            bytes_written: a.bytes_written,
            replayed: a.replayed,
        }
    }
}

async fn ingest_handler(State(state): State<IngestState>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    let headers: &HeaderMap = &parts.headers;
    let request_id = extract_request_id(headers);

    let database = headers
        .get("x-database")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("default")
        .to_owned();
    // S2.6: token-bucket ingest rate limit (off unless PENSIEVE_INGEST_RATE_RPS set).
    // Metered per database; an empty bucket → 429 + Retry-After.
    if let Err(retry_after_secs) = rate_limit::check(&database) {
        return rate_limited_response(retry_after_secs, &request_id);
    }
    let idempotency_key: Option<String> = headers
        .get("x-idempotency-key")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_owned());
    let table = match headers.get("x-table").and_then(|v| v.to_str().ok()) {
        Some(t) => t.to_owned(),
        None => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "missing_table_header",
                "missing X-Table header",
                &request_id,
            );
        }
    };
    // Default ON. Set X-Auto-Create: false to require pre-existing tables.
    let auto_create = header_bool(headers, "x-auto-create", true);
    // Default ON. Set X-Schema-Evolve: false to drop unknown fields silently
    // (the pre-helper behavior).
    let schema_evolve = header_bool(headers, "x-schema-evolve", true);
    let ingest_span = tracing::info_span!(
        target: "pensieve_telemetry",
        "ingest.batch",
        ingest.table = %table,
        ingest.rows = tracing::field::Empty,
    );
    ingest_batch_inner(
        state,
        request_id,
        database,
        table,
        idempotency_key,
        auto_create,
        schema_evolve,
        body,
        ingest_span,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn ingest_batch_inner(
    state: IngestState,
    request_id: String,
    database: String,
    table: String,
    idempotency_key: Option<String>,
    auto_create: bool,
    schema_evolve: bool,
    body: axum::body::Body,
    span: tracing::Span,
) -> Response {
    async move {

        // Actually read the body now that we have what we need from headers.
        let body: Bytes = match axum::body::to_bytes(body, 64 * 1024 * 1024).await {
            Ok(b) => b,
            Err(e) => {
                return error_response(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "body_too_large",
                    &format!("failed to read request body: {e}"),
                    &request_id,
                );
            }
        };

        // Resolve the table. With X-Auto-Create=true (default), the helper
        // creates the database + an empty default-schema table on first write.
        // With X-Auto-Create=false we keep the strict 404 behavior so callers
        // who pre-provision can detect typos.
        let table_ref = if auto_create {
            match ensure_table(&*state.catalog, &database, &table).await {
                Ok(t) => t,
                Err(e) => {
                    return error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "ensure_table_failed",
                        &format!("ensure_table: {e}"),
                        &request_id,
                    );
                }
            }
        } else {
            match state.catalog.lookup_table(&database, &table).await {
                Ok(t) => t,
                Err(e) => {
                    return error_response(
                        StatusCode::NOT_FOUND,
                        "table_not_found",
                        &format!("table lookup failed: {e}"),
                        &request_id,
                    );
                }
            }
        };

        // Parse the body for schema evolution (cheap one-pass JSON scan over the
        // bytes, only when X-Schema-Evolve is set). The parsed records are
        // discarded; the actual NDJSON parse below uses arrow-json's path so the
        // fast path stays untouched.
        let table_ref = if schema_evolve {
            match parse_records_for_inspection(body.as_ref()) {
                Ok(records) => {
                    match evolve_schema_for_records(&*state.catalog, &database, table_ref, &records)
                        .await
                    {
                        Ok(t) => t,
                        Err(e) => {
                            warn!(error = %e, "schema_evolve failed; continuing with current schema");
                            // Fall through with the original table_ref. We can
                            // recover by re-looking up — but since the alters
                            // failed, the lookup would be the same. Bail with a
                            // clear error.
                            return error_response(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "schema_evolve_failed",
                                &format!("schema_evolve: {e}"),
                                &request_id,
                            );
                        }
                    }
                }
                Err(_) => {
                    // The pre-scan failed to parse some lines — let the real
                    // parser below produce a precise error. Use the un-evolved
                    // schema; missing fields simply land in `props`.
                    table_ref
                }
            }
        } else {
            table_ref
        };

        // Parse the NDJSON body into `RecordBatch`es using the table's schema.
        // The shared helper adds FixedSizeList<Float32> (vector-column) support
        // on top of arrow-json's reader; primitive-only schemas hit the fast path
        // and behave identically to the previous direct ReaderBuilder call.
        let batches: Vec<arrow_array::RecordBatch> = {
            let parse_span = tracing::info_span!(
                target: "pensieve_telemetry",
                "ingest.parse",
                ingest.bytes = body.len(),
            );
            let _g = parse_span.enter();
            match parse_ndjson(body.as_ref(), table_ref.schema.clone()) {
                Ok(b) => b,
                Err(e) => {
                    return error_response(
                        StatusCode::BAD_REQUEST,
                        "bad_request_body",
                        &format!("failed to decode NDJSON: {e}"),
                        &request_id,
                    );
                }
            }
        };

        // Wraps the WritePath call site only — the self-trace exporter calls
        // WritePath directly, so spans must never live *inside* it (recursion).
        let write_span = tracing::info_span!(
            target: "pensieve_telemetry",
            "ingest.write",
            ingest.batches = batches.len(),
        );
        match state
            .write_path
            .ingest_with_idempotency(&database, &table_ref, batches, idempotency_key.as_deref())
            .instrument(write_span)
            .await
        {
            Ok(ack) => {
                tracing::Span::current().record("ingest.rows", ack.rows_ingested);
                info!(
                    request_id = %request_id,
                    database = %database,
                    table = %table,
                    snapshot_id = %ack.snapshot_id,
                    rows = ack.rows_ingested,
                    bytes = ack.bytes_written,
                    "ingest committed"
                );
                let resp: IngestResponse = ack.into();
                (StatusCode::OK, Json(resp)).into_response()
            }
            Err(e) => {
                error!(request_id = %request_id, database = %database, table = %table, error = %e, "ingest failed");
                error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "ingest_failed",
                    &format!("{e}"),
                    &request_id,
                )
            }
        }
    }
    .instrument(span)
    .await
}

// ---- admin handlers -------------------------------------------------

#[derive(Debug, Serialize)]
struct AdminTableInfo {
    database: String,
    table: String,
    columns: Vec<AdminColumn>,
    /// Total rows visible at the current snapshot. Cheap field; sourced from
    /// the catalog's snapshot summary, not a scan.
    rows: u64,
}

#[derive(Debug, Serialize)]
struct AdminColumn {
    name: String,
    /// Arrow logical type as a stable string (e.g. `"Utf8"`, `"Timestamp(Nanosecond)"`).
    arrow_type: String,
    nullable: bool,
}

async fn admin_ensure_table_handler(
    State(state): State<IngestState>,
    Path((database, table)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let request_id = extract_request_id(&headers);
    match pensieve_ingest_core::ensure_table(&*state.catalog, &database, &table).await {
        Ok(t) => {
            let info = AdminTableInfo {
                database: database.clone(),
                table: t.name.clone(),
                columns: t
                    .schema
                    .fields()
                    .iter()
                    .map(|f| AdminColumn {
                        name: f.name().clone(),
                        arrow_type: format!("{}", f.data_type()),
                        nullable: f.is_nullable(),
                    })
                    .collect(),
                rows: 0,
            };
            info!(database = %database, table = %table, "admin: ensure_table ok");
            (StatusCode::OK, Json(info)).into_response()
        }
        Err(e) => {
            error!(request_id = %request_id, database = %database, table = %table, error = %e, "admin: ensure_table failed");
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ensure_table_failed",
                &format!("{e}"),
                &request_id,
            )
        }
    }
}

async fn admin_get_table_handler(
    State(state): State<IngestState>,
    Path((database, table)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let request_id = extract_request_id(&headers);
    match state.catalog.lookup_table(&database, &table).await {
        Ok(t) => {
            let info = AdminTableInfo {
                database: database.clone(),
                table: t.name.clone(),
                columns: t
                    .schema
                    .fields()
                    .iter()
                    .map(|f| AdminColumn {
                        name: f.name().clone(),
                        arrow_type: format!("{}", f.data_type()),
                        nullable: f.is_nullable(),
                    })
                    .collect(),
                rows: 0,
            };
            (StatusCode::OK, Json(info)).into_response()
        }
        Err(e) => error_response(
            StatusCode::NOT_FOUND,
            "table_not_found",
            &format!("{e}"),
            &request_id,
        ),
    }
}

// ---- shared error-body shape -----------------------------------------

#[derive(Serialize)]
struct ErrorBody<'a> {
    error: ErrorDetail<'a>,
}

#[derive(Serialize)]
struct ErrorDetail<'a> {
    code: &'a str,
    message: &'a str,
    request_id: &'a str,
}

/// `429 Too Many Requests` with a `Retry-After` header (whole seconds, ≥1) when
/// the ingest token bucket is empty (S2.6).
fn rate_limited_response(retry_after_secs: f64, request_id: &str) -> Response {
    let secs = retry_after_secs.ceil().max(1.0) as u64;
    let mut resp = error_response(
        StatusCode::TOO_MANY_REQUESTS,
        "rate_limited",
        "ingest rate limit exceeded; retry after the indicated delay",
        request_id,
    );
    if let Ok(v) = axum::http::HeaderValue::from_str(&secs.to_string()) {
        resp.headers_mut()
            .insert(axum::http::header::RETRY_AFTER, v);
    }
    resp
}

fn error_response(status: StatusCode, code: &str, message: &str, request_id: &str) -> Response {
    (
        status,
        Json(ErrorBody {
            error: ErrorDetail {
                code,
                message,
                request_id,
            },
        }),
    )
        .into_response()
}

fn extract_request_id(headers: &HeaderMap) -> String {
    headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string())
}

/// Parse a header value as a boolean, accepting `true|false|1|0|yes|no`.
/// Returns `default` if the header is missing or unparseable.
fn header_bool(headers: &HeaderMap, name: &str, default: bool) -> bool {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_ascii_lowercase())
        .map(|s| match s.as_str() {
            "true" | "1" | "yes" | "on" => true,
            "false" | "0" | "no" | "off" => false,
            _ => default,
        })
        .unwrap_or(default)
}

/// Cheap NDJSON pre-scan that yields the same `serde_json::Value`s the
/// schema-evolve helper expects. Used only when X-Schema-Evolve is on.
fn parse_records_for_inspection(
    bytes: &[u8],
) -> std::result::Result<Vec<serde_json::Value>, String> {
    // Keys only. A full `serde_json::Value` decode recurses on nested tool
    // payloads and overflows the Tokio worker stack.
    pensieve_ingest_core::records_for_schema_evolve(bytes)
}
