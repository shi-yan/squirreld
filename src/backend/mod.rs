use async_trait::async_trait;

#[cfg(feature = "dynamodb")]
pub mod dynamodb;

#[cfg(feature = "test-utils")]
pub mod in_memory;

// ── Wire types ────────────────────────────────────────────────────────────────

/// A single outbox entry ready to be pushed to the remote backend.
#[derive(Debug, Clone)]
pub struct OutboxPushEntry {
    pub seq: i64,
    pub record_id: String,
    pub collection: String,
    /// "upsert" or "delete"
    pub operation: String,
    pub hlc: String,
    /// Raw bytes (present for upserts, None for deletes).
    pub data: Option<Vec<u8>>,
    pub schema_version: u32,
    pub format_version: u8,
    pub retries: u32,
}

/// A record received from the remote backend during a pull.
#[derive(Debug, Clone)]
pub struct RemoteRecord {
    pub record_id: String,
    pub collection: String,
    pub hlc: String,
    /// None means the record was deleted (tombstone).
    pub data: Option<Vec<u8>>,
    pub deleted: bool,
    pub schema_version: u32,
    pub format_version: u8,
}

/// Result of a single push attempt.
#[derive(Debug)]
pub enum PushResult {
    /// All entries in the batch were accepted.
    Ok { pushed_seqs: Vec<i64> },
    /// The remote has a higher (or equal) HLC for this record; a pull is needed.
    ConflictAt { record_id: String, seq: i64 },
    /// Transient network or service error; caller should schedule a retry.
    TransientError(String),
}

/// Error type returned by pull operations.
#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    #[error("transient: {0}")]
    Transient(String),
    #[error("configuration error: {0}")]
    Config(String),
}

// ── Trait ─────────────────────────────────────────────────────────────────────

/// Abstraction over a remote record store. Implement this to use a different
/// backend (e.g. a local test double, a custom HTTP API, etc.).
#[async_trait]
pub trait RecordBackend: Send + Sync + 'static {
    /// A stable identifier used as the backend key in `sync_state`.
    fn backend_id(&self) -> &str;

    /// Push a single outbox entry to the remote. The backend MUST enforce the
    /// LWW condition: only accept the write if `attribute_not_exists(hlc) OR
    /// hlc < :incoming_hlc`. Returns [`PushResult::ConflictAt`] if the remote
    /// already has a newer record.
    async fn push_one(&self, entry: &OutboxPushEntry) -> PushResult;

    /// Return all records whose HLC is strictly greater than `checkpoint`.
    /// Pass `None` to fetch everything. Results MUST be sorted by HLC ascending
    /// so the caller can advance the checkpoint incrementally.
    async fn pull_since(
        &self,
        checkpoint: Option<&str>,
    ) -> Result<Vec<RemoteRecord>, BackendError>;

    /// Create the backend table / bucket if it does not already exist.
    /// Called once during engine startup. Must be idempotent.
    async fn ensure_table(&self) -> Result<(), BackendError>;
}
