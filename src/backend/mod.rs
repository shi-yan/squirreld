use std::path::Path;

use async_trait::async_trait;

#[cfg(feature = "dynamodb")]
pub mod dynamodb;

#[cfg(feature = "s3")]
pub mod s3;

#[cfg(feature = "test-utils")]
pub mod in_memory;

// ── Record backend wire types ──────────────────────────────────────────────────

/// A single outbox entry ready to be pushed to the remote backend.
#[derive(Debug, Clone)]
pub struct OutboxPushEntry {
    pub seq: i64,
    pub record_id: String,
    pub collection: String,
    /// "upsert" or "delete"
    pub operation: String,
    pub hlc: String,
    /// Raw payload bytes (ciphertext when `format_version=1`, plaintext otherwise).
    /// None for delete tombstones.
    pub data: Option<Vec<u8>>,
    /// Wrapped Data Encryption Key. Present only when `format_version=1`.
    pub dek_encrypted: Option<Vec<u8>>,
    pub schema_version: u32,
    /// 0 = plaintext, 1 = AES-256-GCM encrypted.
    pub format_version: u8,
    pub retries: u32,
}

/// A record received from the remote backend during a pull.
#[derive(Debug, Clone)]
pub struct RemoteRecord {
    pub record_id: String,
    pub collection: String,
    pub hlc: String,
    /// Raw payload bytes (ciphertext when `format_version=1`, plaintext otherwise).
    /// None for delete tombstones.
    pub data: Option<Vec<u8>>,
    /// Wrapped DEK; present only when `format_version=1`.
    pub dek_encrypted: Option<Vec<u8>>,
    pub deleted: bool,
    pub schema_version: u32,
    /// 0 = plaintext, 1 = AES-256-GCM encrypted.
    pub format_version: u8,
}

/// Result of a single push attempt.
#[derive(Debug)]
pub enum PushResult {
    /// Entry was accepted by the remote.
    Ok { pushed_seqs: Vec<i64> },
    /// The remote has a higher (or equal) HLC for this record; a pull is needed.
    ConflictAt { record_id: String, seq: i64 },
    /// Transient network or service error; caller should schedule a retry.
    TransientError(String),
}

/// Error type returned by backend operations.
#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    #[error("transient: {0}")]
    Transient(String),
    #[error("configuration error: {0}")]
    Config(String),
}

// ── RecordBackend trait ────────────────────────────────────────────────────────

/// Abstraction over a remote record store.
#[async_trait]
pub trait RecordBackend: Send + Sync + 'static {
    fn backend_id(&self) -> &str;

    async fn push_one(&self, entry: &OutboxPushEntry) -> PushResult;

    /// Return all records with HLC > `checkpoint`. Pass `None` for all records.
    /// Results MUST be sorted by HLC ascending.
    async fn pull_since(
        &self,
        checkpoint: Option<&str>,
    ) -> Result<Vec<RemoteRecord>, BackendError>;

    /// Create the backing table/index if absent. Idempotent.
    async fn ensure_table(&self) -> Result<(), BackendError>;
}

// ── Blob backend wire types ────────────────────────────────────────────────────

/// A part that has already been uploaded to a multipart session.
#[derive(Debug, Clone)]
pub struct UploadedPart {
    pub part_number: i32,
    pub etag: String,
}

// ── BlobBackend trait ─────────────────────────────────────────────────────────

/// Abstraction over a remote blob / object store.
///
/// Upload flow for large blobs (≥ `MULTIPART_THRESHOLD`):
/// 1. `create_multipart_upload` → `upload_id`
/// 2. `upload_part` × N → ETags stored locally
/// 3. `complete_multipart_upload` with all parts
///
/// Resume flow (engine restart mid-upload):
/// 1. `list_parts` → discover already-uploaded parts
/// 2. Upload only the missing parts
/// 3. `complete_multipart_upload`
#[async_trait]
pub trait BlobBackend: Send + Sync + 'static {
    fn backend_id(&self) -> &str;

    /// Create the bucket/container if it does not already exist. Idempotent.
    async fn ensure_bucket(&self) -> Result<(), BackendError>;

    /// Upload a small blob (< `MULTIPART_THRESHOLD`) in a single request.
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), BackendError>;

    /// Initiate a multipart upload session. Returns the `upload_id`.
    async fn create_multipart_upload(&self, key: &str) -> Result<String, BackendError>;

    /// Upload one part of a multipart upload. Returns the ETag for this part.
    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        data: Vec<u8>,
    ) -> Result<String, BackendError>;

    /// List parts that have already been uploaded for a given multipart session.
    /// Returns an empty Vec (not an error) if the upload_id has expired.
    async fn list_parts(
        &self,
        key: &str,
        upload_id: &str,
    ) -> Result<Vec<UploadedPart>, BackendError>;

    /// Finalise a multipart upload. `parts` must be sorted by part_number ascending.
    async fn complete_multipart_upload(
        &self,
        key: &str,
        upload_id: &str,
        parts: Vec<UploadedPart>,
    ) -> Result<(), BackendError>;

    /// Download an object and write it to `dest`. Returns the number of bytes written.
    async fn get_object(&self, key: &str, dest: &Path) -> Result<u64, BackendError>;

    /// Delete an object. No-op if the object does not exist.
    async fn delete_object(&self, key: &str) -> Result<(), BackendError>;
}
