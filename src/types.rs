use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

pub use ulid::Ulid;

use crate::hlc::Hlc;

// ── Record types ──────────────────────────────────────────────────────────────

/// A record returned by [`crate::SquirrelEngine::get`] — includes the full data payload.
#[derive(Debug, Clone)]
pub struct Record {
    pub id: Ulid,
    pub collection: String,
    /// Raw bytes as stored by the caller. Decrypted automatically when encryption is enabled.
    pub data: Vec<u8>,
    pub hlc: Hlc,
    pub schema_version: u32,
    pub deleted: bool,
    pub created_at: u64,
    pub updated_at: u64,
}

/// A record header returned by [`crate::SquirrelEngine::list`] — no data bytes.
#[derive(Debug, Clone)]
pub struct RecordMeta {
    pub id: Ulid,
    pub collection: String,
    pub hlc: Hlc,
    pub schema_version: u32,
    pub deleted: bool,
    pub created_at: u64,
    pub updated_at: u64,
}

/// Options for [`crate::SquirrelEngine::put`].
#[derive(Debug, Clone, Default)]
pub struct PutOpts {
    pub encryption: ItemEncryption,
    pub schema_version: Option<u32>,
    /// Values to write into the shadow index for this record.
    /// Keys must match field names declared in [`IndexDef::fields`] or [`IndexDef::fts_fields`].
    pub index_fields: std::collections::HashMap<String, IndexValue>,
}

/// Per-item encryption override. Resolved as: item > collection > engine.
#[derive(Debug, Clone, Default)]
pub enum ItemEncryption {
    /// Inherit from the collection setting, then the engine setting.
    #[default]
    Default,
    /// Force encryption for this specific item, regardless of defaults.
    Enabled,
    /// Force plaintext for this specific item, regardless of defaults.
    Disabled,
}

/// Options for [`crate::SquirrelEngine::list`].
#[derive(Debug, Clone)]
pub struct ListOpts {
    pub limit: Option<usize>,
    pub offset: usize,
    pub include_deleted: bool,
    pub order: SortOrder,
}

impl Default for ListOpts {
    fn default() -> Self {
        Self {
            limit: None,
            offset: 0,
            include_deleted: false,
            order: SortOrder::HlcDesc,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub enum SortOrder {
    #[default]
    HlcDesc,
    HlcAsc,
}

// ── Blob types ────────────────────────────────────────────────────────────────

/// Opaque identifier for a staged/uploaded blob (a ULID string).
pub type BlobId = String;

/// Lifecycle state of a blob.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobStatus {
    /// Staged locally, waiting to be uploaded to the remote.
    Pending,
    /// Upload is in progress (upload_id obtained, parts being sent).
    Uploading,
    /// Upload complete — the blob lives on the remote backend.
    Uploaded,
    /// Blob is available in the local cache (downloaded from remote).
    Cached,
}

impl BlobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending   => "pending",
            Self::Uploading => "uploading",
            Self::Uploaded  => "uploaded",
            Self::Cached    => "cached",
        }
    }

    pub(crate) fn from_db_str(s: &str) -> Option<Self> {
        match s {
            "pending"   => Some(Self::Pending),
            "uploading" => Some(Self::Uploading),
            "uploaded"  => Some(Self::Uploaded),
            "cached"    => Some(Self::Cached),
            _           => None,
        }
    }
}

/// Information about a blob returned by [`crate::SquirrelEngine::blob_info`].
#[derive(Debug, Clone)]
pub struct BlobInfo {
    pub id: BlobId,
    pub status: BlobStatus,
    /// Absolute path to the locally cached file, if available.
    pub local_path: Option<PathBuf>,
    pub size_bytes: Option<u64>,
    pub retries: u32,
    pub last_error: Option<String>,
}

/// Options for [`crate::SquirrelEngine::put_blob`].
#[derive(Debug, Clone, Default)]
pub struct PutBlobOpts {
    /// Associate this blob with a specific record.
    pub record_id: Option<String>,
    pub collection: Option<String>,
}

// ── Sync types ────────────────────────────────────────────────────────────────

/// Summary of a completed sync cycle.
#[derive(Debug, Clone, Default)]
pub struct SyncStats {
    pub pushed: usize,
    pub pulled: usize,
    pub conflicts: usize,
    pub errors: usize,
}

/// Events emitted by the sync/blob engine. Subscribe via [`crate::SquirrelEngine::sync_events`].
#[derive(Debug, Clone)]
pub enum SyncEvent {
    PushComplete(SyncStats),
    PullComplete(SyncStats),
    BlobUploaded { blob_id: BlobId },
    BlobDownloaded { blob_id: BlobId },
    RetryScheduled { seq: i64, retries: u32, next_retry_ms: u64, error: String },
    BlobRetryScheduled { blob_id: BlobId, retries: u32, next_retry_ms: u64, error: String },
}

/// An outbox entry that has experienced at least one failed sync attempt.
#[derive(Debug, Clone)]
pub struct PendingError {
    pub seq: i64,
    pub record_id: String,
    pub collection: String,
    pub retries: u32,
    /// Most recent error message.
    pub last_error: Option<String>,
    /// Unix ms timestamp of the next scheduled retry attempt.
    pub next_retry_at_ms: u64,
}

// ── Index types ───────────────────────────────────────────────────────────────

/// Declares which fields of a collection to index for fast querying.
#[derive(Debug, Clone)]
pub struct IndexDef {
    pub collection: String,
    /// Scalar fields stored in the shadow index table.
    pub fields: Vec<FieldDef>,
    /// Fields to include in the FTS5 full-text index (must be text).
    pub fts_fields: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct FieldDef {
    pub name: String,
    pub affinity: ColumnAffinity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnAffinity {
    Text,
    Integer,
    Real,
}

impl ColumnAffinity {
    pub(crate) fn as_sql_type(self) -> &'static str {
        match self {
            Self::Text    => "TEXT",
            Self::Integer => "INTEGER",
            Self::Real    => "REAL",
        }
    }
}

/// A typed value for a shadow-index field, supplied at [`crate::SquirrelEngine::put`] time.
#[derive(Debug, Clone)]
pub enum IndexValue {
    Text(String),
    Integer(i64),
    Real(f64),
    Null,
}

/// A filter expression used by [`crate::SquirrelEngine::query`].
#[derive(Debug, Clone)]
pub enum QueryFilter {
    Eq      { field: String, value: IndexValue },
    Lt      { field: String, value: IndexValue },
    Gt      { field: String, value: IndexValue },
    Le      { field: String, value: IndexValue },
    Ge      { field: String, value: IndexValue },
    /// Full-text search across all FTS-indexed fields for this collection.
    Contains { text: String },
    And(Vec<QueryFilter>),
    Or(Vec<QueryFilter>),
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Current millisecond unix timestamp as i64 (matches SQLite INTEGER storage).
pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
