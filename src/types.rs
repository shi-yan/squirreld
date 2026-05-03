use std::time::{SystemTime, UNIX_EPOCH};

pub use ulid::Ulid;

use crate::hlc::Hlc;

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

/// Summary of a completed sync cycle.
#[derive(Debug, Clone, Default)]
pub struct SyncStats {
    pub pushed: usize,
    pub pulled: usize,
    pub conflicts: usize,
    pub errors: usize,
}

/// Events emitted by the sync engine. Subscribe via [`crate::SquirrelEngine::sync_events`].
#[derive(Debug, Clone)]
pub enum SyncEvent {
    PushComplete(SyncStats),
    PullComplete(SyncStats),
    BlobUploaded { blob_id: String },
    RetryScheduled { seq: i64, retries: u32, next_retry_ms: u64, error: String },
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

/// Current millisecond unix timestamp as i64 (matches SQLite INTEGER storage).
pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
