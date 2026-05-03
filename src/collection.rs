use std::sync::Arc;

use crate::error::Result;

pub type MigrateFn = Arc<dyn Fn(u32, &[u8]) -> Result<Vec<u8>> + Send + Sync>;

/// Controls whether records in a collection are encrypted.
/// Resolved with precedence: item-level > collection-level > engine-level.
#[derive(Debug, Clone, Default)]
pub enum CollectionEncryption {
    /// Inherit the engine-level setting (default).
    #[default]
    Default,
    /// Always encrypt records in this collection.
    Enabled,
    /// Never encrypt records in this collection.
    Disabled,
}

/// Definition of a shadow index field for a collection.
#[derive(Debug, Clone)]
pub struct IndexDef {
    /// Dot-notation path into the JSON payload, e.g. `"metadata.year"`.
    pub field_path: String,
    pub kind: IndexKind,
}

impl IndexDef {
    pub fn new(field_path: impl Into<String>, kind: IndexKind) -> Self {
        Self { field_path: field_path.into(), kind }
    }
}

#[derive(Debug, Clone)]
pub enum IndexKind {
    Text,
    Integer,
    Float,
    /// JSON array of strings; stored as JSON text, queried with LIKE.
    TextArray,
}

/// Configuration for a named collection. Pass to [`crate::EngineBuilder::collection`].
#[derive(Clone, Default)]
pub struct CollectionConfig {
    /// Expected schema version. If a stored record has a lower version, `migrate` is called.
    pub schema_version: u32,
    /// Encryption override for this collection. Defaults to inheriting the engine setting.
    pub encryption: CollectionEncryption,
    /// Optional migration hook. Receives `(old_version, old_data)`, returns `new_data`.
    /// Implemented in Phase 4.
    pub migrate: Option<MigrateFn>,
    /// Fields to index for structured queries. Shadow table created at startup.
    /// Implemented in Phase 4.
    pub indices: Vec<IndexDef>,
    /// Fields to include in the FTS5 full-text index.
    /// Implemented in Phase 4.
    pub fts_fields: Vec<String>,
}

impl std::fmt::Debug for CollectionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CollectionConfig")
            .field("schema_version", &self.schema_version)
            .field("encryption", &self.encryption)
            .field("has_migrate", &self.migrate.is_some())
            .field("indices", &self.indices.len())
            .field("fts_fields", &self.fts_fields)
            .finish()
    }
}
