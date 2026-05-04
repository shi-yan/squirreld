use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::{
    backend::{BlobBackend, RecordBackend},
    collection::CollectionConfig,
    engine::SquirrelEngine,
    error::{Result, SquirrelError},
    types::KeySource,
};

/// Validated engine configuration produced by [`EngineBuilder::build`].
pub struct EngineConfig {
    pub(crate) db_path:          PathBuf,
    pub(crate) collections:      HashMap<String, CollectionConfig>,
    pub(crate) channel_capacity: usize,
    /// Optional record sync backend.
    pub(crate) record_backend:   Option<Arc<dyn RecordBackend>>,
    /// Optional blob store backend.
    pub(crate) blob_backend:     Option<Arc<dyn BlobBackend>>,
    /// Directory for locally cached blob files (default: `{db_dir}/blob_cache`).
    pub(crate) cache_dir:        Option<PathBuf>,
    /// Source of the Key Encryption Key. `None` means encryption is disabled.
    pub(crate) encryption_key:   Option<KeySource>,
}

/// Fluent builder for [`SquirrelEngine`].
///
/// ```rust,no_run
/// # use squirreld::{SquirrelEngine, collection::CollectionConfig};
/// # async fn example() -> squirreld::error::Result<()> {
/// let engine = SquirrelEngine::builder()
///     .db_path("/tmp/myapp.db")
///     .collection("papers", CollectionConfig::default())
///     .build()
///     .await?;
/// # Ok(()) }
/// ```
pub struct EngineBuilder {
    db_path:          Option<PathBuf>,
    collections:      HashMap<String, CollectionConfig>,
    channel_capacity: usize,
    record_backend:   Option<Arc<dyn RecordBackend>>,
    blob_backend:     Option<Arc<dyn BlobBackend>>,
    cache_dir:        Option<PathBuf>,
    encryption_key:   Option<KeySource>,
}

impl Default for EngineBuilder {
    fn default() -> Self {
        Self {
            db_path:          None,
            collections:      HashMap::new(),
            channel_capacity: 256,
            record_backend:   None,
            blob_backend:     None,
            cache_dir:        None,
            encryption_key:   None,
        }
    }
}

impl EngineBuilder {
    pub fn new() -> Self { Self::default() }

    /// Path to the SQLite database file. Required.
    pub fn db_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.db_path = Some(path.into());
        self
    }

    /// Register a collection.
    pub fn collection(mut self, name: impl Into<String>, config: CollectionConfig) -> Self {
        self.collections.insert(name.into(), config);
        self
    }

    /// Internal command channel capacity (default: 256).
    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    /// Attach a record sync backend (DynamoDB or in-memory test double).
    pub fn record_backend(mut self, backend: Arc<dyn RecordBackend>) -> Self {
        self.record_backend = Some(backend);
        self
    }

    /// Attach a blob store backend (S3 or in-memory test double).
    /// Also requires `db_path` so the engine can compute a default `cache_dir`.
    pub fn blob_backend(mut self, backend: Arc<dyn BlobBackend>) -> Self {
        self.blob_backend = Some(backend);
        self
    }

    /// Override the local blob cache directory (default: `{db_dir}/blob_cache`).
    pub fn cache_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.cache_dir = Some(dir.into());
        self
    }

    /// Enable at-rest encryption with AES-256-GCM.
    /// Provide a [`KeySource`] to supply or derive the Key Encryption Key.
    pub fn encryption_key(mut self, source: KeySource) -> Self {
        self.encryption_key = Some(source);
        self
    }

    /// Validate configuration and open the engine.
    pub async fn build(self) -> Result<SquirrelEngine> {
        let db_path = self.db_path.ok_or_else(|| {
            SquirrelError::Other("db_path is required — call .db_path() on the builder".into())
        })?;
        let config = EngineConfig {
            cache_dir:      self.cache_dir,
            collections:    self.collections,
            channel_capacity: self.channel_capacity,
            record_backend: self.record_backend,
            blob_backend:   self.blob_backend,
            encryption_key: self.encryption_key,
            db_path,
        };
        SquirrelEngine::open(config).await
    }
}
