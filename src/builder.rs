use std::collections::HashMap;
use std::path::PathBuf;

use crate::{
    collection::CollectionConfig,
    engine::SquirrelEngine,
    error::{Result, SquirrelError},
};

/// Validated engine configuration produced by [`EngineBuilder::build`].
pub struct EngineConfig {
    pub(crate) db_path: PathBuf,
    pub(crate) collections: HashMap<String, CollectionConfig>,
    pub(crate) channel_capacity: usize,
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
    db_path: Option<PathBuf>,
    collections: HashMap<String, CollectionConfig>,
    channel_capacity: usize,
}

impl Default for EngineBuilder {
    fn default() -> Self {
        Self {
            db_path: None,
            collections: HashMap::new(),
            channel_capacity: 256,
        }
    }
}

impl EngineBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Path to the SQLite database file. Required.
    pub fn db_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.db_path = Some(path.into());
        self
    }

    /// Register a collection. Can be called multiple times for different collections.
    pub fn collection(mut self, name: impl Into<String>, config: CollectionConfig) -> Self {
        self.collections.insert(name.into(), config);
        self
    }

    /// Capacity of the internal command channel (default: 256).
    /// Provides backpressure: callers block if the actor falls behind.
    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    /// Validate configuration and open the engine.
    pub async fn build(self) -> Result<SquirrelEngine> {
        let db_path = self.db_path.ok_or_else(|| {
            SquirrelError::Other("db_path is required — call .db_path() on the builder".into())
        })?;
        let config = EngineConfig {
            db_path,
            collections: self.collections,
            channel_capacity: self.channel_capacity,
        };
        SquirrelEngine::open(config).await
    }
}
