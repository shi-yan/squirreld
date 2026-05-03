pub mod collection;
pub mod error;
pub mod hlc;
pub mod types;

pub(crate) mod blob;
pub(crate) mod builder;
pub(crate) mod db;
pub(crate) mod engine;
pub(crate) mod sync;

pub mod backend;

pub use builder::EngineBuilder;
pub use db::index::QueryOpts;
pub use engine::SquirrelEngine;
pub use hlc::Hlc;
pub use types::{
    BlobId, BlobInfo, BlobStatus, ColumnAffinity, FieldDef, IndexDef, IndexValue,
    ItemEncryption, ListOpts, PendingError, PutBlobOpts, PutOpts, QueryFilter,
    Record, RecordMeta, SortOrder, SyncEvent, SyncStats, Ulid,
};

#[cfg(feature = "test-utils")]
pub use backend::in_memory::{
    InMemoryBackend, InMemoryBlobBackend, InMemoryBlobStore, InMemoryStore,
};
