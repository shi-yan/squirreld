use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use async_trait::async_trait;

use crate::backend::{BackendError, OutboxPushEntry, PushResult, RecordBackend, RemoteRecord};

/// A fully in-memory `RecordBackend` for unit tests.
///
/// Two `InMemoryBackend` instances that share the same `InMemoryStore` behave
/// as two devices syncing through the same remote.  Create a shared store with
/// [`InMemoryStore::new_shared`] and pass it to each backend.
#[derive(Clone)]
pub struct InMemoryBackend {
    id: String,
    store: Arc<Mutex<InMemoryStore>>,
}

/// Shared remote state underlying one or more [`InMemoryBackend`] instances.
pub struct InMemoryStore {
    /// (record_id, collection) → RemoteRecord
    records: HashMap<(String, String), RemoteRecord>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self { records: HashMap::new() }
    }

    /// Wrap in an `Arc<Mutex<>>` ready to be shared between backends.
    pub fn new_shared() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self::new()))
    }
}

impl Default for InMemoryStore {
    fn default() -> Self { Self::new() }
}

impl InMemoryBackend {
    pub fn new(id: impl Into<String>, store: Arc<Mutex<InMemoryStore>>) -> Self {
        Self { id: id.into(), store }
    }

    /// Convenience: create a backend that does not share state with any other.
    pub fn standalone(id: impl Into<String>) -> Self {
        Self::new(id, InMemoryStore::new_shared())
    }
}

#[async_trait]
impl RecordBackend for InMemoryBackend {
    fn backend_id(&self) -> &str {
        &self.id
    }

    async fn push_one(&self, entry: &OutboxPushEntry) -> PushResult {
        let mut store = self.store.lock().await;
        let key = (entry.record_id.clone(), entry.collection.clone());

        // LWW condition: accept only if no remote record or our HLC is strictly higher.
        if let Some(existing) = store.records.get(&key) {
            if existing.hlc >= entry.hlc {
                return PushResult::ConflictAt {
                    record_id: entry.record_id.clone(),
                    seq: entry.seq,
                };
            }
        }

        store.records.insert(
            key,
            RemoteRecord {
                record_id:      entry.record_id.clone(),
                collection:     entry.collection.clone(),
                hlc:            entry.hlc.clone(),
                data:           entry.data.clone(),
                deleted:        entry.operation == "delete",
                schema_version: entry.schema_version,
                format_version: entry.format_version,
            },
        );
        PushResult::Ok { pushed_seqs: vec![entry.seq] }
    }

    async fn pull_since(
        &self,
        checkpoint: Option<&str>,
    ) -> Result<Vec<RemoteRecord>, BackendError> {
        let store = self.store.lock().await;
        let mut results: Vec<RemoteRecord> = store
            .records
            .values()
            .filter(|r| match checkpoint {
                None => true,
                Some(cp) => r.hlc.as_str() > cp,
            })
            .cloned()
            .collect();
        // Return in ascending HLC order so the caller can advance the checkpoint.
        results.sort_by(|a, b| a.hlc.cmp(&b.hlc));
        Ok(results)
    }

    async fn ensure_table(&self) -> Result<(), BackendError> {
        Ok(()) // nothing to create for an in-memory store
    }
}
