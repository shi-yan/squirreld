use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::backend::{
    BackendError, BlobBackend, OutboxPushEntry, PushResult, RecordBackend, RemoteRecord,
    UploadedPart,
};

// ── InMemory Record Backend ───────────────────────────────────────────────────

/// Shared remote state underlying one or more [`InMemoryBackend`] instances.
pub struct InMemoryStore {
    /// (record_id, collection) → RemoteRecord
    records: HashMap<(String, String), RemoteRecord>,
}

impl InMemoryStore {
    pub fn new() -> Self { Self { records: HashMap::new() } }

    pub fn new_shared() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self::new()))
    }
}

impl Default for InMemoryStore { fn default() -> Self { Self::new() } }

/// A fully in-memory `RecordBackend` for unit tests.
///
/// Share an [`InMemoryStore`] between two backends to simulate two devices
/// syncing through the same remote.
#[derive(Clone)]
pub struct InMemoryBackend {
    id: String,
    store: Arc<Mutex<InMemoryStore>>,
}

impl InMemoryBackend {
    pub fn new(id: impl Into<String>, store: Arc<Mutex<InMemoryStore>>) -> Self {
        Self { id: id.into(), store }
    }

    pub fn standalone(id: impl Into<String>) -> Self {
        Self::new(id, InMemoryStore::new_shared())
    }
}

#[async_trait]
impl RecordBackend for InMemoryBackend {
    fn backend_id(&self) -> &str { &self.id }

    async fn push_one(&self, entry: &OutboxPushEntry) -> PushResult {
        let mut store = self.store.lock().await;
        let key = (entry.record_id.clone(), entry.collection.clone());

        if let Some(existing) = store.records.get(&key) {
            if existing.hlc >= entry.hlc {
                return PushResult::ConflictAt {
                    record_id: entry.record_id.clone(),
                    seq: entry.seq,
                };
            }
        }

        store.records.insert(key, RemoteRecord {
            record_id:      entry.record_id.clone(),
            collection:     entry.collection.clone(),
            hlc:            entry.hlc.clone(),
            data:           entry.data.clone(),
            deleted:        entry.operation == "delete",
            schema_version: entry.schema_version,
            format_version: entry.format_version,
        });
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
                None    => true,
                Some(cp) => r.hlc.as_str() > cp,
            })
            .cloned()
            .collect();
        results.sort_by(|a, b| a.hlc.cmp(&b.hlc));
        Ok(results)
    }

    async fn ensure_table(&self) -> Result<(), BackendError> { Ok(()) }
}

// ── InMemory Blob Backend ─────────────────────────────────────────────────────

/// Shared object store underlying one or more [`InMemoryBlobBackend`] instances.
pub struct InMemoryBlobStore {
    /// key → complete object bytes
    objects: HashMap<String, Vec<u8>>,
    /// (key, upload_id) → Vec of (part_number, data)
    pending_parts: HashMap<(String, String), HashMap<i32, (Vec<u8>, String)>>,
    next_upload_id: u64,
}

impl InMemoryBlobStore {
    pub fn new() -> Self {
        Self {
            objects: HashMap::new(),
            pending_parts: HashMap::new(),
            next_upload_id: 1,
        }
    }

    pub fn new_shared() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self::new()))
    }
}

impl Default for InMemoryBlobStore { fn default() -> Self { Self::new() } }

/// A fully in-memory `BlobBackend` for unit tests.
#[derive(Clone)]
pub struct InMemoryBlobBackend {
    id: String,
    store: Arc<Mutex<InMemoryBlobStore>>,
}

impl InMemoryBlobBackend {
    pub fn new(id: impl Into<String>, store: Arc<Mutex<InMemoryBlobStore>>) -> Self {
        Self { id: id.into(), store }
    }

    pub fn standalone(id: impl Into<String>) -> Self {
        Self::new(id, InMemoryBlobStore::new_shared())
    }
}

#[async_trait]
impl BlobBackend for InMemoryBlobBackend {
    fn backend_id(&self) -> &str { &self.id }

    async fn ensure_bucket(&self) -> Result<(), BackendError> { Ok(()) }

    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), BackendError> {
        let mut store = self.store.lock().await;
        store.objects.insert(key.to_string(), data);
        Ok(())
    }

    async fn create_multipart_upload(&self, _key: &str) -> Result<String, BackendError> {
        let mut store = self.store.lock().await;
        let id = store.next_upload_id.to_string();
        store.next_upload_id += 1;
        Ok(id)
    }

    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        data: Vec<u8>,
    ) -> Result<String, BackendError> {
        let etag = format!("etag-{key}-{upload_id}-{part_number}");
        let mut store = self.store.lock().await;
        store
            .pending_parts
            .entry((key.to_string(), upload_id.to_string()))
            .or_default()
            .insert(part_number, (data, etag.clone()));
        Ok(etag)
    }

    async fn list_parts(
        &self,
        key: &str,
        upload_id: &str,
    ) -> Result<Vec<UploadedPart>, BackendError> {
        let store = self.store.lock().await;
        let parts = store
            .pending_parts
            .get(&(key.to_string(), upload_id.to_string()))
            .map(|m| {
                let mut v: Vec<UploadedPart> = m
                    .iter()
                    .map(|(&pn, (_, etag))| UploadedPart { part_number: pn, etag: etag.clone() })
                    .collect();
                v.sort_by_key(|p| p.part_number);
                v
            })
            .unwrap_or_default();
        Ok(parts)
    }

    async fn complete_multipart_upload(
        &self,
        key: &str,
        upload_id: &str,
        parts: Vec<UploadedPart>,
    ) -> Result<(), BackendError> {
        let mut store = self.store.lock().await;
        let pending = store
            .pending_parts
            .remove(&(key.to_string(), upload_id.to_string()))
            .unwrap_or_default();

        // Concatenate parts in order to form the complete object.
        let mut data = Vec::new();
        let mut sorted = parts;
        sorted.sort_by_key(|p| p.part_number);
        for part in sorted {
            if let Some((part_data, _)) = pending.get(&part.part_number) {
                data.extend_from_slice(part_data);
            }
        }
        store.objects.insert(key.to_string(), data);
        Ok(())
    }

    async fn get_object(&self, key: &str, dest: &Path) -> Result<u64, BackendError> {
        let store = self.store.lock().await;
        let data = store.objects.get(key)
            .ok_or_else(|| BackendError::Transient(format!("object not found: {key}")))?
            .clone();
        let len = data.len() as u64;
        drop(store); // release lock before I/O
        tokio::fs::write(dest, data).await
            .map_err(|e| BackendError::Transient(e.to_string()))?;
        Ok(len)
    }

    async fn delete_object(&self, key: &str) -> Result<(), BackendError> {
        let mut store = self.store.lock().await;
        store.objects.remove(key);
        Ok(())
    }
}
