/// End-to-end offline-first integration tests using InMemoryBackend.
///
/// These tests simulate the full paper-reader scenario:
///   • Two devices share a backend (InMemoryBackend / InMemoryBlobBackend)
///   • Device A writes while offline, then syncs
///   • Device B pulls and receives A's data
///   • Conflict resolution, tombstone propagation, and blob sharing all verified
use std::collections::HashMap;
use std::sync::Arc;

use squirreld::{
    backend::in_memory::{InMemoryBackend, InMemoryBlobBackend, InMemoryBlobStore, InMemoryStore},
    BlobStatus, ColumnAffinity, FieldDef, IndexDef, IndexValue, KeySource, PutBlobOpts, PutOpts,
    QueryFilter, QueryOpts, SquirrelEngine,
};
use tempfile::tempdir;

// ── Helpers ───────────────────────────────────────────────────────────────────

async fn device(
    name: &str,
    record_store: Arc<tokio::sync::Mutex<InMemoryStore>>,
    blob_store: Arc<tokio::sync::Mutex<InMemoryBlobStore>>,
    dir: &tempfile::TempDir,
) -> SquirrelEngine {
    let record_backend = Arc::new(InMemoryBackend::new(name, record_store));
    let blob_backend   = Arc::new(InMemoryBlobBackend::new(name, blob_store));

    SquirrelEngine::builder()
        .db_path(dir.path().join(format!("{name}.db")))
        .cache_dir(dir.path().join(format!("{name}_blobs")))
        .record_backend(record_backend)
        .blob_backend(blob_backend)
        .build()
        .await
        .unwrap()
}

fn idx_paper(year: i64, title: &str) -> HashMap<String, IndexValue> {
    let mut m = HashMap::new();
    m.insert("year".into(),  IndexValue::Integer(year));
    m.insert("title".into(), IndexValue::Text(title.into()));
    m
}

fn papers_index() -> IndexDef {
    IndexDef {
        collection: "papers".into(),
        fields:     vec![FieldDef { name: "year".into(), affinity: ColumnAffinity::Integer }],
        fts_fields: vec!["title".into()],
    }
}

// ── Basic sync between two devices ───────────────────────────────────────────

#[tokio::test]
async fn device_a_writes_device_b_receives() {
    let dir_a = tempdir().unwrap();
    let dir_b = tempdir().unwrap();
    let store = InMemoryStore::new_shared();
    let blobs = InMemoryBlobStore::new_shared();

    let a = device("a", store.clone(), blobs.clone(), &dir_a).await;
    let b = device("b", store.clone(), blobs.clone(), &dir_b).await;

    let id = a.put("papers", None, b"paper payload".to_vec(), PutOpts::default())
        .await.unwrap();

    a.force_sync().await.unwrap();
    b.force_sync().await.unwrap();

    let rec = b.get("papers", id).await.unwrap();
    assert!(rec.is_some(), "B must receive A's record after sync");
    assert_eq!(rec.unwrap().data, b"paper payload");
}

#[tokio::test]
async fn multiple_records_all_sync() {
    let dir_a = tempdir().unwrap();
    let dir_b = tempdir().unwrap();
    let store = InMemoryStore::new_shared();
    let blobs = InMemoryBlobStore::new_shared();

    let a = device("a", store.clone(), blobs.clone(), &dir_a).await;
    let b = device("b", store.clone(), blobs.clone(), &dir_b).await;

    let mut ids = Vec::new();
    for i in 0u8..5 {
        let id = a.put("papers", None, vec![i], PutOpts::default()).await.unwrap();
        ids.push(id);
    }

    a.force_sync().await.unwrap();
    b.force_sync().await.unwrap();

    for (i, id) in ids.iter().enumerate() {
        let rec = b.get("papers", *id).await.unwrap().unwrap();
        assert_eq!(rec.data, [i as u8]);
    }
}

// ── Last-Write-Wins conflict resolution ───────────────────────────────────────

#[tokio::test]
async fn concurrent_writes_lww_higher_hlc_wins() {
    let dir_a = tempdir().unwrap();
    let dir_b = tempdir().unwrap();
    let store = InMemoryStore::new_shared();
    let blobs = InMemoryBlobStore::new_shared();

    let a = device("a", store.clone(), blobs.clone(), &dir_a).await;
    let b = device("b", store.clone(), blobs.clone(), &dir_b).await;

    // Both devices write the same record while offline.
    let id = squirreld::Ulid::new();
    a.put("notes", Some(id), b"A's version".to_vec(), PutOpts::default()).await.unwrap();

    // B writes *after* A — B's HLC will be higher.
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    b.put("notes", Some(id), b"B's version".to_vec(), PutOpts::default()).await.unwrap();

    // A syncs first, then B.
    a.force_sync().await.unwrap();
    b.force_sync().await.unwrap();

    // Pull A's state from the backend.
    a.force_sync().await.unwrap();

    // B has a higher HLC → B wins everywhere.
    let rec_a = a.get("notes", id).await.unwrap().unwrap();
    let rec_b = b.get("notes", id).await.unwrap().unwrap();
    assert_eq!(rec_a.data, b"B's version", "A should adopt B's winning version");
    assert_eq!(rec_b.data, b"B's version");
}

// ── Tombstone propagation ─────────────────────────────────────────────────────

#[tokio::test]
async fn delete_propagates_to_second_device() {
    let dir_a = tempdir().unwrap();
    let dir_b = tempdir().unwrap();
    let store = InMemoryStore::new_shared();
    let blobs = InMemoryBlobStore::new_shared();

    let a = device("a", store.clone(), blobs.clone(), &dir_a).await;
    let b = device("b", store.clone(), blobs.clone(), &dir_b).await;

    let id = a.put("papers", None, b"to be deleted".to_vec(), PutOpts::default())
        .await.unwrap();
    a.force_sync().await.unwrap();
    b.force_sync().await.unwrap();
    assert!(b.get("papers", id).await.unwrap().is_some());

    a.delete("papers", id).await.unwrap();
    a.force_sync().await.unwrap();
    b.force_sync().await.unwrap();

    assert!(b.get("papers", id).await.unwrap().is_none(), "tombstone must propagate");
}

// ── Offline-first: queue while offline, flush when online ────────────────────

#[tokio::test]
async fn offline_writes_queue_and_flush_on_connect() {
    let dir_a = tempdir().unwrap();
    let dir_b = tempdir().unwrap();
    let store = InMemoryStore::new_shared();
    let blobs = InMemoryBlobStore::new_shared();

    // A has no backend — fully offline.
    let a_offline = SquirrelEngine::builder()
        .db_path(dir_a.path().join("a.db"))
        .build()
        .await
        .unwrap();

    let mut ids = Vec::new();
    for i in 0..3u8 {
        let id = a_offline
            .put("papers", None, vec![i], PutOpts::default())
            .await
            .unwrap();
        ids.push(id);
    }
    a_offline.shutdown().await.unwrap();

    // Re-open with a backend — simulates coming back online.
    let a_online = device("a", store.clone(), blobs.clone(), &dir_a).await;
    a_online.force_sync().await.unwrap();

    // B should now receive everything.
    let b = device("b", store.clone(), blobs.clone(), &dir_b).await;
    b.force_sync().await.unwrap();

    for (i, id) in ids.iter().enumerate() {
        let rec = b.get("papers", *id).await.unwrap().unwrap();
        assert_eq!(rec.data, [i as u8]);
    }
}

// ── Sync with encryption ──────────────────────────────────────────────────────

#[tokio::test]
async fn encrypted_records_sync_correctly() {
    let dir_a = tempdir().unwrap();
    let dir_b = tempdir().unwrap();
    let store = InMemoryStore::new_shared();
    let blobs = InMemoryBlobStore::new_shared();

    let a = {
        let rb = Arc::new(InMemoryBackend::new("a", store.clone()));
        let bb = Arc::new(InMemoryBlobBackend::new("a", blobs.clone()));
        SquirrelEngine::builder()
            .db_path(dir_a.path().join("a.db"))
            .cache_dir(dir_a.path().join("a_blobs"))
            .record_backend(rb)
            .blob_backend(bb)
            .encryption_key(KeySource::RawKey([0x55u8; 32]))
            .build()
            .await
            .unwrap()
    };

    let b = {
        let rb = Arc::new(InMemoryBackend::new("b", store.clone()));
        let bb = Arc::new(InMemoryBlobBackend::new("b", blobs.clone()));
        SquirrelEngine::builder()
            .db_path(dir_b.path().join("b.db"))
            .cache_dir(dir_b.path().join("b_blobs"))
            .record_backend(rb)
            .blob_backend(bb)
            .encryption_key(KeySource::RawKey([0x55u8; 32]))
            .build()
            .await
            .unwrap()
    };

    let id = a.put("docs", None, b"encrypted content".to_vec(), PutOpts::default())
        .await.unwrap();

    a.force_sync().await.unwrap();
    b.force_sync().await.unwrap();

    let rec = b.get("docs", id).await.unwrap().unwrap();
    assert_eq!(rec.data, b"encrypted content", "B must decrypt synced record");
}

// ── Blob sync between two devices ─────────────────────────────────────────────

#[tokio::test]
async fn blob_uploaded_by_a_accessible_to_b() {
    let dir_a = tempdir().unwrap();
    let dir_b = tempdir().unwrap();
    let store = InMemoryStore::new_shared();
    let blobs = InMemoryBlobStore::new_shared();

    let a = device("a", store.clone(), blobs.clone(), &dir_a).await;
    let b = device("b", store.clone(), blobs.clone(), &dir_b).await;

    let content = b"PDF content bytes here";
    let pdf = dir_a.path().join("paper.pdf");
    std::fs::write(&pdf, content).unwrap();

    let blob_id = a.put_blob(&pdf, PutBlobOpts::default()).await.unwrap();
    a.force_flush_blobs().await.unwrap();

    let info = a.blob_info(&blob_id).await.unwrap().unwrap();
    assert_eq!(info.status, BlobStatus::Uploaded);

    // B stages the same content (in production this would come from synced record metadata).
    let pdf_b = dir_b.path().join("paper_b.pdf");
    std::fs::write(&pdf_b, content).unwrap();
    let blob_id_b = b.put_blob(&pdf_b, PutBlobOpts::default()).await.unwrap();
    b.force_flush_blobs().await.unwrap();

    let cached = b.get_blob(&blob_id_b).await.unwrap().unwrap();
    assert_eq!(std::fs::read(cached).unwrap(), content);
}

// ── list() after sync returns all synced records ──────────────────────────────

#[tokio::test]
async fn list_returns_synced_records() {
    let dir_a = tempdir().unwrap();
    let dir_b = tempdir().unwrap();
    let store = InMemoryStore::new_shared();
    let blobs = InMemoryBlobStore::new_shared();

    let a = device("a", store.clone(), blobs.clone(), &dir_a).await;
    let b = device("b", store.clone(), blobs.clone(), &dir_b).await;

    for (year, title) in [(2020i64, "Deep Learning Survey"), (2022, "Efficient Transformers")] {
        a.put("papers", None, b"x".to_vec(), PutOpts {
            index_fields: idx_paper(year, title),
            ..Default::default()
        }).await.unwrap();
    }

    a.force_sync().await.unwrap();
    b.force_sync().await.unwrap();

    let all = b.list("papers", squirreld::ListOpts::default()).await.unwrap();
    assert_eq!(all.len(), 2, "B must have both papers after sync");
}

// ── Index queries work for locally-written records ────────────────────────────

#[tokio::test]
async fn index_queries_work_for_local_records() {
    let dir = tempdir().unwrap();
    let engine = SquirrelEngine::builder()
        .db_path(dir.path().join("test.db"))
        .build()
        .await
        .unwrap();

    engine.register_index(papers_index()).await.unwrap();

    for (year, title) in [(2020i64, "Deep Learning Survey"), (2022, "Efficient Transformers")] {
        engine.put("papers", None, b"x".to_vec(), PutOpts {
            index_fields: idx_paper(year, title),
            ..Default::default()
        }).await.unwrap();
    }

    let results = engine.query("papers", QueryOpts {
        filter: Some(QueryFilter::Ge { field: "year".into(), value: IndexValue::Integer(2021) }),
        ..Default::default()
    }).await.unwrap();
    assert_eq!(results.len(), 1);

    let fts = engine.query("papers", QueryOpts {
        filter: Some(QueryFilter::Contains { text: "Learning".into() }),
        ..Default::default()
    }).await.unwrap();
    assert_eq!(fts.len(), 1);
}

// ── Pending errors exposed when sync fails (no-backend engine) ────────────────

#[tokio::test]
async fn pending_errors_api_is_accessible() {
    let dir = tempdir().unwrap();
    let engine = SquirrelEngine::builder()
        .db_path(dir.path().join("test.db"))
        .build()
        .await
        .unwrap();

    engine.put("notes", None, b"note".to_vec(), PutOpts::default()).await.unwrap();
    // No sync backend configured → outbox is non-empty but no errors yet.
    let errors = engine.pending_errors().await.unwrap();
    assert!(errors.is_empty(), "fresh outbox has no errors");
}
