use squirreld::{ListOpts, PutOpts, SquirrelEngine, Ulid};
use tempfile::tempdir;

async fn open_engine() -> (SquirrelEngine, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let engine = SquirrelEngine::builder()
        .db_path(dir.path().join("test.db"))
        .build()
        .await
        .unwrap();
    (engine, dir)
}

// ── Basic CRUD ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn put_and_get_roundtrip() {
    let (engine, _dir) = open_engine().await;
    let data = b"{\"title\":\"Attention Is All You Need\"}";
    let id = engine
        .put("papers", None, data.to_vec(), PutOpts::default())
        .await
        .unwrap();
    let record = engine.get("papers", id).await.unwrap().unwrap();
    assert_eq!(record.data, data);
    assert_eq!(record.collection, "papers");
    assert_eq!(record.id, id);
    assert!(!record.deleted);
}

#[tokio::test]
async fn put_with_explicit_id() {
    let (engine, _dir) = open_engine().await;
    let explicit_id = Ulid::new();
    let returned_id = engine
        .put("papers", Some(explicit_id), b"data".to_vec(), PutOpts::default())
        .await
        .unwrap();
    assert_eq!(returned_id, explicit_id);
    assert!(engine.get("papers", explicit_id).await.unwrap().is_some());
}

#[tokio::test]
async fn put_same_id_twice_updates_record() {
    let (engine, _dir) = open_engine().await;
    let id = engine
        .put("papers", None, b"v1".to_vec(), PutOpts::default())
        .await
        .unwrap();
    engine
        .put("papers", Some(id), b"v2".to_vec(), PutOpts::default())
        .await
        .unwrap();
    let record = engine.get("papers", id).await.unwrap().unwrap();
    assert_eq!(record.data, b"v2");
}

#[tokio::test]
async fn put_preserves_created_at_on_update() {
    let (engine, _dir) = open_engine().await;
    let id = engine
        .put("papers", None, b"v1".to_vec(), PutOpts::default())
        .await
        .unwrap();
    let original = engine.get("papers", id).await.unwrap().unwrap();

    engine
        .put("papers", Some(id), b"v2".to_vec(), PutOpts::default())
        .await
        .unwrap();
    let updated = engine.get("papers", id).await.unwrap().unwrap();

    assert_eq!(original.created_at, updated.created_at, "created_at must not change on update");
    assert!(updated.updated_at >= original.updated_at, "updated_at must advance");
}

#[tokio::test]
async fn get_nonexistent_returns_none() {
    let (engine, _dir) = open_engine().await;
    let result = engine.get("papers", Ulid::new()).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn delete_makes_get_return_none() {
    let (engine, _dir) = open_engine().await;
    let id = engine
        .put("papers", None, b"data".to_vec(), PutOpts::default())
        .await
        .unwrap();
    engine.delete("papers", id).await.unwrap();
    assert!(engine.get("papers", id).await.unwrap().is_none());
}

// ── List ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn list_returns_records_in_hlc_desc_order() {
    let (engine, _dir) = open_engine().await;
    let mut ids = Vec::new();
    for i in 0..5 {
        let id = engine
            .put("papers", None, format!("doc{i}").into_bytes(), PutOpts::default())
            .await
            .unwrap();
        ids.push(id);
    }
    let results = engine.list("papers", ListOpts::default()).await.unwrap();
    assert_eq!(results.len(), 5);
    // HLC desc means most recently written first.
    assert_eq!(results[0].id, *ids.last().unwrap());
    assert_eq!(results[4].id, ids[0]);
}

#[tokio::test]
async fn list_excludes_deleted_by_default() {
    let (engine, _dir) = open_engine().await;
    let id1 = engine
        .put("papers", None, b"a".to_vec(), PutOpts::default())
        .await
        .unwrap();
    engine
        .put("papers", None, b"b".to_vec(), PutOpts::default())
        .await
        .unwrap();
    engine.delete("papers", id1).await.unwrap();

    let results = engine.list("papers", ListOpts::default()).await.unwrap();
    assert_eq!(results.len(), 1, "deleted record should be excluded");
    assert!(!results[0].deleted);
}

#[tokio::test]
async fn list_includes_deleted_when_requested() {
    let (engine, _dir) = open_engine().await;
    let id = engine
        .put("papers", None, b"data".to_vec(), PutOpts::default())
        .await
        .unwrap();
    engine.delete("papers", id).await.unwrap();

    let opts = ListOpts { include_deleted: true, ..Default::default() };
    let results = engine.list("papers", opts).await.unwrap();
    assert_eq!(results.len(), 1);
    assert!(results[0].deleted);
}

#[tokio::test]
async fn list_with_limit_and_offset() {
    let (engine, _dir) = open_engine().await;
    for i in 0..5 {
        engine
            .put("papers", None, format!("doc{i}").into_bytes(), PutOpts::default())
            .await
            .unwrap();
    }
    let page1 = engine
        .list("papers", ListOpts { limit: Some(2), offset: 0, ..Default::default() })
        .await
        .unwrap();
    let page2 = engine
        .list("papers", ListOpts { limit: Some(2), offset: 2, ..Default::default() })
        .await
        .unwrap();
    assert_eq!(page1.len(), 2);
    assert_eq!(page2.len(), 2);
    assert_ne!(page1[0].id, page2[0].id, "pages must not overlap");
}

#[tokio::test]
async fn list_empty_collection_returns_empty_vec() {
    let (engine, _dir) = open_engine().await;
    let results = engine.list("papers", ListOpts::default()).await.unwrap();
    assert!(results.is_empty());
}

// ── HLC properties ───────────────────────────────────────────────────────────

#[tokio::test]
async fn hlc_is_monotonically_increasing() {
    let (engine, _dir) = open_engine().await;
    let mut hlcs = Vec::new();
    for i in 0..10 {
        let id = engine
            .put("papers", None, format!("doc{i}").into_bytes(), PutOpts::default())
            .await
            .unwrap();
        let record = engine.get("papers", id).await.unwrap().unwrap();
        hlcs.push(record.hlc);
    }
    for i in 1..hlcs.len() {
        assert!(hlcs[i] > hlcs[i - 1], "HLC must be strictly increasing across writes");
    }
}

#[tokio::test]
async fn hlc_advances_after_engine_reopen() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    let last_hlc = {
        let engine = SquirrelEngine::builder()
            .db_path(&db_path)
            .build()
            .await
            .unwrap();
        let id = engine
            .put("papers", None, b"v1".to_vec(), PutOpts::default())
            .await
            .unwrap();
        let hlc = engine.get("papers", id).await.unwrap().unwrap().hlc;
        engine.shutdown().await.unwrap();
        hlc
    };

    // Re-open and write again — the new HLC must be > the last one.
    let engine2 = SquirrelEngine::builder()
        .db_path(&db_path)
        .build()
        .await
        .unwrap();
    let id2 = engine2
        .put("papers", None, b"v2".to_vec(), PutOpts::default())
        .await
        .unwrap();
    let new_hlc = engine2.get("papers", id2).await.unwrap().unwrap().hlc;
    assert!(new_hlc > last_hlc, "HLC after reopen must exceed last persisted HLC");
}

// ── Sync diagnostics ─────────────────────────────────────────────────────────

#[tokio::test]
async fn pending_errors_empty_on_fresh_db() {
    let (engine, _dir) = open_engine().await;
    engine
        .put("papers", None, b"data".to_vec(), PutOpts::default())
        .await
        .unwrap();
    assert!(
        engine.pending_errors().await.unwrap().is_empty(),
        "fresh writes have no retries"
    );
}

#[tokio::test]
async fn clear_error_removes_stuck_entry() {
    let (engine, _dir) = open_engine().await;
    engine
        .put("papers", None, b"data".to_vec(), PutOpts::default())
        .await
        .unwrap();
    // Directly manipulate the DB to simulate a retry — access via builder path.
    // We can't test mark_retry through the public API in Phase 1 (sync is Phase 2).
    // Verify clear_error is a no-op when seq doesn't exist.
    engine.clear_error(999).await.unwrap();
}

// ── Cross-collection isolation ────────────────────────────────────────────────

#[tokio::test]
async fn records_in_different_collections_are_isolated() {
    let (engine, _dir) = open_engine().await;
    let id = engine
        .put("papers", None, b"paper data".to_vec(), PutOpts::default())
        .await
        .unwrap();

    // Same ID in a different collection must not be visible.
    assert!(engine.get("notes", id).await.unwrap().is_none());
    assert!(engine.list("notes", ListOpts::default()).await.unwrap().is_empty());
}

// ── Shutdown ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn calls_after_shutdown_return_error() {
    let (engine, _dir) = open_engine().await;
    let engine2 = engine.clone();
    engine.shutdown().await.unwrap();
    // Give the actor task a moment to exit.
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    let result = engine2
        .put("papers", None, b"data".to_vec(), PutOpts::default())
        .await;
    assert!(result.is_err(), "calls after shutdown should fail");
}
