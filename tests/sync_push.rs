/// Tier-1 sync tests — push/pull cycles using InMemoryBackend (no network).
use std::sync::Arc;
use tokio::time::Duration;

use squirreld::{InMemoryBackend, InMemoryStore, ListOpts, PutOpts, SquirrelEngine, Ulid};
use tempfile::tempdir;

async fn engine_with_store(
    store: Arc<tokio::sync::Mutex<InMemoryStore>>,
) -> (SquirrelEngine, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let backend = Arc::new(InMemoryBackend::new("test", store));
    let engine = SquirrelEngine::builder()
        .db_path(dir.path().join("test.db"))
        .record_backend(backend)
        .build()
        .await
        .unwrap();
    (engine, dir)
}

async fn fresh_engine() -> (SquirrelEngine, tempfile::TempDir) {
    engine_with_store(InMemoryStore::new_shared()).await
}

// ── Basic push ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn push_single_record_empties_outbox() {
    let (engine, _dir) = fresh_engine().await;

    engine
        .put("papers", None, b"data".to_vec(), PutOpts::default())
        .await
        .unwrap();

    let stats = engine.force_sync().await.unwrap();
    assert_eq!(stats.pushed, 1, "one record should have been pushed");
    assert_eq!(stats.errors, 0);
    assert!(engine.pending_errors().await.unwrap().is_empty());
}

#[tokio::test]
async fn push_multiple_records() {
    let (engine, _dir) = fresh_engine().await;

    for i in 0..5 {
        engine
            .put("papers", None, format!("doc{i}").into_bytes(), PutOpts::default())
            .await
            .unwrap();
    }

    let stats = engine.force_sync().await.unwrap();
    assert_eq!(stats.pushed, 5);
    assert_eq!(stats.errors, 0);
}

#[tokio::test]
async fn push_then_delete_both_synced() {
    let (engine, _dir) = fresh_engine().await;

    let id = engine
        .put("papers", None, b"data".to_vec(), PutOpts::default())
        .await
        .unwrap();
    engine.force_sync().await.unwrap();

    engine.delete("papers", &id).await.unwrap();
    let stats = engine.force_sync().await.unwrap();
    assert_eq!(stats.pushed, 1, "delete should push a tombstone");
    assert_eq!(stats.errors, 0);
}

// ── Two-device data propagation ───────────────────────────────────────────────

#[tokio::test]
async fn two_devices_share_data_via_force_sync() {
    let store = InMemoryStore::new_shared();
    let (engine_a, _dir_a) = engine_with_store(store.clone()).await;
    let (engine_b, _dir_b) = engine_with_store(store).await;

    engine_a
        .put("papers", None, b"hello from A".to_vec(), PutOpts::default())
        .await
        .unwrap();
    engine_a.force_sync().await.unwrap();

    let stats = engine_b.force_sync().await.unwrap();
    assert!(stats.pulled >= 1, "B should pull A's record; pulled={}", stats.pulled);

    let results = engine_b.list("papers", ListOpts::default()).await.unwrap();
    assert_eq!(results.len(), 1);
}

// ── LWW conflict resolution ───────────────────────────────────────────────────
//
// Strategy: use force_sync to control the ordering precisely.
// Both engines write the same record ID while "offline" (before either syncs).
// We call force_sync explicitly in the order we want, sidestepping the 500ms
// background-kick debounce by making force_sync cut through it immediately.

#[tokio::test]
async fn lww_remote_wins_when_it_has_higher_hlc() {
    let store = InMemoryStore::new_shared();
    let (engine_a, _dir_a) = engine_with_store(store.clone()).await;
    let (engine_b, _dir_b) = engine_with_store(store).await;

    let explicit_id = Ulid::new().to_string();

    // A writes first.
    engine_a
        .put("papers", Some(explicit_id.clone()), b"from A".to_vec(), PutOpts::default())
        .await
        .unwrap();

    // Wait long enough that B's physical timestamp is strictly higher.
    tokio::time::sleep(Duration::from_millis(5)).await;

    // B writes the same record ID — B's HLC will be higher.
    engine_b
        .put("papers", Some(explicit_id.clone()), b"from B".to_vec(), PutOpts::default())
        .await
        .unwrap();

    // B syncs first (pushes B's version — no conflict yet).
    engine_b.force_sync().await.unwrap();

    // A syncs: A's HLC < B's HLC → backend returns ConflictAt for A's push,
    // then pull fetches B's version which overwrites A's local copy.
    let stats_a = engine_a.force_sync().await.unwrap();
    assert_eq!(stats_a.conflicts, 1, "A's push should be rejected (B has higher HLC)");

    // After pull, A must hold B's winning version.
    let rec = engine_a.get("papers", &explicit_id).await.unwrap().unwrap();
    assert_eq!(rec.data, b"from B", "A should have adopted B's version");
}

#[tokio::test]
async fn lww_local_wins_when_it_has_higher_hlc() {
    let store = InMemoryStore::new_shared();
    let (engine_a, _dir_a) = engine_with_store(store.clone()).await;
    let (engine_b, _dir_b) = engine_with_store(store).await;

    let explicit_id = Ulid::new().to_string();

    // B writes first (older HLC).
    engine_b
        .put("papers", Some(explicit_id.clone()), b"from B (old)".to_vec(), PutOpts::default())
        .await
        .unwrap();
    // B syncs — its version is now on the remote.
    engine_b.force_sync().await.unwrap();

    // A writes the same record later (higher HLC).
    tokio::time::sleep(Duration::from_millis(5)).await;
    engine_a
        .put("papers", Some(explicit_id.clone()), b"from A (new)".to_vec(), PutOpts::default())
        .await
        .unwrap();

    // A syncs — A's HLC > remote HLC → push succeeds, no conflict.
    let stats_a = engine_a.force_sync().await.unwrap();
    assert_eq!(stats_a.pushed, 1, "A should win: its HLC is higher");
    assert_eq!(stats_a.conflicts, 0);

    // B pulls — B must now hold A's version.
    engine_b.force_sync().await.unwrap();
    let rec_b = engine_b.get("papers", &explicit_id).await.unwrap().unwrap();
    assert_eq!(rec_b.data, b"from A (new)");
}

// ── Tombstone propagation ─────────────────────────────────────────────────────

#[tokio::test]
async fn tombstone_propagates_to_second_device() {
    let store = InMemoryStore::new_shared();
    let (engine_a, _dir_a) = engine_with_store(store.clone()).await;
    let (engine_b, _dir_b) = engine_with_store(store).await;

    // A writes and both devices sync so B has the record.
    let id = engine_a
        .put("papers", None, b"data".to_vec(), PutOpts::default())
        .await
        .unwrap();
    engine_a.force_sync().await.unwrap();
    engine_b.force_sync().await.unwrap();
    assert!(engine_b.get("papers", &id).await.unwrap().is_some(), "B should have the record");

    // A deletes and syncs the tombstone.
    engine_a.delete("papers", &id).await.unwrap();
    engine_a.force_sync().await.unwrap();

    // B pulls the tombstone.
    engine_b.force_sync().await.unwrap();
    assert!(
        engine_b.get("papers", &id).await.unwrap().is_none(),
        "B should see the record as deleted"
    );
}

// ── Eventual consistency across three sync rounds ─────────────────────────────

#[tokio::test]
async fn two_devices_converge_after_concurrent_writes() {
    let store = InMemoryStore::new_shared();
    let (engine_a, _dir_a) = engine_with_store(store.clone()).await;
    let (engine_b, _dir_b) = engine_with_store(store).await;

    let id = Ulid::new().to_string();
    engine_a
        .put("papers", Some(id.clone()), b"version A".to_vec(), PutOpts::default())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    engine_b
        .put("papers", Some(id.clone()), b"version B".to_vec(), PutOpts::default())
        .await
        .unwrap();

    // Multiple sync rounds until convergence.
    for _ in 0..3 {
        engine_a.force_sync().await.unwrap();
        engine_b.force_sync().await.unwrap();
    }

    let rec_a = engine_a.get("papers", &id).await.unwrap().unwrap();
    let rec_b = engine_b.get("papers", &id).await.unwrap().unwrap();
    assert_eq!(rec_a.data, rec_b.data, "both devices must converge to the same value");
    // B wrote 5ms later so its HLC is higher → B should win.
    assert_eq!(rec_a.data, b"version B");
}
