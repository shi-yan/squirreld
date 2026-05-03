/// Phase-5 encryption tests — AES-256-GCM envelope encryption, passphrase KDF,
/// per-item override, and cross-open persistence.
use squirreld::{ItemEncryption, KeySource, PutOpts, SquirrelEngine, Ulid};
use tempfile::tempdir;

async fn engine_with_raw_key(dir: &tempfile::TempDir, key: [u8; 32]) -> SquirrelEngine {
    SquirrelEngine::builder()
        .db_path(dir.path().join("enc.db"))
        .encryption_key(KeySource::RawKey(key))
        .build()
        .await
        .unwrap()
}

async fn engine_with_passphrase(dir: &tempfile::TempDir, pass: &str) -> SquirrelEngine {
    SquirrelEngine::builder()
        .db_path(dir.path().join("enc.db"))
        .encryption_key(KeySource::Passphrase(pass.into()))
        .build()
        .await
        .unwrap()
}

async fn engine_no_encryption(dir: &tempfile::TempDir) -> SquirrelEngine {
    SquirrelEngine::builder()
        .db_path(dir.path().join("plain.db"))
        .build()
        .await
        .unwrap()
}

fn raw_key() -> [u8; 32] { [0x42u8; 32] }

// ── Raw-key: basic roundtrip ──────────────────────────────────────────────────

#[tokio::test]
async fn put_and_get_roundtrip_with_raw_key() {
    let dir = tempdir().unwrap();
    let engine = engine_with_raw_key(&dir, raw_key()).await;

    let id = engine
        .put("notes", None, b"secret message".to_vec(), PutOpts::default())
        .await
        .unwrap();

    let rec = engine.get("notes", id).await.unwrap().unwrap();
    assert_eq!(rec.data, b"secret message");
}

#[tokio::test]
async fn encrypted_data_is_not_plaintext_on_disk() {
    let dir = tempdir().unwrap();
    let engine = engine_with_raw_key(&dir, raw_key()).await;

    engine
        .put("notes", None, b"top secret".to_vec(), PutOpts::default())
        .await
        .unwrap();
    drop(engine);

    // Read the raw DB bytes and confirm the plaintext is absent.
    let db_bytes = std::fs::read(dir.path().join("enc.db")).unwrap();
    let db_str   = String::from_utf8_lossy(&db_bytes);
    assert!(
        !db_str.contains("top secret"),
        "plaintext must not appear in the encrypted database file"
    );
}

// ── Passphrase derivation ─────────────────────────────────────────────────────

#[tokio::test]
async fn passphrase_roundtrip() {
    let dir = tempdir().unwrap();
    let engine = engine_with_passphrase(&dir, "correct horse battery staple").await;

    let id = engine
        .put("docs", None, b"sensitive doc".to_vec(), PutOpts::default())
        .await
        .unwrap();

    let rec = engine.get("docs", id).await.unwrap().unwrap();
    assert_eq!(rec.data, b"sensitive doc");
}

#[tokio::test]
async fn passphrase_reopens_successfully() {
    let dir  = tempdir().unwrap();
    let pass = "my-passphrase";

    let id = {
        let engine = engine_with_passphrase(&dir, pass).await;
        engine
            .put("docs", None, b"persisted content".to_vec(), PutOpts::default())
            .await
            .unwrap()
    };

    // Re-open with the same passphrase — KDF salt is reloaded from config table.
    let engine2 = engine_with_passphrase(&dir, pass).await;
    let rec = engine2.get("docs", id).await.unwrap().unwrap();
    assert_eq!(rec.data, b"persisted content");
}

#[tokio::test]
async fn wrong_passphrase_fails_to_decrypt() {
    let dir = tempdir().unwrap();

    let id = {
        let engine = engine_with_passphrase(&dir, "correct-pass").await;
        engine
            .put("docs", None, b"secret".to_vec(), PutOpts::default())
            .await
            .unwrap()
    };

    // Re-open with wrong passphrase — salt is reused but KEK differs, so GCM auth fails.
    let engine2 = engine_with_passphrase(&dir, "wrong-pass").await;
    let result = engine2.get("docs", id).await;
    assert!(result.is_err(), "decryption with wrong passphrase must fail");
}

// ── Per-item encryption override ──────────────────────────────────────────────

#[tokio::test]
async fn disabled_item_is_stored_plaintext() {
    let dir    = tempdir().unwrap();
    let engine = engine_with_raw_key(&dir, raw_key()).await;

    // Store one item plaintext even though the engine has a KEK.
    let plain_id = engine
        .put("mixed", None, b"public data".to_vec(), PutOpts {
            encryption: ItemEncryption::Disabled,
            ..Default::default()
        })
        .await
        .unwrap();

    let enc_id = engine
        .put("mixed", None, b"private data".to_vec(), PutOpts::default())
        .await
        .unwrap();

    // Both must round-trip correctly.
    let plain = engine.get("mixed", plain_id).await.unwrap().unwrap();
    let enc   = engine.get("mixed", enc_id).await.unwrap().unwrap();
    assert_eq!(plain.data, b"public data");
    assert_eq!(enc.data,   b"private data");
}

#[tokio::test]
async fn enabled_item_without_kek_returns_error() {
    let dir    = tempdir().unwrap();
    let engine = engine_no_encryption(&dir).await;

    let result = engine
        .put("notes", None, b"data".to_vec(), PutOpts {
            encryption: ItemEncryption::Enabled,
            ..Default::default()
        })
        .await;

    assert!(result.is_err(), "Enabled encryption without a KEK must return an error");
}

// ── Persistence across re-open (raw key) ─────────────────────────────────────

#[tokio::test]
async fn raw_key_reopens_and_decrypts() {
    let dir = tempdir().unwrap();
    let key = raw_key();

    let id = {
        let engine = engine_with_raw_key(&dir, key).await;
        engine
            .put("notes", None, b"reopen check".to_vec(), PutOpts::default())
            .await
            .unwrap()
    };

    let engine2 = engine_with_raw_key(&dir, key).await;
    let rec = engine2.get("notes", id).await.unwrap().unwrap();
    assert_eq!(rec.data, b"reopen check");
}

#[tokio::test]
async fn wrong_raw_key_fails_to_decrypt() {
    let dir = tempdir().unwrap();

    let id = {
        let engine = engine_with_raw_key(&dir, [0x11u8; 32]).await;
        engine
            .put("notes", None, b"locked".to_vec(), PutOpts::default())
            .await
            .unwrap()
    };

    let engine2 = engine_with_raw_key(&dir, [0x22u8; 32]).await;
    let result = engine2.get("notes", id).await;
    assert!(result.is_err(), "wrong raw key must fail to decrypt");
}

// ── Explicit record ID preserved through encryption ───────────────────────────

#[tokio::test]
async fn explicit_id_preserved_after_encryption() {
    let dir    = tempdir().unwrap();
    let engine = engine_with_raw_key(&dir, raw_key()).await;
    let id     = Ulid::new();

    engine
        .put("notes", Some(id), b"known-id content".to_vec(), PutOpts::default())
        .await
        .unwrap();

    let rec = engine.get("notes", id).await.unwrap().unwrap();
    assert_eq!(rec.id, id);
    assert_eq!(rec.data, b"known-id content");
}

// ── No encryption — existing tests still pass ─────────────────────────────────

#[tokio::test]
async fn no_encryption_roundtrip_unchanged() {
    let dir    = tempdir().unwrap();
    let engine = engine_no_encryption(&dir).await;

    let id = engine
        .put("plain", None, b"unencrypted".to_vec(), PutOpts::default())
        .await
        .unwrap();

    let rec = engine.get("plain", id).await.unwrap().unwrap();
    assert_eq!(rec.data, b"unencrypted");
}
