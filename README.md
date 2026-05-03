# squirreld

An offline-first, schema-orthogonal sync library for Rust desktop and mobile apps.

Squirreld stores everything locally in SQLite and syncs to the cloud in the background — with no opinion on your data format, optional per-record AES-256-GCM encryption, a structured query API backed by shadow index tables, FTS5 full-text search, and S3 blob storage with resumable multipart uploads.

---

## Features

- **Offline-first**: reads and writes succeed instantly against a local SQLite database; cloud sync runs asynchronously
- **Schema-orthogonal**: records are opaque byte payloads — bring your own serialisation (JSON, Protobuf, MessagePack, anything)
- **Last-write-wins (LWW)**: conflict resolution via Hybrid Logical Clock; lexicographically sortable, causally consistent across devices
- **Transactional outbox**: every write atomically appends to an outbox; no data is ever lost between crash and reconnect
- **AES-256-GCM encryption**: envelope encryption with per-record DEK wrapped by a KEK; passphrase (Argon2id) or raw key
- **Shadow index + FTS5**: register indexed fields at runtime; query with scalar filters, `AND`/`OR` compounds, and full-text search
- **S3 blob store**: stage local files, upload in the background with 5 MB multipart chunks, resume after interruption
- **Actor model**: single tokio task owns SQLite; `SquirrelEngine` is `Clone` and cheap to share across threads
- **Pluggable backends**: `RecordBackend` and `BlobBackend` traits; DynamoDB + S3 ship out of the box; in-memory doubles for tests

---

## Quick start

```toml
# Cargo.toml
[dependencies]
squirreld = { version = "0.1", features = ["encryption"] }
```

```rust
use squirreld::{KeySource, PutOpts, SquirrelEngine};

#[tokio::main]
async fn main() -> squirreld::error::Result<()> {
    let engine = SquirrelEngine::builder()
        .db_path("/var/lib/myapp/store.db")
        .encryption_key(KeySource::Passphrase("my-secret-passphrase".into()))
        .build()
        .await?;

    // Write a record (encrypted by default when a KEK is configured).
    let id = engine.put("notes", None, b"Hello, squirreld!".to_vec(), PutOpts::default()).await?;

    // Read it back (decrypted transparently).
    let rec = engine.get("notes", id).await?.unwrap();
    assert_eq!(rec.data, b"Hello, squirreld!");

    engine.shutdown().await
}
```

---

## Table of Contents

1. [Engine setup](#1-engine-setup)
2. [Records — CRUD](#2-records--crud)
3. [Indexing and queries](#3-indexing-and-queries)
4. [Full-text search (FTS5)](#4-full-text-search-fts5)
5. [Encryption](#5-encryption)
6. [Blob store](#6-blob-store)
7. [Sync backends](#7-sync-backends)
8. [Sync events](#8-sync-events)
9. [Testing with in-memory doubles](#9-testing-with-in-memory-doubles)
10. [Feature flags](#10-feature-flags)
11. [Design notes](#11-design-notes)

---

## 1. Engine setup

`SquirrelEngine` is built with a fluent builder. All options are optional except `db_path`.

```rust
use std::sync::Arc;
use squirreld::{
    KeySource, SquirrelEngine,
    backend::dynamodb::{DynamoDbBackend, DynamoDbConfig, AwsCredentials},
    backend::s3::{S3Backend, S3Config, S3Credentials},
};

let engine = SquirrelEngine::builder()
    // Required: path to the SQLite database file.
    .db_path("/var/lib/myapp/squirreld.db")

    // Optional: local directory for cached blob files.
    // Default: {db_dir}/blob_cache
    .cache_dir("/var/lib/myapp/blobs")

    // Optional: encrypt all records at rest (AES-256-GCM).
    .encryption_key(KeySource::Passphrase("user-passphrase".into()))

    // Optional: sync records to DynamoDB.
    .record_backend(Arc::new(
        DynamoDbBackend::new(DynamoDbConfig {
            table_name:  "myapp-records".into(),
            region:      "us-east-1".into(),
            credentials: AwsCredentials::FromEnvironment,
        }).await?
    ))

    // Optional: store blobs in S3.
    .blob_backend(Arc::new(
        S3Backend::new(S3Config {
            bucket_name:  "myapp-blobs".into(),
            region:       "us-east-1".into(),
            endpoint_url: None, // Some("http://localhost:4566") for LocalStack
            credentials:  S3Credentials::FromEnvironment,
        }).await?
    ))

    .build()
    .await?;
```

`SquirrelEngine` is `Clone` — share it freely across tasks.

---

## 2. Records — CRUD

### put

```rust
use squirreld::{PutOpts, Ulid};

// Generate a new ID automatically.
let id: Ulid = engine.put("papers", None, payload_bytes, PutOpts::default()).await?;

// Use a caller-supplied ID (idempotent upsert).
let fixed_id = Ulid::new();
engine.put("papers", Some(fixed_id), payload_bytes, PutOpts::default()).await?;
```

### get

```rust
match engine.get("papers", id).await? {
    Some(record) => {
        // record.data is the plaintext bytes (decrypted transparently if encrypted).
        let paper: Paper = serde_json::from_slice(&record.data)?;
        println!("{} — HLC {}", paper.title, record.hlc);
    }
    None => println!("not found (or deleted)"),
}
```

### delete

Soft-deletes the record. Tombstones propagate to other devices during sync.

```rust
engine.delete("papers", id).await?;
```

### list

Returns record metadata (no data bytes) sorted by HLC descending.

```rust
use squirreld::ListOpts;

let metas = engine.list("papers", ListOpts {
    limit:           Some(20),
    offset:          0,
    include_deleted: false,
    order:           squirreld::SortOrder::HlcDesc,
}).await?;

for m in metas {
    println!("{}: schema_v={}", m.id, m.schema_version);
}
```

---

## 3. Indexing and queries

Register a shadow index for a collection, then pass indexed values when writing records. The shadow table is maintained atomically in the same SQLite transaction as the record write.

### Register an index

```rust
use squirreld::{ColumnAffinity, FieldDef, IndexDef};

engine.register_index(IndexDef {
    collection: "papers".into(),
    fields: vec![
        FieldDef { name: "year".into(),        affinity: ColumnAffinity::Integer },
        FieldDef { name: "read_status".into(), affinity: ColumnAffinity::Text },
        FieldDef { name: "author".into(),      affinity: ColumnAffinity::Text },
    ],
    // Text fields to include in FTS5 (covered in the next section).
    fts_fields: vec!["title".into(), "abstract".into()],
}).await?;
```

Call `register_index` once at startup, before writing any records.

### Write with index fields

```rust
use squirreld::{IndexValue, PutOpts};
use std::collections::HashMap;

let mut index_fields = HashMap::new();
index_fields.insert("year".into(),        IndexValue::Integer(2024));
index_fields.insert("read_status".into(), IndexValue::Text("unread".into()));
index_fields.insert("author".into(),      IndexValue::Text("Hinton".into()));
index_fields.insert("title".into(),       IndexValue::Text("Capsule Networks".into()));

engine.put("papers", None, payload_bytes, PutOpts {
    index_fields,
    ..Default::default()
}).await?;
```

### Query with filters

```rust
use squirreld::{IndexValue, QueryFilter, QueryOpts};

// Equality filter.
let results = engine.query("papers", QueryOpts {
    filter: Some(QueryFilter::Eq {
        field: "read_status".into(),
        value: IndexValue::Text("unread".into()),
    }),
    limit:  Some(50),
    ..Default::default()
}).await?;

// Range filter — papers from 2020 onwards.
let recent = engine.query("papers", QueryOpts {
    filter: Some(QueryFilter::Ge {
        field: "year".into(),
        value: IndexValue::Integer(2020),
    }),
    ..Default::default()
}).await?;

// Compound filter.
let combined = engine.query("papers", QueryOpts {
    filter: Some(QueryFilter::And(vec![
        QueryFilter::Ge { field: "year".into(), value: IndexValue::Integer(2020) },
        QueryFilter::Eq { field: "author".into(), value: IndexValue::Text("LeCun".into()) },
    ])),
    ..Default::default()
}).await?;
```

Available filter variants: `Eq`, `Lt`, `Gt`, `Le`, `Ge`, `And`, `Or`, `Contains`.

`query()` returns `Vec<RecordMeta>` (headers only). Call `engine.get()` for the full payload.

---

## 4. Full-text search (FTS5)

Add fields to `IndexDef.fts_fields` at registration time. Pass their values in `PutOpts.index_fields` — the library populates both the shadow index table and the FTS5 virtual table in one transaction.

```rust
// Registration (shown above — include fts_fields).

// Search across all FTS-indexed fields.
let hits = engine.query("papers", QueryOpts {
    filter: Some(QueryFilter::Contains { text: "attention mechanism".into() }),
    ..Default::default()
}).await?;

// Combine FTS with a scalar filter: unread papers that mention "transformers".
let unread_transformers = engine.query("papers", QueryOpts {
    filter: Some(QueryFilter::And(vec![
        QueryFilter::Eq { field: "read_status".into(), value: IndexValue::Text("unread".into()) },
        QueryFilter::Contains { text: "transformers".into() },
    ])),
    ..Default::default()
}).await?;
```

FTS5 uses the `unicode61` tokenizer. Queries follow standard FTS5 syntax — prefix queries (`rust*`), phrase queries (`"exact phrase"`), and boolean operators all work.

---

## 5. Encryption

### Key sources

```rust
use squirreld::KeySource;

// Derive a 256-bit KEK from a passphrase using Argon2id (m=64 MB, t=3, p=1).
// The KDF salt is auto-generated on first open and persisted in the config table,
// so the same KEK is reproduced on every subsequent open with the same passphrase.
KeySource::Passphrase("user-supplied passphrase".into())

// Provide the 32-byte KEK directly.
// Suitable for keys retrieved from a platform keyring (macOS Keychain,
// Linux Secret Service, Windows Credential Manager).
KeySource::RawKey(my_32_byte_key)
```

**Platform keyring integration** — squirreld does not depend on `keyring` directly. Retrieve the key yourself with the [`keyring`](https://crates.io/crates/keyring) crate and pass it as `RawKey`:

```rust
let entry = keyring::Entry::new("my-app", "encryption-key")?;
let hex_key = match entry.get_password() {
    Ok(k) => k,
    Err(_) => {
        // First run: generate and store.
        let key = generate_random_32_bytes();
        entry.set_password(&hex::encode(&key))?;
        hex::encode(&key)
    }
};
let kek: [u8; 32] = hex::decode(&hex_key)?.try_into().unwrap();
engine_builder.encryption_key(KeySource::RawKey(kek))
```

### Encryption scheme

Each record gets a unique 256-bit DEK encrypted with the KEK (envelope encryption):

```
plaintext → AES-256-GCM(DEK) → stored as records.data
DEK       → AES-256-GCM(KEK) → stored as records.dek_encrypted
```

`format_version = 1` in the DB row flags encrypted records. Mixed-mode collections are supported — plain and encrypted records coexist.

### Per-item override

```rust
use squirreld::{ItemEncryption, PutOpts};

// Force plaintext for this record, even though the engine has a KEK.
engine.put("papers", None, public_data, PutOpts {
    encryption: ItemEncryption::Disabled,
    ..Default::default()
}).await?;

// Force encryption (errors if no KEK is configured).
engine.put("papers", None, private_data, PutOpts {
    encryption: ItemEncryption::Enabled,
    ..Default::default()
}).await?;

// Default: encrypt if a KEK is configured, otherwise plaintext.
engine.put("papers", None, data, PutOpts::default()).await?;
```

### Sync and encryption

The outbox always carries **plaintext** payloads. Remote peers receive unencrypted data and store it with `format_version = 0` — each device manages its own encryption independently. This means:

- Two devices can sync even if only one has encryption enabled.
- A device that loses its KEK can re-encrypt records retrieved from the cloud.
- The remote backend (DynamoDB) never sees ciphertext by default.

If true E2E encryption is required — where the backend sees only ciphertext — encrypt the payload yourself before calling `put()` and pass `ItemEncryption::Disabled`.

---

## 6. Blob store

```rust
use squirreld::{PutBlobOpts, BlobStatus};

// Stage a local file for background upload.
let blob_id = engine.put_blob(
    "/home/user/documents/paper.pdf",
    PutBlobOpts {
        record_id:  Some(paper_id.to_string()), // link to a record
        collection: Some("papers".into()),
    },
).await?;
// Returns immediately — upload happens in the background.

// Check upload status.
let info = engine.blob_info(&blob_id).await?.unwrap();
match info.status {
    BlobStatus::Pending   => println!("queued"),
    BlobStatus::Uploading => println!("uploading ({} retries)", info.retries),
    BlobStatus::Uploaded  => println!("done ({} bytes)", info.size_bytes.unwrap_or(0)),
    BlobStatus::Cached    => println!("cached locally"),
}

// Get the local path (triggers download if not cached).
match engine.get_blob(&blob_id).await? {
    Some(path) => {
        // file is available at `path`
        open_pdf(path)?;
    }
    None => {
        // Not yet cached; subscribe to SyncEvent::BlobDownloaded to know when it's ready.
    }
}

// Force an immediate upload+download pass (useful in tests or after reconnect).
engine.force_flush_blobs().await?;
```

Files under 5 MB use `PutObject`; files ≥ 5 MB use multipart upload with 5 MB chunks. Uploads resume after interruption: on restart the worker calls `ListParts` against S3 to find already-uploaded chunks and skips them.

---

## 7. Sync backends

### DynamoDB

```rust
use squirreld::backend::dynamodb::{AwsCredentials, DynamoDbBackend, DynamoDbConfig};

let backend = DynamoDbBackend::new(DynamoDbConfig {
    table_name:  "myapp-records".into(),
    region:      "us-east-1".into(),
    credentials: AwsCredentials::Explicit {
        access_key_id:     std::env::var("AWS_ACCESS_KEY_ID")?,
        secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY")?,
        session_token:     None,
    },
}).await?;
```

The backend calls `ensure_table()` at engine startup — it creates the DynamoDB table and `sync-index` GSI if they don't exist (idempotent).

**Table design**: single-table with `pk = record_id`, `sk = collection`, and a GSI `sync-index` on `(_p="main", hlc)` for efficient pull queries. Pull cost is O(records changed since checkpoint) — not a full table scan.

**Conflict resolution**: push uses a conditional expression `attribute_not_exists(hlc) OR hlc < :local_hlc`. If the remote record has a higher HLC, the push is rejected and a pull is triggered; the local copy is updated if the remote wins.

### S3

```rust
use squirreld::backend::s3::{S3Backend, S3Config, S3Credentials};

let backend = S3Backend::new(S3Config {
    bucket_name:  "myapp-blobs".into(),
    region:       "us-east-1".into(),
    endpoint_url: None,  // Override for LocalStack: Some("http://localhost:4566".into())
    credentials:  S3Credentials::FromEnvironment,
}).await?;
```

### Custom backend

Implement the traits for any storage backend:

```rust
use async_trait::async_trait;
use squirreld::backend::{BackendError, RecordBackend, OutboxPushEntry, PushResult, RemoteRecord};

struct MyBackend { /* ... */ }

#[async_trait]
impl RecordBackend for MyBackend {
    fn backend_id(&self) -> &str { "my-backend" }

    async fn ensure_table(&self) -> Result<(), BackendError> { Ok(()) }

    async fn push_one(&self, entry: &OutboxPushEntry) -> PushResult {
        // store entry.data at entry.record_id; enforce LWW with entry.hlc
        PushResult::Ok { pushed_seqs: vec![entry.seq] }
    }

    async fn pull_since(&self, since: Option<&str>) -> Result<Vec<RemoteRecord>, BackendError> {
        // return records with hlc > since, ordered by hlc ASC
        Ok(vec![])
    }
}
```

---

## 8. Sync events

Subscribe to a broadcast channel to observe sync activity:

```rust
use squirreld::SyncEvent;

let mut events = engine.sync_events();

tokio::spawn(async move {
    while let Ok(event) = events.recv().await {
        match event {
            SyncEvent::PushComplete(stats) => {
                println!("pushed {} records", stats.pushed);
            }
            SyncEvent::PullComplete(stats) => {
                println!("pulled {} records", stats.pulled);
            }
            SyncEvent::BlobUploaded { blob_id } => {
                println!("blob {blob_id} is now on S3");
            }
            SyncEvent::BlobDownloaded { blob_id } => {
                println!("blob {blob_id} is now cached locally");
            }
            SyncEvent::RetryScheduled { seq, retries, next_retry_ms, error } => {
                eprintln!("outbox seq {seq} failed (retry {retries}): {error}");
            }
            SyncEvent::BlobRetryScheduled { blob_id, retries, .. } => {
                eprintln!("blob {blob_id} upload failed (retry {retries})");
            }
        }
    }
});
```

### Inspecting and clearing stuck entries

```rust
// Records that have failed at least one sync attempt.
let errors = engine.pending_errors().await?;
for e in &errors {
    println!("seq={} record={} retries={}: {:?}", e.seq, e.record_id, e.retries, e.last_error);
}

// Manually discard a stuck entry (the record stays locally; only the outbox entry is removed).
engine.clear_error(errors[0].seq).await?;
```

---

## 9. Testing with in-memory doubles

The `test-utils` feature (included in `default`) exports `InMemoryBackend` and `InMemoryBlobBackend`. Both are thread-safe and can be shared between engine instances to simulate two-device sync without any network or filesystem I/O.

```rust
use std::sync::Arc;
use squirreld::{
    backend::in_memory::{InMemoryBackend, InMemoryBlobBackend, InMemoryBlobStore, InMemoryStore},
    PutOpts, SquirrelEngine,
};
use tempfile::tempdir;

#[tokio::test]
async fn two_device_sync() {
    let shared_records = InMemoryStore::new_shared();
    let shared_blobs   = InMemoryBlobStore::new_shared();
    let dir_a = tempdir().unwrap();
    let dir_b = tempdir().unwrap();

    let engine_a = SquirrelEngine::builder()
        .db_path(dir_a.path().join("a.db"))
        .record_backend(Arc::new(InMemoryBackend::new("a", shared_records.clone())))
        .blob_backend(Arc::new(InMemoryBlobBackend::new("a", shared_blobs.clone())))
        .build().await.unwrap();

    let engine_b = SquirrelEngine::builder()
        .db_path(dir_b.path().join("b.db"))
        .record_backend(Arc::new(InMemoryBackend::new("b", shared_records.clone())))
        .blob_backend(Arc::new(InMemoryBlobBackend::new("b", shared_blobs.clone())))
        .build().await.unwrap();

    // A writes a record.
    let id = engine_a.put("notes", None, b"hello".to_vec(), PutOpts::default())
        .await.unwrap();

    // Sync: A pushes, B pulls.
    engine_a.force_sync().await.unwrap();
    engine_b.force_sync().await.unwrap();

    // B has the record.
    let rec = engine_b.get("notes", id).await.unwrap().unwrap();
    assert_eq!(rec.data, b"hello");
}
```

`InMemoryStore::new_shared()` returns an `Arc<Mutex<InMemoryStore>>`. Pass the same instance to both engines to share the "remote" state.

---

## 10. Feature flags

| Flag | Default | Description |
|---|---|---|
| `dynamodb` | ✅ | DynamoDB record sync backend (`aws-sdk-dynamodb`) |
| `s3` | ✅ | S3 blob store backend (`aws-sdk-s3`) |
| `encryption` | ✅ | AES-256-GCM encryption + Argon2id KDF (`aes-gcm`, `argon2`) |
| `test-utils` | ✅ | Exports `InMemoryBackend` and `InMemoryBlobBackend` for downstream tests |
| `integration-tests` | ❌ | Enables LocalStack-backed integration tests (requires Docker) |

To use only the local store with no cloud dependencies:

```toml
squirreld = { version = "0.1", default-features = false, features = ["encryption", "test-utils"] }
```

---

## 11. Design notes

### Hybrid Logical Clock

Every record carries an HLC — a string of the form `{physical_ms:013x}-{logical:04x}-{node_id_hex}` (e.g. `0195f3a1e42b-0003-a7f2e1c9b8d4`). HLCs are lexicographically comparable, so DynamoDB and SQLite sort them correctly with plain `>` without parsing. The node ID (6 random bytes, generated once per device) prevents HLC collisions between devices that write in the same millisecond.

### Transactional outbox

Every `put` and `delete` atomically appends an entry to the `outbox` table in the same SQLite transaction as the record write. The sync loop drains the outbox in FIFO order. If the process crashes between write and sync, the outbox entry survives and the record is pushed on the next run. There is no window where data is written locally but not tracked for sync.

### Connection ownership

`rusqlite::Connection` is `!Sync`. The actor owns one `Connection` for all synchronous record operations. The sync loop and blob worker each open fresh connections (one per DB phase), drop them before any `.await`, and reopen after — never holding a borrow across an async boundary. SQLite WAL mode makes concurrent readers cheap.

### Actor backpressure

The command channel has a bounded capacity (default: 256). If the actor falls behind, callers block on `put`/`get` instead of growing memory unboundedly. Tune with `.channel_capacity(n)` on the builder.

### Retry policy

Both outbox entries (sync) and blob uploads use exponential backoff capped at 5 minutes:

```
next_retry_at = now + min(1000ms × 2^retries, 300_000ms)
```

There is no permanent failure state — entries retry indefinitely. `pending_errors()` surfaces them to the UI; `clear_error()` discards them manually.

---

## License

MIT
