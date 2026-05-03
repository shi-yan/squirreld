# Squirreld — Implementation Plan

Each phase builds on the previous. Phases 1–3 produce a functional (non-encrypted, non-indexed) sync engine that unfolded can use. Phases 4–6 add the quality-of-life features.

---

## Phase 1: Foundation — Local Store + Actor

**Goal**: A working local-only record store behind the MPSC actor. No cloud, no encryption, no indexing. Just a stable API and storage layer.

### Tasks

1. **Cargo setup**
   - Set `edition = "2024"`, add `rusqlite` (bundled), `tokio`, `ulid`, `thiserror`, `tracing`, `serde_json`, `async-trait`
   - Create `src/lib.rs`, `src/engine.rs`, `src/db/`, `src/hlc.rs`, `src/error.rs`

2. **HLC implementation** (`src/hlc.rs`)
   - `struct Hlc { physical_ms: u64, logical: u16, node_id: [u8; 6] }`
   - `impl Display` and `impl FromStr` using format `{physical_ms:013x}-{logical:04x}-{node_id_hex}`
   - `Hlc::now(last: &Hlc) -> Hlc` — returns a new HLC that is strictly greater than `last` and wall clock
   - `impl PartialOrd` based on string comparison (valid since format is lexicographically sortable)

3. **SQLite schema** (`src/db/schema.rs`)
   - `fn create_tables(conn: &Connection) -> Result<()>` — runs the full DDL from ARCHITECTURE §5
   - Enable WAL mode: `PRAGMA journal_mode=WAL`
   - Enable foreign keys: `PRAGMA foreign_keys=ON`

4. **Config table helpers** (`src/db/config.rs`)
   - `fn get_or_create_node_id(conn: &Connection) -> Result<[u8; 6]>`
   - `fn get_string(conn: &Connection, key: &str) -> Result<Option<String>>`
   - `fn set_string(conn: &Connection, key: &str, value: &str) -> Result<()>`

5. **Record CRUD** (`src/db/records.rs`)
   - `fn put(conn: &Connection, collection: &str, id: &str, data: &[u8], hlc: &Hlc, ...) -> Result<()>`
   - `fn get(conn: &Connection, collection: &str, id: &str) -> Result<Option<RecordRow>>`
   - `fn delete(conn: &Connection, collection: &str, id: &str, hlc: &Hlc) -> Result<()>` (sets tombstone)
   - `fn list(conn: &Connection, collection: &str, opts: &ListOpts) -> Result<Vec<RecordRow>>`
   - Each write atomically appends to the outbox in the same transaction

6. **Outbox helpers** (`src/db/outbox.rs`)
   - `fn append(conn: &Connection, entry: &OutboxEntry) -> Result<()>`
   - `fn peek_batch(conn: &Connection, n: usize) -> Result<Vec<OutboxEntry>>`
   - `fn delete_batch(conn: &Connection, seqs: &[i64]) -> Result<()>`
   - `fn mark_error(conn: &Connection, seq: i64, error: &str) -> Result<()>`

7. **Actor + engine handle** (`src/engine.rs`)
   - `enum Command` — Put, Get, Delete, List, Query, Search, PutBlob, GetBlob, BlobStatus, ForceSync, Subscribe, Shutdown
   - `struct SquirrelEngine` — wraps `mpsc::Sender<Command>`
   - `SquirrelEngine::open(config: EngineConfig) -> Result<Self>` — spawns actor task, returns handle
   - Actor main loop: `while let Some(cmd) = rx.recv().await { handle_command(cmd, &state).await }`

8. **Builder API** (`src/builder.rs`)
   - `EngineBuilder` for constructing `EngineConfig`
   - Validates that collection names are unique, index field names don't clash

### Deliverable

```rust
let engine = SquirrelEngine::builder()
    .db_path("~/.local/share/myapp/squirreld.db")
    .collection("papers", CollectionConfig::default())
    .build()
    .await?;

let id = engine.put("papers", None, b"{\"title\":\"Test\"}").await?;
let data = engine.get("papers", id).await?.unwrap();
assert_eq!(data, b"{\"title\":\"Test\"}");
```

---

## Phase 2: Sync Engine — DynamoDB Push/Pull

**Goal**: Records written locally are pushed to DynamoDB. Records written on another device are pulled and merged locally. No encryption, no blob support yet.

### Tasks

1. **Backend HAL** (`src/backend/mod.rs`)
   - Define `RecordBackend` trait (see ARCHITECTURE §7)
   - Define `OutboxEntry`, `RemoteRecord`, `PushResult` types

2. **DynamoDB backend** (`src/backend/dynamodb.rs`)
   - Add `aws-sdk-dynamodb` to Cargo.toml
   - `struct DynamoDbBackend { client: DynamoDbClient, table_name: String, user_id: String }`
   - `impl RecordBackend for DynamoDbBackend`
   - `push`: maps outbox entries to `TransactWriteItems` with condition expression `attribute_not_exists(hlc) OR hlc < :local_hlc`
   - `pull_since`: queries the `hlc-index` GSI with `PK = USER#me AND hlc > :checkpoint`

3. **DynamoDB table bootstrap** (`src/backend/dynamodb.rs`)
   - `DynamoDbBackend::ensure_table(client, table_name) -> Result<()>`
   - Creates the table and GSI if they don't exist; idempotent
   - Documents the required IAM permissions (as a comment or in README)

4. **Sync loop** (`src/sync/mod.rs`)
   - `struct SyncLoop { backend: Arc<dyn RecordBackend>, db: Arc<Mutex<Connection>>, ... }`
   - `async fn run_push_cycle(&self) -> Result<()>` — implements the push algorithm from ARCHITECTURE §8.1
   - `async fn run_pull_cycle(&self) -> Result<()>` — implements the pull algorithm from ARCHITECTURE §8.2
   - `async fn run(&self, mut trigger: mpsc::Receiver<()>)` — waits for trigger, debounces 500ms, runs push then pull
   - Triggers: post-write debounce, 60s periodic timer, `force_sync` command

5. **Conflict resolution in pull**
   - After pulling remote records, cross-check outbox: any outbox entry for a record where remote won gets deleted
   - Any outbox entry where local HLC > remote HLC is kept (will be pushed in the next push cycle)

6. **SyncEvent broadcast**
   - `enum SyncEvent { PushComplete(SyncStats), PullComplete(SyncStats), PermanentFailure { seq: i64, error: String } }`
   - Actor holds a `broadcast::Sender<SyncEvent>`; `engine.sync_events()` returns a receiver

7. **Retry logic**
   - Transient errors: increment `outbox.retries`, apply `min(60 * 2^retries, 300)` second backoff
   - After 10 retries: mark as permanent failure, emit `SyncEvent::PermanentFailure`

### Deliverable

Records written on device A appear on device B after a `force_sync()` on both. LWW conflict tested with two devices writing the same record ID while offline.

---

## Phase 3: Blob Store — S3 + Resumable Uploads

**Goal**: Large files (PDFs, images) can be stored with S3 as the backend. Uploads resume correctly after interruption.

### Tasks

1. **BlobBackend HAL** (`src/backend/mod.rs`)
   - Define `BlobBackend` trait (see ARCHITECTURE §7)

2. **S3 backend** (`src/backend/s3.rs`)
   - Add `aws-sdk-s3` to Cargo.toml
   - Implement all multipart methods
   - `download`: streams `GetObject` response to a `tokio::fs::File`, returns bytes written
   - For files < 5MB: use `PutObject` / `GetObject` directly, skip multipart

3. **Staging area**
   - `src/blob/staging.rs`
   - `fn stage(source: &Path, cache_dir: &Path) -> Result<(BlobId, PathBuf)>` — copies file, assigns ULID
   - `fn cache_path(cache_dir: &Path, blob_id: BlobId) -> PathBuf`

4. **Blob uploader** (`src/blob/uploader.rs`)
   - `struct BlobUploader { backend: Arc<dyn BlobBackend>, db: Arc<Mutex<Connection>>, cache_dir: PathBuf }`
   - `async fn run_upload_cycle(&self) -> Result<()>`:
     - Scan `status IN ('pending', 'uploading')`
     - For 'uploading': call `resume_upload`
     - For 'pending': call `start_upload`
   - `async fn start_upload(&self, blob: BlobRow) -> Result<()>`
   - `async fn resume_upload(&self, blob: BlobRow) -> Result<()>` — calls ListParts, skips uploaded chunks

5. **Blob downloader** (`src/blob/downloader.rs`)
   - `async fn get_blob(blob_id: BlobId, dest: &Path) -> Result<()>`
   - Checks cache first; downloads from S3 if not cached; updates `blobs` table status

6. **Chunk size constant**
   - `const CHUNK_SIZE: usize = 5 * 1024 * 1024;` — 5MB
   - Final chunk may be smaller (S3 allows this)

7. **Actor integration**
   - `PutBlob` command: stage → insert to DB → trigger uploader → return BlobId
   - `GetBlob` command: check cache → maybe download → return path
   - `BlobStatus` command: return current status from `blobs` table

### Deliverable

A 200MB PDF can be:
- Staged and queued in < 100ms (caller is unblocked immediately)
- Uploaded to S3 in the background
- Interrupted mid-upload and resumed correctly on next run

---

## Phase 4: Local Indexing

**Goal**: Callers can define indexed fields and run structured queries and full-text search against local data.

### Tasks

1. **Collection configuration** (`src/collection.rs`)
   - `struct CollectionConfig { schema_version: u32, migrate: Option<MigrateFn>, indices: Vec<IndexDef>, fts_fields: Vec<String> }`
   - `struct IndexDef { field_path: String, kind: IndexKind }`
   - `enum IndexKind { Text, Integer, Float, TextArray }`

2. **Shadow table DDL generation** (`src/db/indices.rs`)
   - `fn ensure_shadow_table(conn: &Connection, collection: &str, indices: &[IndexDef]) -> Result<()>`
   - Generates `CREATE TABLE IF NOT EXISTS idx_{collection} (record_id TEXT PRIMARY KEY, ...)`
   - Column types map from `IndexKind`
   - `fn ensure_fts_table(conn: &Connection, collection: &str, fts_fields: &[String]) -> Result<()>`

3. **Index extraction** (`src/db/indices.rs`)
   - `fn extract_and_upsert(conn: &Connection, collection: &str, record_id: &str, data: &[u8], config: &CollectionConfig) -> Result<()`
   - Uses `serde_json::Value` to navigate nested field paths (dot notation: `"metadata.year"`)
   - Called inside the same transaction as the record write

4. **FTS update trigger**
   - After shadow table upsert, run `INSERT INTO fts_{collection}(record_id, content) VALUES (?, ?)` with concatenated FTS fields
   - FTS deletions handled by `DELETE FROM fts_{collection} WHERE record_id = ?` before insert

5. **QueryExpr to SQL** (`src/query.rs`)
   - `fn to_sql(expr: &QueryExpr, collection: &str) -> (String, Vec<rusqlite::types::Value>)`
   - Generates parameterized SQL; no string interpolation of user data

6. **Schema migration hook**
   - On actor startup: for each collection, run `SELECT id, data, schema_version FROM records WHERE collection = ? AND schema_version < ?`
   - For each stale record: call `migrate(old_version, data)`, write back with new schema_version, update index

### Deliverable

```rust
let results = engine.query("papers",
    QueryExpr::and(
        QueryExpr::eq("year", 2023),
        QueryExpr::eq("tags", "llm")
    )
).await?;

let hits = engine.search("papers", "attention mechanism").await?;
```

---

## Phase 5: E2E Encryption

**Goal**: All data at rest and in transit is encrypted. The backend (DynamoDB + S3) never sees plaintext.

### Tasks

1. **Crypto primitives** (`src/crypto.rs`)
   - Add `aes-gcm`, `argon2`, `rand` to Cargo.toml
   - `fn generate_dek() -> [u8; 32]`
   - `fn encrypt_dek(dek: &[u8; 32], kek: &[u8; 32]) -> Result<Vec<u8>>`
   - `fn decrypt_dek(encrypted_dek: &[u8], kek: &[u8; 32]) -> Result<[u8; 32]>`
   - `fn encrypt_record(plaintext: &[u8], dek: &[u8; 32]) -> Result<Vec<u8>>` — `[nonce(12)] || [ciphertext] || [tag(16)]`
   - `fn decrypt_record(ciphertext: &[u8], dek: &[u8; 32]) -> Result<Vec<u8>>`
   - `fn encrypt_chunk(chunk: &[u8], dek: &[u8; 32], base_nonce: &[u8; 12], chunk_index: u32) -> Result<Vec<u8>>`
   - `fn decrypt_chunk(ciphertext: &[u8], dek: &[u8; 32], base_nonce: &[u8; 12], chunk_index: u32) -> Result<Vec<u8>>`

2. **Key management** (`src/crypto/keychain.rs`)
   - Add `keyring` to Cargo.toml
   - `struct KeyManager { service_name: String }`
   - `fn get_or_create_master_key(&self) -> Result<[u8; 32]>` — reads from keychain, creates and stores if absent
   - `fn derive_from_passphrase(passphrase: &str, salt: &[u8]) -> Result<[u8; 32]>` — Argon2id fallback
   - The salt is stored in the `config` table

3. **Encryption integration in actor**
   - If `EncryptionConfig::Enabled`, wrap all `db::records::put` calls with encrypt/decrypt
   - Index extraction happens **before** encryption (on plaintext)
   - FTS is disabled when encryption is enabled (logged as a warning on startup)
   - `format_version` field set to `1` for all encrypted records

4. **Blob encryption integration**
   - `BlobUploader`: if encryption enabled, encrypt each chunk before uploading
   - Store `dek_encrypted` in S3 user metadata: `x-amz-meta-dek = base64(encrypted_dek)`
   - Store `base_nonce` in S3 user metadata: `x-amz-meta-nonce = base64(base_nonce)`
   - `BlobDownloader`: read metadata, decrypt DEK with KEK, decrypt chunks on the fly

5. **Encrypted shadow indices (optional, initially skipped)**
   - If needed in the future: store `SHA-256(field_value)` in shadow index for equality-only queries
   - Skip for Phase 5; callers can fetch-and-filter in memory

### Deliverable

Engine configured with encryption enabled stores only ciphertext in SQLite and DynamoDB. Master key is retrieved from the OS keychain. The app works normally with no API changes.

---

## Phase 6: Unfolded Integration

**Goal**: Replace unfolded's broken sync engine with squirreld.

### Tasks

1. Add squirreld as a path dependency in unfolded's Cargo.toml
2. Define collections: `papers`, `notes`, `readlist`
3. Register index extractors for each collection
4. Replace the current database access layer with squirreld's API
5. Integrate `sync_events()` receiver with the Tauri event system to update UI on sync state changes
6. Wire S3 PDF uploads/downloads through `put_blob` / `get_blob`
7. Test the full offline scenario: add paper + notes offline, sync, verify on second device

---

## Testing Strategy

Each phase should include:
- **Unit tests** for pure functions (HLC arithmetic, crypto primitives, SQL generation)
- **Integration tests** using a temporary SQLite file (`tempfile` crate)
- **Backend tests** using `localstack` (local AWS emulator) in CI for DynamoDB and S3

Phase 2 onward: a test that simulates two "devices" (two `SquirrelEngine` instances pointing at the same DynamoDB table via localstack) and verifies correct LWW merge behavior.

---

## Open Questions

1. **Passphrase vs. keychain**: Should the engine support both modes simultaneously (keychain with passphrase fallback), or require the caller to pick one? Recommendation: both, with keychain tried first and passphrase-derived key as fallback.

2. **S3 cache eviction**: The local blob cache will grow unbounded. Should squirreld provide a cache eviction policy (e.g. LRU eviction beyond a configured max size), or leave that to the caller? Recommendation: caller-controlled; squirreld provides a `blob_cache_size()` query and a `evict_blob(blob_id)` command.

3. **Outbox dead-letter handling**: After a permanent push failure, should the entry stay in SQLite indefinitely for manual inspection, or be auto-deleted after N days? Recommendation: keep it, and expose a `dead_outbox_entries()` query so callers can surface errors in the UI.

4. **Collection-level encryption toggle**: Should some collections be encrypted and others not? Recommendation: not for now; encryption is engine-wide. Simplicity wins for a personal tool.

5. **Multi-table DynamoDB vs. single-table**: Single-table design chosen for Phase 2. If a collection grows to > 10 million records, the GSI scan for pull could become expensive. At that scale, a per-collection table is better. Not a concern for personal use.
