# Squirreld — Implementation Plan

## Testing Philosophy

Every phase includes tests before moving on. Two test tiers:

**Tier 1 — Unit tests** (`cargo test`, no external deps):
- Pure function tests: HLC arithmetic, crypto primitives, SQL generation, QueryExpr compilation
- Integration tests against a real SQLite file in a `tempdir` (fast, hermetic)
- Sync logic tests using **in-memory mock backends** — structs implementing `RecordBackend` / `BlobBackend` via a `HashMap` and a temp directory. These exercise conflict resolution, outbox ordering, retry scheduling, pull reconciliation, and encryption without any network calls.
- The mock backends ship as a `test-utils` feature in the crate so downstream apps (e.g. unfolded) can use them too.

**Tier 2 — Integration tests** (`cargo test --features integration-tests`, requires Docker):
- Tests against **LocalStack** via the `testcontainers` + `testcontainers-modules` Rust crates. LocalStack emulates DynamoDB and S3 locally.
- The `testcontainers` crate manages the Docker container lifecycle inside the test process — no manual setup required.
- These tests exercise the actual AWS SDK calls: condition expressions, multipart upload protocol, GSI queries, etc.
- Never use real AWS in automated tests. Real AWS is only for manual smoke-testing before a release.

```
crate features:
  default          → core library only
  test-utils       → exports InMemoryBackend, InMemoryBlobBackend for use in downstream tests
  integration-tests → enables LocalStack-backed tests (requires Docker)
```

---

## Phase 1: Foundation — Local Store + Actor

**Goal**: A working local-only record store behind the async MPSC actor. No cloud, no encryption, no indexing. Stable API and storage layer.

### 1.1 Crate structure

```
src/
  lib.rs           — public re-exports
  engine.rs        — SquirrelEngine handle + actor main loop
  builder.rs       — EngineBuilder, EngineConfig, CollectionConfig
  hlc.rs           — Hlc type, tick(), Display, FromStr, PartialOrd
  error.rs         — SquirrelError, Result<T>
  db/
    mod.rs
    schema.rs      — DDL, PRAGMA setup
    config.rs      — config table helpers
    records.rs     — CRUD + outbox append (atomic)
    outbox.rs      — peek_batch, delete_batch, mark_retry
  query.rs         — QueryExpr, to_sql()
  types.rs         — Ulid newtype, RecordMeta, BlobId, BlobStatus, SyncStats, etc.
```

### 1.2 Tasks

1. **Cargo.toml**: `rusqlite/bundled`, `tokio/full`, `ulid`, `thiserror`, `tracing`, `serde_json`, `async-trait`

2. **`hlc.rs`**:
   - `struct Hlc { physical_ms: u64, logical: u16, node_id: [u8; 6] }`
   - `Hlc::tick(last: &Hlc) -> Hlc` — returns `max(wall_clock, last.physical)` with incremented logical
   - `impl Display` → `{physical_ms:013x}-{logical:04x}-{node_id_hex}`
   - `impl FromStr` — parse the above format
   - `impl PartialOrd` — delegates to string comparison (valid because format is lex-sortable)

3. **`db/schema.rs`**:
   - `fn initialize(conn: &Connection) -> Result<()>` — sets WAL + foreign_keys, runs all CREATE TABLE IF NOT EXISTS from ARCHITECTURE §5
   - Re-runnable: uses `IF NOT EXISTS` throughout, safe to call on every startup

4. **`db/config.rs`**:
   - `fn get_or_create_node_id(conn: &Connection) -> Result<[u8; 6]>`
   - `fn get(conn: &Connection, key: &str) -> Result<Option<String>>`
   - `fn set(conn: &Connection, key: &str, value: &str) -> Result<()>`

5. **`db/records.rs`** — all writes append to the outbox in the same transaction:
   - `fn upsert(conn: &Connection, record: &RecordRow) -> Result<()>`
   - `fn get(conn: &Connection, collection: &str, id: &str) -> Result<Option<RecordRow>>`
   - `fn soft_delete(conn: &Connection, collection: &str, id: &str, hlc: &Hlc) -> Result<()>`
   - `fn list(conn: &Connection, collection: &str, opts: &ListOpts) -> Result<Vec<RecordRow>>`

6. **`db/outbox.rs`**:
   - `fn peek_batch(conn: &Connection, limit: usize) -> Result<Vec<OutboxEntry>>` — `WHERE next_retry_at <= now() ORDER BY seq ASC`
   - `fn delete_seqs(conn: &Connection, seqs: &[i64]) -> Result<()>`
   - `fn mark_retry(conn: &Connection, seq: i64, error: &str) -> Result<()>` — increments retries, sets `next_retry_at = now + min(1000 * 2^retries, 300_000)ms`, appends to `error_log` JSON array (capped at 20 entries)

7. **`engine.rs`** — actor + handle:
   - `enum Command { Put{...}, Get{...}, Delete{...}, List{...}, Shutdown{...}, ... }` with `oneshot::Sender` reply channels
   - `struct SquirrelEngine(mpsc::Sender<Command>)` — `#[derive(Clone)]`
   - `SquirrelEngine::open(config: EngineConfig) -> Result<Self>` — opens SQLite, runs `schema::initialize`, spawns actor task
   - Actor loop: `while let Some(cmd) = rx.recv().await { dispatch(cmd, &mut state).await }`

8. **`builder.rs`**:
   - `EngineBuilder` with fluent API
   - Validates: unique collection names, no reserved names (`config`, `outbox`, `blobs`, etc.)

### 1.3 Unit Tests

```
tests/hlc.rs
  - tick() produces strictly increasing values even when wall clock is frozen
  - tick() handles logical counter overflow gracefully
  - Display/FromStr round-trip
  - PartialOrd: string comparison matches semantic ordering

tests/basic_crud.rs  (uses tempdir + real SQLite)
  - put then get returns the same bytes
  - put with same id twice updates the record, preserves the higher HLC
  - soft_delete sets deleted=1, get returns None
  - list returns only non-deleted records in HLC descending order
  - outbox has one entry per write, in seq order

tests/outbox.rs
  - peek_batch respects next_retry_at (future items not returned)
  - mark_retry increments retries and sets correct next_retry_at
  - error_log is a valid JSON array capped at 20 entries
  - delete_seqs removes exactly the specified entries
```

---

## Phase 2: Sync Engine — DynamoDB Push/Pull

**Goal**: Records written locally push to DynamoDB. Records from another device pull and merge locally via LWW.

### 2.1 New modules

```
src/
  backend/
    mod.rs         — RecordBackend trait, OutboxEntry, RemoteRecord, PushResult
    dynamodb.rs    — DynamoDbBackend impl
  sync/
    mod.rs         — SyncLoop, push cycle, pull cycle, debounce timer
```

### 2.2 Tasks

1. **`backend/mod.rs`** — define the trait (see ARCHITECTURE §7):
   ```rust
   pub enum PushResult {
       Ok { pushed: Vec<i64> },               // seqs successfully pushed
       ConflictAt { record_id: String },       // LWW conflict, trigger pull
       TransientError(anyhow::Error),
   }
   ```

2. **`backend/dynamodb.rs`**:
   - Add `aws-sdk-dynamodb` to Cargo.toml
   - `DynamoDbBackend::new(config: DynamoDbConfig) -> Self`
   - `ensure_table()` — creates table + GSI `sync-index` if absent (idempotent)
   - `push()` — maps outbox entries to `TransactWriteItems` with condition `attribute_not_exists(hlc) OR hlc < :local_hlc`
   - `pull_since()` — `Query` on `sync-index` with `KeyConditionExpression: #p = :main AND hlc > :checkpoint`, paginated, returns items sorted by `hlc ASC`

3. **`sync/mod.rs`**:
   - `struct SyncLoop` holds `Arc<dyn RecordBackend>`, `Arc<Mutex<Connection>>`, `broadcast::Sender<SyncEvent>`
   - `async fn run_push_cycle(&self) -> Result<()>` — implements ARCHITECTURE §8.1
   - `async fn run_pull_cycle(&self) -> Result<()>` — implements ARCHITECTURE §8.2
   - `async fn run(self, trigger_rx: mpsc::Receiver<()>)` — main loop:
     - Waits for trigger OR 60s timeout
     - Debounces: if another trigger arrives within 500ms, resets the timer
     - Runs push then pull
   - Pull reconciliation: after a `ConflictAt` from push, run pull, then re-examine the outbox head — delete it if remote won, retry if local still wins

4. **Actor integration**:
   - Post-write: send `()` to the sync trigger channel (non-blocking, drops if full — debounce handles the rest)
   - `ForceSync` command: send trigger and await `SyncStats` reply

5. **`SyncEvent` broadcast** in `types.rs`:
   ```rust
   pub enum SyncEvent {
       PushComplete(SyncStats),
       PullComplete(SyncStats),
       BlobUploaded { blob_id: BlobId },
       RetryScheduled { seq: i64, retries: u32, next_retry_ms: u64, error: String },
   }
   ```

### 2.3 Unit Tests (Tier 1 — in-memory mock backend)

```
tests/sync_push.rs
  - Single device: write N records, push succeeds, outbox is empty
  - Outbox is FIFO: if seq 3 has next_retry_at in the future, seq 4+ are not pushed
  - Transient error: retries incremented, next_retry_at set with correct backoff
  - Backoff ceiling: retries=20 still produces next_retry_at <= now + 300_000ms

tests/sync_pull.rs
  - Remote record not in local: gets inserted
  - Remote HLC > local HLC: local record updated, outbox entry deleted
  - Remote HLC < local HLC: local record untouched, outbox entry kept
  - Remote HLC == local HLC: no-op (same device, idempotent)
  - last_pull_hlc advances to max(remote HLC) after successful pull

tests/sync_conflict.rs
  - Device A and B both write record X while offline
  - A pushes first (succeeds), B pushes second (ConflictAt)
  - B triggers pull: gets A's version
  - If A's HLC > B's HLC: B discards its outbox entry, local = A's version
  - If B's HLC > A's HLC: B retries push (B wins), A pulls on next cycle

tests/sync_two_devices.rs  (Tier 1, two SquirrelEngine instances, shared InMemoryBackend)
  - End-to-end: engine_a writes, force_sync; engine_b force_sync; engine_b.get returns a's data
  - Tombstone propagation: engine_a deletes record; engine_b pulls; engine_b.get returns None
```

### 2.4 Integration Tests (Tier 2 — LocalStack)

```
tests/dynamodb_integration.rs  (#[cfg(feature = "integration-tests")])
  - ensure_table() is idempotent (call twice, no error)
  - push() correctly writes items with condition expressions
  - push() returns ConflictAt when remote HLC is higher
  - pull_since(None) returns all items
  - pull_since(Some(hlc)) returns only items with hlc > checkpoint
  - Pagination: pull_since handles result sets > DynamoDB page size (1MB)
```

---

## Phase 3: Blob Store — S3 + Resumable Uploads

**Goal**: Large files can be staged locally and uploaded to S3 in the background. Uploads resume after interruption.

### 3.1 New modules

```
src/
  backend/
    s3.rs          — S3Backend impl
  blob/
    mod.rs
    staging.rs     — copy to cache dir, assign BlobId
    uploader.rs    — background upload loop, resume logic
    downloader.rs  — cache-first download
```

### 3.2 Tasks

1. **`backend/s3.rs`**: Add `aws-sdk-s3`, `bytes` to Cargo.toml; implement `BlobBackend` trait

2. **`blob/staging.rs`**:
   - `fn stage(source: &Path, cache_dir: &Path) -> Result<(BlobId, PathBuf)>` — copies file, assigns ULID filename
   - `const MULTIPART_THRESHOLD: u64 = 5 * 1024 * 1024` (5MB)

3. **`blob/uploader.rs`**:
   - `async fn run_upload_cycle(...)` — scans `status IN ('pending','uploading') AND next_retry_at <= now()`
   - `start_upload`: CreateMultipartUpload → save upload_id → upload chunks → CompleteMultipartUpload
   - `resume_upload`: ListParts → skip already-uploaded → upload missing → CompleteMultipartUpload
   - On any S3 error: `mark_blob_retry()` — same exponential backoff as outbox
   - On `NoSuchUpload` (7-day expiry): reset to `status='pending'`, clear upload_id

4. **`blob/downloader.rs`**:
   - `async fn get_blob(...)` — check local_path exists → S3 GetObject stream to dest → update status='cached'

5. **`CacheEvictionPolicy` trait** — defined now, `NeverEvict` as default, no eviction logic implemented

6. **Actor integration**:
   - `PutBlob` command: stage → INSERT blobs → trigger upload cycle → return BlobId
   - `GetBlob` command: check cache → maybe download → return path
   - `BlobStatus` command: SELECT from blobs
   - `EvictBlob` command: delete local file, SET status='uploaded', local_path=NULL
   - `BlobCacheSize` command: `SELECT SUM(size_bytes) FROM blobs WHERE local_path IS NOT NULL`

### 3.3 Unit Tests

```
tests/blob_staging.rs
  - stage() copies file to cache dir with ULID filename
  - Small file (< 5MB) uses direct PutObject path (via mock BlobBackend)
  - Large file (> 5MB) uses multipart path

tests/blob_upload.rs  (mock BlobBackend that records calls)
  - Happy path: pending → uploading → uploaded
  - Resume: mock returns parts [1,2,3] from ListParts; uploader only uploads [4,5]
  - Retry: transient error increments retries, sets next_retry_at
  - NoSuchUpload: resets to status='pending', clears upload_id

tests/blob_download.rs
  - Cache hit: returns local path without calling BlobBackend
  - Cache miss: calls download(), updates status='cached', local_path set
  - EvictBlob: local file deleted, status='uploaded', local_path=NULL
```

### 3.4 Integration Tests (LocalStack)

```
tests/s3_integration.rs  (#[cfg(feature = "integration-tests")])
  - Multipart upload completes successfully
  - ListParts returns correct ETags after partial upload
  - Resume from part 3 of 5 uploads only parts 3-5
  - Download returns exact bytes that were uploaded
```

---

## Phase 4: Local Indexing

**Goal**: Callers can define indexed fields and run structured queries and full-text search against local data.

### 4.1 New modules

```
src/
  collection.rs    — CollectionConfig, IndexDef, IndexKind
  db/
    indices.rs     — shadow table DDL, index extraction, FTS update
```

### 4.2 Tasks

1. **`collection.rs`**:
   ```rust
   pub struct CollectionConfig {
       pub schema_version: u32,
       pub encryption: CollectionEncryption,  // Default | Enabled | Disabled
       pub migrate: Option<Arc<dyn Fn(u32, &[u8]) -> Result<Vec<u8>> + Send + Sync>>,
       pub indices: Vec<IndexDef>,
       pub fts_fields: Vec<String>,
   }
   pub struct IndexDef { pub field_path: String, pub kind: IndexKind }
   pub enum IndexKind { Text, Integer, Float, TextArray }
   ```

2. **`db/indices.rs`**:
   - `fn ensure_shadow_table(conn, collection, indices) -> Result<()>` — `CREATE TABLE IF NOT EXISTS idx_{collection} (...)`
   - `fn ensure_fts_table(conn, collection, fts_fields) -> Result<()>` — `CREATE VIRTUAL TABLE IF NOT EXISTS fts_{collection} USING fts5(...)`; skipped if `fts_fields` is empty
   - `fn extract_and_upsert(conn, collection, record_id, plaintext_data, config) -> Result<()>` — runs inside the same transaction as the record write
   - `fn delete_index_entry(conn, collection, record_id) -> Result<()>`
   - Field path resolution: dot notation (`"meta.year"`) and array flattening (`"authors[].name"`)

3. **`query.rs`**:
   - `fn to_sql(collection: &str, expr: &QueryExpr, opts: &ListOpts) -> (String, Vec<rusqlite::types::Value>)` — pure function, fully parameterized

4. **Schema migration on startup**:
   - For each collection with a `migrate` hook: `SELECT id, data, schema_version FROM records WHERE collection=? AND schema_version < ?`
   - Call `migrate(old_version, data)` → write back with new schema_version and updated index

5. **Actor integration**: every `put` now calls `extract_and_upsert` after writing the record

### 4.3 Unit Tests

```
tests/index_extraction.rs
  - Flat field: {"year": 2023} → idx_papers.year = 2023
  - Nested field: {"meta": {"year": 2023}} → correct extraction
  - Array field: {"tags": ["a","b"]} → stored as JSON, queried with LIKE
  - Missing field: NULL stored, query with Eq returns nothing
  - Deleted record: index entry removed

tests/query.rs
  - QueryExpr::Eq generates correct WHERE clause
  - QueryExpr::And / Or generates correct compound WHERE
  - to_sql never interpolates strings (all values are bound parameters)
  - list() with OrderBy HLC desc returns correct order

tests/fts.rs
  - Search "attention mechanism" returns records containing both words
  - Porter stemming: "running" matches "run"
  - Deleted record no longer appears in FTS results
  - Record update re-indexes correctly

tests/migration.rs
  - Record with schema_version=1 is passed to migrate hook when current=2
  - Migrated record has updated schema_version and updated index
  - Records already at current version are not touched
```

---

## Phase 5: E2E Encryption

**Goal**: All data at rest (SQLite, DynamoDB, S3) is encrypted. Keys are app-level; encryption is toggled per item. Zero behavioral change to the rest of the API.

### 5.1 New modules

```
src/
  crypto/
    mod.rs         — public re-exports
    primitives.rs  — AES-256-GCM encrypt/decrypt, DEK generation
    kek.rs         — KeySource, master key derivation, keyring integration
    chunk.rs       — deterministic per-chunk nonce derivation for blobs
```

### 5.2 Tasks

1. **Cargo.toml**: add `aes-gcm`, `argon2`, `rand`, `keyring`

2. **`crypto/primitives.rs`**:
   - `fn generate_dek() -> [u8; 32]` — `rand::thread_rng().fill_bytes`
   - `fn encrypt(plaintext: &[u8], dek: &[u8; 32]) -> Result<Vec<u8>>` → `nonce(12B) || ciphertext || tag(16B)`
   - `fn decrypt(ciphertext: &[u8], dek: &[u8; 32]) -> Result<Vec<u8>>`
   - `fn wrap_dek(dek: &[u8; 32], kek: &[u8; 32]) -> Result<Vec<u8>>` — encrypt DEK with KEK
   - `fn unwrap_dek(wrapped: &[u8], kek: &[u8; 32]) -> Result<[u8; 32]>`

3. **`crypto/kek.rs`**:
   - `fn resolve_kek(source: &KeySource, config_conn: &Connection) -> Result<[u8; 32]>`
   - For `Keychain`: try `keyring::Entry::new(service, username).get_password()`; if absent, generate random key and store it; if keyring unavailable, fall through to error (caller must provide passphrase fallback)
   - For `Passphrase(fn)`: call fn(), derive key with Argon2id (params: m=65536, t=3, p=4); load/generate Argon2 salt from `config` table
   - `keyring` crate targets: macOS Keychain, Linux Secret Service (libsecret), Windows Credential Manager — all three work without additional config

4. **`crypto/chunk.rs`**:
   - `fn chunk_nonce(base_nonce: &[u8; 12], chunk_index: u32) -> [u8; 12]` — XOR base_nonce with little-endian chunk_index in the lower 4 bytes
   - `fn encrypt_chunk(data: &[u8], dek: &[u8; 32], base_nonce: &[u8; 12], chunk_index: u32) -> Result<Vec<u8>>`
   - `fn decrypt_chunk(data: &[u8], dek: &[u8; 32], base_nonce: &[u8; 12], chunk_index: u32) -> Result<Vec<u8>>`

5. **Encryption resolution in actor** (`resolve_encryption` helper):
   ```
   item_opts.encryption == Enabled  → encrypt
   item_opts.encryption == Disabled → plaintext
   item_opts.encryption == Default  → collection default, then engine default
   ```

6. **Record write path with encryption**:
   - `resolve_encryption(...)` → if encrypting: `generate_dek()`, `encrypt(data, dek)`, `wrap_dek(dek, kek)`
   - Set `format_version = 1`, `dek_encrypted = wrapped_dek`
   - Index extraction runs on **plaintext** before encryption, inside same transaction

7. **Record read path**:
   - Check `format_version`; if 1: `unwrap_dek(dek_encrypted, kek)` → `decrypt(data, dek)`
   - Return plaintext to caller

8. **Blob encryption** (in `uploader.rs`):
   - On staging: generate DEK + base_nonce, encrypt each chunk using `crypto::chunk`, store `dek_encrypted` + `base_nonce` in blobs table
   - S3 upload sends ciphertext; stores `dek_encrypted` and `base_nonce` as S3 user metadata

9. **FTS behavior with encryption**: At collection startup, if `CollectionEncryption::Enabled`, skip `ensure_fts_table`. Search falls back to `list() + decrypt + in-memory filter` for encrypted collections.

### 5.3 Unit Tests

```
tests/crypto_primitives.rs
  - encrypt/decrypt round-trip produces original plaintext
  - decrypt with wrong DEK returns Err (auth tag mismatch)
  - wrap_dek/unwrap_dek round-trip
  - unwrap_dek with wrong KEK returns Err
  - generate_dek returns 32 bytes of entropy (not all-zeros, not deterministic across calls)

tests/crypto_chunks.rs
  - chunk_nonce(base, 0) != chunk_nonce(base, 1) (different chunks → different nonces)
  - chunk_nonce is deterministic: same (base, index) always returns same nonce
  - encrypt_chunk/decrypt_chunk round-trip for chunks of various sizes
  - decrypt with wrong chunk_index returns Err

tests/encrypted_records.rs  (real SQLite + mock backend, encryption enabled)
  - put() stores ciphertext in SQLite (data field is not valid UTF-8 JSON)
  - get() returns original plaintext
  - Index table contains plaintext field values (not ciphertext)
  - format_version=1 in records table
  - Mixed collection: one record encrypted, one not — both readable

tests/kek_passphrase.rs
  - resolve_kek with Passphrase: same passphrase → same KEK (deterministic via stored salt)
  - resolve_kek with Passphrase: different passphrase → different KEK
  - Argon2 salt stored in config table on first call, reused on subsequent calls

tests/encrypted_blobs.rs  (mock BlobBackend)
  - staged blob is encrypted before upload (mock receives ciphertext, not original bytes)
  - resume: partial re-encryption of only missing chunks
  - download: decrypted bytes match original file
```

---

## Phase 6: Unfolded Integration

**Goal**: Replace unfolded's broken sync engine with squirreld.

### 6.1 Tasks

1. Add squirreld as a path/git dependency in unfolded's Cargo.toml:
   ```toml
   [dependencies]
   squirreld = { path = "../squirreld", features = ["test-utils"] }
   ```

2. **Collection setup** — define at app startup:

   | Collection | `encryption` | `fts_fields` | `indices` |
   |---|---|---|---|
   | `papers` | `Default` (enabled) | `title`, `abstract` | `title`, `year`, `tags`, `read_status` |
   | `notes` | `Default` (enabled) | `content` | `paper_id`, `page` |
   | `readlist` | `Disabled` | — | `paper_id`, `status`, `progress` |

3. **Passphrase integration**: unfolded already presents a password screen; pass the password via `KeySource::Passphrase` closure.

4. **PDF blobs**: `put_blob(pdf_path, s3_key, BlobPutOpts { encryption: Default, record_id: Some(paper_id) })`. The `papers` record stores the returned `BlobId`.

5. **Replace current database layer**: swap unfolded's ad-hoc SQLite calls with squirreld's `put`/`get`/`query` API.

6. **UI sync status**: subscribe to `engine.sync_events()` receiver; map events to Tauri frontend events for status indicators (syncing spinner, last-synced timestamp, pending errors badge).

7. **End-to-end offline test**: add a test using squirreld's `InMemoryBackend` (`test-utils` feature) that simulates adding a paper + notes offline and verifies correct state after sync.

---

## Dependency Additions by Phase

| Phase | New Cargo deps |
|---|---|
| 1 | `rusqlite/bundled`, `tokio/full`, `ulid`, `thiserror`, `tracing`, `serde_json`, `async-trait`, `tempfile` (test) |
| 2 | `aws-sdk-dynamodb`, `aws-config`, `testcontainers`, `testcontainers-modules` (integration test) |
| 3 | `aws-sdk-s3`, `bytes` |
| 4 | (no new deps) |
| 5 | `aes-gcm`, `argon2`, `rand`, `keyring` |
| 6 | (squirreld added as dep in unfolded) |

---

## Open Questions

1. **Passphrase vs. keychain UX**: Should the engine silently try the keychain first and fall back to prompting for a passphrase, or should the app explicitly choose one? Recommendation: let the app choose via `KeySource`. If the app wants "try keychain, fall back to passphrase", it constructs a small wrapper closure that tries `keyring` first.

2. **S3 key namespace**: Should the S3 key always be fully caller-controlled, or should the engine prefix with `{user_id}/`? Recommendation: caller-controlled, but document the convention `{user_id}/{collection}/{blob_id}` so apps stay consistent.

3. **Outbox inspection API**: `pending_errors()` returns items with at least one failed retry. Should there also be a `clear_error(seq)` command to let the user discard a stuck entry manually? Recommendation: yes — add it in Phase 2, it's a one-liner.
