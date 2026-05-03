# Squirreld — Architecture

## 1. Goals

Squirreld is a Rust library that provides an **offline-first, schema-orthogonal sync engine** for personal apps. It stores everything locally first and syncs to the cloud in the background. Designed for a single developer using multiple personal devices, not for multi-user collaboration.

Core properties:
- **Offline-first**: reads and writes succeed instantly against a local SQLite database; cloud sync happens asynchronously
- **Schema-orthogonal**: the library stores opaque JSON blobs with no opinion on their structure; schema evolution is the caller's responsibility
- **Last-write-wins (LWW)**: conflict resolution uses a Hybrid Logical Clock; the device with the higher HLC always wins
- **HAL-based backends**: a trait abstraction over the cloud layer; DynamoDB + S3 is the first concrete implementation, but other backends can be added
- **Optional E2E encryption**: transparent encryption at the envelope level with no behavioral changes to the rest of the API

---

## 2. Non-Goals

- **Multi-user collaboration / CRDTs**: out of scope; LWW is sufficient for single-user multi-device
- **Web / IndexedDB support**: all storage uses native SQLite; browser support is a future concern
- **GCP / Azure backends**: the HAL makes them *possible* but they will not be built now
- **Automatic schema migrations**: the library detects `schema_version` mismatches and calls a user-supplied migration hook; it does not run migrations on its own

---

## 3. On Adopting Honker

[Honker](https://github.com/russellthehippo/honker) is a SQLite extension that adds Postgres-style NOTIFY/LISTEN semantics and durable task queues to SQLite. The prior design discussion suggested adopting it. **We will not adopt it**, for the following reasons:

1. **Experimental status**: the README explicitly marks it as experimental. A "watertight" sync library cannot depend on an API that may break at any time.
2. **Redundant with the actor model**: since all writes funnel through a single async actor, cross-process wake-up semantics are irrelevant. The actor already knows about every write.
3. **C extension complexity**: honker ships as a loadable SQLite `.so` extension, which complicates the build, cross-compilation, and future WASM targets.
4. **The outbox is simple to own**: a FIFO queue in SQLite with retry tracking is ~150 lines of SQL + Rust. There is no reason to add an external dependency for it.

We implement our own outbox table directly. This is the same transactional outbox pattern, just without the external dep.

---

## 4. Core Abstractions

### 4.1 The Envelope

Every record is stored in an "envelope" that the library manages. The caller provides only the data blob.

| Field | Type | Purpose |
|---|---|---|
| `id` | ULID (text) | Client-generated primary key; lexicographically sortable |
| `collection` | String | Logical grouping, like a table name |
| `data` | Blob | Opaque user payload (JSON or AES-256-GCM ciphertext) |
| `hlc` | String | Hybrid Logical Clock for LWW conflict resolution |
| `schema_version` | u32 | Caller-defined; used to trigger migration hooks |
| `format_version` | u8 | `0` = plaintext, `1` = AES-256-GCM encrypted |
| `dek_encrypted` | Blob? | Encrypted Data Encryption Key (only set when `format_version = 1`) |
| `deleted` | bool | Tombstone marker; hard deletes never happen locally |
| `synced` | bool | Local flag; `true` once the record has been acknowledged by the backend |

### 4.2 Hybrid Logical Clock (HLC)

HLC combines a wall-clock timestamp with a monotonic counter, guaranteeing causality even across devices with drifted clocks.

**Format**: `{physical_ms:013x}-{logical:04x}-{node_id}`

Example: `0195f3a1e42b-0003-a7f2e1`

- `physical_ms`: 48-bit millisecond wall clock, hex-encoded (lexicographically sortable up to year 2527)
- `logical`: 16-bit counter, incremented when two events share the same physical millisecond
- `node_id`: 6-byte random ID generated once per device, stored in the `config` table

**Rules**:
- On write: `new_hlc = max(wall_clock_ms, last_known_hlc.physical) + epsilon`
- On receive from remote: update `last_known_hlc` if the remote is higher

This format is a lexicographically comparable string, so DynamoDB sort key comparisons work directly with `>` without parsing.

### 4.3 Collections

A **collection** is a named logical group of records (equivalent to a table). Callers declare collections when building the engine:

```rust
let engine = SquirrelEngine::builder()
    .collection("papers", CollectionConfig {
        schema_version: 1,
        migrate: Some(Arc::new(|old_version, data| migrate_paper(old_version, data))),
        indices: vec![
            Index::new("title", IndexKind::Text),
            Index::new("year", IndexKind::Integer),
            Index::new("tags", IndexKind::TextArray),
        ],
        fts_fields: vec!["title", "abstract", "notes"],
    })
    .build(config)?;
```

Collections drive index table creation and FTS configuration. The library creates one shadow index table per collection at startup if it does not already exist.

---

## 5. Local Storage — SQLite Schema

All local state lives in a single SQLite database file. WAL mode is mandatory.

```sql
-- Metadata config (device node ID, sync checkpoints, etc.)
CREATE TABLE IF NOT EXISTS config (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Main record store
CREATE TABLE IF NOT EXISTS records (
    id             TEXT PRIMARY KEY,
    collection     TEXT NOT NULL,
    data           BLOB NOT NULL,
    hlc            TEXT NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 0,
    format_version INTEGER NOT NULL DEFAULT 0,
    dek_encrypted  BLOB,
    deleted        INTEGER NOT NULL DEFAULT 0,
    synced         INTEGER NOT NULL DEFAULT 0,
    created_at     INTEGER NOT NULL,  -- unix ms
    updated_at     INTEGER NOT NULL   -- unix ms
);
CREATE INDEX IF NOT EXISTS idx_records_collection ON records(collection, hlc);

-- Outbox: FIFO log of pending cloud pushes
CREATE TABLE IF NOT EXISTS outbox (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    record_id  TEXT    NOT NULL,
    collection TEXT    NOT NULL,
    operation  TEXT    NOT NULL,  -- 'upsert' | 'delete'
    hlc        TEXT    NOT NULL,
    data       BLOB,              -- snapshot of the record at write time
    created_at INTEGER NOT NULL,
    retries    INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
);

-- Blob (S3 file) tracking
CREATE TABLE IF NOT EXISTS blobs (
    id             TEXT PRIMARY KEY,  -- ULID
    record_id      TEXT,              -- linked metadata record (nullable)
    collection     TEXT,
    local_path     TEXT,              -- path in staging cache
    s3_key         TEXT NOT NULL,
    size_bytes     INTEGER,
    upload_id      TEXT,              -- S3 multipart upload ID
    status         TEXT NOT NULL,     -- 'pending'|'uploading'|'uploaded'|'downloading'|'cached'|'error'
    format_version INTEGER NOT NULL DEFAULT 0,
    dek_encrypted  BLOB,
    retries        INTEGER NOT NULL DEFAULT 0,
    last_error     TEXT,
    created_at     INTEGER NOT NULL,
    updated_at     INTEGER NOT NULL
);

-- Per-part tracking for resumable multipart uploads
CREATE TABLE IF NOT EXISTS blob_parts (
    blob_id     TEXT    NOT NULL REFERENCES blobs(id),
    part_number INTEGER NOT NULL,
    etag        TEXT,              -- returned by S3 on success
    uploaded    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (blob_id, part_number)
);

-- Sync state: high-water marks per backend
CREATE TABLE IF NOT EXISTS sync_state (
    backend       TEXT PRIMARY KEY,  -- e.g. 'dynamodb'
    last_pull_hlc TEXT,              -- highest HLC seen in last pull
    last_push_seq INTEGER            -- last successfully pushed outbox seq
);
```

Shadow index tables are created dynamically at startup per collection:

```sql
-- Generated for collection "papers"
CREATE TABLE IF NOT EXISTS idx_papers (
    record_id TEXT PRIMARY KEY REFERENCES records(id) ON DELETE CASCADE,
    title     TEXT,
    year      INTEGER,
    tags      TEXT  -- JSON array stored as text
);

-- FTS5 virtual table per collection
CREATE VIRTUAL TABLE IF NOT EXISTS fts_papers USING fts5(
    record_id UNINDEXED,
    content,
    tokenize = 'porter unicode61'
);
```

---

## 6. Actor Model & Public API

The entire engine runs inside a single **async actor** (a tokio task). The public `SquirrelEngine` handle is cheap to clone and communicates with the actor via a bounded `mpsc` channel.

```
                    ┌─────────────────────────────────┐
App Thread(s)       │          Actor Task              │
                    │                                  │
handle.put() ──────►│  SQLite write                    │
handle.get() ──────►│  + outbox append (atomic)        │
handle.query() ────►│  + index update  (atomic)        │
handle.put_blob() ──►│                                  │
                    │  ┌──────────────────────────┐    │
                    │  │  Sync Loop (sub-task)     │    │
                    │  │  - push outbox → DynamoDB │    │
                    │  │  - pull DynamoDB → local  │    │
                    │  │  - upload blobs → S3      │    │
                    │  └──────────────────────────┘    │
                    └─────────────────────────────────┘
```

Bounded channel backpressure prevents unbounded memory growth if the application writes faster than the actor can process.

### 6.1 Public API Surface

```rust
#[derive(Clone)]
pub struct SquirrelEngine { /* opaque */ }

impl SquirrelEngine {
    // Record operations
    pub async fn put(&self, collection: &str, id: Option<Ulid>, data: &[u8]) -> Result<Ulid>;
    pub async fn get(&self, collection: &str, id: Ulid) -> Result<Option<Vec<u8>>>;
    pub async fn delete(&self, collection: &str, id: Ulid) -> Result<()>;
    pub async fn list(&self, collection: &str, opts: ListOpts) -> Result<Vec<RecordMeta>>;
    pub async fn query(&self, collection: &str, expr: QueryExpr) -> Result<Vec<RecordMeta>>;
    pub async fn search(&self, collection: &str, text: &str) -> Result<Vec<RecordMeta>>;

    // Blob operations
    pub async fn put_blob(&self, local_path: &Path, s3_key: &str, record_id: Option<Ulid>) -> Result<BlobId>;
    pub async fn get_blob(&self, blob_id: BlobId, dest: &Path) -> Result<()>;
    pub async fn blob_status(&self, blob_id: BlobId) -> Result<BlobStatus>;

    // Sync
    pub async fn force_sync(&self) -> Result<SyncStats>;
    pub fn sync_events(&self) -> broadcast::Receiver<SyncEvent>;

    // Lifecycle
    pub async fn shutdown(self) -> Result<()>;
}
```

`get` and `query` return the raw bytes the caller originally stored. The library is not involved in serialization—callers bring their own serde logic.

### 6.2 Index Queries

`QueryExpr` is a simple expression tree for filtering on indexed fields:

```rust
pub enum QueryExpr {
    Eq(String, IndexValue),
    Lt(String, IndexValue),
    Gt(String, IndexValue),
    And(Box<QueryExpr>, Box<QueryExpr>),
    Or(Box<QueryExpr>, Box<QueryExpr>),
}
```

This translates directly to a SQL `WHERE` clause against the shadow index table. Complex queries the library cannot express are handled by the caller (fetch all, filter in application code).

---

## 7. Backend HAL

Two traits abstract the cloud layer:

```rust
/// Record-level backend (maps to DynamoDB)
#[async_trait]
pub trait RecordBackend: Send + Sync {
    fn backend_id(&self) -> &str;

    /// Push a batch of outbox entries. Must be all-or-nothing if the backend
    /// supports conditional writes (e.g. DynamoDB TransactWriteItems).
    async fn push(&self, entries: &[OutboxEntry]) -> Result<PushResult>;

    /// Pull all records with HLC > `since`. The backend is responsible for
    /// maintaining the GSI / change log needed to answer this efficiently.
    async fn pull_since(&self, since: &Hlc) -> Result<Vec<RemoteRecord>>;
}

/// Blob-level backend (maps to S3)
#[async_trait]
pub trait BlobBackend: Send + Sync {
    async fn initiate_multipart(&self, key: &str, metadata: &HashMap<String, String>) -> Result<String>;
    async fn upload_part(&self, key: &str, upload_id: &str, part: i32, data: Bytes) -> Result<String>; // returns ETag
    async fn list_parts(&self, key: &str, upload_id: &str) -> Result<Vec<(i32, String)>>;
    async fn complete_multipart(&self, key: &str, upload_id: &str, parts: &[(i32, String)]) -> Result<()>;
    async fn abort_multipart(&self, key: &str, upload_id: &str) -> Result<()>;
    async fn download(&self, key: &str, dest: &Path) -> Result<u64>; // returns bytes written
}
```

### 7.1 DynamoDB Backend — Table Design

Single-table design:

| Attribute | Type | Notes |
|---|---|---|
| `PK` | String | `USER#{user_id}` — constant per user |
| `SK` | String | `{collection}#{record_id}` |
| `hlc` | String | HLC string (also used as `last_modified`) |
| `data` | Binary | Plaintext JSON or AES-256-GCM ciphertext |
| `schema_version` | Number | |
| `format_version` | Number | `0` or `1` |
| `dek_encrypted` | Binary | Present only when `format_version = 1` |
| `deleted` | Boolean | Tombstone |

**GSI** for pull queries:
- GSI PK: `PK` (same user partition)
- GSI SK: `hlc`
- Name: `hlc-index`

This allows: `query GSI where PK = USER#me AND hlc > :last_checkpoint` — returning only records modified since the last pull.

**Conditional push expression**:
```
attribute_not_exists(hlc) OR hlc < :local_hlc
```

If the remote record already has a higher HLC (another device pushed first), the condition fails and the push returns a `ConditionalCheckFailedException`. This is not an error; it means the remote won and we should pull.

### 7.2 DynamoDB Push Batching

DynamoDB `TransactWriteItems` supports up to **100 items per transaction** and is all-or-nothing. We use this as the push batch size.

If the transaction fails due to a `ConditionalCheckFailedException`:
- Do **not** discard the outbox entries
- Trigger a pull cycle immediately
- After the pull resolves the conflict (remote wins → discard local; local wins → keep in outbox), retry the push

If the transaction fails due to a transient error (throttling, network):
- Increment retry counter
- Apply exponential backoff: `base * 2^retries` (base = 1s, max = 5 minutes)
- After 10 retries, move the entry to `dead` status and emit a `SyncEvent::PermanentFailure`

The outbox is strictly FIFO. We never skip an entry to push a later one, because later entries may depend on earlier ones. Stop-on-error is the correct behavior.

---

## 8. Sync Loop

The sync loop runs as a sub-task within the actor. It is triggered by:
1. Any successful local write (debounced with a 500ms window)
2. App startup
3. Network reconnect event
4. Periodic timer (every 60 seconds)
5. Explicit `force_sync()` call

### 8.1 Push Cycle

```
1. Read oldest N (≤100) outbox entries
2. If empty: done
3. Call backend.push(entries)
4. On success:
   a. DELETE pushed entries from outbox
   b. UPDATE records SET synced = 1 WHERE id IN (...)
   c. Go to step 1 (process next batch)
5. On ConditionalCheckFailedException:
   a. Run pull cycle (see 8.2)
   b. Re-evaluate outbox head:
      - If remote won: DELETE the conflicting outbox entry
      - If local won: keep entry, retry push
6. On transient error:
   a. Increment retries, schedule retry with backoff
```

### 8.2 Pull Cycle

```
1. Read last_pull_hlc from sync_state (NULL on first run = pull everything)
2. Call backend.pull_since(last_pull_hlc)
3. For each remote record R:
   a. Read local record L with same (collection, id)
   b. If L does not exist:
      - INSERT R into local records table
      - Run index extractors on R
   c. If R.hlc > L.hlc:
      - UPDATE L with R's data/metadata
      - Rebuild index entries for this record
      - DELETE any outbox entries for this record_id (remote won)
      - Mark synced = 1
   d. If L.hlc >= R.hlc:
      - Skip (local version is current or newer; push cycle will handle it)
4. Update sync_state.last_pull_hlc = max(all R.hlc)
```

---

## 9. Blob Store (S3)

### 9.1 Write Path

```
put_blob(local_path, s3_key) →
  1. Copy file to staging: ~/.cache/{app}/staging/{blob_id}
  2. INSERT INTO blobs (id, local_path, s3_key, status='pending', ...)
  3. Return BlobId immediately (caller is unblocked)
  4. Background uploader picks up status='pending' blobs:
     a. Call S3 CreateMultipartUpload → get upload_id
     b. UPDATE blobs SET status='uploading', upload_id=... 
     c. Split file into 5MB chunks
     d. For each chunk:
        - Encrypt if enabled (see §10.2)
        - Call S3 UploadPart → get ETag
        - INSERT INTO blob_parts (blob_id, part_number, etag, uploaded=1)
     e. Call S3 CompleteMultipartUpload with all (part_number, etag) pairs
     f. UPDATE blobs SET status='uploaded'
     g. Emit SyncEvent::BlobUploaded
```

### 9.2 Resume Logic

On startup (and on any upload error), the uploader scans for `status='uploading'` blobs:

```
1. For blob B with status='uploading':
   a. Call S3 ListParts(B.s3_key, B.upload_id) → get server-confirmed parts
   b. Cross-reference with blob_parts table
   c. Re-upload only missing parts
   d. Continue to CompleteMultipartUpload
```

If `upload_id` is NULL (app crashed before step 4b), restart from CreateMultipartUpload.

If S3 reports the upload ID has expired (after 7 days of inactivity), abort and start fresh.

### 9.3 Read Path

```
get_blob(blob_id, dest) →
  1. Check local cache: if staging file exists → copy to dest, done
  2. If status='uploaded': start S3 download
     a. Stream GetObject to dest
     b. Decrypt if needed (see §10.3)
     c. UPDATE blobs SET status='cached', local_path=dest
  3. Return path to dest
```

Downloads do not use the multipart API. For very large files (>1GB), the AWS SDK streams the response directly to disk without loading into memory.

### 9.4 Chunk Size Decision

**5MB per chunk** matches the S3 minimum part size for multipart uploads (except for the final part). This is a reasonable balance between:
- Too small: overhead from many API calls
- Too large: wasted work on resume after a crash

For files under 5MB, a direct PutObject is used instead of multipart.

---

## 10. E2E Encryption

Encryption is configured at the engine level (not per-record). When enabled, **all** records and blobs in the engine are encrypted.

### 10.1 Key Hierarchy

```
Master Key (KEK)
  └── Data Encryption Key (DEK) per record/blob
        └── Encrypted payload
```

**Master Key (KEK)**:
- Stored in the OS keychain via [`keyring`](https://crates.io/crates/keyring) crate
- If no keychain is available (headless server): derived from a passphrase using Argon2id (memory=64MB, iterations=3, parallelism=4)
- 256 bits, never written to disk in plaintext

**Data Encryption Key (DEK)**:
- 256-bit random key, generated fresh per record
- Encrypted with KEK using AES-256-GCM
- Stored in the `dek_encrypted` field of the record (local SQLite and DynamoDB)
- Rotation: re-encrypt DEK with new KEK, no need to re-encrypt the data

### 10.2 Record Encryption

```
plaintext = JSON bytes
nonce     = random 12 bytes
ciphertext = AES-256-GCM(plaintext, key=DEK, nonce=nonce)
stored    = [nonce (12B)] || [ciphertext] || [auth_tag (16B)]
```

`format_version = 1` signals to the reader that decryption is required.

### 10.3 Blob (S3) Encryption

Each blob has its own DEK. Chunks are encrypted independently to support resumable uploads.

```
For chunk i:
  nonce_i   = base_nonce XOR (i as u96)  -- deterministic per chunk
  ciphertext_i = AES-256-GCM(chunk_data, key=DEK, nonce=nonce_i)
```

The `base_nonce` (12 bytes) is generated once per blob and stored in the `blobs` table. The `dek_encrypted` for the blob is stored as S3 user metadata (`x-amz-meta-dek`).

The deterministic nonce-per-chunk property means: if we need to re-encrypt chunk 4 during a resume, we derive `nonce_4` from `base_nonce` without needing to track it separately.

### 10.4 Searchability with Encryption

SQLite FTS5 and shadow index equality queries do not work on ciphertext. When encryption is enabled:
- FTS5 is disabled (queries fall back to in-memory filtering)
- Index extractors run on plaintext **before** encryption and store the values into shadow index tables as **hash-keyed**: `SHA-256(field_value)` stored as hex. This allows exact-match queries (e.g. `WHERE title_hash = SHA256('Attention Is All You Need')`) but not range queries or full-text search

This is a known limitation of encrypted local-first databases. Range queries require either using a trusted local device (always has plaintext) or a dedicated searchable encryption scheme (too complex for now).

In practice: for unfolded, search happens only on the local device where the DEK is decryptable. The app decrypts the record, then searches in memory. Encrypted shadow indices are only needed for list/filter operations that must not expose data even to local SQL.

---

## 11. Local Indexing

### 11.1 Index Extractors

When a collection is registered, the caller provides index definitions. At write time, the actor:

1. Decrypts the record if needed
2. Parses the JSON
3. Extracts each indexed field using the configured path (e.g. `"metadata.year"` for nested fields)
4. Upserts the shadow index table row

All of this happens inside the same SQLite transaction as the record write, so indices are always consistent with the data.

### 11.2 FTS5

Each collection with `fts_fields` configured gets an FTS5 virtual table. The content is the concatenation of the configured fields, separated by newlines. FTS5 uses Porter stemming (`tokenize = 'porter unicode61'`).

FTS index updates happen in the same transaction as record writes via a SQLite trigger on the shadow index table.

### 11.3 Query Translation

`QueryExpr` is translated to SQL against the shadow index table with a `JOIN` to `records`:

```rust
// QueryExpr::And(Eq("year", 2023), Eq("tags", "attention"))
// →
// SELECT r.id, r.data, r.hlc FROM records r
// JOIN idx_papers p ON p.record_id = r.id
// WHERE p.year = 2023 AND p.tags LIKE '%"attention"%'
// ORDER BY r.hlc DESC
```

---

## 12. Dependency Choices

| Crate | Purpose |
|---|---|
| `rusqlite` | SQLite bindings with bundled feature (no system dep) |
| `tokio` | Async runtime |
| `aws-sdk-dynamodb` | DynamoDB backend |
| `aws-sdk-s3` | S3 backend |
| `ulid` | ULID generation |
| `aes-gcm` | AES-256-GCM encryption |
| `argon2` | Key derivation from passphrase |
| `keyring` | OS keychain integration |
| `serde_json` | JSON parsing for index extraction |
| `async-trait` | Async trait support |
| `thiserror` | Error types |
| `tracing` | Structured logging |

`rusqlite` with `bundled` feature embeds SQLite at compile time, avoiding dependency on the system SQLite version. This is important for reproducible builds across different OS versions.

---

## 13. Unfolded Integration Notes

The first consumer of squirreld is [unfolded](https://github.com/shi-yan/unfolded), a research paper reader with notes.

Expected collections:
- `papers`: metadata (title, authors, year, abstract, tags, PDF S3 key)
- `notes`: user notes and highlights, linked to a paper by `paper_id`
- `readlist`: a paper's read-list status and progress

The PDF files themselves are large blobs stored in S3 via the blob API. The `papers` record contains the `blob_id` reference.

Index configuration for `papers`:
- Indexed: `title`, `year`, `tags` (for filter), `read_status`
- FTS: `title`, `abstract`, `notes` content

On first launch with no internet connection, unfolded works entirely offline. The sync engine queues everything in the outbox. When connectivity is restored, all writes are pushed to DynamoDB and any PDF uploads resume from where they left off.
