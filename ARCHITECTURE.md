# Squirreld — Architecture

## 1. Goals

Squirreld is a Rust library that provides an **offline-first, schema-orthogonal sync engine** for personal apps. It stores everything locally first and syncs to the cloud in the background. Designed for a single developer using multiple personal devices, not for multi-user collaboration.

Core properties:
- **Offline-first**: reads and writes succeed instantly against a local SQLite database; cloud sync happens asynchronously
- **Schema-orthogonal**: the library stores opaque JSON blobs with no opinion on their structure; schema evolution is the caller's responsibility
- **Last-write-wins (LWW)**: conflict resolution uses a Hybrid Logical Clock; the device with the higher HLC always wins
- **HAL-based backends**: a trait abstraction over the cloud layer; DynamoDB + S3 is the first concrete implementation, but other backends can be added
- **Optional per-item E2E encryption**: transparent encryption at the envelope level; keys are app-level, but each record and blob individually chooses whether to encrypt

---

## 2. Non-Goals

- **Multi-user collaboration / CRDTs**: out of scope; LWW is sufficient for single-user multi-device
- **Web / IndexedDB support**: all storage uses native SQLite; browser support is a future concern
- **GCP / Azure backends**: the HAL makes them *possible* but they will not be built now
- **Automatic schema migrations**: the library detects `schema_version` mismatches and calls a user-supplied migration hook; it does not run migrations on its own
- **Cache eviction**: the library tracks blob cache state and exposes the interface, but eviction policy is the caller's responsibility (see §9.4)

---

## 3. On Adopting Honker

[Honker](https://github.com/russellthehippo/honker) is a SQLite extension that adds Postgres-style NOTIFY/LISTEN semantics and durable task queues to SQLite. **We will not adopt it** for the core engine, for these reasons:

1. **Experimental status**: the README explicitly marks it as experimental. A "watertight" sync library cannot depend on an API that may break at any time.
2. **Redundant with the actor model**: all writes funnel through a single async actor, so cross-process wake-up is irrelevant. The actor knows about every write immediately.
3. **C extension complexity**: honker ships as a loadable SQLite `.so` / `.dylib`, which complicates cross-compilation and reproducible builds.
4. **The outbox + retry scheduler is simple to own**: a FIFO outbox with a `next_retry_at` column and the existing 60-second periodic timer gives us everything honker's cron mechanism provides — without the dependency.

The one genuinely useful idea from honker is the **cron-job style retry scheduler**. We replicate it simply: the outbox table has a `next_retry_at` integer (unix ms). The sync loop's periodic tick runs `SELECT ... WHERE next_retry_at <= now()`. Exponential backoff is implemented by updating `next_retry_at = now + min(1000ms * 2^retries, 5min)` on each failure. No external library needed.

A future periodic-task / job-scheduler crate (for use cases like polling the Gemini batch API) is a separate concern and out of scope for squirreld.

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
| `format_version` | u8 | `0` = plaintext JSON, `1` = AES-256-GCM encrypted |
| `dek_encrypted` | Blob? | Encrypted Data Encryption Key; present only when `format_version = 1` |
| `deleted` | bool | Tombstone marker; hard deletes never happen locally |
| `synced` | bool | `true` once the backend has acknowledged this version |

The `format_version` is stored **per record**. Two records in the same collection can have different `format_version` values. The reader always checks `format_version` before deciding whether to decrypt.

### 4.2 Hybrid Logical Clock (HLC)

HLC combines a wall-clock timestamp with a monotonic counter, guaranteeing causality even across devices with drifted clocks.

**Format**: `{physical_ms:013x}-{logical:04x}-{node_id_hex}`

Example: `0195f3a1e42b-0003-a7f2e1c9b8d4`

- `physical_ms`: 48-bit millisecond wall clock, zero-padded hex (lexicographically sortable until year 2527)
- `logical`: 16-bit counter, incremented when two events share the same physical millisecond
- `node_id`: 6-byte random ID generated once per device at first run, stored in the local `config` table

**Properties**:
- The string representation is lexicographically comparable — DynamoDB and SQLite sort it correctly with plain `>` without any parsing
- `Hlc::tick(last: &Hlc) -> Hlc` returns a value strictly greater than both `last` and `wall_clock_now()`
- On receiving a remote HLC, update the local "max seen" so future local writes are causally after the remote event

### 4.3 Collections

A **collection** is a named logical group of records (equivalent to a table). Callers declare collections when building the engine. The library creates the shadow index table and FTS table for each collection at startup.

```rust
let engine = SquirrelEngine::builder()
    .db_path("~/.local/share/myapp/squirreld.db")
    .encryption(EncryptionConfig::Enabled {
        key_source: KeySource::Passphrase(Arc::new(|| prompt_user_for_password())),
    })
    .collection("papers", CollectionConfig {
        schema_version: 2,
        encryption: CollectionEncryption::Default,  // inherit engine default
        migrate: Some(Arc::new(migrate_paper)),
        indices: vec![
            Index::new("title",  IndexKind::Text),
            Index::new("year",   IndexKind::Integer),
            Index::new("tags",   IndexKind::TextArray),
        ],
        fts_fields: vec!["title", "abstract", "notes"],
    })
    .collection("tracks", CollectionConfig {
        schema_version: 1,
        encryption: CollectionEncryption::Disabled,  // music metadata is not sensitive
        ..Default::default()
    })
    .build()
    .await?;
```

### 4.4 Per-Item Encryption Override

Encryption is resolved with this precedence (highest wins):

```
item-level override  >  collection-level default  >  engine-level default
```

On write, the caller can pass a `PutOpts`:

```rust
// Force plaintext even though the collection default is encrypted
engine.put("docs", None, data, PutOpts {
    encryption: ItemEncryption::Disabled,
    ..Default::default()
}).await?;
```

`format_version` and `dek_encrypted` are stored per-record, so the reader always knows how to handle it regardless of current engine configuration. An unencrypted record written today can coexist with an encrypted record in the same collection.

---

## 5. Local Storage — SQLite Schema

All local state lives in a single SQLite database file. WAL mode is mandatory.

```sql
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- Metadata: device node ID, sync checkpoints, Argon2 salt, etc.
CREATE TABLE IF NOT EXISTS config (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Main record store
CREATE TABLE IF NOT EXISTS records (
    id             TEXT PRIMARY KEY,            -- ULID
    collection     TEXT NOT NULL,
    data           BLOB NOT NULL,               -- plaintext JSON or ciphertext
    hlc            TEXT NOT NULL,               -- HLC string
    schema_version INTEGER NOT NULL DEFAULT 0,
    format_version INTEGER NOT NULL DEFAULT 0,  -- 0=plain, 1=AES-256-GCM
    dek_encrypted  BLOB,                        -- encrypted DEK; NULL when format_version=0
    deleted        INTEGER NOT NULL DEFAULT 0,  -- tombstone
    synced         INTEGER NOT NULL DEFAULT 0,
    created_at     INTEGER NOT NULL,            -- unix ms
    updated_at     INTEGER NOT NULL             -- unix ms
);
CREATE INDEX IF NOT EXISTS idx_records_collection_hlc
    ON records(collection, hlc DESC);

-- Outbox: FIFO queue of pending cloud pushes
CREATE TABLE IF NOT EXISTS outbox (
    seq           INTEGER PRIMARY KEY AUTOINCREMENT,
    record_id     TEXT    NOT NULL,
    collection    TEXT    NOT NULL,
    operation     TEXT    NOT NULL,  -- 'upsert' | 'delete'
    hlc           TEXT    NOT NULL,
    data          BLOB,              -- snapshot of record at write time (encrypted if applicable)
    created_at    INTEGER NOT NULL,
    next_retry_at INTEGER NOT NULL DEFAULT 0,  -- unix ms; 0 = ready immediately
    retries       INTEGER NOT NULL DEFAULT 0,
    error_log     TEXT               -- JSON array of {ts, error} entries for debugging
);
CREATE INDEX IF NOT EXISTS idx_outbox_ready
    ON outbox(next_retry_at, seq);

-- Blob (large file) tracking
CREATE TABLE IF NOT EXISTS blobs (
    id             TEXT PRIMARY KEY,   -- ULID
    record_id      TEXT,               -- linked metadata record (nullable)
    collection     TEXT,
    local_path     TEXT,               -- absolute path in the local cache
    s3_key         TEXT NOT NULL,
    size_bytes     INTEGER,
    upload_id      TEXT,               -- S3 multipart upload ID; NULL before initiation
    status         TEXT NOT NULL,      -- 'pending'|'uploading'|'uploaded'|'downloading'|'cached'|'error'
    format_version INTEGER NOT NULL DEFAULT 0,
    dek_encrypted  BLOB,
    base_nonce     BLOB,               -- 12-byte base nonce for chunk encryption
    retries        INTEGER NOT NULL DEFAULT 0,
    next_retry_at  INTEGER NOT NULL DEFAULT 0,
    error_log      TEXT,
    created_at     INTEGER NOT NULL,
    updated_at     INTEGER NOT NULL
);

-- Per-chunk tracking for resumable multipart uploads
CREATE TABLE IF NOT EXISTS blob_parts (
    blob_id     TEXT    NOT NULL REFERENCES blobs(id) ON DELETE CASCADE,
    part_number INTEGER NOT NULL,
    etag        TEXT,                  -- returned by S3 after successful upload
    uploaded    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (blob_id, part_number)
);

-- Pull checkpoint per backend
CREATE TABLE IF NOT EXISTS sync_state (
    backend          TEXT PRIMARY KEY,  -- e.g. 'dynamodb'
    last_pull_hlc    TEXT               -- highest HLC seen in the last successful pull
);
```

Shadow index tables are generated at startup per collection:

```sql
-- Auto-generated for collection "papers"
CREATE TABLE IF NOT EXISTS idx_papers (
    record_id TEXT PRIMARY KEY REFERENCES records(id) ON DELETE CASCADE,
    title     TEXT,
    year      INTEGER,
    tags      TEXT        -- JSON array stored as text
);

-- FTS5 virtual table (disabled automatically when encryption is active for a collection)
CREATE VIRTUAL TABLE IF NOT EXISTS fts_papers USING fts5(
    record_id UNINDEXED,
    content,
    tokenize = 'porter unicode61'
);
```

---

## 6. Actor Model & Public API

The entire engine runs inside a single **async tokio task** (an actor). The public `SquirrelEngine` handle is cheap to clone and communicates with the actor via a bounded `mpsc` channel.

```
                  ┌───────────────────────────────────────┐
App threads       │            Actor Task                  │
                  │                                        │
engine.put() ────►│  1. Decrypt-for-index if encrypted     │
engine.get() ────►│  2. Write record to SQLite             │
engine.query() ──►│  3. Update shadow index + FTS          │  all in one
engine.put_blob()►│  4. Append to outbox                   │  transaction
                  │                                        │
                  │  ┌─────────────────────────────────┐  │
                  │  │  Sync sub-task                   │  │
                  │  │  triggered by write (500ms       │  │
                  │  │  debounce) + 60s timer +         │  │
                  │  │  force_sync()                    │  │
                  │  │                                  │  │
                  │  │  Push: outbox → DynamoDB         │  │
                  │  │  Pull: DynamoDB → local          │  │
                  │  │  Upload: staging → S3            │  │
                  │  └─────────────────────────────────┘  │
                  └───────────────────────────────────────┘
```

A **bounded channel** provides natural backpressure: if the actor falls behind, callers block instead of growing memory unboundedly.

### 6.1 Public API

```rust
#[derive(Clone)]
pub struct SquirrelEngine { /* opaque */ }

impl SquirrelEngine {
    // Record CRUD
    pub async fn put(&self, collection: &str, id: Option<Ulid>, data: &[u8], opts: PutOpts) -> Result<Ulid>;
    pub async fn get(&self, collection: &str, id: Ulid) -> Result<Option<Vec<u8>>>;
    pub async fn delete(&self, collection: &str, id: Ulid) -> Result<()>;

    // Queries (against shadow index tables)
    pub async fn list(&self, collection: &str, opts: ListOpts) -> Result<Vec<RecordMeta>>;
    pub async fn query(&self, collection: &str, expr: QueryExpr) -> Result<Vec<RecordMeta>>;
    pub async fn search(&self, collection: &str, text: &str) -> Result<Vec<RecordMeta>>;

    // Blob store
    pub async fn put_blob(&self, local_path: &Path, s3_key: &str, opts: BlobPutOpts) -> Result<BlobId>;
    pub async fn get_blob(&self, blob_id: BlobId, dest: &Path) -> Result<()>;
    pub async fn blob_status(&self, blob_id: BlobId) -> Result<BlobStatus>;
    pub async fn blob_cache_size(&self) -> Result<u64>;          // total bytes in local cache
    pub async fn evict_blob(&self, blob_id: BlobId) -> Result<()>; // remove local copy, keep metadata

    // Sync
    pub async fn force_sync(&self) -> Result<SyncStats>;
    pub async fn pending_errors(&self) -> Result<Vec<PendingError>>; // items with retries > 0
    pub fn sync_events(&self) -> broadcast::Receiver<SyncEvent>;

    // Lifecycle
    pub async fn shutdown(self) -> Result<()>;
}

pub struct PutOpts {
    pub encryption: ItemEncryption,  // Default | Enabled | Disabled
    pub schema_version: Option<u32>,
}

pub struct BlobPutOpts {
    pub record_id: Option<Ulid>,
    pub encryption: ItemEncryption,
}

pub enum ItemEncryption { Default, Enabled, Disabled }

pub enum SyncEvent {
    PushComplete(SyncStats),
    PullComplete(SyncStats),
    BlobUploaded { blob_id: BlobId },
    RetryScheduled { retries: u32, next_at: SystemTime, error: String },
}
```

`get` returns the raw bytes originally stored. The library handles encrypt/decrypt transparently. Callers own their own serialization.

### 6.2 QueryExpr

```rust
pub enum QueryExpr {
    Eq(String, IndexValue),
    Lt(String, IndexValue),
    Gt(String, IndexValue),
    And(Box<QueryExpr>, Box<QueryExpr>),
    Or(Box<QueryExpr>, Box<QueryExpr>),
}
```

Translates directly to a parameterized SQL `WHERE` clause against the shadow index table. No string interpolation — all values go through `rusqlite`'s parameter binding.

---

## 7. Backend HAL

Two traits abstract the cloud layer. The DynamoDB + S3 pair is the first implementation; other backends (local filesystem, custom HTTP server, etc.) can implement the same traits.

```rust
#[async_trait]
pub trait RecordBackend: Send + Sync {
    fn backend_id(&self) -> &str;

    /// Push a batch of outbox entries atomically.
    /// Implementations should use conditional writes to enforce LWW.
    async fn push(&self, entries: &[OutboxEntry]) -> Result<PushResult>;

    /// Return all records with HLC strictly greater than `since`.
    /// Must be ordered by HLC ascending.
    async fn pull_since(&self, since: Option<&Hlc>) -> Result<Vec<RemoteRecord>>;
}

#[async_trait]
pub trait BlobBackend: Send + Sync {
    async fn initiate_multipart(&self, key: &str, metadata: &HashMap<String, String>) -> Result<String>;
    async fn upload_part(&self, key: &str, upload_id: &str, part: i32, data: Bytes) -> Result<String>;
    async fn list_parts(&self, key: &str, upload_id: &str) -> Result<Vec<(i32, String)>>;
    async fn complete_multipart(&self, key: &str, upload_id: &str, parts: &[(i32, String)]) -> Result<()>;
    async fn abort_multipart(&self, key: &str, upload_id: &str) -> Result<()>;
    async fn download(&self, key: &str, dest: &Path) -> Result<u64>;
}

/// Placeholder for cache eviction policy. The default implementation never evicts.
pub trait CacheEvictionPolicy: Send + Sync {
    fn should_evict(&self, blob: &BlobInfo) -> bool;
}

pub struct NeverEvict;
impl CacheEvictionPolicy for NeverEvict {
    fn should_evict(&self, _: &BlobInfo) -> bool { false }
}
```

### 7.1 DynamoDB Backend — Table Design

Each application configures its own DynamoDB table name. The table uses a simple primary key design:

| Attribute | DynamoDB type | Role |
|---|---|---|
| `pk` | String (Partition Key) | `record_id` (ULID) — globally unique across collections |
| `sk` | String (Sort Key) | `collection` name |
| `hlc` | String | HLC for LWW and GSI sort |
| `data` | Binary | Plaintext JSON or AES-256-GCM ciphertext |
| `schema_version` | Number | |
| `format_version` | Number | `0` or `1` |
| `dek_encrypted` | Binary | Present only when `format_version = 1` |
| `deleted` | Boolean | Tombstone |
| `_p` | String | Constant `"main"` — GSI partition key |

**GSI `sync-index`** for pull queries:
- GSI Partition Key: `_p` (constant `"main"` on every item)
- GSI Sort Key: `hlc`

**Why this is an indexed range query, not a scan**: The GSI physically organizes all items sorted by `hlc` within the `_p = "main"` partition. The pull query `KeyConditionExpression: _p = :p AND hlc > :checkpoint` reads only the items modified since the checkpoint, in HLC order, stopping as soon as it reaches the end of the result set. Cost = O(results returned), not O(table size). A full table scan (`Scan` operation) would be O(table size) and is never used.

**Conditional push expression** (LWW enforcement):
```
attribute_not_exists(hlc) OR hlc < :local_hlc
```

If the remote record already has a higher HLC (another device pushed first), DynamoDB returns `ConditionalCheckFailedException`. This is not an error — it means the remote won. The push cycle responds by triggering a pull, after which the conflicting outbox entry is discarded (remote wins) or retried (local wins after pull reconciliation).

**TransactWriteItems limit**: up to 100 items per transaction, all-or-nothing. This is the push batch size.

### 7.2 DynamoDB Configuration

```rust
pub struct DynamoDbConfig {
    pub table_name: String,    // app-specific, e.g. "unfolded_records"
    pub region: String,
    pub user_id: String,       // used in S3 key prefix; not in DynamoDB keys
    pub credentials: AwsCredentials,
}

pub enum AwsCredentials {
    /// Read from ~/.aws/credentials or ~/.aws/config (profile name optional).
    /// Also accepts AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars.
    /// This is the default and covers the developer's local machine.
    FromEnvironment { profile: Option<String> },
    /// Explicit credentials supplied by the application layer.
    Explicit { access_key_id: String, secret_access_key: String, session_token: Option<String> },
}
```

Credential resolution order for `FromEnvironment`: environment variables → AWS config file → instance metadata (EC2/ECS). The `aws-config` crate handles this automatically when `AwsCredentials::FromEnvironment` is used.

The `ensure_table` method creates the table and GSI on first run (idempotent). Required IAM permissions: `dynamodb:GetItem`, `dynamodb:PutItem`, `dynamodb:DeleteItem`, `dynamodb:TransactWriteItems`, `dynamodb:Query` (on the GSI).

---

## 8. Sync Loop

The sync loop runs as a sub-task inside the actor. Triggers:

| Trigger | Condition |
|---|---|
| Post-write debounce | Any local write; fires 500ms after the last write in a burst |
| Periodic timer | Every 60 seconds unconditionally |
| Startup | Once when the engine is first opened |
| `force_sync()` | On explicit caller request |
| Pull triggered by push conflict | When DynamoDB rejects a push with `ConditionalCheckFailedException` |

### 8.1 Push Cycle

```
1. SELECT from outbox WHERE next_retry_at <= now() ORDER BY seq ASC LIMIT 100
2. If empty: done
3. Call backend.push(batch)
4. On success:
   a. DELETE the pushed entries from outbox (in the same local transaction)
   b. UPDATE records SET synced = 1 WHERE id IN (...)
   c. Emit SyncEvent::PushComplete
   d. Go to step 1 to process the next batch
5. On ConditionalCheckFailedException (LWW conflict):
   a. Immediately run pull cycle (§8.2)
   b. Re-examine the conflicting entry:
      - Remote HLC won → DELETE that outbox entry
      - Local HLC won → keep entry, retry push immediately
6. On transient error (network, throttling):
   a. retries += 1
   b. next_retry_at = now + min(1000ms × 2^retries, 300_000ms)
   c. Append {ts, error} to outbox.error_log (JSON array, capped at 20 entries)
   d. Emit SyncEvent::RetryScheduled
   e. Stop the current push cycle; the timer will pick it up later
```

**There is no permanent failure state.** Items retry indefinitely with exponential backoff, up to a 5-minute ceiling between attempts. The `pending_errors()` API surfaces these items so the app UI can show sync status. The user can inspect or manually clear stuck entries.

### 8.2 Pull Cycle

```
1. Read last_pull_hlc from sync_state (NULL on first run → pull everything)
2. Call backend.pull_since(last_pull_hlc) → stream of RemoteRecord ordered by hlc ASC
3. For each RemoteRecord R:
   a. Fetch local record L = records WHERE id = R.id AND collection = R.collection
   b. If L does not exist:
      - INSERT R into records, run index extraction on R
   c. If R.hlc > L.hlc:
      - UPDATE L with R's data and metadata
      - Re-run index extraction and FTS update for this record
      - DELETE FROM outbox WHERE record_id = R.id (remote won; discard local pending writes)
      - SET L.synced = 1
   d. If L.hlc >= R.hlc:
      - Skip (local is current or ahead; push cycle handles it)
4. UPDATE sync_state SET last_pull_hlc = max(all R.hlc seen)
5. Emit SyncEvent::PullComplete
```

---

## 9. Blob Store (S3)

### 9.1 Write Path

```
put_blob(local_path, s3_key, opts) →
  1. Assign a ULID as blob_id
  2. Copy source file to cache: {cache_dir}/{blob_id}
     (if encryption enabled: encrypt on the fly during copy, chunk by chunk)
  3. INSERT INTO blobs (id, local_path, s3_key, status='pending', format_version, ...)
  4. Return BlobId immediately — caller is unblocked

Background uploader (runs in the sync sub-task):
  5. SELECT * FROM blobs WHERE status='pending' AND next_retry_at <= now()
  6. Call S3 CreateMultipartUpload → get upload_id
  7. UPDATE blobs SET status='uploading', upload_id=...
  8. For each 5MB chunk:
     a. Read from local cache (already encrypted if applicable)
     b. S3 UploadPart → ETag
     c. INSERT INTO blob_parts (blob_id, part_number, etag, uploaded=1)
  9. S3 CompleteMultipartUpload with all (part_number, etag) pairs
 10. UPDATE blobs SET status='uploaded'
 11. Emit SyncEvent::BlobUploaded
```

Files under 5MB use `PutObject` / `GetObject` directly, skipping multipart.

### 9.2 Resume Logic

On startup, and after any upload error, the uploader scans `status = 'uploading'`:

```
For blob B with status='uploading' and upload_id IS NOT NULL:
  1. S3 ListParts(B.s3_key, B.upload_id) → server-confirmed parts
  2. Compare against blob_parts table → find missing part numbers
  3. Upload only missing parts
  4. CompleteMultipartUpload

If upload_id IS NULL (crash before step 6 above):
  → Restart from CreateMultipartUpload

If S3 returns NoSuchUpload (upload_id expired after 7 days):
  → Abort, reset status='pending', retries += 1
```

### 9.3 Read Path

```
get_blob(blob_id, dest) →
  1. Check blobs table for local_path; if file exists at that path → copy to dest, done
  2. status='uploaded' → begin S3 GetObject
     a. Stream response to dest
     b. If format_version=1: decrypt each chunk on the fly using stored DEK + base_nonce
     c. UPDATE blobs SET status='cached', local_path=dest
  3. Return dest path
```

### 9.4 Cache Eviction Interface

Squirreld tracks the local cache but does not evict automatically. The interface is defined so callers can plug in a policy later:

```rust
pub trait CacheEvictionPolicy: Send + Sync {
    fn should_evict(&self, blob: &BlobInfo) -> bool;
}

pub struct BlobInfo {
    pub id: BlobId,
    pub size_bytes: u64,
    pub last_accessed: SystemTime,
    pub status: BlobStatus,
}

// Default: never evict. Replace with LRU or size-based policy as needed.
pub struct NeverEvict;
```

`engine.evict_blob(id)` removes the local cache file and resets status to `'uploaded'`, freeing disk space while preserving the remote copy.

---

## 10. E2E Encryption

### 10.1 Key Hierarchy (Envelope Encryption)

```
App Master Key (KEK)  ← one per engine instance, held in RAM only
  └── Data Encryption Key (DEK)  ← one per record or blob, random 256-bit
        └── Encrypted payload
```

**Why envelope encryption**: If the KEK changes (e.g. password rotation), only the DEK needs to be re-encrypted — not the data itself. If a single DEK is somehow compromised, all other records remain safe.

### 10.2 Key Sources

```rust
pub enum EncryptionConfig {
    Disabled,
    Enabled { key_source: KeySource },
}

pub enum KeySource {
    /// OS keychain: macOS Keychain, Linux Secret Service, Windows Credential Manager.
    /// All three are supported by the `keyring` crate. Requires a keyring daemon on Linux
    /// (gnome-keyring / KWallet). Falls back to Passphrase if the keychain is unavailable.
    Keychain { service: String, username: String },

    /// Caller-supplied passphrase callback. Called once at startup to get the passphrase.
    /// The master key is derived using Argon2id (memory=64MB, iterations=3, parallelism=4).
    /// The Argon2 salt is stored in the `config` table.
    /// This is the correct choice for apps that already present a login/password screen.
    Passphrase(Arc<dyn Fn() -> Result<String> + Send + Sync>),

    /// Raw 32-byte key. For testing only — do not use in production.
    RawKey([u8; 32]),
}
```

The `keyring` crate supports all three target platforms (macOS, Linux, Windows). For apps that already ask the user for a password (like opensesame), `KeySource::Passphrase` is the natural choice — the app collects the password at login and passes a closure that returns it.

### 10.3 Record Encryption

```
plaintext    = the JSON bytes from the caller
dek          = random 256-bit key
nonce        = random 12 bytes
ciphertext   = AES-256-GCM(plaintext, key=dek, nonce=nonce)
stored_data  = nonce(12B) || ciphertext || auth_tag(16B)
dek_encrypted = AES-256-GCM(dek, key=kek, nonce=random_12B) [stored separately]
```

On read: decrypt DEK with KEK → decrypt data with DEK.

### 10.4 Blob (S3) Encryption — Chunk-Level

Each blob has its own DEK and a `base_nonce` (12 bytes, stored in the `blobs` table). Each 5MB chunk is encrypted independently:

```
nonce_i = base_nonce XOR (i as u96, little-endian)
chunk_ciphertext_i = AES-256-GCM(chunk_i, key=dek, nonce=nonce_i)
```

The `dek_encrypted` and `base_nonce` are stored as S3 user-defined metadata (`x-amz-meta-dek`, `x-amz-meta-nonce`) alongside the object.

Deterministic per-chunk nonces mean that on resume, chunk N can be re-encrypted from scratch without replaying prior chunks.

### 10.5 Searchability with Encryption

When a collection has encryption enabled (at collection or item level):

- **FTS5 is disabled** for that collection. Full-text search falls back to fetch-all + in-memory filtering on the decrypted data.
- **Shadow index fields** store the **plaintext values extracted before encryption**. Index extraction runs on plaintext before the record is encrypted, and writes to the shadow table as normal. This is secure because the shadow table lives in the same locally-trusted SQLite file — the same threat model that allows the DEK to be decrypted at all.

This is the correct tradeoff for a local-first personal app: the local device is trusted, and search only happens locally.

---

## 11. Local Indexing

### 11.1 Index Extractors

Field paths use dot notation for nested access: `"metadata.year"` navigates `{"metadata": {"year": 2023}}`. Arrays are flattened: `"authors[].name"` extracts all names.

Index extraction runs inside the same SQLite transaction as the record write. Shadow table is always consistent with the record data.

### 11.2 FTS5

Each collection with `fts_fields` configured gets an FTS5 virtual table using Porter stemming. FTS content is the concatenation of the configured fields, separated by spaces. FTS updates happen via SQLite triggers on the shadow index table insert/update/delete.

FTS is disabled for a collection if **any** record in that collection uses encryption (to avoid storing plaintext fragments in the FTS table when the record data is ciphertext). When encryption is collection-wide, FTS is simply not created. When per-item encryption is mixed, FTS is created but only plain-format records are indexed.

### 11.3 Query Translation

`QueryExpr` compiles to a parameterized SQL query against the shadow table joined with `records`:

```sql
-- QueryExpr::And(Eq("year", 2023), Eq("tags", "llm"))
SELECT r.id, r.hlc, r.schema_version, r.format_version
FROM records r
JOIN idx_papers p ON p.record_id = r.id
WHERE p.year = ?1 AND p.tags LIKE ?2
  AND r.deleted = 0
ORDER BY r.hlc DESC
LIMIT ?3 OFFSET ?4
```

All values are bound as parameters — no user-controlled string interpolation.

---

## 12. Dependency Summary

| Crate | Purpose |
|---|---|
| `rusqlite` + `bundled` feature | SQLite, embedded at compile time (no system dep) |
| `tokio` | Async runtime |
| `aws-sdk-dynamodb` | DynamoDB backend |
| `aws-sdk-s3` | S3 backend |
| `ulid` | ULID generation |
| `aes-gcm` | AES-256-GCM encryption |
| `argon2` | Master key derivation from passphrase |
| `keyring` | OS keychain (macOS / Linux Secret Service / Windows Credential Manager) |
| `serde_json` | JSON parsing for index extraction |
| `async-trait` | Async trait support |
| `bytes` | `Bytes` type for streaming blob parts |
| `thiserror` | Error types |
| `tracing` | Structured logging |

`rusqlite` with `bundled` compiles SQLite from source. This guarantees the same SQLite version across all machines and avoids dependency on the system SQLite — important for reproducible builds on Ubuntu LTS where the system SQLite may be years behind.

---

## 13. Unfolded Integration Notes

The first consumer of squirreld is [unfolded](https://github.com/shi-yan/unfolded), a research paper reader with notes.

Expected collections:

| Collection | Encryption | FTS fields | Index fields |
|---|---|---|---|
| `papers` | Enabled | `title`, `abstract` | `title`, `year`, `tags`, `read_status` |
| `notes` | Enabled | `content` | `paper_id`, `page`, `created_at` |
| `readlist` | Disabled | — | `paper_id`, `status`, `progress` |

PDFs are stored as blobs with encryption enabled. The `papers` record holds the `blob_id` reference.

On first launch offline: all writes queue in the outbox. On next network connection, the sync loop pushes everything and uploads pending PDFs, resuming any interrupted multipart uploads automatically.
