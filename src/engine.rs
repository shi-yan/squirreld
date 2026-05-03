use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;

use rusqlite::Connection;
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::{
    blob::{BlobTrigger, BlobWorker},
    builder::EngineConfig,
    collection::CollectionConfig,
    db,
    db::{index::QueryOpts, records::RecordRow},
    error::{Result, SquirrelError},
    hlc::Hlc,
    sync::SyncTrigger,
    types::*,
};

// ──────────────────────────────────────────────────────────────────────────────
// Internal command enum
// ──────────────────────────────────────────────────────────────────────────────

enum Command {
    Put {
        collection: String,
        id: Option<Ulid>,
        data: Vec<u8>,
        opts: PutOpts,
        reply: oneshot::Sender<Result<Ulid>>,
    },
    Get {
        collection: String,
        id: Ulid,
        reply: oneshot::Sender<Result<Option<Record>>>,
    },
    Delete {
        collection: String,
        id: Ulid,
        reply: oneshot::Sender<Result<()>>,
    },
    List {
        collection: String,
        opts: ListOpts,
        reply: oneshot::Sender<Result<Vec<RecordMeta>>>,
    },
    PendingErrors {
        reply: oneshot::Sender<Result<Vec<PendingError>>>,
    },
    ClearError {
        seq: i64,
        reply: oneshot::Sender<Result<()>>,
    },
    ForceSync {
        reply: oneshot::Sender<Result<SyncStats>>,
    },
    // ── Blob commands ───────────────────────────────────────────────────────
    PutBlob {
        src_path: PathBuf,
        opts: PutBlobOpts,
        reply: oneshot::Sender<Result<BlobId>>,
    },
    BlobInfo {
        blob_id: BlobId,
        reply: oneshot::Sender<Result<Option<BlobInfo>>>,
    },
    GetBlob {
        blob_id: BlobId,
        reply: oneshot::Sender<Result<Option<PathBuf>>>,
    },
    ForceFlushBlobs {
        reply: oneshot::Sender<Result<()>>,
    },
    // ── Index commands ──────────────────────────────────────────────────────
    RegisterIndex {
        def: IndexDef,
        reply: oneshot::Sender<Result<()>>,
    },
    Query {
        collection: String,
        opts: QueryOpts,
        reply: oneshot::Sender<Result<Vec<RecordMeta>>>,
    },
    Shutdown,
}

// ──────────────────────────────────────────────────────────────────────────────
// Actor state
// ──────────────────────────────────────────────────────────────────────────────

struct ActorState {
    conn: Connection,
    last_hlc: Hlc,
    _collections: HashMap<String, CollectionConfig>,
    _events_tx: broadcast::Sender<SyncEvent>,
    sync_trigger: Option<mpsc::Sender<SyncTrigger>>,
    blob_trigger: Option<mpsc::Sender<BlobTrigger>>,
    cache_dir: PathBuf,
    /// Shadow indexes keyed by collection name.
    indexes: HashMap<String, IndexDef>,
}

// ──────────────────────────────────────────────────────────────────────────────
// Public handle
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct SquirrelEngine {
    tx: mpsc::Sender<Command>,
    events_tx: broadcast::Sender<SyncEvent>,
}

impl SquirrelEngine {
    pub fn builder() -> crate::builder::EngineBuilder {
        crate::builder::EngineBuilder::new()
    }

    pub(crate) async fn open(config: EngineConfig) -> Result<Self> {
        let conn = Connection::open(&config.db_path)?;
        db::schema::initialize(&conn)?;
        let node_id = db::config::get_or_create_node_id(&conn)?;

        let last_hlc = match db::records::max_hlc(&conn)? {
            Some(s) => Hlc::from_str(&s).unwrap_or_else(|_| Hlc::new(node_id)),
            None    => Hlc::new(node_id),
        };

        let (events_tx, _) = broadcast::channel(64);
        let (tx, rx) = mpsc::channel(config.channel_capacity);

        // Default cache directory: sibling to the DB file.
        let cache_dir = config.cache_dir.unwrap_or_else(|| {
            config.db_path.parent()
                .unwrap_or_else(|| std::path::Path::new("."))
                .join("blob_cache")
        });

        // Optionally start the record sync loop.
        let sync_trigger = if let Some(backend) = config.record_backend {
            let (trigger_tx, trigger_rx) = mpsc::channel(8);
            let sync_loop = crate::sync::SyncLoop::new(
                config.db_path.clone(),
                node_id,
                backend,
                events_tx.clone(),
                trigger_rx,
            );
            tokio::spawn(sync_loop.run());
            Some(trigger_tx)
        } else {
            None
        };

        // Optionally start the blob worker.
        let blob_trigger = if let Some(backend) = config.blob_backend {
            let (trigger_tx, trigger_rx) = mpsc::channel(32);
            let worker = BlobWorker::new(
                config.db_path.clone(),
                cache_dir.clone(),
                backend,
                events_tx.clone(),
                trigger_rx,
            );
            tokio::spawn(worker.run());
            Some(trigger_tx)
        } else {
            None
        };

        let state = ActorState {
            conn,
            last_hlc,
            _collections: config.collections,
            _events_tx: events_tx.clone(),
            sync_trigger,
            blob_trigger,
            cache_dir,
            indexes: HashMap::new(),
        };

        tokio::spawn(actor_loop(rx, state));
        Ok(Self { tx, events_tx })
    }

    // ── Record operations ───────────────────────────────────────────────────

    pub async fn put(
        &self,
        collection: &str,
        id: Option<Ulid>,
        data: Vec<u8>,
        opts: PutOpts,
    ) -> Result<Ulid> {
        let (tx, rx) = oneshot::channel();
        self.send(Command::Put { collection: collection.into(), id, data, opts, reply: tx }).await?;
        rx.await.map_err(|_| SquirrelError::ActorClosed)?
    }

    pub async fn get(&self, collection: &str, id: Ulid) -> Result<Option<Record>> {
        let (tx, rx) = oneshot::channel();
        self.send(Command::Get { collection: collection.into(), id, reply: tx }).await?;
        rx.await.map_err(|_| SquirrelError::ActorClosed)?
    }

    pub async fn delete(&self, collection: &str, id: Ulid) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.send(Command::Delete { collection: collection.into(), id, reply: tx }).await?;
        rx.await.map_err(|_| SquirrelError::ActorClosed)?
    }

    pub async fn list(&self, collection: &str, opts: ListOpts) -> Result<Vec<RecordMeta>> {
        let (tx, rx) = oneshot::channel();
        self.send(Command::List { collection: collection.into(), opts, reply: tx }).await?;
        rx.await.map_err(|_| SquirrelError::ActorClosed)?
    }

    // ── Record sync diagnostics ─────────────────────────────────────────────

    pub async fn pending_errors(&self) -> Result<Vec<PendingError>> {
        let (tx, rx) = oneshot::channel();
        self.send(Command::PendingErrors { reply: tx }).await?;
        rx.await.map_err(|_| SquirrelError::ActorClosed)?
    }

    pub async fn clear_error(&self, seq: i64) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.send(Command::ClearError { seq, reply: tx }).await?;
        rx.await.map_err(|_| SquirrelError::ActorClosed)?
    }

    pub async fn force_sync(&self) -> Result<SyncStats> {
        let (tx, rx) = oneshot::channel();
        self.send(Command::ForceSync { reply: tx }).await?;
        rx.await.map_err(|_| SquirrelError::ActorClosed)?
    }

    // ── Blob operations ─────────────────────────────────────────────────────

    /// Stage a local file as a blob and schedule it for background upload.
    /// Returns the `BlobId`; the actual upload happens asynchronously.
    pub async fn put_blob(
        &self,
        src_path: impl Into<PathBuf>,
        opts: PutBlobOpts,
    ) -> Result<BlobId> {
        let (tx, rx) = oneshot::channel();
        self.send(Command::PutBlob { src_path: src_path.into(), opts, reply: tx }).await?;
        rx.await.map_err(|_| SquirrelError::ActorClosed)?
    }

    /// Return metadata for a blob, or `None` if the blob ID is not found.
    pub async fn blob_info(&self, blob_id: &BlobId) -> Result<Option<BlobInfo>> {
        let (tx, rx) = oneshot::channel();
        self.send(Command::BlobInfo { blob_id: blob_id.clone(), reply: tx }).await?;
        rx.await.map_err(|_| SquirrelError::ActorClosed)?
    }

    /// Return the local path for a blob if it is available in the cache.
    /// Triggers a background download if the blob is `Uploaded` but not cached.
    pub async fn get_blob(&self, blob_id: &BlobId) -> Result<Option<PathBuf>> {
        let (tx, rx) = oneshot::channel();
        self.send(Command::GetBlob { blob_id: blob_id.clone(), reply: tx }).await?;
        rx.await.map_err(|_| SquirrelError::ActorClosed)?
    }

    /// Trigger an immediate upload+download pass and wait for it to complete.
    /// Useful in tests or when the caller needs blobs fully synced before proceeding.
    pub async fn force_flush_blobs(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.send(Command::ForceFlushBlobs { reply: tx }).await?;
        rx.await.map_err(|_| SquirrelError::ActorClosed)?
    }

    // ── Index operations ────────────────────────────────────────────────────

    /// Register a shadow index for a collection.
    /// Creates the backing SQLite tables and records the definition so that
    /// subsequent `put` calls automatically maintain the index.
    pub async fn register_index(&self, def: IndexDef) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.send(Command::RegisterIndex { def, reply: tx }).await?;
        rx.await.map_err(|_| SquirrelError::ActorClosed)?
    }

    /// Query a collection using an optional filter and sort/page options.
    /// Returns record headers (no data bytes); call `get` for the full payload.
    pub async fn query(&self, collection: &str, opts: QueryOpts) -> Result<Vec<RecordMeta>> {
        let (tx, rx) = oneshot::channel();
        self.send(Command::Query { collection: collection.into(), opts, reply: tx }).await?;
        rx.await.map_err(|_| SquirrelError::ActorClosed)?
    }

    // ── Events ──────────────────────────────────────────────────────────────

    pub fn sync_events(&self) -> broadcast::Receiver<SyncEvent> {
        self.events_tx.subscribe()
    }

    // ── Lifecycle ───────────────────────────────────────────────────────────

    pub async fn shutdown(self) -> Result<()> {
        let _ = self.tx.send(Command::Shutdown).await;
        Ok(())
    }

    async fn send(&self, cmd: Command) -> Result<()> {
        self.tx.send(cmd).await.map_err(|_| SquirrelError::ActorClosed)
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Actor loop
// ──────────────────────────────────────────────────────────────────────────────

async fn actor_loop(mut rx: mpsc::Receiver<Command>, mut state: ActorState) {
    while let Some(cmd) = rx.recv().await {
        match cmd {
            Command::Put { collection, id, data, opts, reply } => {
                let result = handle_put(&mut state, collection, id, data, opts);
                if result.is_ok() { kick_sync(&state); }
                let _ = reply.send(result);
            }
            Command::Get { collection, id, reply } => {
                let _ = reply.send(handle_get(&state, &collection, id));
            }
            Command::Delete { collection, id, reply } => {
                let result = handle_delete(&mut state, &collection, id);
                if result.is_ok() { kick_sync(&state); }
                let _ = reply.send(result);
            }
            Command::List { collection, opts, reply } => {
                let _ = reply.send(handle_list(&state, &collection, opts));
            }
            Command::PendingErrors { reply } => {
                let _ = reply.send(handle_pending_errors(&state));
            }
            Command::ClearError { seq, reply } => {
                let _ = reply.send(db::outbox::clear_error(&state.conn, seq));
            }
            Command::ForceSync { reply } => {
                let trigger = state.sync_trigger.clone();
                let _ = reply.send(force_sync_via(trigger).await);
            }
            Command::PutBlob { src_path, opts, reply } => {
                let _ = reply.send(handle_put_blob(&state, src_path, opts));
            }
            Command::BlobInfo { blob_id, reply } => {
                let _ = reply.send(handle_blob_info(&state, &blob_id));
            }
            Command::GetBlob { blob_id, reply } => {
                let _ = reply.send(handle_get_blob(&state, &blob_id));
            }
            Command::ForceFlushBlobs { reply } => {
                let trigger = state.blob_trigger.clone();
                let _ = reply.send(force_flush_blobs_via(trigger).await);
            }
            Command::RegisterIndex { def, reply } => {
                let _ = reply.send(handle_register_index(&mut state, def));
            }
            Command::Query { collection, opts, reply } => {
                let _ = reply.send(handle_query(&state, &collection, opts));
            }
            Command::Shutdown => break,
        }
    }
    tracing::debug!("squirreld actor shut down");
}

fn kick_sync(state: &ActorState) {
    if let Some(tx) = &state.sync_trigger {
        let _ = tx.try_send(SyncTrigger::Kick);
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Record handlers
// ──────────────────────────────────────────────────────────────────────────────

fn handle_put(
    state: &mut ActorState,
    collection: String,
    id: Option<Ulid>,
    data: Vec<u8>,
    opts: PutOpts,
) -> Result<Ulid> {
    let id  = id.unwrap_or_else(Ulid::new);
    let hlc = state.last_hlc.tick();
    let now = now_ms();

    let tx = state.conn.transaction()?;
    db::records::upsert(&tx, &RecordRow {
        id:             id.to_string(),
        collection:     collection.clone(),
        data:           data.clone(),
        hlc:            hlc.to_string(),
        schema_version: opts.schema_version.unwrap_or(0),
        format_version: 0,
        dek_encrypted:  None,
        deleted:        false,
        synced:         false,
        created_at:     now,
        updated_at:     now,
    })?;
    db::outbox::append(&tx, &id.to_string(), &collection, "upsert", &hlc.to_string(), Some(&data))?;

    // Maintain shadow index if registered for this collection.
    if let Some(def) = state.indexes.get(&collection) {
        if !opts.index_fields.is_empty() || !def.fields.is_empty() {
            db::index::upsert_index_row(&tx, def, &id.to_string(), &opts.index_fields)?;
        }
        if !def.fts_fields.is_empty() {
            db::index::upsert_fts_row(&tx, def, &id.to_string(), &opts.index_fields)?;
        }
    }

    tx.commit()?;
    state.last_hlc = hlc;
    Ok(id)
}

fn handle_get(state: &ActorState, collection: &str, id: Ulid) -> Result<Option<Record>> {
    let row = db::records::get(&state.conn, collection, &id.to_string())?;
    Ok(row.filter(|r| !r.deleted).map(to_record))
}

fn handle_delete(state: &mut ActorState, collection: &str, id: Ulid) -> Result<()> {
    let hlc = state.last_hlc.tick();
    let now = now_ms();
    let tx  = state.conn.transaction()?;
    db::records::soft_delete(&tx, collection, &id.to_string(), &hlc.to_string(), now)?;
    db::outbox::append(&tx, &id.to_string(), collection, "delete", &hlc.to_string(), None)?;

    if let Some(def) = state.indexes.get(collection) {
        db::index::delete_index_rows(&tx, def, &id.to_string())?;
    }

    tx.commit()?;
    state.last_hlc = hlc;
    Ok(())
}

fn handle_list(state: &ActorState, collection: &str, opts: ListOpts) -> Result<Vec<RecordMeta>> {
    let asc = matches!(opts.order, SortOrder::HlcAsc);
    db::records::list(&state.conn, collection, opts.limit, opts.offset, opts.include_deleted, asc)
        .map(|rows| rows.into_iter().map(to_meta).collect())
}

fn handle_pending_errors(state: &ActorState) -> Result<Vec<PendingError>> {
    db::outbox::list_pending_errors(&state.conn).map(|entries| {
        entries.into_iter().map(|e| PendingError {
            seq:              e.seq,
            record_id:        e.record_id,
            collection:       e.collection,
            retries:          e.retries,
            last_error:       e.last_error,
            next_retry_at_ms: e.next_retry_at as u64,
        }).collect()
    })
}

async fn force_sync_via(trigger: Option<mpsc::Sender<SyncTrigger>>) -> Result<SyncStats> {
    match trigger {
        None     => Ok(SyncStats::default()),
        Some(tx) => {
            let (reply_tx, reply_rx) = oneshot::channel();
            let _ = tx.send(SyncTrigger::ForceSync(reply_tx)).await;
            reply_rx.await.map_err(|_| SquirrelError::ActorClosed)
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Blob handlers
// ──────────────────────────────────────────────────────────────────────────────

fn handle_put_blob(
    state: &ActorState,
    src_path: PathBuf,
    opts: PutBlobOpts,
) -> Result<BlobId> {
    let blob_id = Ulid::new().to_string();
    let s3_key  = format!("blobs/{blob_id}");
    let now     = now_ms();

    db::blobs::insert(&state.conn, &db::blobs::BlobRow {
        id:             blob_id.clone(),
        record_id:      opts.record_id,
        collection:     opts.collection,
        local_path:     None, // set by blob worker after staging
        s3_key:         s3_key.clone(),
        size_bytes:     None,
        upload_id:      None,
        status:         "pending".into(),
        format_version: 0,
        retries:        0,
        next_retry_at:  0,
        last_error:     None,
        error_log:      None,
        created_at:     now,
        updated_at:     now,
    })?;

    // Kick the blob worker to stage and upload.
    if let Some(tx) = &state.blob_trigger {
        let _ = tx.try_send(BlobTrigger::Stage {
            blob_id: blob_id.clone(),
            src: src_path,
        });
    }

    Ok(blob_id)
}

fn handle_blob_info(state: &ActorState, blob_id: &str) -> Result<Option<BlobInfo>> {
    let row = db::blobs::get(&state.conn, blob_id)?;
    Ok(row.map(|r| BlobInfo {
        id:         r.id,
        status:     BlobStatus::from_db_str(&r.status)
                        .unwrap_or(BlobStatus::Pending),
        local_path: r.local_path.map(PathBuf::from),
        size_bytes: r.size_bytes.map(|s| s as u64),
        retries:    r.retries,
        last_error: r.last_error,
    }))
}

fn handle_get_blob(state: &ActorState, blob_id: &str) -> Result<Option<PathBuf>> {
    let row = match db::blobs::get(&state.conn, blob_id)? {
        None    => return Ok(None),
        Some(r) => r,
    };

    // If the file is locally available, return its path.
    if let Some(ref p) = row.local_path {
        let path = PathBuf::from(p);
        if path.exists() {
            return Ok(Some(path));
        }
    }

    // If the blob is uploaded but not cached, trigger a download.
    if row.status == BlobStatus::Uploaded.as_str() {
        if let Some(tx) = &state.blob_trigger {
            let _ = tx.try_send(BlobTrigger::Download { blob_id: blob_id.to_string() });
        }
    }

    Ok(None) // not yet available; caller can poll blob_info or subscribe to SyncEvent
}

async fn force_flush_blobs_via(trigger: Option<mpsc::Sender<BlobTrigger>>) -> Result<()> {
    match trigger {
        None     => Ok(()),
        Some(tx) => {
            let (reply_tx, reply_rx) = oneshot::channel();
            let _ = tx.send(BlobTrigger::ForceFlush(reply_tx)).await;
            reply_rx.await.map_err(|_| SquirrelError::ActorClosed)
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Index handlers
// ──────────────────────────────────────────────────────────────────────────────

fn handle_register_index(state: &mut ActorState, def: IndexDef) -> Result<()> {
    db::index::create_shadow_table(&state.conn, &def)?;
    db::index::create_fts_table(&state.conn, &def)?;
    state.indexes.insert(def.collection.clone(), def);
    Ok(())
}

fn handle_query(state: &ActorState, collection: &str, opts: QueryOpts) -> Result<Vec<RecordMeta>> {
    let def = state.indexes.get(collection);
    db::index::query(&state.conn, collection, def, &opts)
        .map(|rows| rows.into_iter().map(to_meta).collect())
}

// ──────────────────────────────────────────────────────────────────────────────
// Row → public type conversions
// ──────────────────────────────────────────────────────────────────────────────

fn to_record(r: RecordRow) -> Record {
    Record {
        id:             r.id.parse().unwrap_or_else(|_| Ulid::new()),
        collection:     r.collection,
        data:           r.data,
        hlc:            r.hlc.parse().unwrap_or_else(|_| Hlc::new([0; 6])),
        schema_version: r.schema_version,
        deleted:        r.deleted,
        created_at:     r.created_at as u64,
        updated_at:     r.updated_at as u64,
    }
}

fn to_meta(r: RecordRow) -> RecordMeta {
    RecordMeta {
        id:             r.id.parse().unwrap_or_else(|_| Ulid::new()),
        collection:     r.collection,
        hlc:            r.hlc.parse().unwrap_or_else(|_| Hlc::new([0; 6])),
        schema_version: r.schema_version,
        deleted:        r.deleted,
        created_at:     r.created_at as u64,
        updated_at:     r.updated_at as u64,
    }
}
