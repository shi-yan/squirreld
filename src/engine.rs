use std::collections::HashMap;
use std::str::FromStr;

use rusqlite::Connection;
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::{
    builder::EngineConfig,
    collection::CollectionConfig,
    db,
    db::records::RecordRow,
    error::{Result, SquirrelError},
    hlc::Hlc,
    sync::SyncTrigger,
    types::*,
};

// ──────────────────────────────────────────────────────────────────────────────
// Internal command enum (actor message protocol)
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
    Shutdown,
}

// ──────────────────────────────────────────────────────────────────────────────
// Actor state (owned exclusively by the actor task)
// ──────────────────────────────────────────────────────────────────────────────

struct ActorState {
    conn: Connection,
    /// Monotonically increasing clock; persisted across restarts via max_hlc query.
    last_hlc: Hlc,
    /// Collection configs keyed by name. Used in later phases for indexing/encryption.
    _collections: HashMap<String, CollectionConfig>,
    /// Broadcast sender for sync lifecycle events.
    _events_tx: broadcast::Sender<SyncEvent>,
    /// Send a trigger to the sync loop after every write. None when no backend is configured.
    sync_trigger: Option<mpsc::Sender<SyncTrigger>>,
}

// ──────────────────────────────────────────────────────────────────────────────
// Public handle
// ──────────────────────────────────────────────────────────────────────────────

/// A cheap-to-clone handle to the squirreld actor.
///
/// All operations are async and communicate with a single background task via
/// a bounded MPSC channel. The actor serialises all SQLite writes, ensuring
/// thread-safety without external locking.
#[derive(Clone)]
pub struct SquirrelEngine {
    tx: mpsc::Sender<Command>,
    events_tx: broadcast::Sender<SyncEvent>,
}

impl SquirrelEngine {
    /// Create a new engine builder.
    pub fn builder() -> crate::builder::EngineBuilder {
        crate::builder::EngineBuilder::new()
    }

    /// Open the engine from a pre-built config. Prefer [`SquirrelEngine::builder`] instead.
    pub(crate) async fn open(config: EngineConfig) -> Result<Self> {
        let conn = Connection::open(&config.db_path)?;
        db::schema::initialize(&conn)?;
        let node_id = db::config::get_or_create_node_id(&conn)?;

        // Restore the HLC from the DB so we never issue a clock value we already used.
        let last_hlc = match db::records::max_hlc(&conn)? {
            Some(s) => Hlc::from_str(&s).unwrap_or_else(|_| Hlc::new(node_id)),
            None => Hlc::new(node_id),
        };

        let (events_tx, _) = broadcast::channel(64);
        let (tx, rx) = mpsc::channel(config.channel_capacity);

        // Optionally start the sync loop.
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

        let state = ActorState {
            conn,
            last_hlc,
            _collections: config.collections,
            _events_tx: events_tx.clone(),
            sync_trigger,
        };

        tokio::spawn(actor_loop(rx, state));

        Ok(Self { tx, events_tx })
    }

    // ── Record operations ───────────────────────────────────────────────────

    /// Write a record. Generates a new ULID if `id` is `None`.
    /// The write is durable (SQLite fsync) and queued in the outbox before returning.
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

    /// Fetch a record by collection + id. Returns `None` if absent or soft-deleted.
    pub async fn get(&self, collection: &str, id: Ulid) -> Result<Option<Record>> {
        let (tx, rx) = oneshot::channel();
        self.send(Command::Get { collection: collection.into(), id, reply: tx }).await?;
        rx.await.map_err(|_| SquirrelError::ActorClosed)?
    }

    /// Soft-delete a record. Sets a tombstone; the row is retained for sync purposes.
    pub async fn delete(&self, collection: &str, id: Ulid) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.send(Command::Delete { collection: collection.into(), id, reply: tx }).await?;
        rx.await.map_err(|_| SquirrelError::ActorClosed)?
    }

    /// List records in a collection. Returns metadata only — no data bytes.
    /// Use [`SquirrelEngine::get`] to fetch the full record after listing.
    pub async fn list(&self, collection: &str, opts: ListOpts) -> Result<Vec<RecordMeta>> {
        let (tx, rx) = oneshot::channel();
        self.send(Command::List { collection: collection.into(), opts, reply: tx }).await?;
        rx.await.map_err(|_| SquirrelError::ActorClosed)?
    }

    // ── Sync diagnostics ────────────────────────────────────────────────────

    /// Returns outbox entries that have experienced at least one failed sync attempt.
    /// Use this to display sync-error indicators in the UI.
    pub async fn pending_errors(&self) -> Result<Vec<PendingError>> {
        let (tx, rx) = oneshot::channel();
        self.send(Command::PendingErrors { reply: tx }).await?;
        rx.await.map_err(|_| SquirrelError::ActorClosed)?
    }

    /// Discard a stuck outbox entry identified by its `seq`. The record itself is
    /// unaffected — only the pending-push entry is removed.
    pub async fn clear_error(&self, seq: i64) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.send(Command::ClearError { seq, reply: tx }).await?;
        rx.await.map_err(|_| SquirrelError::ActorClosed)?
    }

    /// Trigger an immediate sync cycle and wait for it to complete.
    /// Returns `Ok(SyncStats::default())` if no backend is configured.
    pub async fn force_sync(&self) -> Result<SyncStats> {
        let (tx, rx) = oneshot::channel();
        self.send(Command::ForceSync { reply: tx }).await?;
        rx.await.map_err(|_| SquirrelError::ActorClosed)?
    }

    /// Subscribe to sync lifecycle events. Returns a broadcast receiver; multiple
    /// subscribers are supported.
    pub fn sync_events(&self) -> broadcast::Receiver<SyncEvent> {
        self.events_tx.subscribe()
    }

    // ── Lifecycle ───────────────────────────────────────────────────────────

    /// Gracefully shut down the engine. Waits for any in-flight command to complete.
    pub async fn shutdown(self) -> Result<()> {
        // Best-effort: if the actor is already gone, that's fine.
        let _ = self.tx.send(Command::Shutdown).await;
        Ok(())
    }

    // ── Internal helpers ────────────────────────────────────────────────────

    async fn send(&self, cmd: Command) -> Result<()> {
        self.tx.send(cmd).await.map_err(|_| SquirrelError::ActorClosed)
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Actor implementation
// ──────────────────────────────────────────────────────────────────────────────

async fn actor_loop(mut rx: mpsc::Receiver<Command>, mut state: ActorState) {
    while let Some(cmd) = rx.recv().await {
        match cmd {
            Command::Put { collection, id, data, opts, reply } => {
                let result = handle_put(&mut state, collection, id, data, opts);
                if result.is_ok() {
                    kick_sync(&state);
                }
                let _ = reply.send(result);
            }
            Command::Get { collection, id, reply } => {
                let _ = reply.send(handle_get(&state, &collection, id));
            }
            Command::Delete { collection, id, reply } => {
                let result = handle_delete(&mut state, &collection, id);
                if result.is_ok() {
                    kick_sync(&state);
                }
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
                // Clone the sender before awaiting so no &state borrow crosses an .await.
                let trigger = state.sync_trigger.clone();
                let _ = reply.send(force_sync_via(trigger).await);
            }
            Command::Shutdown => break,
        }
    }
    tracing::debug!("squirreld actor shut down");
}

/// Non-blocking: fire-and-forget sync trigger. Drops silently if the channel is
/// full (the sync loop is already running or about to run).
fn kick_sync(state: &ActorState) {
    if let Some(tx) = &state.sync_trigger {
        let _ = tx.try_send(SyncTrigger::Kick);
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Command handlers
// ──────────────────────────────────────────────────────────────────────────────

fn handle_put(
    state: &mut ActorState,
    collection: String,
    id: Option<Ulid>,
    data: Vec<u8>,
    opts: PutOpts,
) -> Result<Ulid> {
    let id = id.unwrap_or_else(Ulid::new);
    let hlc = state.last_hlc.tick();
    let now = now_ms();

    let tx = state.conn.transaction()?;

    db::records::upsert(
        &tx,
        &RecordRow {
            id: id.to_string(),
            collection: collection.clone(),
            data: data.clone(),
            hlc: hlc.to_string(),
            schema_version: opts.schema_version.unwrap_or(0),
            format_version: 0, // encryption implemented in Phase 5
            dek_encrypted: None,
            deleted: false,
            synced: false,
            created_at: now,   // preserved by ON CONFLICT if record already exists
            updated_at: now,
        },
    )?;

    db::outbox::append(&tx, &id.to_string(), &collection, "upsert", &hlc.to_string(), Some(&data))?;

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

    let tx = state.conn.transaction()?;
    db::records::soft_delete(&tx, collection, &id.to_string(), &hlc.to_string(), now)?;
    db::outbox::append(&tx, &id.to_string(), collection, "delete", &hlc.to_string(), None)?;
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
        entries
            .into_iter()
            .map(|e| PendingError {
                seq: e.seq,
                record_id: e.record_id,
                collection: e.collection,
                retries: e.retries,
                last_error: e.last_error,
                next_retry_at_ms: e.next_retry_at as u64,
            })
            .collect()
    })
}

/// Sends a ForceSync trigger and waits for the sync loop to reply.
/// Takes an owned sender (not &ActorState) so no non-Send value crosses an .await.
async fn force_sync_via(trigger: Option<mpsc::Sender<SyncTrigger>>) -> Result<SyncStats> {
    match trigger {
        None => Ok(SyncStats::default()),
        Some(tx) => {
            let (reply_tx, reply_rx) = oneshot::channel();
            let _ = tx.send(SyncTrigger::ForceSync(reply_tx)).await;
            reply_rx.await.map_err(|_| SquirrelError::ActorClosed)
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Row → public type conversions
// ──────────────────────────────────────────────────────────────────────────────

fn to_record(r: RecordRow) -> Record {
    Record {
        id: r.id.parse().unwrap_or_else(|_| Ulid::new()),
        collection: r.collection,
        data: r.data,
        hlc: r.hlc.parse().unwrap_or_else(|_| Hlc::new([0; 6])),
        schema_version: r.schema_version,
        deleted: r.deleted,
        created_at: r.created_at as u64,
        updated_at: r.updated_at as u64,
    }
}

fn to_meta(r: RecordRow) -> RecordMeta {
    RecordMeta {
        id: r.id.parse().unwrap_or_else(|_| Ulid::new()),
        collection: r.collection,
        hlc: r.hlc.parse().unwrap_or_else(|_| Hlc::new([0; 6])),
        schema_version: r.schema_version,
        deleted: r.deleted,
        created_at: r.created_at as u64,
        updated_at: r.updated_at as u64,
    }
}
