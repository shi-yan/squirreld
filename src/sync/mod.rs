use std::path::PathBuf;
use std::sync::Arc;

use rusqlite::Connection;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::{Duration, Instant};
use tracing::{debug, warn};

use crate::{
    backend::{OutboxPushEntry, PushResult, RecordBackend, RemoteRecord},
    db::{self, outbox::OutboxEntry, records::RecordRow},
    types::{SyncEvent, SyncStats, now_ms},
};

// ── Trigger ───────────────────────────────────────────────────────────────────

pub(crate) enum SyncTrigger {
    /// Fire-and-forget: a record was written, sync soon.
    Kick,
    /// Caller wants to await the result of the next full sync cycle.
    ForceSync(oneshot::Sender<SyncStats>),
}

// ── SyncLoop ──────────────────────────────────────────────────────────────────

pub(crate) struct SyncLoop {
    db_path:    PathBuf,
    backend:    Arc<dyn RecordBackend>,
    events_tx:  broadcast::Sender<SyncEvent>,
    trigger_rx: mpsc::Receiver<SyncTrigger>,
}

const DEBOUNCE_MS:   u64   = 500;
const PERIODIC_SECS: u64   = 60;
const PUSH_BATCH:    usize = 25;

impl SyncLoop {
    pub(crate) fn new(
        db_path:    PathBuf,
        _node_id:   [u8; 6],
        backend:    Arc<dyn RecordBackend>,
        events_tx:  broadcast::Sender<SyncEvent>,
        trigger_rx: mpsc::Receiver<SyncTrigger>,
    ) -> Self {
        Self { db_path, backend, events_tx, trigger_rx }
    }

    pub(crate) async fn run(mut self) {
        // Ensure the remote table/index exists once at startup.
        if let Err(e) = self.backend.ensure_table().await {
            warn!("sync loop: ensure_table failed: {e}");
        }

        let periodic = Duration::from_secs(PERIODIC_SECS);
        let mut deadline = Instant::now() + periodic;

        loop {
            let mut force_replies: Vec<oneshot::Sender<SyncStats>> = Vec::new();

            let triggered = tokio::time::timeout_at(deadline, self.trigger_rx.recv()).await;

            match triggered {
                Err(_elapsed) => {
                    deadline = Instant::now() + periodic;
                }
                Ok(None) => {
                    debug!("sync loop: trigger channel closed, exiting");
                    return;
                }
                Ok(Some(msg)) => {
                    let is_force = matches!(msg, SyncTrigger::ForceSync(_));
                    if let SyncTrigger::ForceSync(tx) = msg {
                        force_replies.push(tx);
                    }
                    if !is_force {
                        // Regular kick: debounce to coalesce rapid writes.
                        // A ForceSync arriving during this window cuts it short.
                        let debounce_end = Instant::now() + Duration::from_millis(DEBOUNCE_MS);
                        loop {
                            match tokio::time::timeout_at(debounce_end, self.trigger_rx.recv()).await {
                                Ok(Some(SyncTrigger::ForceSync(tx))) => {
                                    force_replies.push(tx);
                                    break; // ForceSync always runs immediately
                                }
                                Ok(Some(SyncTrigger::Kick)) => {}
                                Ok(None) | Err(_) => break,
                            }
                        }
                    }
                    deadline = Instant::now() + periodic;
                }
            }

            let push_stats = self.run_push_cycle().await;
            let pull_stats = self.run_pull_cycle().await;

            let combined = SyncStats {
                pushed:    push_stats.pushed,
                pulled:    pull_stats.pulled,
                conflicts: push_stats.conflicts,
                errors:    push_stats.errors + pull_stats.errors,
            };

            let _ = self.events_tx.send(SyncEvent::PushComplete(push_stats));
            let _ = self.events_tx.send(SyncEvent::PullComplete(pull_stats));

            for tx in force_replies {
                let _ = tx.send(combined.clone());
            }
        }
    }

    // ── Push cycle ────────────────────────────────────────────────────────────
    //
    // Design: never hold a &Connection across an .await boundary (rusqlite::Connection
    // is !Sync so &Connection is !Send).  We open a fresh connection for each
    // synchronous DB phase, let it drop before any .await, then open another one
    // for DB writes.  SQLite WAL mode makes concurrent readers cheap.

    async fn run_push_cycle(&self) -> SyncStats {
        let mut stats = SyncStats::default();

        // --- Synchronous DB read phase ---
        let push_entries: Vec<OutboxPushEntry> = {
            let conn = match Connection::open(&self.db_path) {
                Ok(c)  => c,
                Err(e) => { warn!("push: open DB: {e}"); stats.errors += 1; return stats; }
            };
            let entries = match db::outbox::peek_batch(&conn, PUSH_BATCH) {
                Ok(e)  => e,
                Err(e) => { warn!("push: peek_batch: {e}"); stats.errors += 1; return stats; }
            };
            entries.iter()
                .filter_map(|e| build_push_entry(&conn, e))
                .collect()
            // conn dropped here — no borrow crosses an .await
        };

        if push_entries.is_empty() { return stats; }

        // --- Async network phase + synchronous DB write phase (interleaved per entry) ---
        for entry in &push_entries {
            let result = self.backend.push_one(entry).await;   // <-- .await here

            // --- Synchronous DB write phase ---
            let conn = match Connection::open(&self.db_path) {
                Ok(c)  => c,
                Err(e) => { warn!("push: open DB for write: {e}"); stats.errors += 1; break; }
            };
            match result {
                PushResult::Ok { pushed_seqs } => {
                    let _ = db::outbox::delete_seqs(&conn, &pushed_seqs);
                    let _ = mark_synced(&conn, &entry.record_id, &entry.collection);
                    stats.pushed += 1;
                }
                PushResult::ConflictAt { record_id, seq } => {
                    debug!("push: conflict on {record_id}, will reconcile in pull");
                    let _ = db::outbox::delete_seqs(&conn, &[seq]);
                    stats.conflicts += 1;
                }
                PushResult::TransientError(msg) => {
                    warn!("push: transient error for seq={}: {msg}", entry.seq);
                    let _ = db::outbox::mark_retry(&conn, entry.seq, &msg);
                    let _ = self.events_tx.send(SyncEvent::RetryScheduled {
                        seq:           entry.seq,
                        retries:       entry.retries + 1,
                        next_retry_ms: now_ms() as u64,
                        error:         msg,
                    });
                    stats.errors += 1;
                    break; // stop pushing — preserve FIFO ordering
                }
            }
            // conn dropped here
        }

        stats
    }

    // ── Pull cycle ────────────────────────────────────────────────────────────

    async fn run_pull_cycle(&self) -> SyncStats {
        let mut stats = SyncStats::default();

        // --- Synchronous DB read phase ---
        let checkpoint: Option<String> = {
            let conn = match Connection::open(&self.db_path) {
                Ok(c)  => c,
                Err(e) => { warn!("pull: open DB: {e}"); stats.errors += 1; return stats; }
            };
            match db::sync_state::get_checkpoint(&conn, self.backend.backend_id()) {
                Ok(cp) => cp,
                Err(e) => { warn!("pull: get_checkpoint: {e}"); stats.errors += 1; return stats; }
            }
            // conn dropped
        };

        // --- Async network phase ---
        let remote_records: Vec<RemoteRecord> =
            match self.backend.pull_since(checkpoint.as_deref()).await {
                Ok(r)  => r,
                Err(e) => { warn!("pull: pull_since: {e}"); stats.errors += 1; return stats; }
            };

        if remote_records.is_empty() { return stats; }

        // --- Synchronous DB write phase ---
        {
            let conn = match Connection::open(&self.db_path) {
                Ok(c)  => c,
                Err(e) => { warn!("pull: open DB for write: {e}"); stats.errors += 1; return stats; }
            };
            let mut new_checkpoint = checkpoint.clone();

            for remote in &remote_records {
                if let Err(e) = apply_remote_record(&conn, remote) {
                    warn!("pull: apply_remote_record: {e}");
                    stats.errors += 1;
                    continue;
                }
                stats.pulled += 1;

                match &new_checkpoint {
                    None     => new_checkpoint = Some(remote.hlc.clone()),
                    Some(cp) if remote.hlc > *cp => new_checkpoint = Some(remote.hlc.clone()),
                    _ => {}
                }
            }

            if let Some(cp) = new_checkpoint {
                if Some(&cp) != checkpoint.as_ref() {
                    let _ = db::sync_state::set_checkpoint(
                        &conn, self.backend.backend_id(), &cp,
                    );
                }
            }
            // conn dropped
        }

        stats
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn build_push_entry(conn: &Connection, entry: &OutboxEntry) -> Option<OutboxPushEntry> {
    let (schema_version, format_version) = if entry.operation == "delete" {
        (0u32, 0u8)
    } else {
        let row = db::records::get(conn, &entry.collection, &entry.record_id).ok()??;
        (row.schema_version, row.format_version)
    };

    Some(OutboxPushEntry {
        seq:            entry.seq,
        record_id:      entry.record_id.clone(),
        collection:     entry.collection.clone(),
        operation:      entry.operation.clone(),
        hlc:            entry.hlc.clone(),
        data:           entry.data.clone(),
        schema_version,
        format_version,
        retries:        entry.retries,
    })
}

fn apply_remote_record(
    conn: &Connection,
    remote: &RemoteRecord,
) -> crate::error::Result<()> {
    let local = db::records::get(conn, &remote.collection, &remote.record_id)?;

    if let Some(ref local_row) = local {
        if local_row.hlc >= remote.hlc {
            return Ok(()); // local is same or newer
        }
    }

    let now = now_ms();
    let created_at = local.as_ref().map(|r| r.created_at).unwrap_or(now);

    let row = RecordRow {
        id:             remote.record_id.clone(),
        collection:     remote.collection.clone(),
        data:           remote.data.clone().unwrap_or_default(),
        hlc:            remote.hlc.clone(),
        schema_version: remote.schema_version,
        format_version: remote.format_version,
        dek_encrypted:  None,
        deleted:        remote.deleted,
        synced:         true,
        created_at,
        updated_at:     now,
    };
    db::records::upsert_remote(conn, &row)?;

    // Remove any stale outbox entry so we don't re-push what the remote already has.
    remove_outbox_for_record(conn, &remote.record_id, &remote.collection)?;
    Ok(())
}

fn mark_synced(conn: &Connection, record_id: &str, collection: &str) -> crate::error::Result<()> {
    conn.execute(
        "UPDATE records SET synced = 1 WHERE id = ?1 AND collection = ?2",
        rusqlite::params![record_id, collection],
    )?;
    Ok(())
}

fn remove_outbox_for_record(
    conn: &Connection,
    record_id: &str,
    collection: &str,
) -> crate::error::Result<()> {
    conn.execute(
        "DELETE FROM outbox WHERE record_id = ?1 AND collection = ?2",
        rusqlite::params![record_id, collection],
    )?;
    Ok(())
}
