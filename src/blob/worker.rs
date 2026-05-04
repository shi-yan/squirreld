use std::path::{Path, PathBuf};
use std::sync::Arc;

use rusqlite::Connection;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::{Duration, Instant};
use tracing::{debug, warn};

use crate::{
    backend::{BlobBackend, UploadedPart},
    db,
    db::blobs::BlobRow,
    types::{BlobId, BlobStatus, SyncEvent, now_ms},
};

// ── Trigger ───────────────────────────────────────────────────────────────────

pub(crate) enum BlobTrigger {
    /// Stage src_path → cache_dir/{blob_id}, then upload.
    Stage { blob_id: BlobId, src: PathBuf },
    /// Scan all pending/uploading blobs and resume uploads.
    Tick,
    /// Download blob_id from remote into cache_dir.
    Download { blob_id: BlobId },
    /// Run a full upload+download pass and reply with completion.
    ForceFlush(oneshot::Sender<()>),
}

// ── Constants ─────────────────────────────────────────────────────────────────

/// 5 MiB — minimum S3 multipart part size (except the last part).
pub(crate) const MULTIPART_THRESHOLD: u64 = 5 * 1024 * 1024;
const CHUNK_SIZE: u64 = MULTIPART_THRESHOLD;
const PERIODIC_SECS: u64 = 60;

// ── BlobWorker ────────────────────────────────────────────────────────────────

pub(crate) struct BlobWorker {
    db_path:    PathBuf,
    cache_dir:  PathBuf,
    backend:    Arc<dyn BlobBackend>,
    events_tx:  broadcast::Sender<SyncEvent>,
    trigger_rx: mpsc::Receiver<BlobTrigger>,
}

impl BlobWorker {
    pub(crate) fn new(
        db_path:    PathBuf,
        cache_dir:  PathBuf,
        backend:    Arc<dyn BlobBackend>,
        events_tx:  broadcast::Sender<SyncEvent>,
        trigger_rx: mpsc::Receiver<BlobTrigger>,
    ) -> Self {
        Self { db_path, cache_dir, backend, events_tx, trigger_rx }
    }

    pub(crate) async fn run(mut self) {
        if let Err(e) = self.backend.ensure_bucket().await {
            warn!("blob worker: ensure_bucket failed: {e}");
        }
        // Ensure cache directory exists.
        if let Err(e) = tokio::fs::create_dir_all(&self.cache_dir).await {
            warn!("blob worker: create cache_dir failed: {e}");
        }

        let periodic = Duration::from_secs(PERIODIC_SECS);
        let mut deadline = Instant::now() + periodic;
        let mut flush_replies: Vec<oneshot::Sender<()>> = Vec::new();

        loop {
            let msg = tokio::time::timeout_at(deadline, self.trigger_rx.recv()).await;
            match msg {
                Err(_elapsed) => {
                    deadline = Instant::now() + periodic;
                }
                Ok(None) => {
                    debug!("blob worker: trigger channel closed, exiting");
                    return;
                }
                Ok(Some(BlobTrigger::ForceFlush(tx))) => {
                    flush_replies.push(tx);
                    deadline = Instant::now() + periodic;
                }
                Ok(Some(BlobTrigger::Stage { blob_id, src })) => {
                    self.handle_stage(blob_id, src).await;
                    deadline = Instant::now() + periodic;
                    continue; // go back to waiting for more messages
                }
                Ok(Some(BlobTrigger::Download { blob_id })) => {
                    self.handle_download(blob_id).await;
                    deadline = Instant::now() + periodic;
                    continue;
                }
                Ok(Some(BlobTrigger::Tick)) => {
                    deadline = Instant::now() + periodic;
                }
            }

            // Run upload + download cycles.
            self.run_upload_cycle().await;
            self.run_download_cycle().await;

            for tx in flush_replies.drain(..) {
                let _ = tx.send(());
            }
        }
    }

    // ── Stage ─────────────────────────────────────────────────────────────────

    async fn handle_stage(&self, blob_id: BlobId, src: PathBuf) {
        let dest = self.cache_dir.join(&blob_id);

        // Copy the source file into the cache directory.
        let size = match tokio::fs::copy(&src, &dest).await {
            Ok(n)  => n,
            Err(e) => {
                warn!("blob worker: stage copy failed for {blob_id}: {e}");
                let conn = open_conn(&self.db_path);
                let _ = db::blobs::mark_retry(&conn, &blob_id, &e.to_string());
                return;
            }
        };

        let conn = open_conn(&self.db_path);
        let _ = db::blobs::set_local_path_and_size(
            &conn,
            &blob_id,
            dest.to_str().unwrap_or(""),
            size,
        );

        // Kick off the upload immediately.
        self.run_upload_cycle().await;
    }

    // ── Upload cycle ──────────────────────────────────────────────────────────

    async fn run_upload_cycle(&self) {
        let blobs = {
            let conn = open_conn(&self.db_path);
            match db::blobs::list_ready(&conn, &["pending", "uploading"]) {
                Ok(b)  => b,
                Err(e) => { warn!("blob upload: list_ready: {e}"); return; }
            }
        };

        for blob in blobs {
            if blob.local_path.is_none() {
                // Not yet staged — skip until staging completes.
                continue;
            }
            self.upload_blob(blob).await;
        }
    }

    async fn upload_blob(&self, blob: BlobRow) {
        let local_path = match blob.local_path.as_ref() {
            Some(p) => PathBuf::from(p),
            None    => return,
        };
        let size_bytes = match blob.size_bytes {
            Some(s) => s as u64,
            None    => match tokio::fs::metadata(&local_path).await {
                Ok(m)  => m.len(),
                Err(e) => {
                    self.record_blob_error(&blob.id, &e.to_string());
                    return;
                }
            },
        };

        let result = if size_bytes < MULTIPART_THRESHOLD {
            self.upload_single(&blob, &local_path).await
        } else {
            self.upload_multipart(&blob, &local_path, size_bytes).await
        };

        match result {
            Ok(()) => {
                let conn = open_conn(&self.db_path);
                let _ = db::blobs::set_status(&conn, &blob.id, &BlobStatus::Uploaded);
                let _ = self.events_tx.send(SyncEvent::BlobUploaded { blob_id: blob.id.clone() });
            }
            Err(e) => {
                warn!("blob upload failed for {}: {e}", blob.id);
                self.record_blob_error(&blob.id, &e);
            }
        }
    }

    async fn upload_single(&self, blob: &BlobRow, path: &Path) -> Result<(), String> {
        let data = tokio::fs::read(path).await
            .map_err(|e| e.to_string())?;
        self.backend.put_object(&blob.s3_key, data).await
            .map_err(|e| e.to_string())
    }

    async fn upload_multipart(
        &self,
        blob: &BlobRow,
        path: &Path,
        size_bytes: u64,
    ) -> Result<(), String> {
        // Determine or create the multipart upload session.
        let upload_id = match &blob.upload_id {
            Some(id) => id.clone(),
            None => {
                let id = self.backend
                    .create_multipart_upload(&blob.s3_key)
                    .await
                    .map_err(|e| e.to_string())?;
                let conn = open_conn(&self.db_path);
                let _ = db::blobs::set_upload_id(&conn, &blob.id, &id);
                id
            }
        };

        // Ask S3 which parts are already uploaded (for resume).
        let remote_parts = self.backend
            .list_parts(&blob.s3_key, &upload_id)
            .await
            .map_err(|e| e.to_string())?;

        // If list_parts returned empty for an existing upload_id, the session expired.
        if remote_parts.is_empty() && blob.upload_id.is_some() {
            let conn = open_conn(&self.db_path);
            let _ = db::blobs::clear_upload_id(&conn, &blob.id);
            let _ = db::blobs::clear_parts(&conn, &blob.id);
            return Err("multipart upload expired; will restart on next cycle".into());
        }

        // Sync remote parts to local DB.
        {
            let conn = open_conn(&self.db_path);
            for p in &remote_parts {
                let _ = db::blobs::upsert_part(&conn, &blob.id, p.part_number, &p.etag);
            }
        }

        let uploaded_part_numbers: std::collections::HashSet<i32> =
            remote_parts.iter().map(|p| p.part_number).collect();

        // Upload missing parts.
        let total_parts =
            ((size_bytes + CHUNK_SIZE - 1) / CHUNK_SIZE) as i32;
        let mut all_parts = remote_parts;

        let mut file = tokio::fs::File::open(path).await
            .map_err(|e| e.to_string())?;

        for part_number in 1..=total_parts {
            if uploaded_part_numbers.contains(&part_number) {
                continue; // already uploaded
            }

            let offset = (part_number as u64 - 1) * CHUNK_SIZE;
            file.seek(std::io::SeekFrom::Start(offset)).await
                .map_err(|e| e.to_string())?;

            let remaining = size_bytes - offset;
            let chunk_len = remaining.min(CHUNK_SIZE) as usize;
            let mut chunk = vec![0u8; chunk_len];
            file.read_exact(&mut chunk).await
                .map_err(|e| e.to_string())?;

            let etag = self.backend
                .upload_part(&blob.s3_key, &upload_id, part_number, chunk)
                .await
                .map_err(|e| e.to_string())?;

            let conn = open_conn(&self.db_path);
            let _ = db::blobs::upsert_part(&conn, &blob.id, part_number, &etag);
            all_parts.push(UploadedPart { part_number, etag });
        }

        // Complete the upload.
        all_parts.sort_by_key(|p| p.part_number);
        self.backend
            .complete_multipart_upload(&blob.s3_key, &upload_id, all_parts)
            .await
            .map_err(|e| e.to_string())
    }

    // ── Download cycle ────────────────────────────────────────────────────────

    async fn run_download_cycle(&self) {
        let blobs = {
            let conn = open_conn(&self.db_path);
            match db::blobs::list_ready(&conn, &["download_pending"]) {
                Ok(b)  => b,
                Err(_) => return,
            }
        };
        for blob in blobs {
            self.handle_download(blob.id).await;
        }
    }

    async fn handle_download(&self, blob_id: BlobId) {
        let blob = {
            let conn = open_conn(&self.db_path);
            match db::blobs::get(&conn, &blob_id) {
                Ok(Some(b)) => b,
                _           => return,
            }
        };

        let dest = self.cache_dir.join(&blob_id);
        match self.backend.get_object(&blob.s3_key, &dest).await {
            Ok(size) => {
                let conn = open_conn(&self.db_path);
                let _ = db::blobs::set_local_path_and_size(
                    &conn, &blob_id, dest.to_str().unwrap_or(""), size,
                );
                let _ = db::blobs::set_status(&conn, &blob_id, &BlobStatus::Cached);
                let _ = self.events_tx.send(SyncEvent::BlobDownloaded { blob_id });
            }
            Err(e) => {
                warn!("blob download failed for {blob_id}: {e}");
                self.record_blob_error(&blob_id, &e.to_string());
            }
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn record_blob_error(&self, blob_id: &str, error: &str) {
        let conn = open_conn(&self.db_path);
        let _ = db::blobs::mark_retry(&conn, blob_id, error);
        if let Ok(Some(b)) = db::blobs::get(&conn, blob_id) {
            let _ = self.events_tx.send(SyncEvent::BlobRetryScheduled {
                blob_id:       blob_id.to_string(),
                retries:       b.retries,
                next_retry_ms: now_ms() as u64,
                error:         error.to_string(),
            });
        }
    }
}

// ── Utility ───────────────────────────────────────────────────────────────────

/// Open a database connection for the blob worker. Panics on failure (unrecoverable).
fn open_conn(db_path: &Path) -> Connection {
    let conn = Connection::open(db_path).expect("blob worker: open DB");
    db::schema::initialize(&conn).expect("blob worker: schema init");
    conn
}
