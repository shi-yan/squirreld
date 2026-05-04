use rusqlite::{Connection, OptionalExtension};

use crate::{
    error::Result,
    types::{BlobStatus, now_ms},
};

// ── Row types ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub(crate) struct BlobRow {
    pub id:             String,
    pub record_id:      Option<String>,
    pub collection:     Option<String>,
    pub local_path:     Option<String>,
    pub s3_key:         String,
    pub size_bytes:     Option<i64>,
    pub upload_id:      Option<String>,
    pub status:         String,
    pub format_version: u8,
    pub retries:        u32,
    pub next_retry_at:  i64,
    pub last_error:     Option<String>,
    pub error_log:      Option<String>,
    pub created_at:     i64,
    pub updated_at:     i64,
}

#[derive(Debug, Clone)]
pub(crate) struct BlobPartRow {
    pub blob_id:     String,
    pub part_number: i32,
    pub etag:        Option<String>,
    pub uploaded:    bool,
}

// ── Blob CRUD ─────────────────────────────────────────────────────────────────

pub fn insert(conn: &Connection, row: &BlobRow) -> Result<()> {
    conn.execute(
        "INSERT INTO blobs
             (id, record_id, collection, local_path, s3_key, size_bytes, upload_id,
              status, format_version, retries, next_retry_at, created_at, updated_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,0,0,?10,?11)",
        rusqlite::params![
            &row.id,
            &row.record_id,
            &row.collection,
            &row.local_path,
            &row.s3_key,
            row.size_bytes,
            &row.upload_id,
            &row.status,
            row.format_version as i64,
            row.created_at,
            row.updated_at,
        ],
    )?;
    Ok(())
}

pub fn get(conn: &Connection, id: &str) -> Result<Option<BlobRow>> {
    conn.query_row(
        "SELECT id, record_id, collection, local_path, s3_key, size_bytes, upload_id,
                status, format_version, retries, next_retry_at, last_error, error_log,
                created_at, updated_at
         FROM blobs WHERE id = ?1",
        rusqlite::params![id],
        from_row,
    )
    .optional()
    .map_err(Into::into)
}

/// List blobs ready for upload/download (next_retry_at <= now, status in list).
pub fn list_ready(conn: &Connection, statuses: &[&str]) -> Result<Vec<BlobRow>> {
    if statuses.is_empty() { return Ok(vec![]); }
    let placeholders = statuses.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT id, record_id, collection, local_path, s3_key, size_bytes, upload_id,
                status, format_version, retries, next_retry_at, last_error, error_log,
                created_at, updated_at
         FROM blobs
         WHERE status IN ({placeholders}) AND next_retry_at <= ?{}
         ORDER BY created_at ASC",
        statuses.len() + 1
    );
    let now = now_ms();
    let mut params: Vec<rusqlite::types::Value> = statuses
        .iter()
        .map(|s| rusqlite::types::Value::Text((*s).to_string()))
        .collect();
    params.push(rusqlite::types::Value::Integer(now));

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params), from_row)?;
    rows.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
}

pub fn set_status(conn: &Connection, id: &str, status: &BlobStatus) -> Result<()> {
    conn.execute(
        "UPDATE blobs SET status = ?1, updated_at = ?2 WHERE id = ?3",
        rusqlite::params![status.as_str(), now_ms(), id],
    )?;
    Ok(())
}

pub fn set_local_path_and_size(
    conn: &Connection,
    id: &str,
    local_path: &str,
    size_bytes: u64,
) -> Result<()> {
    conn.execute(
        "UPDATE blobs SET local_path = ?1, size_bytes = ?2, updated_at = ?3 WHERE id = ?4",
        rusqlite::params![local_path, size_bytes as i64, now_ms(), id],
    )?;
    Ok(())
}

pub fn set_upload_id(conn: &Connection, id: &str, upload_id: &str) -> Result<()> {
    conn.execute(
        "UPDATE blobs SET upload_id = ?1, status = 'uploading', updated_at = ?2 WHERE id = ?3",
        rusqlite::params![upload_id, now_ms(), id],
    )?;
    Ok(())
}

pub fn clear_upload_id(conn: &Connection, id: &str) -> Result<()> {
    conn.execute(
        "UPDATE blobs SET upload_id = NULL, status = 'pending', retries = 0,
                 next_retry_at = 0, updated_at = ?1 WHERE id = ?2",
        rusqlite::params![now_ms(), id],
    )?;
    Ok(())
}

/// Exponential backoff retry, capped at 5 minutes. Same scheme as outbox.
pub fn mark_retry(conn: &Connection, id: &str, error: &str) -> Result<()> {
    let (retries, existing_log): (i64, Option<String>) = conn.query_row(
        "SELECT retries, error_log FROM blobs WHERE id = ?1",
        rusqlite::params![id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    ).optional()?.unwrap_or((0, None));

    let new_retries = retries + 1;
    let backoff_ms = 1000i64
        .saturating_mul(2i64.saturating_pow(retries as u32))
        .min(300_000);
    let next_retry_at = now_ms() + backoff_ms;
    let new_log = append_error_log(existing_log.as_deref(), error);

    conn.execute(
        "UPDATE blobs
         SET retries = ?1, next_retry_at = ?2, last_error = ?3, error_log = ?4,
             updated_at = ?5
         WHERE id = ?6",
        rusqlite::params![new_retries, next_retry_at, error, new_log, now_ms(), id],
    )?;
    Ok(())
}

// ── Blob parts CRUD ───────────────────────────────────────────────────────────

pub fn upsert_part(
    conn: &Connection,
    blob_id: &str,
    part_number: i32,
    etag: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO blob_parts (blob_id, part_number, etag, uploaded)
         VALUES (?1, ?2, ?3, 1)
         ON CONFLICT(blob_id, part_number) DO UPDATE SET etag = excluded.etag, uploaded = 1",
        rusqlite::params![blob_id, part_number, etag],
    )?;
    Ok(())
}

pub fn list_parts(conn: &Connection, blob_id: &str) -> Result<Vec<BlobPartRow>> {
    let mut stmt = conn.prepare(
        "SELECT blob_id, part_number, etag, uploaded FROM blob_parts
         WHERE blob_id = ?1 ORDER BY part_number ASC",
    )?;
    let rows = stmt.query_map(rusqlite::params![blob_id], |r| {
        Ok(BlobPartRow {
            blob_id:     r.get(0)?,
            part_number: r.get(1)?,
            etag:        r.get(2)?,
            uploaded:    r.get::<_, i64>(3)? != 0,
        })
    })?;
    rows.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
}

pub fn clear_parts(conn: &Connection, blob_id: &str) -> Result<()> {
    conn.execute(
        "DELETE FROM blob_parts WHERE blob_id = ?1",
        rusqlite::params![blob_id],
    )?;
    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<BlobRow> {
    Ok(BlobRow {
        id:             r.get(0)?,
        record_id:      r.get(1)?,
        collection:     r.get(2)?,
        local_path:     r.get(3)?,
        s3_key:         r.get(4)?,
        size_bytes:     r.get(5)?,
        upload_id:      r.get(6)?,
        status:         r.get(7)?,
        format_version: r.get::<_, i64>(8)? as u8,
        retries:        r.get::<_, i64>(9)? as u32,
        next_retry_at:  r.get(10)?,
        last_error:     r.get(11)?,
        error_log:      r.get(12)?,
        created_at:     r.get(13)?,
        updated_at:     r.get(14)?,
    })
}

fn append_error_log(existing: Option<&str>, error: &str) -> String {
    let entry = serde_json::json!({ "ts": now_ms(), "error": error });
    let mut log: Vec<serde_json::Value> = existing
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default();
    log.push(entry);
    if log.len() > 20 { log.drain(..log.len() - 20); }
    serde_json::to_string(&log).unwrap_or_else(|_| "[]".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema;
    use rusqlite::Connection;

    fn mem() -> Connection {
        let c = Connection::open_in_memory().unwrap();
        schema::initialize(&c).unwrap();
        c
    }

    fn sample_row(id: &str) -> BlobRow {
        BlobRow {
            id:             id.into(),
            record_id:      None,
            collection:     None,
            local_path:     Some("/tmp/test".into()),
            s3_key:         format!("blobs/{id}"),
            size_bytes:     Some(1024),
            upload_id:      None,
            status:         "pending".into(),
            format_version: 0,
            retries:        0,
            next_retry_at:  0,
            last_error:     None,
            error_log:      None,
            created_at:     1_000_000,
            updated_at:     1_000_000,
        }
    }

    #[test]
    fn insert_then_get_roundtrip() {
        let conn = mem();
        insert(&conn, &sample_row("b1")).unwrap();
        let got = get(&conn, "b1").unwrap().unwrap();
        assert_eq!(got.id, "b1");
        assert_eq!(got.status, "pending");
    }

    #[test]
    fn set_status_updates_row() {
        let conn = mem();
        insert(&conn, &sample_row("b1")).unwrap();
        set_status(&conn, "b1", &BlobStatus::Uploaded).unwrap();
        let got = get(&conn, "b1").unwrap().unwrap();
        assert_eq!(got.status, "uploaded");
    }

    #[test]
    fn list_ready_filters_by_status() {
        let conn = mem();
        insert(&conn, &sample_row("b1")).unwrap();
        let mut row2 = sample_row("b2");
        row2.status = "uploaded".into();
        insert(&conn, &row2).unwrap();

        let pending = list_ready(&conn, &["pending"]).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, "b1");
    }

    #[test]
    fn mark_retry_increments_and_sets_backoff() {
        let conn = mem();
        insert(&conn, &sample_row("b1")).unwrap();
        mark_retry(&conn, "b1", "network error").unwrap();
        let got = get(&conn, "b1").unwrap().unwrap();
        assert_eq!(got.retries, 1);
        assert!(got.next_retry_at >= now_ms() + 900);
    }

    #[test]
    fn part_upsert_and_list() {
        let conn = mem();
        insert(&conn, &sample_row("b1")).unwrap();
        upsert_part(&conn, "b1", 1, "etag1").unwrap();
        upsert_part(&conn, "b1", 2, "etag2").unwrap();
        let parts = list_parts(&conn, "b1").unwrap();
        assert_eq!(parts.len(), 2);
        assert!(parts.iter().all(|p| p.uploaded));
    }

    #[test]
    fn clear_parts_removes_all() {
        let conn = mem();
        insert(&conn, &sample_row("b1")).unwrap();
        upsert_part(&conn, "b1", 1, "e1").unwrap();
        clear_parts(&conn, "b1").unwrap();
        assert!(list_parts(&conn, "b1").unwrap().is_empty());
    }
}
