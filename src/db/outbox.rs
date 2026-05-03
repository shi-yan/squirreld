use rusqlite::{Connection, OptionalExtension};

use crate::{error::Result, types::now_ms};

#[allow(dead_code)] // all fields used in Phase 2 sync loop
pub(crate) struct OutboxEntry {
    pub seq: i64,
    pub record_id: String,
    pub collection: String,
    pub operation: String,
    pub hlc: String,
    pub data: Option<Vec<u8>>,
    pub created_at: i64,
    pub next_retry_at: i64,
    pub retries: u32,
    pub last_error: Option<String>,
    pub error_log: Option<String>,
}

/// Append a new entry to the outbox. Must be called inside the same transaction as the
/// corresponding record write.
pub fn append(
    conn: &Connection,
    record_id: &str,
    collection: &str,
    operation: &str,
    hlc: &str,
    data: Option<&[u8]>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO outbox (record_id, collection, operation, hlc, data, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        rusqlite::params![record_id, collection, operation, hlc, data, now_ms()],
    )?;
    Ok(())
}

/// Return up to `limit` outbox entries that are ready to push (next_retry_at <= now),
#[allow(dead_code)] // used in Phase 2 push cycle
/// in strict FIFO order.
pub fn peek_batch(conn: &Connection, limit: usize) -> Result<Vec<OutboxEntry>> {
    let now = now_ms();
    let mut stmt = conn.prepare(
        "SELECT seq, record_id, collection, operation, hlc, data,
                created_at, next_retry_at, retries, last_error, error_log
         FROM outbox
         WHERE next_retry_at <= ?1
         ORDER BY seq ASC
         LIMIT ?2",
    )?;
    let rows = stmt.query_map(rusqlite::params![now, limit as i64], from_row)?;
    rows.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
}

/// Remove successfully pushed entries from the outbox.
#[allow(dead_code)] // used in Phase 2 push cycle
pub fn delete_seqs(conn: &Connection, seqs: &[i64]) -> Result<()> {
    if seqs.is_empty() {
        return Ok(());
    }
    let placeholders = seqs.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = format!("DELETE FROM outbox WHERE seq IN ({placeholders})");
    let params: Vec<rusqlite::types::Value> = seqs
        .iter()
        .map(|&s| rusqlite::types::Value::Integer(s))
        .collect();
    conn.execute(&sql, rusqlite::params_from_iter(params))?;
    Ok(())
}

/// Record a failed push attempt. Increments retries, applies exponential backoff
#[allow(dead_code)] // used in Phase 2 push cycle
/// (1s * 2^retries, capped at 5 minutes), and appends to the error log.
pub fn mark_retry(conn: &Connection, seq: i64, error: &str) -> Result<()> {
    let entry = conn.query_row(
        "SELECT retries, error_log FROM outbox WHERE seq = ?1",
        rusqlite::params![seq],
        |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Option<String>>(1)?)),
    )
    .optional()?;

    let (retries, existing_log) = match entry {
        None => return Ok(()), // entry was already deleted — nothing to do
        Some(v) => v,
    };

    let new_retries = retries + 1;
    let backoff_ms = 1000i64
        .saturating_mul(2i64.saturating_pow(retries as u32))
        .min(300_000); // cap at 5 minutes
    let next_retry_at = now_ms() + backoff_ms;
    let new_log = append_to_error_log(existing_log.as_deref(), error);

    conn.execute(
        "UPDATE outbox
         SET retries = ?1, next_retry_at = ?2, last_error = ?3, error_log = ?4
         WHERE seq = ?5",
        rusqlite::params![new_retries, next_retry_at, error, new_log, seq],
    )?;
    Ok(())
}

/// Returns all outbox entries that have had at least one failed attempt.
pub fn list_pending_errors(conn: &Connection) -> Result<Vec<OutboxEntry>> {
    let mut stmt = conn.prepare(
        "SELECT seq, record_id, collection, operation, hlc, data,
                created_at, next_retry_at, retries, last_error, error_log
         FROM outbox
         WHERE retries > 0
         ORDER BY seq ASC",
    )?;
    let rows = stmt.query_map([], from_row)?;
    rows.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
}

/// Manually discard a stuck outbox entry (user-initiated). The record itself is unaffected.
pub fn clear_error(conn: &Connection, seq: i64) -> Result<()> {
    conn.execute("DELETE FROM outbox WHERE seq = ?1", rusqlite::params![seq])?;
    Ok(())
}

#[allow(dead_code)]
fn append_to_error_log(existing: Option<&str>, error: &str) -> String {
    let entry = serde_json::json!({ "ts": now_ms(), "error": error });
    let mut log: Vec<serde_json::Value> = existing
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default();
    log.push(entry);
    if log.len() > 20 {
        log.drain(..log.len() - 20);
    }
    serde_json::to_string(&log).unwrap_or_else(|_| "[]".into())
}

fn from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<OutboxEntry> {
    Ok(OutboxEntry {
        seq:          r.get(0)?,
        record_id:    r.get(1)?,
        collection:   r.get(2)?,
        operation:    r.get(3)?,
        hlc:          r.get(4)?,
        data:         r.get(5)?,
        created_at:   r.get(6)?,
        next_retry_at: r.get(7)?,
        retries:      r.get::<_, i64>(8)? as u32,
        last_error:   r.get(9)?,
        error_log:    r.get(10)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema;

    fn mem_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        schema::initialize(&conn).unwrap();
        conn
    }

    fn insert_one(conn: &Connection) -> i64 {
        append(conn, "rec1", "papers", "upsert", "0000000000001-0000-aabbcc112233", Some(b"data")).unwrap();
        conn.last_insert_rowid()
    }

    #[test]
    fn append_creates_entry_ready_immediately() {
        let conn = mem_conn();
        let seq = insert_one(&conn);
        let batch = peek_batch(&conn, 10).unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].seq, seq);
        assert_eq!(batch[0].retries, 0);
    }

    #[test]
    fn delete_seqs_removes_entries() {
        let conn = mem_conn();
        let seq = insert_one(&conn);
        delete_seqs(&conn, &[seq]).unwrap();
        assert!(peek_batch(&conn, 10).unwrap().is_empty());
    }

    #[test]
    fn delete_seqs_with_empty_slice_is_noop() {
        let conn = mem_conn();
        insert_one(&conn);
        delete_seqs(&conn, &[]).unwrap();
        assert_eq!(peek_batch(&conn, 10).unwrap().len(), 1);
    }

    #[test]
    fn mark_retry_increments_retries() {
        let conn = mem_conn();
        let seq = insert_one(&conn);
        mark_retry(&conn, seq, "network error").unwrap();
        let _batch = peek_batch(&conn, 10).unwrap(); // future entry — may be 0 due to backoff
        // Check via direct query instead
        let retries: i64 = conn
            .query_row("SELECT retries FROM outbox WHERE seq = ?1", rusqlite::params![seq], |r| r.get(0))
            .unwrap();
        assert_eq!(retries, 1);
    }

    #[test]
    fn mark_retry_sets_backoff() {
        let conn = mem_conn();
        let seq = insert_one(&conn);
        let before = now_ms();
        mark_retry(&conn, seq, "err").unwrap();
        let after = now_ms();

        let next_retry: i64 = conn
            .query_row("SELECT next_retry_at FROM outbox WHERE seq = ?1", rusqlite::params![seq], |r| r.get(0))
            .unwrap();
        // backoff for retries=0: 1000ms * 2^0 = 1000ms
        assert!(next_retry >= before + 1000, "next_retry_at should be at least 1s in the future");
        assert!(next_retry <= after + 1100, "next_retry_at should not be too far in the future");
    }

    #[test]
    fn mark_retry_backoff_is_capped_at_5_minutes() {
        let conn = mem_conn();
        let seq = insert_one(&conn);
        // Force retries to a high value by calling mark_retry many times.
        // After 9 retries, 1000 * 2^9 = 512_000ms > 300_000ms cap.
        for _ in 0..10 {
            mark_retry(&conn, seq, "err").unwrap();
        }
        let next_retry: i64 = conn
            .query_row("SELECT next_retry_at FROM outbox WHERE seq = ?1", rusqlite::params![seq], |r| r.get(0))
            .unwrap();
        assert!(
            next_retry <= now_ms() + 300_100,
            "backoff must not exceed 5 minutes ({next_retry} > {})",
            now_ms() + 300_000
        );
    }

    #[test]
    fn error_log_accumulates_entries() {
        let conn = mem_conn();
        let seq = insert_one(&conn);
        mark_retry(&conn, seq, "error 1").unwrap();
        mark_retry(&conn, seq, "error 2").unwrap();
        let log_str: String = conn
            .query_row("SELECT error_log FROM outbox WHERE seq = ?1", rusqlite::params![seq], |r| r.get(0))
            .unwrap();
        let log: Vec<serde_json::Value> = serde_json::from_str(&log_str).unwrap();
        assert_eq!(log.len(), 2);
        assert_eq!(log[0]["error"], "error 1");
        assert_eq!(log[1]["error"], "error 2");
    }

    #[test]
    fn error_log_is_capped_at_20_entries() {
        let conn = mem_conn();
        let seq = insert_one(&conn);
        for i in 0..25 {
            mark_retry(&conn, seq, &format!("err {i}")).unwrap();
        }
        let log_str: String = conn
            .query_row("SELECT error_log FROM outbox WHERE seq = ?1", rusqlite::params![seq], |r| r.get(0))
            .unwrap();
        let log: Vec<serde_json::Value> = serde_json::from_str(&log_str).unwrap();
        assert_eq!(log.len(), 20, "error_log must be capped at 20 entries");
        // Most recent errors should be kept (oldest are dropped).
        assert_eq!(log[19]["error"], "err 24");
    }

    #[test]
    fn peek_batch_respects_next_retry_at() {
        let conn = mem_conn();
        let seq = insert_one(&conn);
        // Schedule the entry far in the future
        conn.execute(
            "UPDATE outbox SET next_retry_at = ?1 WHERE seq = ?2",
            rusqlite::params![now_ms() + 1_000_000, seq],
        )
        .unwrap();
        assert!(peek_batch(&conn, 10).unwrap().is_empty(), "future entry must not be returned");
    }

    #[test]
    fn list_pending_errors_requires_at_least_one_retry() {
        let conn = mem_conn();
        let seq = insert_one(&conn);
        assert!(list_pending_errors(&conn).unwrap().is_empty(), "fresh entry has no retries");
        mark_retry(&conn, seq, "err").unwrap();
        assert_eq!(list_pending_errors(&conn).unwrap().len(), 1);
    }

    #[test]
    fn clear_error_removes_entry() {
        let conn = mem_conn();
        let seq = insert_one(&conn);
        mark_retry(&conn, seq, "err").unwrap();
        clear_error(&conn, seq).unwrap();
        assert!(list_pending_errors(&conn).unwrap().is_empty());
    }
}
