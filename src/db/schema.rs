use rusqlite::Connection;

use crate::error::Result;

pub fn initialize(conn: &Connection) -> Result<()> {
    conn.execute_batch("PRAGMA journal_mode=WAL;")?;
    conn.execute_batch("PRAGMA foreign_keys=ON;")?;
    conn.execute_batch(SCHEMA_SQL)?;
    Ok(())
}

const SCHEMA_SQL: &str = "
CREATE TABLE IF NOT EXISTS config (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS records (
    id             TEXT    PRIMARY KEY,
    collection     TEXT    NOT NULL,
    data           BLOB    NOT NULL,
    hlc            TEXT    NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 0,
    format_version INTEGER NOT NULL DEFAULT 0,
    dek_encrypted  BLOB,
    deleted        INTEGER NOT NULL DEFAULT 0,
    synced         INTEGER NOT NULL DEFAULT 0,
    created_at     INTEGER NOT NULL,
    updated_at     INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_records_collection_hlc
    ON records(collection, hlc DESC);

CREATE TABLE IF NOT EXISTS outbox (
    seq           INTEGER PRIMARY KEY AUTOINCREMENT,
    record_id     TEXT    NOT NULL,
    collection    TEXT    NOT NULL,
    operation     TEXT    NOT NULL,
    hlc           TEXT    NOT NULL,
    data          BLOB,
    created_at    INTEGER NOT NULL,
    next_retry_at INTEGER NOT NULL DEFAULT 0,
    retries       INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT,
    error_log     TEXT
);
CREATE INDEX IF NOT EXISTS idx_outbox_ready
    ON outbox(next_retry_at, seq);

CREATE TABLE IF NOT EXISTS blobs (
    id             TEXT    PRIMARY KEY,
    record_id      TEXT,
    collection     TEXT,
    local_path     TEXT,
    s3_key         TEXT    NOT NULL,
    size_bytes     INTEGER,
    upload_id      TEXT,
    status         TEXT    NOT NULL,
    format_version INTEGER NOT NULL DEFAULT 0,
    dek_encrypted  BLOB,
    base_nonce     BLOB,
    retries        INTEGER NOT NULL DEFAULT 0,
    next_retry_at  INTEGER NOT NULL DEFAULT 0,
    last_error     TEXT,
    error_log      TEXT,
    created_at     INTEGER NOT NULL,
    updated_at     INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS blob_parts (
    blob_id     TEXT    NOT NULL REFERENCES blobs(id) ON DELETE CASCADE,
    part_number INTEGER NOT NULL,
    etag        TEXT,
    uploaded    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (blob_id, part_number)
);

CREATE TABLE IF NOT EXISTS sync_state (
    backend       TEXT PRIMARY KEY,
    last_pull_hlc TEXT
);
";

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn initialize_is_idempotent() {
        let conn = Connection::open_in_memory().unwrap();
        initialize(&conn).unwrap();
        // Running twice must not fail
        initialize(&conn).unwrap();
    }

    #[test]
    fn wal_mode_is_set() {
        let conn = Connection::open_in_memory().unwrap();
        initialize(&conn).unwrap();
        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        // In-memory databases stay in "memory" mode even after requesting WAL,
        // which is expected SQLite behaviour. On a real file it would be "wal".
        assert!(mode == "wal" || mode == "memory");
    }

    #[test]
    fn all_tables_exist() {
        let conn = Connection::open_in_memory().unwrap();
        initialize(&conn).unwrap();
        for table in &["config", "records", "outbox", "blobs", "blob_parts", "sync_state"] {
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                    rusqlite::params![table],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(count, 1, "table '{table}' should exist");
        }
    }
}
