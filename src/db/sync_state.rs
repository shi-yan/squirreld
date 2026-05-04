use rusqlite::{Connection, OptionalExtension};

use crate::error::Result;

/// Return the last-pull HLC checkpoint for the given backend, or None if no
/// pull has completed yet.
pub fn get_checkpoint(conn: &Connection, backend_id: &str) -> Result<Option<String>> {
    conn.query_row(
        "SELECT last_pull_hlc FROM sync_state WHERE backend = ?1",
        rusqlite::params![backend_id],
        |r| r.get(0),
    )
    .optional()
    .map(|o| o.flatten())
    .map_err(Into::into)
}

/// Persist the last-pull HLC checkpoint for the given backend.
pub fn set_checkpoint(conn: &Connection, backend_id: &str, hlc: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO sync_state (backend, last_pull_hlc)
         VALUES (?1, ?2)
         ON CONFLICT(backend) DO UPDATE SET last_pull_hlc = excluded.last_pull_hlc",
        rusqlite::params![backend_id, hlc],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema;
    use rusqlite::Connection;

    fn mem() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        schema::initialize(&conn).unwrap();
        conn
    }

    #[test]
    fn get_checkpoint_returns_none_initially() {
        let conn = mem();
        assert!(get_checkpoint(&conn, "dynamodb").unwrap().is_none());
    }

    #[test]
    fn set_then_get_roundtrip() {
        let conn = mem();
        set_checkpoint(&conn, "dynamodb", "0000000000001-0000-aabbcc112233").unwrap();
        assert_eq!(
            get_checkpoint(&conn, "dynamodb").unwrap().as_deref(),
            Some("0000000000001-0000-aabbcc112233")
        );
    }

    #[test]
    fn set_checkpoint_is_upsert() {
        let conn = mem();
        set_checkpoint(&conn, "dynamodb", "0000000000001-0000-aabbcc112233").unwrap();
        set_checkpoint(&conn, "dynamodb", "0000000000002-0000-aabbcc112233").unwrap();
        assert_eq!(
            get_checkpoint(&conn, "dynamodb").unwrap().as_deref(),
            Some("0000000000002-0000-aabbcc112233")
        );
    }

    #[test]
    fn multiple_backends_are_independent() {
        let conn = mem();
        set_checkpoint(&conn, "backend-a", "0000000000001-0000-aabbcc112233").unwrap();
        set_checkpoint(&conn, "backend-b", "0000000000002-0000-aabbcc112233").unwrap();
        assert_eq!(
            get_checkpoint(&conn, "backend-a").unwrap().as_deref(),
            Some("0000000000001-0000-aabbcc112233")
        );
        assert_eq!(
            get_checkpoint(&conn, "backend-b").unwrap().as_deref(),
            Some("0000000000002-0000-aabbcc112233")
        );
    }
}
