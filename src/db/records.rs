use rusqlite::{Connection, OptionalExtension};

use crate::error::Result;

/// Internal row representation. Not exposed in the public API.
pub(crate) struct RecordRow {
    pub id: String,
    pub collection: String,
    pub data: Vec<u8>,
    pub hlc: String,
    pub schema_version: u32,
    pub format_version: u8,
    pub dek_encrypted: Option<Vec<u8>>,
    pub deleted: bool,
    #[allow(dead_code)] // used in Phase 2 sync loop and Phase 5 encryption
    pub synced: bool,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Insert or update a record. `created_at` is preserved on update (not overwritten).
pub fn upsert(conn: &Connection, row: &RecordRow) -> Result<()> {
    conn.execute(
        "INSERT INTO records
             (id, collection, data, hlc, schema_version, format_version,
              dek_encrypted, deleted, synced, created_at, updated_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,0,0,?8,?9)
         ON CONFLICT(id) DO UPDATE SET
             collection     = excluded.collection,
             data           = excluded.data,
             hlc            = excluded.hlc,
             schema_version = excluded.schema_version,
             format_version = excluded.format_version,
             dek_encrypted  = excluded.dek_encrypted,
             deleted        = 0,
             synced         = 0,
             updated_at     = excluded.updated_at",
        rusqlite::params![
            &row.id,
            &row.collection,
            &row.data,
            &row.hlc,
            row.schema_version as i64,
            row.format_version as i64,
            &row.dek_encrypted,
            row.created_at,
            row.updated_at,
        ],
    )?;
    Ok(())
}

pub fn get(conn: &Connection, collection: &str, id: &str) -> Result<Option<RecordRow>> {
    conn.query_row(
        "SELECT id, collection, data, hlc, schema_version, format_version,
                dek_encrypted, deleted, synced, created_at, updated_at
         FROM records
         WHERE id = ?1 AND collection = ?2",
        rusqlite::params![id, collection],
        from_row,
    )
    .optional()
    .map_err(Into::into)
}

/// Set the deleted tombstone and advance the HLC. Does not physically remove the row.
pub fn soft_delete(conn: &Connection, collection: &str, id: &str, hlc: &str, now: i64) -> Result<()> {
    conn.execute(
        "UPDATE records
         SET deleted = 1, hlc = ?1, synced = 0, updated_at = ?2
         WHERE id = ?3 AND collection = ?4",
        rusqlite::params![hlc, now, id, collection],
    )?;
    Ok(())
}

pub fn list(
    conn: &Connection,
    collection: &str,
    limit: Option<usize>,
    offset: usize,
    include_deleted: bool,
    asc: bool,
) -> Result<Vec<RecordRow>> {
    let order = if asc { "ASC" } else { "DESC" };
    let deleted_clause = if include_deleted { "" } else { " AND deleted = 0" };
    let sql = format!(
        "SELECT id, collection, data, hlc, schema_version, format_version,
                dek_encrypted, deleted, synced, created_at, updated_at
         FROM records
         WHERE collection = ?1{deleted_clause}
         ORDER BY hlc {order}
         LIMIT ?2 OFFSET ?3"
    );
    // SQLite treats LIMIT -1 as "no limit".
    let limit_val = limit.map(|l| l as i64).unwrap_or(-1);
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(
        rusqlite::params![collection, limit_val, offset as i64],
        from_row,
    )?;
    rows.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
}

/// Returns the lexicographically maximum HLC across all records, or None if the table is empty.
pub fn max_hlc(conn: &Connection) -> Result<Option<String>> {
    conn.query_row("SELECT MAX(hlc) FROM records", [], |r| r.get(0))
        .optional()
        .map(|o| o.flatten())
        .map_err(Into::into)
}

fn from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<RecordRow> {
    Ok(RecordRow {
        id:             r.get(0)?,
        collection:     r.get(1)?,
        data:           r.get(2)?,
        hlc:            r.get(3)?,
        schema_version: r.get::<_, i64>(4)? as u32,
        format_version: r.get::<_, i64>(5)? as u8,
        dek_encrypted:  r.get(6)?,
        deleted:        r.get::<_, i64>(7)? != 0,
        synced:         r.get::<_, i64>(8)? != 0,
        created_at:     r.get(9)?,
        updated_at:     r.get(10)?,
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

    fn make_row(id: &str, collection: &str, data: &[u8], hlc: &str) -> RecordRow {
        RecordRow {
            id: id.into(),
            collection: collection.into(),
            data: data.to_vec(),
            hlc: hlc.into(),
            schema_version: 0,
            format_version: 0,
            dek_encrypted: None,
            deleted: false,
            synced: false,
            created_at: 1_000_000,
            updated_at: 1_000_000,
        }
    }

    #[test]
    fn upsert_then_get_roundtrip() {
        let conn = mem_conn();
        let row = make_row("id1", "papers", b"hello", "0000000000001-0000-aabbcc112233");
        upsert(&conn, &row).unwrap();
        let got = get(&conn, "papers", "id1").unwrap().unwrap();
        assert_eq!(got.data, b"hello");
        assert_eq!(got.collection, "papers");
        assert!(!got.deleted);
    }

    #[test]
    fn upsert_updates_existing_record() {
        let conn = mem_conn();
        upsert(&conn, &make_row("id1", "papers", b"v1", "0000000000001-0000-aabbcc112233")).unwrap();
        upsert(&conn, &make_row("id1", "papers", b"v2", "0000000000002-0000-aabbcc112233")).unwrap();
        let got = get(&conn, "papers", "id1").unwrap().unwrap();
        assert_eq!(got.data, b"v2");
    }

    #[test]
    fn upsert_preserves_created_at_on_update() {
        let conn = mem_conn();
        let mut row = make_row("id1", "papers", b"v1", "0000000000001-0000-aabbcc112233");
        row.created_at = 42_000;
        upsert(&conn, &row).unwrap();

        // Second upsert passes a different created_at — should be ignored.
        let mut row2 = make_row("id1", "papers", b"v2", "0000000000002-0000-aabbcc112233");
        row2.created_at = 99_999;
        upsert(&conn, &row2).unwrap();

        let got = get(&conn, "papers", "id1").unwrap().unwrap();
        assert_eq!(got.created_at, 42_000, "created_at must not change on update");
    }

    #[test]
    fn get_returns_none_for_missing_id() {
        let conn = mem_conn();
        assert!(get(&conn, "papers", "no-such-id").unwrap().is_none());
    }

    #[test]
    fn get_returns_none_for_wrong_collection() {
        let conn = mem_conn();
        upsert(&conn, &make_row("id1", "papers", b"x", "0000000000001-0000-aabbcc112233")).unwrap();
        assert!(get(&conn, "notes", "id1").unwrap().is_none());
    }

    #[test]
    fn soft_delete_sets_tombstone() {
        let conn = mem_conn();
        upsert(&conn, &make_row("id1", "papers", b"x", "0000000000001-0000-aabbcc112233")).unwrap();
        soft_delete(&conn, "papers", "id1", "0000000000002-0000-aabbcc112233", 2_000_000).unwrap();
        let got = get(&conn, "papers", "id1").unwrap().unwrap();
        assert!(got.deleted);
    }

    #[test]
    fn list_excludes_deleted_by_default() {
        let conn = mem_conn();
        upsert(&conn, &make_row("id1", "papers", b"x", "0000000000001-0000-aabbcc112233")).unwrap();
        upsert(&conn, &make_row("id2", "papers", b"y", "0000000000002-0000-aabbcc112233")).unwrap();
        soft_delete(&conn, "papers", "id1", "0000000000003-0000-aabbcc112233", 3_000_000).unwrap();
        let rows = list(&conn, "papers", None, 0, false, false).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "id2");
    }

    #[test]
    fn list_includes_deleted_when_requested() {
        let conn = mem_conn();
        upsert(&conn, &make_row("id1", "papers", b"x", "0000000000001-0000-aabbcc112233")).unwrap();
        soft_delete(&conn, "papers", "id1", "0000000000002-0000-aabbcc112233", 2_000_000).unwrap();
        let rows = list(&conn, "papers", None, 0, true, false).unwrap();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].deleted);
    }

    #[test]
    fn list_orders_by_hlc_desc() {
        let conn = mem_conn();
        upsert(&conn, &make_row("id1", "papers", b"a", "0000000000001-0000-aabbcc112233")).unwrap();
        upsert(&conn, &make_row("id2", "papers", b"b", "0000000000003-0000-aabbcc112233")).unwrap();
        upsert(&conn, &make_row("id3", "papers", b"c", "0000000000002-0000-aabbcc112233")).unwrap();
        let rows = list(&conn, "papers", None, 0, false, false).unwrap();
        let hlcs: Vec<&str> = rows.iter().map(|r| r.hlc.as_str()).collect();
        assert_eq!(hlcs[0], "0000000000003-0000-aabbcc112233");
        assert_eq!(hlcs[1], "0000000000002-0000-aabbcc112233");
        assert_eq!(hlcs[2], "0000000000001-0000-aabbcc112233");
    }

    #[test]
    fn list_respects_limit_and_offset() {
        let conn = mem_conn();
        for i in 1u64..=5 {
            let hlc = format!("{i:013x}-0000-aabbcc112233");
            upsert(&conn, &make_row(&format!("id{i}"), "papers", b"x", &hlc)).unwrap();
        }
        let page1 = list(&conn, "papers", Some(2), 0, false, false).unwrap();
        let page2 = list(&conn, "papers", Some(2), 2, false, false).unwrap();
        assert_eq!(page1.len(), 2);
        assert_eq!(page2.len(), 2);
        assert_ne!(page1[0].id, page2[0].id);
    }

    #[test]
    fn max_hlc_returns_none_for_empty_table() {
        let conn = mem_conn();
        assert!(max_hlc(&conn).unwrap().is_none());
    }

    #[test]
    fn max_hlc_returns_highest_hlc() {
        let conn = mem_conn();
        upsert(&conn, &make_row("id1", "papers", b"x", "0000000000001-0000-aabbcc112233")).unwrap();
        upsert(&conn, &make_row("id2", "papers", b"y", "0000000000003-0000-aabbcc112233")).unwrap();
        upsert(&conn, &make_row("id3", "papers", b"z", "0000000000002-0000-aabbcc112233")).unwrap();
        assert_eq!(
            max_hlc(&conn).unwrap().as_deref(),
            Some("0000000000003-0000-aabbcc112233")
        );
    }
}
