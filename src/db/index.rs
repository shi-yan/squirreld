use std::collections::HashMap;

use rusqlite::{Connection, types::Value};

use crate::{
    error::{Result, SquirrelError},
    types::{IndexDef, IndexValue, QueryFilter, SortOrder},
};

use super::records::RecordRow;

// ── Identifier sanitisation ───────────────────────────────────────────────────

/// Returns `Ok(name)` if `name` is a valid SQL identifier, else `Err`.
pub(crate) fn sanitize_ident(name: &str) -> Result<&str> {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return Err(SquirrelError::Other(format!("invalid identifier: {name:?}"))),
    }
    if chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
        Ok(name)
    } else {
        Err(SquirrelError::Other(format!("invalid identifier: {name:?}")))
    }
}

// ── Table creation ────────────────────────────────────────────────────────────

/// Create the scalar shadow index table `sidx_{collection}`.
pub fn create_shadow_table(conn: &Connection, def: &IndexDef) -> Result<()> {
    let col = sanitize_ident(&def.collection)?;
    if def.fields.is_empty() {
        return Ok(());
    }
    let cols = def.fields.iter()
        .map(|f| {
            sanitize_ident(&f.name).map(|n| format!("{n} {}", f.affinity.as_sql_type()))
        })
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    conn.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS sidx_{col} (
             record_id TEXT PRIMARY KEY,
             {cols}
         )"
    ))?;
    Ok(())
}

/// Create the FTS5 virtual table `fts_{collection}`.
pub fn create_fts_table(conn: &Connection, def: &IndexDef) -> Result<()> {
    let col = sanitize_ident(&def.collection)?;
    if def.fts_fields.is_empty() {
        return Ok(());
    }
    let cols = def.fts_fields.iter()
        .map(|f| sanitize_ident(f).map(|n| n.to_string()))
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    conn.execute_batch(&format!(
        "CREATE VIRTUAL TABLE IF NOT EXISTS fts_{col} \
         USING fts5(record_id UNINDEXED, {cols}, tokenize=\"unicode61\")"
    ))?;
    Ok(())
}

// ── Row maintenance ───────────────────────────────────────────────────────────

/// Upsert the shadow index row for `record_id` using the caller-supplied field values.
pub fn upsert_index_row(
    conn: &Connection,
    def: &IndexDef,
    record_id: &str,
    values: &HashMap<String, IndexValue>,
) -> Result<()> {
    let col = sanitize_ident(&def.collection)?;
    if def.fields.is_empty() {
        return Ok(());
    }

    let cols: Vec<&str> = def.fields.iter()
        .map(|f| sanitize_ident(&f.name))
        .collect::<Result<_>>()?;

    let placeholders = cols.iter().enumerate()
        .map(|(i, _)| format!("?{}", i + 2))
        .collect::<Vec<_>>()
        .join(", ");

    let update_set = cols.iter().enumerate()
        .map(|(i, c)| format!("{c} = ?{}", i + 2))
        .collect::<Vec<_>>()
        .join(", ");

    let col_list = cols.join(", ");
    let sql = format!(
        "INSERT INTO sidx_{col} (record_id, {col_list}) VALUES (?1, {placeholders})
         ON CONFLICT(record_id) DO UPDATE SET {update_set}"
    );

    let mut params: Vec<Value> = vec![Value::Text(record_id.to_string())];
    for field in &def.fields {
        let v = match values.get(&field.name) {
            Some(iv) => to_sql_value(iv),
            None     => Value::Null,
        };
        params.push(v);
    }

    conn.execute(&sql, rusqlite::params_from_iter(params.iter()))?;
    Ok(())
}

/// Upsert the FTS row for `record_id`.
pub fn upsert_fts_row(
    conn: &Connection,
    def: &IndexDef,
    record_id: &str,
    values: &HashMap<String, IndexValue>,
) -> Result<()> {
    let col = sanitize_ident(&def.collection)?;
    if def.fts_fields.is_empty() {
        return Ok(());
    }

    // FTS5 does not support ON CONFLICT — delete then insert.
    conn.execute(
        &format!("DELETE FROM fts_{col} WHERE record_id = ?1"),
        rusqlite::params![record_id],
    )?;

    let cols: Vec<&str> = def.fts_fields.iter()
        .map(|f| sanitize_ident(f))
        .collect::<Result<_>>()?;

    let placeholders = (2..=cols.len() + 1)
        .map(|i| format!("?{i}"))
        .collect::<Vec<_>>()
        .join(", ");

    let col_list = cols.join(", ");
    let sql = format!(
        "INSERT INTO fts_{col} (record_id, {col_list}) VALUES (?1, {placeholders})"
    );

    let mut params: Vec<Value> = vec![Value::Text(record_id.to_string())];
    for field_name in &def.fts_fields {
        let v = match values.get(field_name) {
            Some(IndexValue::Text(s)) => Value::Text(s.clone()),
            Some(IndexValue::Integer(n)) => Value::Text(n.to_string()),
            _ => Value::Text(String::new()),
        };
        params.push(v);
    }

    conn.execute(&sql, rusqlite::params_from_iter(params.iter()))?;
    Ok(())
}

/// Remove the shadow index and FTS rows for `record_id` (called on delete).
pub fn delete_index_rows(conn: &Connection, def: &IndexDef, record_id: &str) -> Result<()> {
    let col = sanitize_ident(&def.collection)?;
    if !def.fields.is_empty() {
        conn.execute(
            &format!("DELETE FROM sidx_{col} WHERE record_id = ?1"),
            rusqlite::params![record_id],
        )?;
    }
    if !def.fts_fields.is_empty() {
        conn.execute(
            &format!("DELETE FROM fts_{col} WHERE record_id = ?1"),
            rusqlite::params![record_id],
        )?;
    }
    Ok(())
}

// ── Query ─────────────────────────────────────────────────────────────────────

pub struct QueryOpts {
    pub filter: Option<QueryFilter>,
    pub limit: Option<usize>,
    pub offset: usize,
    pub include_deleted: bool,
    pub order: SortOrder,
}

impl Default for QueryOpts {
    fn default() -> Self {
        Self {
            filter: None,
            limit: None,
            offset: 0,
            include_deleted: false,
            order: SortOrder::HlcDesc,
        }
    }
}

pub fn query(
    conn: &Connection,
    collection: &str,
    def: Option<&IndexDef>,
    opts: &QueryOpts,
) -> Result<Vec<RecordRow>> {
    let col = sanitize_ident(collection)?;

    // Determine which tables we need to join.
    let has_shadow = def.map(|d| !d.fields.is_empty()).unwrap_or(false);
    let has_fts    = def.map(|d| !d.fts_fields.is_empty()).unwrap_or(false);

    let mut params: Vec<Value> = Vec::new();

    // ?1 = collection
    params.push(Value::Text(collection.to_string()));

    // Build WHERE fragments for the filter.
    let filter_sql = if let Some(f) = &opts.filter {
        let clause = build_filter_sql(f, col, has_shadow, has_fts, &mut params)?;
        format!(" AND ({clause})")
    } else {
        String::new()
    };

    // ?N = limit, ?(N+1) = offset (appended last so filter params are contiguous).
    let limit_idx  = params.len() + 1;
    let offset_idx = params.len() + 2;
    params.push(Value::Integer(opts.limit.map(|l| l as i64).unwrap_or(-1)));
    params.push(Value::Integer(opts.offset as i64));

    let deleted_clause = if opts.include_deleted { "" } else { " AND r.deleted = 0" };
    let order = match opts.order { SortOrder::HlcAsc => "ASC", _ => "DESC" };

    let join_shadow = if has_shadow {
        format!("JOIN sidx_{col} s ON s.record_id = r.id")
    } else {
        String::new()
    };

    let sql = format!(
        "SELECT r.id, r.collection, r.data, r.hlc, r.schema_version, r.format_version, \
                r.dek_encrypted, r.deleted, r.synced, r.created_at, r.updated_at \
         FROM records r \
         {join_shadow} \
         WHERE r.collection = ?1{deleted_clause}{filter_sql} \
         ORDER BY r.hlc {order} \
         LIMIT ?{limit_idx} OFFSET ?{offset_idx}"
    );

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(
        rusqlite::params_from_iter(params.iter()),
        super::records::from_row_pub,
    )?;
    rows.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
}

// ── Filter → SQL ──────────────────────────────────────────────────────────────

/// Built-in columns that live on the `records` table (`r.`), not the shadow index.
/// These are always addressed with `r.` even when a shadow index exists.
const RECORD_COLUMNS: &[&str] = &[
    "id", "hlc", "schema_version", "deleted", "created_at", "updated_at",
];

fn col_prefix<'a>(field: &str, has_shadow: bool) -> &'a str {
    if !has_shadow || RECORD_COLUMNS.contains(&field) { "r." } else { "s." }
}

fn build_filter_sql(
    filter: &QueryFilter,
    collection: &str,
    has_shadow: bool,
    has_fts: bool,
    params: &mut Vec<Value>,
) -> Result<String> {
    match filter {
        QueryFilter::Eq { field, value } => {
            let col = sanitize_ident(field)?;
            let prefix = col_prefix(field, has_shadow);
            params.push(to_sql_value(value));
            Ok(format!("{prefix}{col} = ?{}", params.len()))
        }
        QueryFilter::Lt { field, value } => {
            let col = sanitize_ident(field)?;
            let prefix = col_prefix(field, has_shadow);
            params.push(to_sql_value(value));
            Ok(format!("{prefix}{col} < ?{}", params.len()))
        }
        QueryFilter::Gt { field, value } => {
            let col = sanitize_ident(field)?;
            let prefix = col_prefix(field, has_shadow);
            params.push(to_sql_value(value));
            Ok(format!("{prefix}{col} > ?{}", params.len()))
        }
        QueryFilter::Le { field, value } => {
            let col = sanitize_ident(field)?;
            let prefix = col_prefix(field, has_shadow);
            params.push(to_sql_value(value));
            Ok(format!("{prefix}{col} <= ?{}", params.len()))
        }
        QueryFilter::Ge { field, value } => {
            let col = sanitize_ident(field)?;
            let prefix = col_prefix(field, has_shadow);
            params.push(to_sql_value(value));
            Ok(format!("{prefix}{col} >= ?{}", params.len()))
        }
        QueryFilter::Contains { text } => {
            if !has_fts {
                return Err(SquirrelError::Other(
                    "QueryFilter::Contains requires an FTS index (fts_fields) for this collection".into()
                ));
            }
            params.push(Value::Text(text.clone()));
            Ok(format!(
                "r.id IN (SELECT record_id FROM fts_{collection} WHERE fts_{collection} MATCH ?{})",
                params.len()
            ))
        }
        QueryFilter::And(filters) => {
            if filters.is_empty() {
                return Ok("1".to_string());
            }
            let parts = filters.iter()
                .map(|f| build_filter_sql(f, collection, has_shadow, has_fts, params))
                .collect::<Result<Vec<_>>>()?;
            Ok(format!("({})", parts.join(" AND ")))
        }
        QueryFilter::Or(filters) => {
            if filters.is_empty() {
                return Ok("0".to_string());
            }
            let parts = filters.iter()
                .map(|f| build_filter_sql(f, collection, has_shadow, has_fts, params))
                .collect::<Result<Vec<_>>>()?;
            Ok(format!("({})", parts.join(" OR ")))
        }
    }
}

// ── Value conversion ──────────────────────────────────────────────────────────

fn to_sql_value(v: &IndexValue) -> Value {
    match v {
        IndexValue::Text(s)    => Value::Text(s.clone()),
        IndexValue::Integer(n) => Value::Integer(*n),
        IndexValue::Real(f)    => Value::Real(*f),
        IndexValue::Null       => Value::Null,
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{records, schema},
        types::{ColumnAffinity, FieldDef, IndexDef, IndexValue, QueryFilter},
    };

    fn mem_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        schema::initialize(&conn).unwrap();
        conn
    }

    fn simple_def(collection: &str) -> IndexDef {
        IndexDef {
            collection: collection.into(),
            fields: vec![
                FieldDef { name: "year".into(),  affinity: ColumnAffinity::Integer },
                FieldDef { name: "title".into(), affinity: ColumnAffinity::Text },
            ],
            fts_fields: vec!["title".into()],
        }
    }

    fn insert_record(conn: &Connection, id: &str, hlc: &str) {
        records::upsert(conn, &records::RecordRow {
            id: id.into(),
            collection: "papers".into(),
            data: b"x".to_vec(),
            hlc: hlc.into(),
            schema_version: 0,
            format_version: 0,
            dek_encrypted: None,
            deleted: false,
            synced: false,
            created_at: 1_000_000,
            updated_at: 1_000_000,
        }).unwrap();
    }

    #[test]
    fn create_tables_is_idempotent() {
        let conn = mem_conn();
        let def = simple_def("papers");
        create_shadow_table(&conn, &def).unwrap();
        create_shadow_table(&conn, &def).unwrap(); // second call is a no-op
        create_fts_table(&conn, &def).unwrap();
        create_fts_table(&conn, &def).unwrap();
    }

    #[test]
    fn upsert_and_query_eq() {
        let conn = mem_conn();
        let def = simple_def("papers");
        create_shadow_table(&conn, &def).unwrap();
        create_fts_table(&conn, &def).unwrap();

        insert_record(&conn, "id1", "0000000000001-0000-aabbcc112233");
        insert_record(&conn, "id2", "0000000000002-0000-aabbcc112233");

        let mut vals1 = HashMap::new();
        vals1.insert("year".into(),  IndexValue::Integer(2020));
        vals1.insert("title".into(), IndexValue::Text("Rust programming".into()));
        upsert_index_row(&conn, &def, "id1", &vals1).unwrap();
        upsert_fts_row(&conn, &def, "id1", &vals1).unwrap();

        let mut vals2 = HashMap::new();
        vals2.insert("year".into(),  IndexValue::Integer(2023));
        vals2.insert("title".into(), IndexValue::Text("Async systems".into()));
        upsert_index_row(&conn, &def, "id2", &vals2).unwrap();
        upsert_fts_row(&conn, &def, "id2", &vals2).unwrap();

        let opts = QueryOpts {
            filter: Some(QueryFilter::Eq {
                field: "year".into(),
                value: IndexValue::Integer(2020),
            }),
            ..Default::default()
        };
        let rows = query(&conn, "papers", Some(&def), &opts).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "id1");
    }

    #[test]
    fn query_gt_filter() {
        let conn = mem_conn();
        let def = simple_def("papers");
        create_shadow_table(&conn, &def).unwrap();
        create_fts_table(&conn, &def).unwrap();

        for (id, year, hlc_t) in [("id1", 2019i64, 1u64), ("id2", 2021, 2), ("id3", 2023, 3)] {
            insert_record(&conn, id, &format!("{hlc_t:013x}-0000-aabbcc112233"));
            let mut vals = HashMap::new();
            vals.insert("year".into(), IndexValue::Integer(year));
            upsert_index_row(&conn, &def, id, &vals).unwrap();
        }

        let opts = QueryOpts {
            filter: Some(QueryFilter::Gt { field: "year".into(), value: IndexValue::Integer(2020) }),
            ..Default::default()
        };
        let rows = query(&conn, "papers", Some(&def), &opts).unwrap();
        assert_eq!(rows.len(), 2);
        let ids: Vec<&str> = rows.iter().map(|r| r.id.as_str()).collect();
        assert!(ids.contains(&"id2"));
        assert!(ids.contains(&"id3"));
    }

    #[test]
    fn query_fts_contains() {
        let conn = mem_conn();
        let def = simple_def("papers");
        create_shadow_table(&conn, &def).unwrap();
        create_fts_table(&conn, &def).unwrap();

        insert_record(&conn, "id1", "0000000000001-0000-aabbcc112233");
        insert_record(&conn, "id2", "0000000000002-0000-aabbcc112233");

        let mut vals1 = HashMap::new();
        vals1.insert("title".into(), IndexValue::Text("distributed systems paper".into()));
        upsert_fts_row(&conn, &def, "id1", &vals1).unwrap();
        upsert_index_row(&conn, &def, "id1", &vals1).unwrap();

        let mut vals2 = HashMap::new();
        vals2.insert("title".into(), IndexValue::Text("machine learning survey".into()));
        upsert_fts_row(&conn, &def, "id2", &vals2).unwrap();
        upsert_index_row(&conn, &def, "id2", &vals2).unwrap();

        let opts = QueryOpts {
            filter: Some(QueryFilter::Contains { text: "distributed".into() }),
            ..Default::default()
        };
        let rows = query(&conn, "papers", Some(&def), &opts).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "id1");
    }

    #[test]
    fn query_and_filter() {
        let conn = mem_conn();
        let def = simple_def("papers");
        create_shadow_table(&conn, &def).unwrap();
        create_fts_table(&conn, &def).unwrap();

        for (id, year, title, t) in [
            ("id1", 2020i64, "Rust async", 1u64),
            ("id2", 2021, "Rust networking", 2),
            ("id3", 2019, "Go concurrency", 3),
        ] {
            insert_record(&conn, id, &format!("{t:013x}-0000-aabbcc112233"));
            let mut vals = HashMap::new();
            vals.insert("year".into(), IndexValue::Integer(year));
            vals.insert("title".into(), IndexValue::Text(title.into()));
            upsert_index_row(&conn, &def, id, &vals).unwrap();
            upsert_fts_row(&conn, &def, id, &vals).unwrap();
        }

        let opts = QueryOpts {
            filter: Some(QueryFilter::And(vec![
                QueryFilter::Ge { field: "year".into(), value: IndexValue::Integer(2020) },
                QueryFilter::Contains { text: "Rust".into() },
            ])),
            ..Default::default()
        };
        let rows = query(&conn, "papers", Some(&def), &opts).unwrap();
        assert_eq!(rows.len(), 2);
        let ids: Vec<&str> = rows.iter().map(|r| r.id.as_str()).collect();
        assert!(ids.contains(&"id1"));
        assert!(ids.contains(&"id2"));
    }

    #[test]
    fn delete_removes_index_rows() {
        let conn = mem_conn();
        let def = simple_def("papers");
        create_shadow_table(&conn, &def).unwrap();
        create_fts_table(&conn, &def).unwrap();

        insert_record(&conn, "id1", "0000000000001-0000-aabbcc112233");
        let mut vals = HashMap::new();
        vals.insert("year".into(), IndexValue::Integer(2020));
        vals.insert("title".into(), IndexValue::Text("test paper".into()));
        upsert_index_row(&conn, &def, "id1", &vals).unwrap();
        upsert_fts_row(&conn, &def, "id1", &vals).unwrap();

        delete_index_rows(&conn, &def, "id1").unwrap();

        let opts = QueryOpts {
            filter: Some(QueryFilter::Eq { field: "year".into(), value: IndexValue::Integer(2020) }),
            ..Default::default()
        };
        let rows = query(&conn, "papers", Some(&def), &opts).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn invalid_identifier_returns_error() {
        assert!(sanitize_ident("valid_name").is_ok());
        assert!(sanitize_ident("1starts_with_digit").is_err());
        assert!(sanitize_ident("has space").is_err());
        assert!(sanitize_ident("has-dash").is_err());
        assert!(sanitize_ident("").is_err());
    }
}
