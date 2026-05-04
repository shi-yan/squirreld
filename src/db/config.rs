use rusqlite::{Connection, OptionalExtension};

use crate::error::{Result, SquirrelError};

/// Load the device node ID from the config table, generating and persisting one if absent.
pub fn get_or_create_node_id(conn: &Connection) -> Result<[u8; 6]> {
    if let Some(val) = get(conn, "node_id")? {
        let bytes = hex_decode(&val)
            .map_err(|_| SquirrelError::Other(format!("corrupt node_id in config: {val}")))?;
        if bytes.len() != 6 {
            return Err(SquirrelError::Other("node_id must be exactly 6 bytes".into()));
        }
        let mut arr = [0u8; 6];
        arr.copy_from_slice(&bytes);
        Ok(arr)
    } else {
        use rand::RngExt;
        let mut node_id = [0u8; 6];
        rand::rng().fill(&mut node_id[..]);
        set(conn, "node_id", &hex_encode(&node_id))?;
        Ok(node_id)
    }
}

pub fn get(conn: &Connection, key: &str) -> Result<Option<String>> {
    conn.query_row(
        "SELECT value FROM config WHERE key = ?1",
        rusqlite::params![key],
        |row| row.get(0),
    )
    .optional()
    .map_err(Into::into)
}

pub fn set(conn: &Connection, key: &str, value: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO config (key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        rusqlite::params![key, value],
    )?;
    Ok(())
}

/// Load the KDF salt used for passphrase → KEK derivation, generating one on first use.
pub fn get_or_create_kek_salt(conn: &Connection) -> Result<[u8; 16]> {
    if let Some(val) = get(conn, "kek_salt")? {
        let bytes = hex_decode(&val)
            .map_err(|_| SquirrelError::Other("corrupt kek_salt in config".into()))?;
        if bytes.len() != 16 {
            return Err(SquirrelError::Other("kek_salt must be 16 bytes".into()));
        }
        let mut arr = [0u8; 16];
        arr.copy_from_slice(&bytes);
        Ok(arr)
    } else {
        use rand::RngExt;
        let mut salt = [0u8; 16];
        rand::rng().fill(&mut salt[..]);
        set(conn, "kek_salt", &hex_encode(&salt))?;
        Ok(salt)
    }
}

pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub(crate) fn hex_decode(s: &str) -> std::result::Result<Vec<u8>, ()> {
    if s.len() % 2 != 0 {
        return Err(());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| ()))
        .collect()
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

    #[test]
    fn node_id_is_created_on_first_call() {
        let conn = mem_conn();
        let id = get_or_create_node_id(&conn).unwrap();
        assert_ne!(id, [0u8; 6], "node_id should not be all zeros");
    }

    #[test]
    fn node_id_is_stable_across_calls() {
        let conn = mem_conn();
        let id1 = get_or_create_node_id(&conn).unwrap();
        let id2 = get_or_create_node_id(&conn).unwrap();
        assert_eq!(id1, id2, "node_id must not change between calls");
    }

    #[test]
    fn get_set_roundtrip() {
        let conn = mem_conn();
        set(&conn, "foo", "bar").unwrap();
        assert_eq!(get(&conn, "foo").unwrap(), Some("bar".into()));
    }

    #[test]
    fn set_is_upsert() {
        let conn = mem_conn();
        set(&conn, "key", "v1").unwrap();
        set(&conn, "key", "v2").unwrap();
        assert_eq!(get(&conn, "key").unwrap(), Some("v2".into()));
    }

    #[test]
    fn get_missing_key_returns_none() {
        let conn = mem_conn();
        assert_eq!(get(&conn, "nonexistent").unwrap(), None);
    }
}
