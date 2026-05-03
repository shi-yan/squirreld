/// Phase-4 index tests — shadow table, FTS5, and query API.
use std::collections::HashMap;

use squirreld::{
    ColumnAffinity, FieldDef, IndexDef, IndexValue, QueryFilter, QueryOpts,
    SquirrelEngine, PutOpts, Ulid,
};
use tempfile::tempdir;

async fn open_engine() -> (SquirrelEngine, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let engine = SquirrelEngine::builder()
        .db_path(dir.path().join("test.db"))
        .build()
        .await
        .unwrap();
    (engine, dir)
}

fn papers_index() -> IndexDef {
    IndexDef {
        collection: "papers".into(),
        fields: vec![
            FieldDef { name: "year".into(),   affinity: ColumnAffinity::Integer },
            FieldDef { name: "author".into(), affinity: ColumnAffinity::Text },
        ],
        fts_fields: vec!["title".into()],
    }
}

fn idx(fields: &[(&str, IndexValue)]) -> HashMap<String, IndexValue> {
    fields.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
}

// ── Registration ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn register_index_is_idempotent() {
    let (engine, _dir) = open_engine().await;
    engine.register_index(papers_index()).await.unwrap();
    engine.register_index(papers_index()).await.unwrap();
}

// ── Scalar filter ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn query_eq_filter_returns_matching_records() {
    let (engine, _dir) = open_engine().await;
    engine.register_index(papers_index()).await.unwrap();

    engine.put("papers", None, b"a".to_vec(), PutOpts {
        index_fields: idx(&[
            ("year",   IndexValue::Integer(2020)),
            ("author", IndexValue::Text("Alice".into())),
            ("title",  IndexValue::Text("Rust systems".into())),
        ]),
        ..Default::default()
    }).await.unwrap();

    engine.put("papers", None, b"b".to_vec(), PutOpts {
        index_fields: idx(&[
            ("year",   IndexValue::Integer(2023)),
            ("author", IndexValue::Text("Bob".into())),
            ("title",  IndexValue::Text("Async futures".into())),
        ]),
        ..Default::default()
    }).await.unwrap();

    let results = engine.query("papers", QueryOpts {
        filter: Some(QueryFilter::Eq {
            field: "year".into(),
            value: IndexValue::Integer(2020),
        }),
        ..Default::default()
    }).await.unwrap();

    assert_eq!(results.len(), 1);
}

#[tokio::test]
async fn query_gt_filter() {
    let (engine, _dir) = open_engine().await;
    engine.register_index(papers_index()).await.unwrap();

    for year in [2018i64, 2020, 2022, 2024] {
        engine.put("papers", None, b"x".to_vec(), PutOpts {
            index_fields: idx(&[("year", IndexValue::Integer(year))]),
            ..Default::default()
        }).await.unwrap();
    }

    let results = engine.query("papers", QueryOpts {
        filter: Some(QueryFilter::Gt { field: "year".into(), value: IndexValue::Integer(2020) }),
        ..Default::default()
    }).await.unwrap();

    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn query_le_filter() {
    let (engine, _dir) = open_engine().await;
    engine.register_index(papers_index()).await.unwrap();

    for year in [2018i64, 2020, 2022] {
        engine.put("papers", None, b"x".to_vec(), PutOpts {
            index_fields: idx(&[("year", IndexValue::Integer(year))]),
            ..Default::default()
        }).await.unwrap();
    }

    let results = engine.query("papers", QueryOpts {
        filter: Some(QueryFilter::Le { field: "year".into(), value: IndexValue::Integer(2020) }),
        ..Default::default()
    }).await.unwrap();

    assert_eq!(results.len(), 2);
}

// ── FTS full-text search ──────────────────────────────────────────────────────

#[tokio::test]
async fn query_fts_contains() {
    let (engine, _dir) = open_engine().await;
    engine.register_index(papers_index()).await.unwrap();

    engine.put("papers", None, b"a".to_vec(), PutOpts {
        index_fields: idx(&[("title", IndexValue::Text("distributed systems design".into()))]),
        ..Default::default()
    }).await.unwrap();

    engine.put("papers", None, b"b".to_vec(), PutOpts {
        index_fields: idx(&[("title", IndexValue::Text("machine learning survey".into()))]),
        ..Default::default()
    }).await.unwrap();

    let results = engine.query("papers", QueryOpts {
        filter: Some(QueryFilter::Contains { text: "distributed".into() }),
        ..Default::default()
    }).await.unwrap();

    assert_eq!(results.len(), 1);
}

// ── Compound filters ──────────────────────────────────────────────────────────

#[tokio::test]
async fn query_and_combines_scalar_and_fts() {
    let (engine, _dir) = open_engine().await;
    engine.register_index(papers_index()).await.unwrap();

    for (year, title) in [
        (2020i64, "Rust async systems"),
        (2020,    "Python web frameworks"),
        (2018,    "Rust concurrency"),
    ] {
        engine.put("papers", None, b"x".to_vec(), PutOpts {
            index_fields: idx(&[
                ("year",  IndexValue::Integer(year)),
                ("title", IndexValue::Text(title.into())),
            ]),
            ..Default::default()
        }).await.unwrap();
    }

    let results = engine.query("papers", QueryOpts {
        filter: Some(QueryFilter::And(vec![
            QueryFilter::Eq { field: "year".into(), value: IndexValue::Integer(2020) },
            QueryFilter::Contains { text: "Rust".into() },
        ])),
        ..Default::default()
    }).await.unwrap();

    assert_eq!(results.len(), 1);
}

#[tokio::test]
async fn query_or_filter() {
    let (engine, _dir) = open_engine().await;
    engine.register_index(papers_index()).await.unwrap();

    for (year, author) in [(2018i64, "Alice"), (2021, "Bob"), (2023, "Carol")] {
        engine.put("papers", None, b"x".to_vec(), PutOpts {
            index_fields: idx(&[
                ("year",   IndexValue::Integer(year)),
                ("author", IndexValue::Text(author.into())),
            ]),
            ..Default::default()
        }).await.unwrap();
    }

    let results = engine.query("papers", QueryOpts {
        filter: Some(QueryFilter::Or(vec![
            QueryFilter::Eq { field: "author".into(), value: IndexValue::Text("Alice".into()) },
            QueryFilter::Eq { field: "author".into(), value: IndexValue::Text("Carol".into()) },
        ])),
        ..Default::default()
    }).await.unwrap();

    assert_eq!(results.len(), 2);
}

// ── Delete removes from index ─────────────────────────────────────────────────

#[tokio::test]
async fn deleted_record_absent_from_query() {
    let (engine, _dir) = open_engine().await;
    engine.register_index(papers_index()).await.unwrap();

    let id = engine.put("papers", None, b"x".to_vec(), PutOpts {
        index_fields: idx(&[("year", IndexValue::Integer(2022))]),
        ..Default::default()
    }).await.unwrap();

    engine.delete("papers", &id).await.unwrap();

    let results = engine.query("papers", QueryOpts {
        filter: Some(QueryFilter::Eq { field: "year".into(), value: IndexValue::Integer(2022) }),
        ..Default::default()
    }).await.unwrap();

    assert!(results.is_empty(), "deleted record must not appear in query results");
}

// ── No index — plain query still works ───────────────────────────────────────

#[tokio::test]
async fn query_without_index_returns_all_records() {
    let (engine, _dir) = open_engine().await;
    // No register_index call — query falls back to a plain records scan.

    for _ in 0..3 {
        engine.put("papers", None, b"x".to_vec(), PutOpts::default()).await.unwrap();
    }

    let results = engine.query("papers", QueryOpts::default()).await.unwrap();
    assert_eq!(results.len(), 3);
}

// ── Limit / offset ────────────────────────────────────────────────────────────

#[tokio::test]
async fn query_respects_limit_and_offset() {
    let (engine, _dir) = open_engine().await;
    engine.register_index(papers_index()).await.unwrap();

    for year in 2010i64..2020 {
        engine.put("papers", None, b"x".to_vec(), PutOpts {
            index_fields: idx(&[("year", IndexValue::Integer(year))]),
            ..Default::default()
        }).await.unwrap();
    }

    let page1 = engine.query("papers", QueryOpts { limit: Some(3), offset: 0, ..Default::default() }).await.unwrap();
    let page2 = engine.query("papers", QueryOpts { limit: Some(3), offset: 3, ..Default::default() }).await.unwrap();

    assert_eq!(page1.len(), 3);
    assert_eq!(page2.len(), 3);
    assert_ne!(page1[0].id, page2[0].id);
}

// ── Explicit record id ────────────────────────────────────────────────────────

#[tokio::test]
async fn put_with_explicit_id_indexes_correctly() {
    let (engine, _dir) = open_engine().await;
    engine.register_index(papers_index()).await.unwrap();

    let id = Ulid::new().to_string();
    engine.put("papers", Some(id.clone()), b"content".to_vec(), PutOpts {
        index_fields: idx(&[
            ("year",  IndexValue::Integer(1999)),
            ("title", IndexValue::Text("classic paper".into())),
        ]),
        ..Default::default()
    }).await.unwrap();

    let results = engine.query("papers", QueryOpts {
        filter: Some(QueryFilter::Eq { field: "year".into(), value: IndexValue::Integer(1999) }),
        ..Default::default()
    }).await.unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, id);
}

// ── schema_version is queryable without declaring it in IndexDef ──────────────

#[tokio::test]
async fn query_by_schema_version() {
    let (engine, _dir) = open_engine().await;
    engine.register_index(papers_index()).await.unwrap();

    // Write records at different schema versions.
    for v in [0u32, 0, 1, 2] {
        engine.put("papers", None, b"x".to_vec(), PutOpts {
            schema_version: Some(v),
            ..Default::default()
        }).await.unwrap();
    }

    // Records needing migration (schema_version < 1).
    let outdated = engine.query("papers", QueryOpts {
        filter: Some(QueryFilter::Lt {
            field: "schema_version".into(),
            value: IndexValue::Integer(1),
        }),
        ..Default::default()
    }).await.unwrap();
    assert_eq!(outdated.len(), 2, "two records at v0 need migration");

    // Current schema.
    let current = engine.query("papers", QueryOpts {
        filter: Some(QueryFilter::Eq {
            field: "schema_version".into(),
            value: IndexValue::Integer(2),
        }),
        ..Default::default()
    }).await.unwrap();
    assert_eq!(current.len(), 1);
}
