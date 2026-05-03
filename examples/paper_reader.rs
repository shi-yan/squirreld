/// End-to-end demo of squirreld — paper reader scenario.
///
/// Demonstrates:
///   - Engine setup with passphrase encryption
///   - Index registration for structured queries
///   - CRUD with encrypted payloads
///   - Scalar filter queries and FTS full-text search
///   - Blob staging and background upload
///   - SyncEvent subscription
///   - Graceful shutdown
///
/// Run with:  cargo run --example paper_reader
use std::collections::HashMap;
use std::sync::Arc;

use squirreld::{
    backend::in_memory::{InMemoryBlobBackend, InMemoryBlobStore},
    ColumnAffinity, FieldDef, IndexDef, IndexValue, ItemEncryption, KeySource,
    PutBlobOpts, PutOpts, QueryFilter, QueryOpts, SquirrelEngine, SyncEvent,
};
use tempfile::tempdir;

// ── Paper payload ─────────────────────────────────────────────────────────────

#[derive(serde::Serialize, serde::Deserialize)]
struct Paper {
    title:       String,
    authors:     Vec<String>,
    year:        i64,
    tags:        Vec<String>,
    abstract_:   String,
    read_status: String,
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir        = tempdir()?;
    let blob_store = InMemoryBlobStore::new_shared();
    let backend    = Arc::new(InMemoryBlobBackend::new("demo", blob_store));

    // ── 1. Build the engine ───────────────────────────────────────────────────
    let engine = SquirrelEngine::builder()
        .db_path(dir.path().join("papers.db"))
        .cache_dir(dir.path().join("blobs"))
        // Encrypt all records by default with a passphrase-derived KEK.
        .encryption_key(KeySource::Passphrase("correct horse battery staple".into()))
        // Attach the blob backend (S3Backend in production).
        .blob_backend(backend)
        .build()
        .await?;

    // ── 2. Register collection index ──────────────────────────────────────────
    engine.register_index(IndexDef {
        collection: "papers".into(),
        fields: vec![
            FieldDef { name: "year".into(),        affinity: ColumnAffinity::Integer },
            FieldDef { name: "read_status".into(), affinity: ColumnAffinity::Text },
        ],
        fts_fields: vec!["title".into(), "abstract_".into()],
    }).await?;

    // ── 3. Subscribe to sync events ───────────────────────────────────────────
    let mut events = engine.sync_events();
    tokio::spawn(async move {
        while let Ok(ev) = events.recv().await {
            match ev {
                SyncEvent::BlobUploaded { blob_id } =>
                    println!("[event] blob uploaded: {blob_id}"),
                SyncEvent::PushComplete(stats) =>
                    println!("[event] push complete — pushed {}", stats.pushed),
                SyncEvent::PullComplete(stats) =>
                    println!("[event] pull complete — pulled {}", stats.pulled),
                _ => {}
            }
        }
    });

    // ── 4. Insert papers ──────────────────────────────────────────────────────
    let papers: &[Paper] = &[
        Paper {
            title:       "Attention Is All You Need".into(),
            authors:     vec!["Vaswani et al.".into()],
            year:        2017,
            tags:        vec!["transformers".into(), "nlp".into()],
            abstract_:   "We propose a new network architecture based solely on attention mechanisms.".into(),
            read_status: "read".into(),
        },
        Paper {
            title:       "BERT: Pre-training of Deep Bidirectional Transformers".into(),
            authors:     vec!["Devlin et al.".into()],
            year:        2019,
            tags:        vec!["transformers".into(), "pretraining".into()],
            abstract_:   "We introduce BERT, designed to pretrain deep bidirectional representations.".into(),
            read_status: "unread".into(),
        },
        Paper {
            title:       "Scaling Laws for Neural Language Models".into(),
            authors:     vec!["Kaplan et al.".into()],
            year:        2020,
            tags:        vec!["scaling".into(), "language models".into()],
            abstract_:   "We study empirical scaling laws for language model performance.".into(),
            read_status: "unread".into(),
        },
    ];

    let mut paper_ids = Vec::new();
    for paper in papers {
        let data = serde_json::to_vec(paper)?;
        let id = engine.put("papers", None, data, PutOpts {
            index_fields: {
                let mut m = HashMap::new();
                m.insert("year".into(),        IndexValue::Integer(paper.year));
                m.insert("read_status".into(), IndexValue::Text(paper.read_status.clone()));
                m.insert("title".into(),       IndexValue::Text(paper.title.clone()));
                m.insert("abstract_".into(),   IndexValue::Text(paper.abstract_.clone()));
                m
            },
            ..Default::default()
        }).await?;
        println!("stored paper [{id}]: {}", paper.title);
        paper_ids.push(id);
    }

    // ── 5. Scalar query: papers from 2019 onwards ─────────────────────────────
    println!("\n--- Papers from 2019 onwards ---");
    let recent = engine.query("papers", QueryOpts {
        filter: Some(QueryFilter::Ge {
            field: "year".into(),
            value: IndexValue::Integer(2019),
        }),
        ..Default::default()
    }).await?;
    for meta in &recent {
        let rec = engine.get("papers", &meta.id).await?.unwrap();
        let p: Paper = serde_json::from_slice(&rec.data)?;
        println!("  {} ({})", p.title, p.year);
    }

    // ── 6. Full-text search ───────────────────────────────────────────────────
    println!("\n--- FTS: 'attention' ---");
    let hits = engine.query("papers", QueryOpts {
        filter: Some(QueryFilter::Contains { text: "attention".into() }),
        ..Default::default()
    }).await?;
    for meta in &hits {
        let rec = engine.get("papers", &meta.id).await?.unwrap();
        let p: Paper = serde_json::from_slice(&rec.data)?;
        println!("  {}", p.title);
    }

    // ── 7. Combined filter: unread + FTS ─────────────────────────────────────
    println!("\n--- Unread papers about transformers ---");
    let combined = engine.query("papers", QueryOpts {
        filter: Some(QueryFilter::And(vec![
            QueryFilter::Eq {
                field: "read_status".into(),
                value: IndexValue::Text("unread".into()),
            },
            QueryFilter::Contains { text: "transformers".into() },
        ])),
        ..Default::default()
    }).await?;
    for meta in &combined {
        let rec = engine.get("papers", &meta.id).await?.unwrap();
        let p: Paper = serde_json::from_slice(&rec.data)?;
        println!("  {}", p.title);
    }

    // ── 8. Attach a PDF blob to the first paper ───────────────────────────────
    let pdf_path = dir.path().join("paper.pdf");
    std::fs::write(&pdf_path, b"%PDF-1.4 fake pdf content for demo")?;

    let blob_id = engine.put_blob(&pdf_path, PutBlobOpts {
        record_id:  Some(paper_ids[0].to_string()),
        collection: Some("papers".into()),
    }).await?;
    println!("\nstaged blob [{blob_id}] for paper 0");

    // Upload synchronously for demo purposes.
    engine.force_flush_blobs().await?;

    let info = engine.blob_info(&blob_id).await?.unwrap();
    println!("blob status after flush: {:?}", info.status);

    // ── 9. Persist across re-open ─────────────────────────────────────────────
    println!("\n--- Re-opening engine with same passphrase ---");
    drop(engine);

    let engine2 = SquirrelEngine::builder()
        .db_path(dir.path().join("papers.db"))
        .encryption_key(KeySource::Passphrase("correct horse battery staple".into()))
        .build()
        .await?;

    let rec = engine2.get("papers", &paper_ids[0]).await?.unwrap();
    let p: Paper = serde_json::from_slice(&rec.data)?;
    println!("re-opened and decrypted: \"{}\"", p.title);

    // ── 10. Disable encryption for one public record ──────────────────────────
    let public_id = engine2.put("papers", None, b"public abstract".to_vec(), PutOpts {
        encryption: ItemEncryption::Disabled,
        ..Default::default()
    }).await?;
    let public = engine2.get("papers", &public_id).await?.unwrap();
    assert_eq!(public.data, b"public abstract");
    println!("plaintext override record read back correctly");

    engine2.shutdown().await?;
    println!("\nDone.");
    Ok(())
}
