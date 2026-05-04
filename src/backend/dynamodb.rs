use async_trait::async_trait;
use tracing::{debug, warn};
use aws_sdk_dynamodb::{
    Client,
    types::{
        AttributeDefinition, AttributeValue, BillingMode, GlobalSecondaryIndex, KeySchemaElement,
        KeyType, Projection, ProjectionType, ScalarAttributeType,
    },
};

use crate::backend::{BackendError, OutboxPushEntry, PushResult, RecordBackend, RemoteRecord};

// ── Configuration ─────────────────────────────────────────────────────────────

pub struct DynamoDbConfig {
    pub table_name: String,
    /// AWS region string (e.g. "us-east-1").
    pub region: String,
    /// Override endpoint URL — useful for LocalStack in tests.
    pub endpoint_url: Option<String>,
    pub credentials: AwsCredentials,
}

pub enum AwsCredentials {
    /// Read from the standard AWS credential chain: env vars, ~/.aws/credentials, IAM role, etc.
    FromEnvironment,
    /// Supply credentials directly (e.g. for tests or non-standard deployments).
    Explicit {
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
    },
}

// ── Backend ───────────────────────────────────────────────────────────────────

pub struct DynamoDbBackend {
    client: Client,
    table_name: String,
}

impl DynamoDbBackend {
    pub async fn new(config: DynamoDbConfig) -> Result<Self, BackendError> {
        let sdk_config = build_sdk_config(&config).await?;
        let client = Client::new(&sdk_config);
        Ok(Self { client, table_name: config.table_name })
    }

    fn av_s(s: impl Into<String>) -> AttributeValue {
        AttributeValue::S(s.into())
    }

    fn av_n(n: impl ToString) -> AttributeValue {
        AttributeValue::N(n.to_string())
    }

    fn av_b(b: Vec<u8>) -> AttributeValue {
        AttributeValue::B(aws_smithy_types::Blob::new(b))
    }

    fn item_from_entry(entry: &OutboxPushEntry) -> std::collections::HashMap<String, AttributeValue> {
        let mut item = std::collections::HashMap::new();
        item.insert("record_id".into(),      Self::av_s(&entry.record_id));
        item.insert("collection".into(),     Self::av_s(&entry.collection));
        item.insert("hlc".into(),            Self::av_s(&entry.hlc));
        item.insert("operation".into(),      Self::av_s(&entry.operation));
        item.insert("schema_version".into(), Self::av_n(entry.schema_version));
        item.insert("format_version".into(), Self::av_n(entry.format_version));
        item.insert("deleted".into(),        Self::av_n(if entry.operation == "delete" { 1 } else { 0 }));
        // GSI partition key — all records share this value so the GSI acts as a full-table scan.
        item.insert("_p".into(), Self::av_s("main"));
        if let Some(data) = &entry.data {
            item.insert("data".into(), Self::av_b(data.clone()));
        }
        if let Some(dek) = &entry.dek_encrypted {
            item.insert("dek_encrypted".into(), Self::av_b(dek.clone()));
        }
        item
    }

    fn remote_record_from_item(
        item: &std::collections::HashMap<String, AttributeValue>,
    ) -> Option<RemoteRecord> {
        let record_id  = item.get("record_id")?.as_s().ok()?.clone();
        let collection = item.get("collection")?.as_s().ok()?.clone();
        let hlc        = item.get("hlc")?.as_s().ok()?.clone();
        let deleted    = item.get("deleted")
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<u8>().ok())
            .unwrap_or(0) != 0;
        let schema_version = item.get("schema_version")
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<u32>().ok())
            .unwrap_or(0);
        let format_version = item.get("format_version")
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<u8>().ok())
            .unwrap_or(0);
        let data = item.get("data")
            .and_then(|v| v.as_b().ok())
            .map(|b| b.clone().into_inner());
        let dek_encrypted = item.get("dek_encrypted")
            .and_then(|v| v.as_b().ok())
            .map(|b| b.clone().into_inner());
        Some(RemoteRecord { record_id, collection, hlc, data, dek_encrypted, deleted, schema_version, format_version })
    }
}

#[async_trait]
impl RecordBackend for DynamoDbBackend {
    fn backend_id(&self) -> &str {
        &self.table_name
    }

    async fn push_one(&self, entry: &OutboxPushEntry) -> PushResult {
        let item = Self::item_from_entry(entry);

        // Condition: accept only if the item doesn't exist yet, or our HLC is strictly higher.
        let result = self.client
            .put_item()
            .table_name(&self.table_name)
            .set_item(Some(item))
            .condition_expression("attribute_not_exists(hlc) OR hlc < :local_hlc")
            .expression_attribute_values(":local_hlc", Self::av_s(&entry.hlc))
            .send()
            .await;

        match result {
            Ok(_) => PushResult::Ok { pushed_seqs: vec![entry.seq] },
            Err(e) => {
                let err_str = format!("{e:#?}");
                // DynamoDB returns ConditionalCheckFailedException when condition fails.
                if err_str.contains("ConditionalCheckFailedException") {
                    PushResult::ConflictAt { record_id: entry.record_id.clone(), seq: entry.seq }
                } else {
                    PushResult::TransientError(err_str)
                }
            }
        }
    }

    async fn pull_since(
        &self,
        checkpoint: Option<&str>,
    ) -> Result<Vec<RemoteRecord>, BackendError> {
        let mut records = Vec::new();
        let mut last_key = None;

        loop {
            let mut req = self.client
                .query()
                .table_name(&self.table_name)
                .index_name("sync-index")
                .key_condition_expression(match checkpoint {
                    Some(_) => "#p = :main AND hlc > :checkpoint",
                    None    => "#p = :main",
                })
                .expression_attribute_names("#p", "_p")
                .expression_attribute_values(":main", Self::av_s("main"))
                .scan_index_forward(true); // ascending by HLC

            if let Some(cp) = checkpoint {
                req = req.expression_attribute_values(":checkpoint", Self::av_s(cp));
            }

            if let Some(key) = last_key.take() {
                req = req.set_exclusive_start_key(Some(key));
            }

            let resp = req.send().await
                .map_err(|e| BackendError::Transient(format!("{e:#?}")))?;

            for item in resp.items() {
                if let Some(r) = Self::remote_record_from_item(item) {
                    records.push(r);
                }
            }

            last_key = resp.last_evaluated_key().cloned();
            if last_key.is_none() { break; }
        }

        Ok(records)
    }

    async fn ensure_table(&self) -> Result<(), BackendError> {
        match self.client.describe_table().table_name(&self.table_name).send().await {
            Ok(_) => return Ok(()),
            Err(e) => {
                let detail = format!("{e:#?}");
                if !detail.contains("ResourceNotFoundException") {
                    warn!(table = %self.table_name, "describe_table failed: {detail}");
                    return Err(BackendError::Config(format!("describe_table: {detail}")));
                }
                debug!(table = %self.table_name, "table not found, will attempt create_table");
            }
        }

        // Create the table with a GSI for range-query pulls.
        self.client
            .create_table()
            .table_name(&self.table_name)
            .billing_mode(BillingMode::PayPerRequest)
            // Primary key: PK=record_id (ULID), SK=collection
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("record_id")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .map_err(|e| BackendError::Config(e.to_string()))?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("collection")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .map_err(|e| BackendError::Config(e.to_string()))?,
            )
            // GSI attributes
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("_p")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .map_err(|e| BackendError::Config(e.to_string()))?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("hlc")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .map_err(|e| BackendError::Config(e.to_string()))?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("record_id")
                    .key_type(KeyType::Hash)
                    .build()
                    .map_err(|e| BackendError::Config(e.to_string()))?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("collection")
                    .key_type(KeyType::Range)
                    .build()
                    .map_err(|e| BackendError::Config(e.to_string()))?,
            )
            .global_secondary_indexes(
                GlobalSecondaryIndex::builder()
                    .index_name("sync-index")
                    .key_schema(
                        KeySchemaElement::builder()
                            .attribute_name("_p")
                            .key_type(KeyType::Hash)
                            .build()
                            .map_err(|e| BackendError::Config(e.to_string()))?,
                    )
                    .key_schema(
                        KeySchemaElement::builder()
                            .attribute_name("hlc")
                            .key_type(KeyType::Range)
                            .build()
                            .map_err(|e| BackendError::Config(e.to_string()))?,
                    )
                    .projection(
                        Projection::builder()
                            .projection_type(ProjectionType::All)
                            .build(),
                    )
                    .build()
                    .map_err(|e| BackendError::Config(e.to_string()))?,
            )
            .send()
            .await
            .map_err(|e| BackendError::Config(format!("create_table: {e:#?}")))?;

        Ok(())
    }
}

// ── SDK config builder ────────────────────────────────────────────────────────

async fn build_sdk_config(config: &DynamoDbConfig) -> Result<aws_config::SdkConfig, BackendError> {
    use aws_config::Region;

    let region = Region::new(config.region.clone());

    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest()).region(region);

    if let Some(url) = &config.endpoint_url {
        loader = loader.endpoint_url(url.as_str());
    }

    match &config.credentials {
        AwsCredentials::FromEnvironment => {}
        AwsCredentials::Explicit { access_key_id, secret_access_key, session_token } => {
            use aws_credential_types::Credentials;
            let creds = Credentials::new(
                access_key_id,
                secret_access_key,
                session_token.clone(),
                None,
                "squirreld-explicit",
            );
            loader = loader.credentials_provider(creds);
        }
    }

    Ok(loader.load().await)
}
