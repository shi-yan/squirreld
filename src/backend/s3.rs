use std::path::Path;

use async_trait::async_trait;
use aws_sdk_s3::{
    Client,
    types::{
        BucketLocationConstraint, CompletedMultipartUpload, CompletedPart,
        CreateBucketConfiguration,
    },
};

use crate::backend::{BackendError, BlobBackend, UploadedPart};

// ── Configuration ─────────────────────────────────────────────────────────────

pub struct S3Config {
    pub bucket_name: String,
    pub region: String,
    /// Override endpoint URL — set to LocalStack endpoint in tests.
    pub endpoint_url: Option<String>,
    pub credentials: S3Credentials,
}

pub enum S3Credentials {
    /// Read from the standard AWS credential chain.
    FromEnvironment,
    Explicit {
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
    },
}

// ── Backend ───────────────────────────────────────────────────────────────────

pub struct S3Backend {
    client:      Client,
    bucket_name: String,
    region:      String,
}

impl S3Backend {
    pub async fn new(config: S3Config) -> Result<Self, BackendError> {
        let region = config.region.clone();
        let sdk_config = build_sdk_config(&config).await?;
        let client = Client::new(&sdk_config);
        Ok(Self { client, bucket_name: config.bucket_name, region })
    }
}

#[async_trait]
impl BlobBackend for S3Backend {
    fn backend_id(&self) -> &str { &self.bucket_name }

    async fn ensure_bucket(&self) -> Result<(), BackendError> {
        // HEAD bucket — succeeds if it exists, fails if not
        let result = self.client
            .head_bucket()
            .bucket(&self.bucket_name)
            .send()
            .await;
        if result.is_ok() { return Ok(()); }

        // us-east-1 is S3's default region and must NOT include a
        // LocationConstraint. All other regions require one.
        let mut req = self.client
            .create_bucket()
            .bucket(&self.bucket_name);

        if self.region != "us-east-1" {
            let constraint = BucketLocationConstraint::from(self.region.as_str());
            let cfg = CreateBucketConfiguration::builder()
                .location_constraint(constraint)
                .build();
            req = req.create_bucket_configuration(cfg);
        }

        req.send()
            .await
            .map_err(|e| BackendError::Config(e.to_string()))?;
        Ok(())
    }

    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), BackendError> {
        self.client
            .put_object()
            .bucket(&self.bucket_name)
            .key(key)
            .body(data.into())
            .send()
            .await
            .map_err(|e| BackendError::Transient(e.to_string()))?;
        Ok(())
    }

    async fn create_multipart_upload(&self, key: &str) -> Result<String, BackendError> {
        let resp = self.client
            .create_multipart_upload()
            .bucket(&self.bucket_name)
            .key(key)
            .send()
            .await
            .map_err(|e| BackendError::Transient(e.to_string()))?;

        resp.upload_id()
            .map(|s| s.to_string())
            .ok_or_else(|| BackendError::Transient("S3 returned no upload_id".into()))
    }

    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        data: Vec<u8>,
    ) -> Result<String, BackendError> {
        let resp = self.client
            .upload_part()
            .bucket(&self.bucket_name)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(data.into())
            .send()
            .await
            .map_err(|e| BackendError::Transient(e.to_string()))?;

        resp.e_tag()
            .map(|s| s.to_string())
            .ok_or_else(|| BackendError::Transient("S3 returned no ETag for part".into()))
    }

    async fn list_parts(
        &self,
        key: &str,
        upload_id: &str,
    ) -> Result<Vec<UploadedPart>, BackendError> {
        let resp = self.client
            .list_parts()
            .bucket(&self.bucket_name)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await;

        match resp {
            Ok(out) => {
                let parts = out.parts().iter()
                    .filter_map(|p| {
                        Some(UploadedPart {
                            part_number: p.part_number().unwrap_or(0),
                            etag: p.e_tag()?.to_string(),
                        })
                    })
                    .collect();
                Ok(parts)
            }
            Err(e) => {
                let msg = e.to_string();
                // NoSuchUpload means the upload ID expired — caller resets and retries.
                if msg.contains("NoSuchUpload") {
                    Ok(vec![])
                } else {
                    Err(BackendError::Transient(msg))
                }
            }
        }
    }

    async fn complete_multipart_upload(
        &self,
        key: &str,
        upload_id: &str,
        parts: Vec<UploadedPart>,
    ) -> Result<(), BackendError> {
        let completed_parts: Vec<CompletedPart> = parts
            .into_iter()
            .map(|p| {
                CompletedPart::builder()
                    .part_number(p.part_number)
                    .e_tag(p.etag)
                    .build()
            })
            .collect();

        let upload = CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();

        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket_name)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(upload)
            .send()
            .await
            .map_err(|e| BackendError::Transient(e.to_string()))?;
        Ok(())
    }

    async fn get_object(&self, key: &str, dest: &Path) -> Result<u64, BackendError> {
        let resp = self.client
            .get_object()
            .bucket(&self.bucket_name)
            .key(key)
            .send()
            .await
            .map_err(|e| BackendError::Transient(e.to_string()))?;

        let data = resp.body.collect().await
            .map_err(|e| BackendError::Transient(e.to_string()))?
            .into_bytes();
        let len = data.len() as u64;

        tokio::fs::write(dest, data).await
            .map_err(|e| BackendError::Transient(e.to_string()))?;
        Ok(len)
    }

    async fn delete_object(&self, key: &str) -> Result<(), BackendError> {
        self.client
            .delete_object()
            .bucket(&self.bucket_name)
            .key(key)
            .send()
            .await
            .map_err(|e| BackendError::Transient(e.to_string()))?;
        Ok(())
    }
}

// ── SDK config ────────────────────────────────────────────────────────────────

async fn build_sdk_config(config: &S3Config) -> Result<aws_config::SdkConfig, BackendError> {
    use aws_config::Region;
    let region = Region::new(config.region.clone());
    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest()).region(region);

    if let Some(url) = &config.endpoint_url {
        loader = loader.endpoint_url(url.as_str());
    }

    match &config.credentials {
        S3Credentials::FromEnvironment => {}
        S3Credentials::Explicit { access_key_id, secret_access_key, session_token } => {
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
