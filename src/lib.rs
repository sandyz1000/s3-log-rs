use aws_sdk_s3::operation::get_object::{GetObjectError, GetObjectOutput};
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::{Client, Error as S3Error};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::error::Error;
use std::io::{self, Read, Write};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct S3WAL {
    client: Client,
    bucket_name: String,
    prefix: String,
    length: Arc<Mutex<u64>>,
}

#[derive(Debug, Clone)]
pub struct Record {
    pub offset: u64,
    pub data: Vec<u8>,
}

impl S3WAL {
    pub fn new(client: Client, bucket_name: String, prefix: String) -> Self {
        S3WAL {
            client,
            bucket_name,
            prefix,
            length: Arc::new(Mutex::new(0)),
        }
    }

    /// Generates S3 object keys and parses offsets from keys.
    fn get_object_key(&self, offset: u64) -> String {
        format!("{}/{}", self.prefix, format!("{:020}", offset))
    }

    fn get_offset_from_key(&self, key: &str) -> Result<u64, Box<dyn Error>> {
        let num_str = key.trim_start_matches(&format!("{}/", self.prefix));
        Ok(num_str.parse::<u64>()?)
    }

    /// Calculate the checksum using the buffer, this is to check the data integrity
    fn calculate_checksum(buf: &[u8]) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(buf);
        hasher.finalize().into()
    }

    fn validate_checksum(data: &[u8]) -> bool {
        if data.len() < 32 {
            return false;
        }
        let (record_data, stored_checksum) = data.split_at(data.len() - 32);
        Self::calculate_checksum(record_data) == stored_checksum
    }

    fn prepare_body(offset: u64, data: &[u8]) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut buf = Vec::with_capacity(8 + data.len() + 32);
        buf.write_u64::<BigEndian>(offset)?;
        buf.extend_from_slice(data);
        let checksum = Self::calculate_checksum(&buf);
        buf.extend_from_slice(&checksum);
        Ok(buf)
    }

    pub async fn append(&self, data: &[u8]) -> Result<u64, Box<dyn Error>> {
        let mut length_guard = self.length.lock().await;
        let next_offset = *length_guard + 1;

        let buf = Self::prepare_body(next_offset, data)?;
        let key = self.get_object_key(next_offset);

        self.client
            .put_object()
            .bucket(&self.bucket_name)
            .key(&key)
            .body(buf.into())
            .send()
            .await
            .map_err(|e| format!("Failed to put object to S3: {}", e))?;

        *length_guard = next_offset;
        Ok(next_offset)
    }

    /// Reads a WAL entry, validates its checksum, and returns the record.
    pub async fn read(&self, offset: u64) -> Result<Record, Box<dyn Error>> {
        let key = self.get_object_key(offset);

        let result = self
            .client
            .get_object()
            .bucket(&self.bucket_name)
            .key(&key)
            .send()
            .await?;

        let res = result.body.collect().await?;
        let data = res.to_vec();

        if data.len() < 40 {
            return Err("Invalid record: data too short".into());
        }

        let stored_offset = (&data[..8]).read_u64::<BigEndian>()?;
        if stored_offset != offset {
            return Err(format!(
                "Offset mismatch: expected {}, got {}",
                offset, stored_offset
            )
            .into());
        }

        if !Self::validate_checksum(&data) {
            return Err("Checksum mismatch".into());
        }

        Ok(Record {
            offset: stored_offset,
            data: data[8..data.len() - 32].to_vec(),
        })
    }

    /// Uses S3's list_objects_v2 with pagination to find the object with the maximum offset and retrieves it.
    pub async fn last_record(&self) -> Result<Record, Box<dyn Error>> {
        let mut max_offset = 0u64;
        let mut continuation_token = None;

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket_name)
                .prefix(&self.prefix);

            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }

            let response = request.send().await?;

            for object in response.contents() {
                if let Some(key) = object.key() {
                    if let Ok(offset) = self.get_offset_from_key(key) {
                        max_offset = max_offset.max(offset);
                    }
                }
            }

            if response.is_truncated().unwrap_or(false) {
                continuation_token = response.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }

        if max_offset == 0 {
            return Err("WAL is empty".into());
        }

        let mut length_guard = self.length.lock().await;
        *length_guard = max_offset;
        self.read(max_offset).await
    }
}

// Code taken from: https://stackoverflow.com/questions/29963449/golang-like-defer-in-rust
pub(crate) struct ScopeCall<F: FnMut()> {
    c: F,
}
impl<F: FnMut()> Drop for ScopeCall<F> {
    fn drop(&mut self) {
        (self.c)();
    }
}

macro_rules! defer {
    ($e:expr) => {
        let _scope_call = ScopeCall {
            c: || -> () {
                $e;
            },
        };
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::config::endpoint::Endpoint;
    use aws_sdk_s3::config::{Credentials, Region};
    use aws_sdk_s3::error::SdkError;
    use aws_sdk_s3::operation::delete_objects::DeleteObjectsError;
    use aws_sdk_s3::types::ObjectIdentifier;
    use aws_sdk_s3::{Client, Error};
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};
    use std::error::Error as StdError;
    use std::sync::Arc;
    use tokio::sync::Mutex; // Assuming the S3WAL implementation is in `s3_wal` module.

    fn generate_random_str() -> String {
        thread_rng()
            .sample_iter(&Alphanumeric)
            .take(16)
            .map(char::from)
            .collect()
    }

    fn setup_minio_client() -> Client {
        let config = aws_sdk_s3::config::Builder::new()
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                "minioadmin",
                "minioadmin",
                None,
                None,
                "s3",
            ))
            .endpoint_resolver(Endpoint::immutable(
                "http://127.0.0.1:9000".parse().unwrap(),
            ))
            .build();
        Client::from_conf(config)
    }

    async fn setup_bucket(client: &Client, bucket_name: &str) -> Result<(), Box<dyn StdError>> {
        match client.create_bucket().bucket(bucket_name).send().await {
        Ok(_) => Ok(()),
        Err(SdkError::ServiceError(e)) if matches!(e.err(), aws_sdk_s3::operation::create_bucket::CreateBucketError::BucketAlreadyExists(_) | aws_sdk_s3::operation::create_bucket::CreateBucketError::BucketAlreadyOwnedByYou(_)) => Ok(()),
        Err(e) => Err(Box::new(e)),
    }
    }

    async fn empty_bucket(
        client: &Client,
        bucket_name: &str,
        prefix: &str,
    ) -> Result<(), Box<dyn StdError>> {
        let mut continuation_token = None;
        loop {
            let mut request = client.list_objects_v2().bucket(bucket_name).prefix(prefix);
            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }
            let response = request.send().await?;
            let objects: Vec<ObjectIdentifier> = response.contents()
                .iter()
                .filter_map(|obj| {
                    obj.key()
                        .map(|key| ObjectIdentifier::builder().key(key).build().unwrap())
                })
                .collect();

            if !objects.is_empty() {
                for o in &objects {
                    client
                        .delete_objects()
                        .bucket(bucket_name)
                        .delete(
                            aws_sdk_s3::types::Delete::builder()
                                .objects(o.clone())
                                .build()?,
                        )
                        .send()
                        .await?;
                }
            }
            
            if response.is_truncated().unwrap_or(false) {
                continuation_token = response.next_continuation_token().map(|s|s.to_string());
            } else {
                break;
            }
        }
        Ok(())
    }

    async fn get_wal() -> (S3WAL, impl FnOnce() -> ()) {
        let client = setup_minio_client();
        let bucket_name = format!("test-wal-bucket-{}", generate_random_str());
        let prefix = generate_random_str();

        if let Err(e) = setup_bucket(&client, &bucket_name).await {
            panic!("Failed to set up bucket: {:?}", e);
        }

        let cleanup = {
            let client = client.clone();
            let bucket_name = bucket_name.clone();
            let prefix = prefix.clone();
            move || {
                let runtime = tokio::runtime::Runtime::new().unwrap();
                runtime.block_on(async {
                    if let Err(e) = empty_bucket(&client, &bucket_name, &prefix).await {
                        eprintln!("Failed to empty bucket: {:?}", e);
                    }
                    if let Err(e) = client.delete_bucket().bucket(bucket_name).send().await {
                        eprintln!("Failed to delete bucket: {:?}", e);
                    }
                });
            }
        };

        (S3WAL::new(client, bucket_name, prefix), cleanup)
    }

    #[tokio::test]
    async fn test_append_and_read_single() {
        let (wal, cleanup) = get_wal().await;
        defer!(cleanup());

        let ctx = ();
        let test_data = b"hello world".to_vec();

        let offset = wal
            .append(ctx, &test_data)
            .await
            .expect("Failed to append data");
        assert_eq!(offset, 1);

        let record = wal.read(ctx, offset).await.expect("Failed to read data");
        assert_eq!(record.offset, offset);
        assert_eq!(record.data, test_data);
    }

    #[tokio::test]
    async fn test_append_multiple() {
        let (wal, cleanup) = get_wal().await;
        defer!(cleanup());

        let ctx = ();
        let test_data = vec![
            b"First message".to_vec(),
            b"Second message".to_vec(),
            b"Third message".to_vec(),
        ];

        let mut offsets = Vec::new();
        for data in &test_data {
            let offset = wal.append(ctx, data).await.expect("Failed to append data");
            offsets.push(offset);
        }

        for (i, offset) in offsets.iter().enumerate() {
            let record = wal.read(ctx, *offset).await.expect("Failed to read data");
            assert_eq!(record.offset, *offset);
            assert_eq!(record.data, test_data[i]);
        }
    }

    #[tokio::test]
    async fn test_read_non_existent() {
        let (wal, cleanup) = get_wal().await;
        defer!(cleanup());

        let ctx = ();
        let result = wal.read(ctx, 99999).await;
        assert!(result.is_err(), "Expected error for non-existent record");
    }

    #[tokio::test]
    async fn test_append_empty() {
        let (wal, cleanup) = get_wal().await;
        defer!(cleanup());

        let ctx = ();
        let offset = wal
            .append(ctx, b"")
            .await
            .expect("Failed to append empty data");

        let record = wal
            .read(ctx, offset)
            .await
            .expect("Failed to read empty record");
        assert!(record.data.is_empty(), "Expected empty data");
    }

    #[tokio::test]
    async fn test_append_large() {
        let (wal, cleanup) = get_wal().await;
        defer!(cleanup());

        let ctx = ();
        let large_data: Vec<u8> = (0..10 * 1024 * 1024).map(|i| (i % 256) as u8).collect();

        let offset = wal
            .append(ctx, &large_data)
            .await
            .expect("Failed to append large data");

        let record = wal
            .read(ctx, offset)
            .await
            .expect("Failed to read large record");
        assert_eq!(record.data, large_data);
    }

    #[tokio::test]
    async fn test_last_record() {
        let (wal, cleanup) = get_wal().await;
        defer!(cleanup());

        let ctx = ();
        let mut last_data = vec![];
        for i in 1..=1234 {
            last_data = format!("Data-{}", i).into_bytes();
            wal.append(ctx, &last_data)
                .await
                .expect("Failed to append data");
        }

        let record = wal
            .last_record(ctx)
            .await
            .expect("Failed to get last record");
        assert_eq!(record.offset, 1234);
        assert_eq!(record.data, last_data);
    }
}
