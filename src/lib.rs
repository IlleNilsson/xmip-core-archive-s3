#![forbid(unsafe_code)]

//! S3 archive: an [`ArchiveStore`] that keeps each retained item as one
//! object in a bucket, its metadata as a second object beside it, and
//! restores the item by getting both back.
//!
//! A xmip-core-archive **technology** (repository-model.md): it depends on
//! the archive capability for the [`ArchiveStore`] trait and its item,
//! receipt and error types, and on the S3 transport technology for the
//! signed requests — Signature Version 4, path-style, one connection a
//! call. The same four fields every archive technology carries —
//! `data_type`, `identifier`, `bytes`, `metadata` — are laid out as
//! `object.rs` says: the bytes at `<prefix>/<data_type>/<identifier>`, the
//! metadata text at the same key with `.meta` appended.
//!
//! An archive never deletes (ADR-0040): this one puts and gets, nothing
//! else. The receipt is `s3://<bucket>/<key>` with the SHA-256 of the bytes
//! as its checksum, and restoring checks the bytes that come back against
//! it.

pub mod object;

use std::time::Duration;

use archive::{ArchiveError, ArchiveItem, ArchiveReceipt, ArchiveStore};
use s3::Client;

/// An archive that keeps items as objects under one prefix of one bucket.
pub struct S3Archive {
    endpoint: String,
    region: String,
    bucket: String,
    access_key: String,
    secret_key: String,
    prefix: String,
    timeout: Option<Duration>,
}

impl S3Archive {
    /// An archive writing to `bucket` at `endpoint` — `http://host:port` or
    /// `https://host:port` — in `region`, under no prefix. Any real endpoint
    /// wants [`Self::with_credentials`].
    #[must_use]
    pub fn new(
        endpoint: impl Into<String>,
        region: impl Into<String>,
        bucket: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            region: region.into(),
            bucket: bucket.into(),
            access_key: String::new(),
            secret_key: String::new(),
            prefix: String::new(),
            timeout: None,
        }
    }

    /// The access key and secret key every request is signed with.
    #[must_use]
    pub fn with_credentials(
        mut self,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        self.access_key = access_key.into();
        self.secret_key = secret_key.into();
        self
    }

    /// The prefix every key is laid out under, `retained` say.
    #[must_use]
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Give up on an endpoint that stops answering.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    fn client(&self) -> Result<Client, ArchiveError> {
        let client = Client::new(
            &self.endpoint,
            &self.region,
            &self.access_key,
            &self.secret_key,
        )
        .map_err(error)?;
        Ok(match self.timeout {
            Some(timeout) => client.timing_out_after(timeout),
            None => client,
        })
    }
}

impl ArchiveStore for S3Archive {
    fn archive(&self, item: ArchiveItem) -> Result<ArchiveReceipt, ArchiveError> {
        let key = object::key(&self.prefix, &item.data_type, &item.identifier);
        let metadata = object::encode_metadata(&item.metadata);
        let client = self.client()?;
        client.put(&self.bucket, &key, &item.bytes).map_err(error)?;
        client
            .put(&self.bucket, &object::meta_key(&key), metadata.as_bytes())
            .map_err(error)?;
        Ok(ArchiveReceipt {
            location: format!("s3://{}/{key}", self.bucket),
            checksum: Some(object::sha256_hex(&item.bytes)),
        })
    }

    fn restore(&self, receipt: &ArchiveReceipt) -> Result<ArchiveItem, ArchiveError> {
        let (bucket, key) = parse_location(&receipt.location)?;
        let (data_type, identifier) =
            object::split_key(&self.prefix, key).ok_or_else(|| ArchiveError {
                message: format!(
                    "{key} is not laid out as {}/<data_type>/<identifier>",
                    self.prefix
                ),
            })?;
        let client = self.client()?;
        let bytes = client.get(bucket, key).map_err(error)?;
        if let Some(expected) = &receipt.checksum {
            let actual = object::sha256_hex(&bytes);
            if &actual != expected {
                return Err(ArchiveError {
                    message: format!(
                        "{}: the bytes came back with checksum {actual}, not {expected}",
                        receipt.location
                    ),
                });
            }
        }
        let metadata = client.get(bucket, &object::meta_key(key)).map_err(error)?;
        let metadata = String::from_utf8(metadata).map_err(error)?;
        Ok(ArchiveItem {
            data_type,
            identifier,
            bytes,
            metadata: object::decode_metadata(&metadata),
        })
    }
}

/// The bucket and key a receipt names: `s3://<bucket>/<key>`.
fn parse_location(location: &str) -> Result<(&str, &str), ArchiveError> {
    location
        .strip_prefix("s3://")
        .and_then(|rest| rest.split_once('/'))
        .filter(|(bucket, key)| !bucket.is_empty() && !key.is_empty())
        .ok_or_else(|| ArchiveError {
            message: format!("{location} is not s3://bucket/key"),
        })
}

fn error(cause: impl std::fmt::Display) -> ArchiveError {
    ArchiveError {
        message: cause.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s3::{Event, Session};
    use std::net::TcpListener;
    use std::thread::JoinHandle;

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn item(id: &str) -> ArchiveItem {
        ArchiveItem {
            data_type: "json".to_string(),
            identifier: id.to_string(),
            bytes: b"{\"kept\":true}".to_vec(),
            metadata: vec![("source".to_string(), "playground".to_string())],
        }
    }

    /// A far end that answers `requests` signed requests, one connection
    /// each, and then hands back what it holds and what it saw.
    fn far_end(requests: usize) -> (String, JoinHandle<(Session, Vec<Event>)>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let address = listener.local_addr().expect("address").to_string();
        let handle = std::thread::spawn(move || {
            let mut session =
                Session::new("eu-north-1", "AKID", "secret").timing_out_after(secs(2));
            let events = (0..requests)
                .map(|_| session.serve_one(&listener).expect("served"))
                .collect();
            (session, events)
        });
        (address, handle)
    }

    fn store(address: &str) -> S3Archive {
        S3Archive::new(format!("http://{address}"), "eu-north-1", "orders-archive")
            .with_credentials("AKID", "secret")
            .with_prefix("retained")
            .timing_out_after(secs(2))
    }

    #[test]
    fn an_archived_item_is_two_objects_and_the_receipt_names_the_first() {
        let (address, far_end) = far_end(2);
        let original = item("json#1");
        let receipt = store(&address).archive(original.clone()).expect("archive");
        assert_eq!(receipt.location, "s3://orders-archive/retained/json/json#1");
        assert_eq!(
            receipt.checksum.as_deref(),
            Some(object::sha256_hex(&original.bytes).as_str())
        );
        let (session, events) = far_end.join().expect("thread");
        let held = session.objects();
        assert_eq!(held.len(), 2);
        assert_eq!(
            held.get("orders-archive/retained/json/json#1"),
            Some(&original.bytes)
        );
        assert_eq!(
            held.get("orders-archive/retained/json/json#1.meta"),
            Some(&b"source\x1fplayground".to_vec())
        );
        assert!(matches!(&events[0], Event::Stored(arrived)
            if arrived.origin_uri == "s3://orders-archive/retained/json/json#1"));
    }

    #[test]
    fn the_objects_restore_the_item_through_the_same_session() {
        let (address, far_end) = far_end(4);
        let store = store(&address);
        let original = item("json#2");
        let receipt = store.archive(original.clone()).expect("archive");
        let restored = store.restore(&receipt).expect("restore");
        assert_eq!(restored, original, "both objects read back give the item");
        let (_, events) = far_end.join().expect("thread");
        assert_eq!(
            events[2],
            Event::Retrieved("s3://orders-archive/retained/json/json#2".to_string())
        );
        assert_eq!(
            events[3],
            Event::Retrieved("s3://orders-archive/retained/json/json#2.meta".to_string())
        );
    }

    #[test]
    fn a_missing_object_a_changed_checksum_and_a_foreign_receipt_are_refused() {
        let (address, far_end) = far_end(4);
        let store = store(&address);
        let mut receipt = store.archive(item("json#3")).expect("archive");
        receipt.checksum = Some("0".repeat(64));
        let changed = store.restore(&receipt).expect_err("checksum");
        assert!(changed.message.contains("not 0000"), "{changed}");
        receipt.location = "s3://orders-archive/retained/json/none".to_string();
        receipt.checksum = None;
        let missing = store.restore(&receipt).expect_err("missing");
        assert!(missing.message.contains("404 NoSuchKey"), "{missing}");
        far_end.join().expect("thread");
        for location in [
            "gcs://bucket/key",
            "s3://bucket",
            "s3://orders-archive/elsewhere/n",
        ] {
            receipt.location = location.to_string();
            assert!(store.restore(&receipt).is_err(), "{location}");
        }
    }
}
