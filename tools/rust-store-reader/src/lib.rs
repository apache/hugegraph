// Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
// See the NOTICE file distributed with this work for additional information.
// The ASF licenses this file to you under the Apache License, Version 2.0.
// http://www.apache.org/licenses/LICENSE-2.0
use anyhow::{Context, Result};
use rocksdb::{IteratorMode, Options, DB};
use sha2::{Digest, Sha256};
use std::path::Path;

#[derive(Debug, Eq, PartialEq)]
pub struct StoreDigest {
    pub entries: u64,
    pub sha256: String,
}

/// Computes a deterministic digest while leaving the database untouched.
pub fn digest(path: impl AsRef<Path>) -> Result<StoreDigest> {
    let path = path.as_ref();
    let mut options = Options::default();
    options.create_if_missing(false);
    let families = DB::list_cf(&options, path)
        .with_context(|| format!("list column families in {}", path.display()))?;
    let descriptors = families
        .iter()
        .map(|name| rocksdb::ColumnFamilyDescriptor::new(name, Options::default()));
    let db = DB::open_cf_descriptors(&options, path, descriptors)
        .with_context(|| format!("open RocksDB {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut entries = 0;
    for family in families {
        let handle = db.cf_handle(&family).context("column family handle")?;
        hasher.update((family.len() as u64).to_be_bytes());
        hasher.update(family.as_bytes());
        for item in db.iterator_cf(handle, IteratorMode::Start) {
            let (key, value) = item?;
            entries += 1;
            hasher.update((key.len() as u64).to_be_bytes());
            hasher.update(&key);
            hasher.update((value.len() as u64).to_be_bytes());
            hasher.update(&value);
        }
    }
    Ok(StoreDigest {
        entries,
        sha256: hex::encode(hasher.finalize()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_path_is_error() {
        assert!(digest("/definitely/missing/hugegraph-db").is_err());
    }

    #[test]
    fn digest_is_stable_for_fixture() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        db.put(b"k1", b"v1").unwrap();
        db.put(b"k2", b"v2").unwrap();
        drop(db);
        let first = digest(dir.path()).unwrap();
        let second = digest(dir.path()).unwrap();
        assert_eq!(first.entries, 2);
        assert_eq!(first, second);
    }
}
