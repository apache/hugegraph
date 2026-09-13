// Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
// See the NOTICE file distributed with this work for additional information.
// The ASF licenses this file to you under the Apache License, Version 2.0.
// http://www.apache.org/licenses/LICENSE-2.0
use anyhow::{Context, Result};
use rocksdb::{IteratorMode, DB};
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
    let db = DB::open_default(path).with_context(|| format!("open RocksDB {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut entries = 0;
    for item in db.iterator(IteratorMode::Start) {
        let (key, value) = item?;
        entries += 1;
        hasher.update((key.len() as u64).to_be_bytes());
        hasher.update(&key);
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(&value);
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
}
