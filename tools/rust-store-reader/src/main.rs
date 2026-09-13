// Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
// See the NOTICE file distributed with this work for additional information.
// The ASF licenses this file to You under the Apache License, Version 2.0.
// http://www.apache.org/licenses/LICENSE-2.0
use anyhow::{Context, Result};
use rocksdb::{IteratorMode, DB};
use sha2::{Digest, Sha256};
use std::{env, path::Path};
fn main() -> Result<()> {
    let path = env::args()
        .nth(1)
        .context("usage: hugegraph-store-reader <rocksdb-path>")?;
    let db = DB::open_default(Path::new(&path)).with_context(|| format!("open RocksDB {path}"))?;
    let mut count = 0u64;
    let mut digest = Sha256::new();
    for item in db.iterator(IteratorMode::Start) {
        let (key, value) = item?;
        count += 1;
        digest.update((key.len() as u64).to_be_bytes());
        digest.update(&key);
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(&value);
    }
    println!(
        "{{\"path\":{path:?},\"default_column_family_entries\":{count},\"sha256\":\"{}\"}}",
        hex::encode(digest.finalize())
    );
    Ok(())
}
