#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
# See the NOTICE file distributed with this work for additional information.
# The ASF licenses this file under the Apache License, Version 2.0.
# http://www.apache.org/licenses/LICENSE-2.0
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
manifest="$root/tools/rust-partition-poc/Cargo.toml"
cargo fmt --manifest-path "$manifest" -- --check
cargo clippy --manifest-path "$manifest" --all-targets -- -D warnings
cargo test --manifest-path "$manifest"
