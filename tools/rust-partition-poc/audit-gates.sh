#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with this
# work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0.
# See the License for the specific language governing permissions and
# limitations under the License.
# http://www.apache.org/licenses/LICENSE-2.0

set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
workflow="$root/.github/workflows/rust-partition-poc.yml"
for cmd in 'cargo fmt -- --check' 'cargo clippy --all-targets -- -D warnings' 'cargo test --locked' 'cargo mutants --exclude src/main.rs --timeout 30 --jobs 2'; do
  grep -Fq "$cmd" "$workflow" || { echo "missing CI gate: $cmd" >&2; exit 1; }
done
printf 'workflow=%s\n' "$workflow"
printf 'gates=fmt,clippy,test,mutation\n'
