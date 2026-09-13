#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
workflow="$root/.github/workflows/rust-partition-poc.yml"
for cmd in 'cargo fmt -- --check' 'cargo clippy --all-targets -- -D warnings' 'cargo test --locked' 'cargo mutants --exclude src/main.rs --timeout 30 --jobs 2'; do
  grep -Fq "$cmd" "$workflow" || { echo "missing CI gate: $cmd" >&2; exit 1; }
done
printf 'workflow=%s\n' "$workflow"
printf 'gates=fmt,clippy,test,mutation\n'
