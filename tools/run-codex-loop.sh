#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
sentinel="${CODEX_LOOP_SENTINEL:-/tmp/hugegraph-rust-rewrite-go}"
prompt="继续原目标。不要询问是否继续，不要只汇报状态。只要还有可执行工作，就立刻修改、测试、归档证据、提交或推送。不要调用 blocked。只有 Go/No-Go 清单明确为 GO 后，创建 ${sentinel} 并停止。"
lock_file="${CODEX_LOOP_LOCK:-/tmp/hugegraph-rust-rewrite-codex.lock}"
log_file="${CODEX_LOOP_LOG:-${repo_dir}/docs/evidence/rust-rewrite/codex-loop.log}"

cd "$repo_dir" || exit 1
command -v codex >/dev/null || { echo "codex CLI not found" >&2; exit 127; }
exec 9>"$lock_file"
flock -n 9 || { echo "another Codex loop is already running: $lock_file" >&2; exit 2; }
while [[ ! -e "$sentinel" ]]; do
    date -u +"%Y-%m-%dT%H:%M:%SZ loop-start" | tee -a "$log_file"
    codex resume --last \
        --dangerously-bypass-approvals-and-sandbox \
        "$prompt" 2>&1 | tee -a "$log_file"
    status=$?
    [[ -e "$sentinel" ]] && break
    echo "codex session exited with status $status; resuming in 2s" >&2
    sleep 2
done

echo "codex loop finished: $sentinel exists"
