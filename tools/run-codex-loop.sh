#!/usr/bin/env bash
set -u

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
sentinel="${CODEX_LOOP_SENTINEL:-/tmp/hugegraph-rust-rewrite-go}"
prompt='继续原目标。不要询问是否继续，不要只汇报状态。只要还有可执行工作，就立刻修改、测试、归档证据、提交或推送。不要调用 blocked。只有 Go/No-Go 清单明确为 GO 后，创建 /tmp/hugegraph-rust-rewrite-go 并停止。'

cd "$repo_dir" || exit 1
while [[ ! -e "$sentinel" ]]; do
    codex resume --last \
        --dangerously-bypass-approvals-and-sandbox \
        "$prompt"
    status=$?
    [[ -e "$sentinel" ]] && break
    echo "codex session exited with status $status; resuming in 2s" >&2
    sleep 2
done

echo "codex loop finished: $sentinel exists"
