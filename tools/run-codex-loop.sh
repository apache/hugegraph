#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
sentinel="${CODEX_LOOP_SENTINEL:-/tmp/hugegraph-rust-rewrite-go}"
prompt="继续原目标。先读取 git status、最近提交和 docs/evidence 中的进度记录；不要覆盖或撤销已有未提交修改。不要询问是否继续，不要只汇报状态。只要还有可执行工作，就立刻修改、测试、归档证据、提交或推送。不要调用 blocked。只有本地契约覆盖、核心场景覆盖、独立 Oracle 和变异验证全部完成后，创建 ${sentinel} 并停止。"
lock_file="${CODEX_LOOP_LOCK:-/tmp/hugegraph-rust-rewrite-codex.lock}"
log_file="${CODEX_LOOP_LOG:-${repo_dir}/docs/evidence/rust-rewrite/codex-loop.log}"
max_iterations="${CODEX_LOOP_MAX_ITERATIONS:-20}"
max_failures="${CODEX_LOOP_MAX_FAILURES:-3}"
pause_file="${CODEX_LOOP_PAUSE_FILE:-/tmp/hugegraph-rust-rewrite-codex.pause}"
iteration=0
failures=0

cd "$repo_dir" || exit 1
command -v codex >/dev/null || { echo "codex CLI not found" >&2; exit 127; }
exec 9>"$lock_file"
flock -n 9 || { echo "another Codex loop is already running: $lock_file" >&2; exit 2; }
while [[ ! -e "$sentinel" ]]; do
    while [[ -e "$pause_file" ]]; do
        echo "codex loop paused: $pause_file exists" | tee -a "$log_file"
        sleep 5
    done
    ((++iteration))
    if (( iteration > max_iterations )); then
        echo "codex loop stopped after $max_iterations iterations; review log: $log_file" | tee -a "$log_file" >&2
        exit 3
    fi
    date -u +"%Y-%m-%dT%H:%M:%SZ loop-start" | tee -a "$log_file"
    # A live interactive session owns the latest thread, so resuming it from a
    # second process causes a thread-store conflict. Start an independent,
    # non-interactive worker for each iteration instead.
    set +e
    codex exec \
        --dangerously-bypass-approvals-and-sandbox \
        "$prompt" 2>&1 | tee -a "$log_file"
    status=${PIPESTATUS[0]}
    set -e
    if tail -30 "$log_file" 2>/dev/null | grep -q '401 Unauthorized'; then
        echo "codex loop stopped: background Codex authentication failed (401 Unauthorized)" | tee -a "$log_file" >&2
        exit 4
    fi
    if (( status == 0 )); then
        failures=0
    else
        ((++failures))
        if (( failures >= max_failures )); then
            echo "codex loop stopped after $failures consecutive failures" | tee -a "$log_file" >&2
            exit 5
        fi
    fi
    [[ -e "$sentinel" ]] && break
    echo "codex session exited with status $status; resuming in 2s" >&2
    sleep $((2 ** failures))
done

echo "codex loop finished: $sentinel exists"
