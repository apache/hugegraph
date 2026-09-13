#!/usr/bin/env bash
set -euo pipefail
state="${CODEX_SESSION_POC_STATE:-/tmp/hugegraph-codex-session-poc.json}"
prompt='只输出 SESSION_POC_READY，不修改文件。'
if [[ ! -s "$state" ]]; then
  echo '=== create ==='
  codex exec --json "$prompt" | tee "$state"
else
  echo '=== resume ==='
  cat "$state"
  session_id="$(python3 - "$state" <<'PY'
import json, sys
for line in open(sys.argv[1]):
    try:
        x=json.loads(line)
    except Exception:
        continue
    if x.get('type') in ('thread.started','session.started'):
        print(x.get('thread_id') or x.get('session_id')); break
PY
)"
  [[ -n "$session_id" ]] || { echo 'No session ID found' >&2; exit 2; }
  codex exec resume "$session_id" --json '继续同一会话，只输出 SESSION_POC_RESUMED，不修改文件。'
fi
