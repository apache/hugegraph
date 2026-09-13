#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
python3 "$ROOT/test_checker.py"
python3 "$ROOT/test_capture_history.py"
