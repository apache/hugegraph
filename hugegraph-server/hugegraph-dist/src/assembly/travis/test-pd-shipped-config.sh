#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Every PD configuration that ships in the archive or the jar must carry the
# same REST hardening: no wildcard actuator exposure (that path is anonymous),
# an auth.secret-key that is present and empty, and no copy of the secret that
# earlier revisions published. A fix applied to one variant and not the others
# is what this catches.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../.." && pwd)"
PUBLISHED_SECRET='FXQXbJtbCLxODc6tGci732pkH1cyf8Qg'
FAIL=0

check() {
    local file="$1" rel="${1#"${ROOT}/"}" before="$FAIL"
    [[ -f "$file" ]] || { echo "  FAIL ${rel}: missing"; FAIL=1; return; }

    # The actuator exposure specifically: a config that grows an unrelated
    # include: above this block must not satisfy the check by accident.
    local exposure
    exposure=$(awk '/^[[:space:]]*exposure:/ {found = 1; next}
                    found && /^[[:space:]]*include:/ {sub(/^[[:space:]]*include:[[:space:]]*/, ""); print; exit}' "$file")
    if [[ "$exposure" == *'*'* ]]; then
        echo "  FAIL ${rel}: actuator exposure is a wildcard (${exposure})"; FAIL=1
    fi
    if ! grep -qE '^[[:space:]]*secret-key:[[:space:]]*$' "$file"; then
        echo "  FAIL ${rel}: auth.secret-key must be present and empty"; FAIL=1
    fi
    if grep -q "${PUBLISHED_SECRET}" "$file"; then
        echo "  FAIL ${rel}: contains the published secret"; FAIL=1
    fi
    # Only when nothing above raised FAIL, or the file contradicts its own report
    if [[ "$FAIL" == "$before" ]]; then
        echo "  ok   ${rel}"
    fi
}

echo "PD shipped configuration hardening"
for f in "${ROOT}"/hugegraph-pd/hg-pd-dist/src/assembly/static/conf/application.yml* \
         "${ROOT}"/hugegraph-pd/hg-pd-service/src/main/resources/application.yml; do
    check "$f"
done
[[ "$FAIL" -eq 0 ]] && echo "all shipped PD configs pass" || { echo "shipped PD config check failed"; exit 1; }
