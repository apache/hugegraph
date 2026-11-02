#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Exercise preparation publication/failure with process fixtures. Real JNI
# compatibility requires a separate native read/write smoke test.
set -Eeuo pipefail
source_bin="$(cd "$(dirname "${BASH_SOURCE[0]}")/../static/bin" && pwd)"
test_root=$(mktemp -d)
trap 'rm -rf "$test_root"' EXIT
mkdir -p "$test_root/tools"
python3 - "$test_root/jni.jar" <<'PY'
import sys
import zipfile

header = bytearray(20)
header[:6] = b'\x7fELF\x02\x01'
header[18:20] = b'\x3e\x00'
with zipfile.ZipFile(sys.argv[1], 'w') as jar:
    jar.writestr('org/rocksdb/SidePluginRepo.class', b'fixture')
    jar.writestr('librocksdbjni-linux64.so', header)
PY
cat > "$test_root/tools/ldd" <<'SH'
#!/bin/sh
exit 0
SH
cat > "$test_root/tools/uname" <<'SH'
#!/bin/sh
case "$1" in -s) echo Linux ;; -m) echo x86_64 ;; *) exit 1 ;; esac
SH
cat > "$test_root/tools/java" <<'SH'
#!/bin/bash
set -eu
profile="$TOPLINGDB_EASY_MIGRATE_CONF"
if [[ "${*: -2:1}" == --profile ]]; then
    profile="${@: -1}"
fi
printf '%s\n' "$profile" >> "$PROBE_LOG"
printf '%s\n' "$TOPLINGDB_EASY_MIGRATE_CONF" >> "$PROBE_ENV_LOG"
if grep -q 'missing-plugin' "$profile"; then
    exit 1
fi
SH
chmod +x "$test_root/tools/"*
export PATH="$test_root/tools:$PATH"
export TOPLING_JNI_JAR="$test_root/jni.jar"
TOPLING_JNI_SHA256=$(sha256sum "$TOPLING_JNI_JAR" | awk '{print $1}')
export TOPLING_JNI_SHA256
unset TOPLINGDB_EASY_MIGRATE_CONF

for scenario in default invalid override relative cdpath symlink; do
    component="$test_root/$scenario"
    mkdir -p "$component/bin" "$component/conf"
    cp "$source_bin/prepare-topling.sh" "$component/bin/"
    printf '%s\n' 'http: {auto_start_http: false}' > "$component/conf/toplingdb.yaml"
    export PROBE_LOG="$component/probes"
    export PROBE_ENV_LOG="$component/probe-env"
    unset TOPLINGDB_EASY_MIGRATE_CONF
    if [[ "$scenario" != default ]]; then
        printf '%s\n' 'missing-plugin' > "$component/conf/toplingdb.yaml"
    fi
    if [[ "$scenario" == override || "$scenario" == relative ||
          "$scenario" == cdpath || "$scenario" == symlink ]]; then
        printf '%s\n' 'http: {auto_start_http: false}' > "$component/custom.yaml"
        export TOPLINGDB_EASY_MIGRATE_CONF="$component/custom.yaml"
    fi
    if [[ "$scenario" == relative || "$scenario" == cdpath ]]; then
        export TOPLINGDB_EASY_MIGRATE_CONF="$scenario/custom.yaml"
    fi
    if [[ "$scenario" == symlink ]]; then
        mkdir -p "$component/physical/child"
        ln -s "$component/physical/child" "$component/alias"
        cp "$component/custom.yaml" "$component/physical/custom.yaml"
        printf '%s\n' 'missing-plugin' > "$component/custom.yaml"
        export TOPLINGDB_EASY_MIGRATE_CONF="$component/alias/../custom.yaml"
    fi
    script="$component/bin/prepare-topling.sh"
    fixture_cdpath=""
    if [[ "$scenario" == cdpath ]]; then
        fixture_cdpath=.
        script="$scenario/bin/prepare-topling.sh"
    fi
    status=0
    (cd "$test_root" && CDPATH="$fixture_cdpath" bash "$script") > "$component/output" 2>&1 || status=$?
    # Both the minimal capability and selected profile must be probed.
    [[ $(wc -l < "$PROBE_LOG") == 2 ]]
    capability=$(head -1 "$PROBE_LOG")
    [[ "$capability" == "$component"/.topling-prepare.*/probe.yaml ]]
    # Both JVMs initialize under disposable options. Only the importer's
    # explicit --profile argument names the production configuration.
    [[ $(head -1 "$PROBE_ENV_LOG") == "$capability" ]]
    [[ $(tail -1 "$PROBE_ENV_LOG") == "$capability" ]]
    if [[ "$scenario" == invalid ]]; then
        [[ "$status" != 0 && ! -e "$component/topling" ]]
        grep -q 'configuration validation failed' "$component/output"
    else
        [[ "$status" == 0 ]]
        (cd "$component/topling" && sha256sum -c easy-migrate.sha256 >/dev/null)
        [[ ! -e "$component/topling/ToplingRuntimeProbe.java" ]]
        expected="${TOPLINGDB_EASY_MIGRATE_CONF:-$component/conf/toplingdb.yaml}"
        case "$scenario" in
            relative | cdpath) expected="$component/custom.yaml" ;;
            symlink) expected="$component/physical/custom.yaml" ;;
        esac
        [[ $(tail -1 "$PROBE_LOG") == "$expected" ]]
    fi
    [[ -z $(find "$component" -maxdepth 1 -name '.topling-prepare.*' -print -quit) ]]
done
echo 'Topling capability/profile preparation contract passed'
