#!/bin/bash
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
# Run only after stopping this component. Runtime preparation never changes data.
set -Eeuo pipefail
TOP="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[ "$(uname -s)/$(uname -m)" = Linux/x86_64 ] || {
    echo 'Error: Topling requires Linux x86_64' >&2; exit 1;
}
[[ "${TOPLING_JNI_JAR:-}" = /* ]] && [ -r "$TOPLING_JNI_JAR" ] || {
    echo 'Error: TOPLING_JNI_JAR must be an absolute readable file' >&2; exit 1;
}
[[ "${TOPLING_JNI_SHA256:-}" =~ ^[[:xdigit:]]{64}$ ]] || {
    echo 'Error: TOPLING_JNI_SHA256 must contain 64 hexadecimal characters' >&2; exit 1;
}
[ ! -e "$TOP/topling" ] && [ ! -L "$TOP/topling" ] || {
    echo 'Error: runtime already prepared; use a fresh stopped distribution to replace it' >&2; exit 1;
}
staging=$(mktemp -d "$TOP/.topling-prepare.XXXXXX")
trap 'rm -rf "$staging"' EXIT
cp "$TOPLING_JNI_JAR" "$staging/rocksdbjni.jar"
expected=$(printf '%s' "$TOPLING_JNI_SHA256" | tr 'A-F' 'a-f')
actual=$(sha256sum "$staging/rocksdbjni.jar" | awk '{print $1}')
[ "$actual" = "$expected" ] || { echo 'Error: Topling JNI SHA-256 mismatch' >&2; exit 1; }
entries=$(unzip -Z1 "$staging/rocksdbjni.jar")
[ "$(grep -cx 'org/rocksdb/SidePluginRepo.class' <<< "$entries" || true)" = 1 ] &&
[ "$(grep -cx 'librocksdbjni-linux64.so' <<< "$entries" || true)" = 1 ] &&
[ "$(grep -Ec '(^|/)librocksdbjni-linux64[.]so$' <<< "$entries" || true)" = 1 ] || {
    echo 'Error: expected Topling Java marker and exactly one Linux x86_64 JNI entry' >&2; exit 1;
}
unzip -p "$staging/rocksdbjni.jar" librocksdbjni-linux64.so > "$staging/librocksdbjni-linux64.so"
header=$(od -An -tx1 -N20 "$staging/librocksdbjni-linux64.so" | tr -d ' \n')
[[ "$header" = 7f454c460201* ]] && [ "${header:36:4}" = 3e00 ] || {
    echo 'Error: JNI is not a little-endian Linux x86_64 ELF library' >&2; exit 1;
}
# Ubuntu's t64 transition renamed libaio; keep compatibility inside this component.
if [ ! -e /usr/lib/x86_64-linux-gnu/libaio.so.1 ] &&
   [ -e /usr/lib/x86_64-linux-gnu/libaio.so.1t64 ]; then
    ln -s /usr/lib/x86_64-linux-gnu/libaio.so.1t64 "$staging/libaio.so.1"
fi
ldd_output=$(LD_LIBRARY_PATH="$staging${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
             ldd "$staging/librocksdbjni-linux64.so")
if grep -q 'not found' <<< "$ldd_output"; then
    printf 'Error: install the missing native dependencies first:\n%s\n' "$ldd_output" >&2
    exit 1
fi
# Verify EasyMigrate behavior in a disposable DB before touching business data.
# Class markers and hashes identify a library, but do not prove this capability.
cat > "$staging/probe.yaml" <<'YAML'
http: {auto_start_http: false}
CFOptions:
  default:
    write_buffer_size: 17M
DBOptions:
  default:
    create_if_missing: true
    memtable_as_log_index: false
YAML
cat > "$staging/ToplingRuntimeProbe.java" <<'JAVA'
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

class ToplingRuntimeProbe {
    public static void main(String[] args) throws Exception {
        if (!"17".equals(System.getProperty("java.specification.version"))) {
            throw new IllegalStateException("Prepare Topling with Java 17");
        }
        RocksDB.loadLibrary();
        Path database = Path.of(args[0]);
        try (Options options = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(options, database.toString())) {
            db.put(new byte[]{1}, new byte[]{2});
            if (db.get(new byte[]{1})[0] != 2) {
                throw new IllegalStateException("Topling capability probe could not read its write");
            }
        }
        Pattern configured = Pattern.compile("(?m)^\\s*write_buffer_size\\s*=\\s*17825792\\s*$");
        boolean applied = false;
        try (var paths = Files.list(database)) {
            for (Path path : paths.filter(p -> p.getFileName().toString().startsWith("OPTIONS-")).toList()) {
                applied |= configured.matcher(Files.readString(path)).find();
            }
        }
        if (!applied) {
            throw new IllegalStateException("Selected JNI does not apply EasyMigrate configuration");
        }
    }
}
JAVA
if ! LD_LIBRARY_PATH="$staging${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
     LD_PRELOAD="$staging/librocksdbjni-linux64.so" \
     TOPLINGDB_EASY_MIGRATE_CONF="$staging/probe.yaml" \
     java --add-opens=java.base/java.nio=ALL-UNNAMED --source 17 \
     --class-path "$staging/rocksdbjni.jar" "$staging/ToplingRuntimeProbe.java" "$staging/probe-db"; then
    echo 'Error: Java 17 JDK / Topling EasyMigrate capability probe failed' >&2
    exit 1
fi
native_hash=$(sha256sum "$staging/librocksdbjni-linux64.so" | awk '{print $1}')
printf '%s  rocksdbjni.jar\n%s  librocksdbjni-linux64.so\n' "$actual" "$native_hash" > "$staging/easy-migrate.sha256"
rm -rf "$staging/probe-db" "$staging/probe.yaml" "$staging/ToplingRuntimeProbe.java"
mkdir "$staging/rocksdb_resource"
status=0
unzip -j "$staging/rocksdbjni.jar" '*.html' '*.css' -d "$staging/rocksdb_resource" >/dev/null 2>&1 || status=$?
[ "$status" = 0 ] || [ "$status" = 11 ] || { echo 'Error: cannot extract native web resources' >&2; exit 1; }
# Publish readable runtime assets for a service UID different from the preparer.
find "$staging" -type d -exec chmod 0755 {} +
find "$staging" -type f -exec chmod 0644 {} +
mv -T "$staging" "$TOP/topling"
echo "Prepared component-local Topling runtime: $TOP/topling"
