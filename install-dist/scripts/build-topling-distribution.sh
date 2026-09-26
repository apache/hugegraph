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

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR"/../.. && pwd)"
COMPONENT="${1:-}"
VERSION="${2:-}"

if [ -z "$COMPONENT" ]; then
    echo "Usage: $0 <server|pd|store> [version]" >&2
    exit 1
fi
if [ "$(uname -s)" != "Linux" ] || [ "$(uname -m)" != "x86_64" ]; then
    echo "Error: Topling distributions can only be prepared on Linux x86_64" >&2
    exit 1
fi
if [ -z "$VERSION" ]; then
    VERSION=$(mvn -f "$PROJECT_ROOT/pom.xml" \
                  help:evaluate -Dexpression=project.version -q -DforceStdout)
fi
if [[ ! "$VERSION" =~ ^[0-9A-Za-z][0-9A-Za-z._-]*$ ]]; then
    echo "Error: unsafe project version: $VERSION" >&2
    exit 1
fi

case "$COMPONENT" in
    server)
        COMPONENT_PARENT="$PROJECT_ROOT/hugegraph-server"
        STANDARD_NAME="apache-hugegraph-server-$VERSION"
        CONFIG_FILE="conf/graphs/hugegraph.properties"
        ;;
    pd)
        COMPONENT_PARENT="$PROJECT_ROOT/hugegraph-pd"
        STANDARD_NAME="apache-hugegraph-pd-$VERSION"
        CONFIG_FILE="conf/application.yml"
        ;;
    store)
        COMPONENT_PARENT="$PROJECT_ROOT/hugegraph-store"
        STANDARD_NAME="apache-hugegraph-store-$VERSION"
        CONFIG_FILE="conf/application-pd.yml"
        ;;
    *)
        echo "Error: unsupported component '$COMPONENT'" >&2
        exit 1
        ;;
esac

STANDARD_DIR="$COMPONENT_PARENT/$STANDARD_NAME"
TOPLING_NAME="$STANDARD_NAME-topling"
TOPLING_DIR="$COMPONENT_PARENT/$TOPLING_NAME"
TOPLING_TAR="$COMPONENT_PARENT/$TOPLING_NAME.tar.gz"
TOPLING_SOURCE_LIB="$PROJECT_ROOT/hugegraph-server/hugegraph-dist/src/assembly/static/lib/topling"
TOPLING_JARS=("$TOPLING_SOURCE_LIB"/rocksdbjni*.jar)

if [ ! -d "$STANDARD_DIR" ]; then
    echo "Error: standard distribution not found: $STANDARD_DIR" >&2
    exit 1
fi
if ! command -v rsync >/dev/null 2>&1; then
    echo "Error: rsync is required to build a clean Topling distribution" >&2
    exit 1
fi
if [ "${#TOPLING_JARS[@]}" -ne 1 ] || [ ! -f "${TOPLING_JARS[0]}" ]; then
    echo "Error: expected exactly one ToplingDB JAR under: $TOPLING_SOURCE_LIB" >&2
    exit 1
fi

STAGING_ROOT="$(mktemp -d "$COMPONENT_PARENT/.topling-dist.XXXXXX")"
cleanup() {
    rm -rf "$STAGING_ROOT"
}
trap cleanup EXIT
STAGING_DIR="$STAGING_ROOT/$TOPLING_NAME"

mkdir -p "$STAGING_DIR"
rsync --archive \
      --exclude='/bin/pid' \
      --exclude='/library' \
      --exclude='/logs' \
      --exclude='/pd_data' \
      --exclude='/rocksdb-data' \
      --exclude='/storage' \
      "$STANDARD_DIR/" "$STAGING_DIR/"

for runtime_path in \
    bin/pid \
    library \
    logs \
    pd_data \
    rocksdb-data \
    storage; do
    if [ -e "$STAGING_DIR/$runtime_path" ] ||
       [ -L "$STAGING_DIR/$runtime_path" ]; then
        echo "Error: runtime state leaked into staging: $runtime_path" >&2
        exit 1
    fi
done

mkdir -p "$STAGING_DIR/lib/topling"
cp "${TOPLING_JARS[0]}" "$STAGING_DIR/lib/topling/"

case "$COMPONENT" in
    server)
        sed -i '/^[[:space:]#]*rocksdb\.provider[[:space:]]*=/d' \
            "$STAGING_DIR/$CONFIG_FILE"
        printf '\nrocksdb.provider=topling\n' >> "$STAGING_DIR/$CONFIG_FILE"
        ;;
    pd)
        sed -i \
            -e 's/^# rocksdb:/rocksdb:/' \
            -e 's/^#   provider: rocksdb/  provider: topling/' \
            "$STAGING_DIR/$CONFIG_FILE"
        ;;
    store)
        sed -i 's/^  # provider: topling/  provider: topling/' \
            "$STAGING_DIR/$CONFIG_FILE"
        ;;
esac

if ! grep -Eq '^[[:space:]]*(rocksdb\.provider=|provider:[[:space:]]*)topling([[:space:]]|$)' \
        "$STAGING_DIR/$CONFIG_FILE"; then
    echo "Error: failed to select ToplingDB in $STAGING_DIR/$CONFIG_FILE" >&2
    exit 1
fi

"$STAGING_DIR/bin/prepare-topling.sh"

NATIVE_LIBRARY="$STAGING_DIR/library/librocksdbjni-linux64.so"
if [ ! -r "$NATIVE_LIBRARY" ]; then
    echo "Error: prepared native library not found: $NATIVE_LIBRARY" >&2
    exit 1
fi
if command -v ldd >/dev/null 2>&1 &&
   ldd "$NATIVE_LIBRARY" 2>/dev/null | grep -q 'not found'; then
    echo "Error: prepared ToplingDB native library has unresolved dependencies" >&2
    ldd "$NATIVE_LIBRARY" >&2 || true
    exit 1
fi

JAR_SHA256=$(sha256sum "${TOPLING_JARS[0]}" | awk '{ print $1 }')
printf '%s\n' \
    "provider=topling" \
    "component=$COMPONENT" \
    "version=$VERSION" \
    "jar=$(basename "${TOPLING_JARS[0]}")" \
    "jar.sha256=$JAR_SHA256" \
    > "$STAGING_DIR/lib/topling/runtime.properties"

tar -czf "$STAGING_ROOT/$TOPLING_NAME.tar.gz" \
    -C "$STAGING_ROOT" "$TOPLING_NAME"

if [ -e "$TOPLING_DIR" ] &&
   [[ "$TOPLING_DIR" != "$COMPONENT_PARENT"/apache-hugegraph-*-topling ]]; then
    echo "Error: refusing to replace unexpected path: $TOPLING_DIR" >&2
    exit 1
fi
rm -rf "$TOPLING_DIR"
rm -f "$TOPLING_TAR"
mv "$STAGING_DIR" "$TOPLING_DIR"
mv "$STAGING_ROOT/$TOPLING_NAME.tar.gz" "$TOPLING_TAR"

echo "[build-topling-distribution] Directory: $TOPLING_DIR"
echo "[build-topling-distribution] Archive: $TOPLING_TAR"
