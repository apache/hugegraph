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

set -euo pipefail

require_env() {
    local name="$1"
    if [[ -z "${!name:-}" ]]; then
        echo "ERROR: missing required env '${name}'" >&2
        exit 2
    fi
}

json_escape() {
    local s="$1"
    s=${s//\\/\\\\}
    s=${s//\"/\\\"}
    s=${s//$'\n'/}
    printf "%s" "$s"
}

is_truthy() {
    case "${1:-}" in
        1|[Tt][Rr][Uu][Ee]|[Yy]|[Yy][Ee][Ss]|[Oo][Nn]) return 0 ;;
        *) return 1 ;;
    esac
}

require_env "HG_STORE_PD_ADDRESS"
require_env "HG_STORE_GRPC_HOST"
require_env "HG_STORE_RAFT_ADDRESS"

: "${HG_STORE_GRPC_PORT:=8500}"
: "${HG_STORE_REST_PORT:=8520}"
: "${HG_STORE_DATA_PATH:=/hugegraph-store/storage}"
: "${HG_CLOUD_STORAGE_PROVIDER:=s3}"
: "${HG_CLOUD_STORAGE_ENABLED:=true}"
: "${HG_CLOUD_STORAGE_BUCKET:=hugegraph-store}"
: "${HG_CLOUD_STORAGE_REGION:=us-east-1}"
: "${HG_CLOUD_STORAGE_ENDPOINT:=http://minio:9000}"
: "${HG_CLOUD_STORAGE_ACCESS_KEY:=minioadmin}"
: "${HG_CLOUD_STORAGE_SECRET_KEY:=minioadmin}"
: "${HG_CLOUD_STORAGE_PATH_PREFIX:=hugegraph}"

export SPRING_APPLICATION_JSON="$(cat <<JSON
{
  "pdserver": { "address": "$(json_escape "${HG_STORE_PD_ADDRESS}")" },
  "grpc": {
    "host": "$(json_escape "${HG_STORE_GRPC_HOST}")",
    "port": "$(json_escape "${HG_STORE_GRPC_PORT}")"
  },
  "raft": { "address": "$(json_escape "${HG_STORE_RAFT_ADDRESS}")" },
  "server": { "port": "$(json_escape "${HG_STORE_REST_PORT}")" },
  "app": { "data-path": "$(json_escape "${HG_STORE_DATA_PATH}")" },
  "cloud": {
    "storage": {
      "enabled": "$(json_escape "${HG_CLOUD_STORAGE_ENABLED}")",
      "provider": "$(json_escape "${HG_CLOUD_STORAGE_PROVIDER}")",
      "path-prefix": "$(json_escape "${HG_CLOUD_STORAGE_PATH_PREFIX}")",
      "s3": {
        "bucket": "$(json_escape "${HG_CLOUD_STORAGE_BUCKET}")",
        "region": "$(json_escape "${HG_CLOUD_STORAGE_REGION}")",
        "endpoint": "$(json_escape "${HG_CLOUD_STORAGE_ENDPOINT}")",
        "access-key": "$(json_escape "${HG_CLOUD_STORAGE_ACCESS_KEY}")",
        "secret-key": "$(json_escape "${HG_CLOUD_STORAGE_SECRET_KEY}")"
      }
    }
  }
}
JSON
)"

STORE_JAR=$(echo /hugegraph-store/lib/hg-store-node-*.jar)

if [[ ! -f "$STORE_JAR" ]]; then
  echo "ERROR: store node jar not found under /hugegraph-store/lib/" >&2
  exit 2
fi

# Avoid duplicate/conflicting RocksDB API jars in PropertiesLauncher loader.path.
rm -f /hugegraph-store/plugins/rocksdbjni-*.jar >/dev/null 2>&1 || true

# RocksDB native callback threads may resolve Java types via AppClassLoader; keep rocksdbjni
# explicitly on the JVM system classpath to avoid NoClassDefFoundError for callback types.
ROCKSDB_JAR=""
if compgen -G "/hugegraph-store/lib/rocksdbjni-*.jar" > /dev/null; then
  ROCKSDB_JAR=$(ls -1 /hugegraph-store/lib/rocksdbjni-*.jar | sort | tail -n 1)
elif compgen -G "/hugegraph-store/plugins/rocksdbjni-*.jar" > /dev/null; then
  ROCKSDB_JAR=$(ls -1 /hugegraph-store/plugins/rocksdbjni-*.jar | sort | tail -n 1)
fi

APP_CLASSPATH="$STORE_JAR"
if [[ -n "$ROCKSDB_JAR" ]]; then
  APP_CLASSPATH="$ROCKSDB_JAR:$APP_CLASSPATH"
  echo "Using explicit rocksdbjni on system classpath: $ROCKSDB_JAR"
else
  if is_truthy "$HG_CLOUD_STORAGE_ENABLED"; then
    echo "ERROR: no external rocksdbjni jar found while cloud storage is enabled" >&2
    echo "ERROR: refusing to start to avoid callback NoClassDefFoundError at runtime" >&2
    exit 2
  fi
  echo "WARNING: no external rocksdbjni jar found; continuing because cloud storage is disabled" >&2
fi

exec java ${JAVA_OPTS:-} \
  -Dlog4j.configurationFile=/hugegraph-store/conf/log4j2.xml \
  -Dspring.config.location=/hugegraph-store/conf/application.yml \
  -Dloader.path=/hugegraph-store/plugins \
  -cp "$APP_CLASSPATH" \
  org.springframework.boot.loader.PropertiesLauncher

