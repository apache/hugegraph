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
# Generate the Hubble properties file a Compose topology mounts, with PD's
# REST secret written in as operations.pd.password.
#
#   usage: set-hubble-pd-password.sh <hstore|hstore-ha> [secret]
#
# Reads conf/hubble/<name>.properties.example (tracked) and writes
# conf/hubble/<name>.local.properties (ignored by git), so the secret never
# lands in a tracked file. The secret defaults to $HG_PD_AUTH_SECRET_KEY. The
# value never goes through a sed replacement, where & # and backslash are
# special, and backslashes are doubled for the .properties format. Run this
# before `docker compose up`: if the target is missing Docker creates an empty
# directory at the bind path and Hubble starts with no configuration.
set -euo pipefail

name=${1:?usage: $0 <hstore|hstore-ha> [secret]}
secret=${2:-${HG_PD_AUTH_SECRET_KEY:-}}
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/conf/hubble"
example="${dir}/${name}.properties.example"
out="${dir}/${name}.local.properties"

[[ -f "$example" ]] || { echo "no such topology: ${name} (expected ${example})" >&2; exit 1; }
[[ -n "$secret" ]] || { echo "secret is empty; load .env first (set -a; . ./.env; set +a)" >&2; exit 1; }
case "$secret" in
    *$'\n'*|*$'\r'*) echo "secret contains a line break, which a .properties value cannot hold" >&2; exit 1 ;;
esac

escaped=${secret//\\/\\\\}
tmp=$(mktemp "${out}.XXXXXX")
trap 'rm -f "$tmp"' EXIT
{
    printf '# Generated from %s by set-hubble-pd-password.sh; not tracked by git.\n' "$(basename "$example")"
    grep -v '^operations\.pd\.password=' "$example" || true
    printf 'operations.pd.password=%s\n' "$escaped"
} > "$tmp"
# Hubble runs unprivileged and the mount is read-only, so the file must be world-readable
chmod 644 "$tmp"
mv "$tmp" "$out"
trap - EXIT
echo "wrote ${out}"
