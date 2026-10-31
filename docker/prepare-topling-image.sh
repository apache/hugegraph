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
# Called from the stopped component directory during an image build only.
set -euo pipefail

if [[ -z "${TOPLING_JNI_URL:-}" || -z "${TOPLING_JNI_SHA256:-}" ]] &&
   [[ -f /tmp/topling-runtime.env ]]; then
    requested_url="${TOPLING_JNI_URL:-}"
    requested_sha="${TOPLING_JNI_SHA256:-}"
    # Checked-in, reviewed artifact defaults; explicit build inputs win.
    source /tmp/topling-runtime.env
    TOPLING_JNI_URL="${requested_url:-${TOPLING_JNI_URL:-}}"
    TOPLING_JNI_SHA256="${requested_sha:-${TOPLING_JNI_SHA256:-}}"
fi

[[ "${TOPLING_JNI_SHA256:-}" =~ ^[[:xdigit:]]{64}$ ]] || {
    echo 'Error: Topling builds require a trusted TOPLING_JNI_SHA256' >&2
    exit 1
}
export TOPLING_JNI_SHA256

if [[ -r /run/topling-jni/rocksdbjni.jar ]]; then
    export TOPLING_JNI_JAR=/run/topling-jni/rocksdbjni.jar
else
    [[ "${TOPLING_JNI_URL:-}" == https://* ]] || {
        echo 'Error: Topling builds require an HTTPS TOPLING_JNI_URL or a topling-jni build context containing rocksdbjni.jar' >&2
        exit 1
    }
    download_dir=$(mktemp -d)
    trap 'rm -rf "$download_dir"' EXIT
    curl --fail --location --retry 3 --proto '=https' --proto-redir '=https' \
        --output "$download_dir/rocksdbjni.jar" "$TOPLING_JNI_URL"
    export TOPLING_JNI_JAR="$download_dir/rocksdbjni.jar"
fi
# The shared preparer verifies the hash, ELF/dependencies, and real EasyMigrate
# behavior before installing a component-local runtime and checksum receipt.
bash bin/prepare-topling.sh

# Put the optional runtime's notices next to the installed JNI instead of in
# the generic Maven/source distribution, which does not contain this artifact.
license_stage=topling/.license-stage
rm -rf "$license_stage" topling/licenses
mkdir -p "$license_stage"
unzip -qq "$TOPLING_JNI_JAR" 'META-INF/licenses/*' -d "$license_stage"
[[ -d "$license_stage/META-INF/licenses" &&
   -s "$license_stage/META-INF/licenses/manifest.sha256" ]] || {
    echo 'Error: Topling JNI is missing embedded license materials' >&2
    exit 1
}
mv "$license_stage/META-INF/licenses" topling/licenses
rm -rf "$license_stage"
cat > topling/LICENSE <<'EOF'
ToplingDB JNI community runtime license materials
=================================================

The complete license texts distributed with this optional JNI runtime are in
this directory's licenses/ subdirectory. The immutable rocksdbjni.jar retains
the same META-INF/licenses/ collection and checksum manifest.

RocksDB core uses its Apache License 2.0 option. Public Topling components and
bundled dependencies retain their respective Apache-2.0, BSD, MIT and Boost
licenses. See NOTICE for principal attributions and
META-INF/topling-build-info.txt inside rocksdbjni.jar for source revisions and
build configuration.
EOF
cat > topling/NOTICE <<'EOF'
ToplingDB JNI community runtime

This optional runtime includes software developed by Topling, Inc., together
with code from RocksDB, LevelDB and bundled public dependencies.

Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
Copyright (c) 2011 The LevelDB Authors. All rights reserved.
Copyright (c) 2021, Topling Inc. All rights reserved.
Copyright (c) 2020, Bytedance Inc. All rights reserved.

Original component notices and license texts are retained in licenses/ beside
this file and in META-INF/licenses/ inside rocksdbjni.jar. Source revisions and
the selected community components are recorded in
META-INF/topling-build-info.txt inside rocksdbjni.jar.
EOF
chmod 0644 topling/LICENSE topling/NOTICE
find topling/licenses -type f -exec chmod 0644 {} +

# Community images need no proprietary factory registration. Keep the full
# distribution profile unchanged and install the image's smaller native profile.
cp "$(dirname "${BASH_SOURCE[0]}")/toplingdb.yaml" conf/toplingdb.yaml
