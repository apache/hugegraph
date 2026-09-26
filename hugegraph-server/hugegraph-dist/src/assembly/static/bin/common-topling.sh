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
IFS=$'\n\t'
trap 'echo "[common-topling] error at line ${LINENO}: ${BASH_COMMAND}" >&2' ERR

GITHUB="https://github.com"

function abs_path() {
    local SOURCE
    SOURCE="${BASH_SOURCE[0]}"
    while [[ -h "$SOURCE" ]]; do
        local DIR
        DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
        SOURCE="$(readlink "$SOURCE")"
        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    cd -P "$(dirname "$SOURCE")" && pwd
}

function extract_so_with_jar() {
    local jar_file="$1"
    local dest_dir="$2"
    local abs_jar_path

    if [ ! -f "$jar_file" ]; then
        echo "'$jar_file' Not Exist" >&2
        return 1
    fi

    mkdir -p "$dest_dir" || {
        echo "Cannot mkdir '$dest_dir'" >&2
        return 1
    }

    if command -v realpath >/dev/null 2>&1; then
        abs_jar_path="$(realpath "$jar_file")"
    else
        abs_jar_path="$(readlink -f "$jar_file")"
    fi
    if ! command -v unzip >/dev/null 2>&1; then
        echo "Error: 'unzip' command not found. Please install unzip." >&2
        return 1
    fi
    unzip -j -o "$abs_jar_path" "*.so" -d "$dest_dir" > /dev/null 2>&1 || {
        local code=$?
        if [ $code -eq 11 ]; then
            echo "Error: No .so files found in '$abs_jar_path' (unzip exit 11)" >&2
        else
            echo "Error: unzip failed (exit $code) for '$abs_jar_path'" >&2
        fi
        return $code
    }
}

function extract_html_css_from_jar() {
    local jar_file="$1"
    local dest_dir="$2"
    local abs_jar_path
    local resource_target="$dest_dir/rocksdb_resource"

    if [ ! -f "$jar_file" ]; then
        echo "Error: JAR file '$jar_file' does not exist." >&2
        return 1
    fi

    mkdir -p "$resource_target" || {
        echo "Error: Cannot create resource directory '$resource_target'." >&2
        return 1
    }

    if command -v realpath >/dev/null 2>&1; then
        abs_jar_path="$(realpath "$jar_file")"
    else
        abs_jar_path="$(readlink -f "$jar_file")"
    fi
    if ! command -v unzip >/dev/null 2>&1; then
        echo "Error: 'unzip' command not found. Please install unzip." >&2
        return 1
    fi
    unzip -j -o "$abs_jar_path" "*.html" "*.css" -d "$resource_target" > /dev/null || {
        local code=$?
        if [ $code -eq 11 ]; then
            echo "Notice: No .html or .css files found in '$jar_file'." >&2
            return 0
        else
            echo "Error: unzip failed with exit code $code" >&2
            return $code
        fi
    }

}

function ensure_libaio_symlink() {
    local dest_dir="$1"
    # Keep the Ubuntu 24.04 compatibility link inside the component runtime.
    # Installation must never require sudo or modify /usr/lib.
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        if [ "${ID:-}" = "ubuntu" ] &&
           command -v dpkg >/dev/null 2>&1 &&
           dpkg --compare-versions "${VERSION_ID:-0}" "ge" "24.04" &&
           [ ! -e /usr/lib/x86_64-linux-gnu/libaio.so.1 ] &&
           [ -e /usr/lib/x86_64-linux-gnu/libaio.so.1t64 ]; then
            mkdir -p "$dest_dir"
            ln -sfn /usr/lib/x86_64-linux-gnu/libaio.so.1t64 \
                    "$dest_dir/libaio.so.1"
            echo "Prepared component-local libaio.so.1 compatibility link"
        fi
    fi
}

function download_and_verify() {
    local url=$1
    local filepath=$2
    local expected_sha256=$3
    local actual_sha256

    if [[ -f $filepath ]]; then
        echo "File $filepath exists. Verifying SHA-256 checksum..."
        actual_sha256=$(sha256sum "$filepath" | awk '{ print $1 }')
        if [[ "$actual_sha256" != "$expected_sha256" ]]; then
            echo "SHA-256 checksum verification failed for $filepath. Expected: $expected_sha256, but got: $actual_sha256"
            echo "Deleting $filepath..."
            rm -f "$filepath"
        else
            echo "SHA-256 checksum verification succeeded for $filepath."
            return 0
        fi
    fi

    echo "Downloading $filepath..."
    if ! curl -fL --retry 2 --retry-delay 2 --retry-max-time 90 \
              --connect-timeout 10 --max-time 30 -o "$filepath" "$url"; then
        echo "Failed to download $filepath" >&2
        rm -f "$filepath"
        return 1
    fi

    actual_sha256=$(sha256sum "$filepath" | awk '{ print $1 }')
    if [[ "$actual_sha256" != "$expected_sha256" ]]; then
        echo "SHA-256 checksum verification failed for $filepath after download. Expected: $expected_sha256, but got: $actual_sha256"
        rm -f "$filepath"
        return 1
    fi

    return 0
}

function download_and_setup_jemalloc() {
    local arch lib_file download_url expected_sha256 system_lib top
    top=$1
    system_lib=""

    if [[ "${LD_PRELOAD:-}" == *"libjemalloc"* ]]; then
        return 0
    fi

    # Prefer system-installed jemalloc if available
    # Try ldconfig first to locate the shared object
    if command -v ldconfig >/dev/null 2>&1; then
        system_lib=$(ldconfig -p 2>/dev/null | awk '/jemalloc/{print $4}' | head -n1)
    fi
    # Fallback to common library paths if ldconfig is not available or found nothing
    if [[ -z "$system_lib" ]]; then
        for p in \
            /usr/lib/libjemalloc.so \
            /usr/lib/libjemalloc.so.2 \
            /usr/lib64/libjemalloc.so \
            /usr/lib64/libjemalloc.so.2 \
            /usr/local/lib/libjemalloc.so \
            /usr/local/lib/libjemalloc.so.2 \
            /usr/lib/x86_64-linux-gnu/libjemalloc.so \
            /usr/lib/x86_64-linux-gnu/libjemalloc.so.2 \
            /usr/lib/aarch64-linux-gnu/libjemalloc.so \
            /usr/lib/aarch64-linux-gnu/libjemalloc.so.2; do
            if [[ -f "$p" ]]; then
                system_lib="$p"
                break
            fi
        done
    fi

    # If found, set LD_PRELOAD and return immediately
    if [[ -n "$system_lib" ]]; then
        export LD_PRELOAD="${system_lib}${LD_PRELOAD:+:$LD_PRELOAD}"
        return 0
    fi

    # Detect system architecture
    arch=$(uname -m)

    # System jemalloc not found, try to download the correct library for the architecture
    # Checksums match apache/hugegraph-doc@567625c6ec66907fc60f1864146fbec91b5f6204.
    if [[ $arch == "aarch64" || $arch == "arm64" ]]; then
        lib_file="$top/bin/libjemalloc_aarch64.so"
        download_url="${GITHUB}/apache/hugegraph-doc/raw/binary-1.5/dist/server/libjemalloc_aarch64.so"
        expected_sha256="6b7e6099b6da798829c6ce6fcb55a787508841edd52446332a73300889dcd1dc"
    elif [[ $arch == "x86_64" ]]; then
        lib_file="$top/bin/libjemalloc.so"
        download_url="${GITHUB}/apache/hugegraph-doc/raw/binary-1.5/dist/server/libjemalloc.so"
        expected_sha256="53b25e8626e1605cbd8b60befb3431cabc1b8851a54285e0dda412796feab67d"
    else
        echo "Unsupported architecture: $arch"
        return 1
    fi

    # Download and verify jemalloc library (fallback when system lib not found)
    if download_and_verify "$download_url" "$lib_file" "$expected_sha256"; then
        export LD_PRELOAD="${lib_file}${LD_PRELOAD:+:$LD_PRELOAD}"
    else
        echo "Failed to verify or download jemalloc for $arch, skipping"
        return 1
    fi
}

function require_topling_platform() {
    local os_name machine_arch

    os_name="$(uname -s)"
    machine_arch="$(uname -m)"
    if [ "$os_name" != "Linux" ] ||
       [[ "$machine_arch" != "x86_64" ]]; then
        printf 'Error: ToplingDB native runtime supports Linux x86_64 only; ' >&2
        printf 'current platform is %s/%s\n' "$os_name" "$machine_arch" >&2
        return 1
    fi
}

function prepare_toplingdb() {
    local lib_dir="$1"
    local dest_dir="$2"
    local top_override="${3:-}"

    require_topling_platform || return 1

    local top
    if [ -n "$top_override" ]; then
        top="$top_override"
    else
        top="$(cd "$lib_dir"/../ && pwd)" || {
            echo "Error: failed to resolve the ToplingDB installation directory" >&2
            return 1
        }
    fi

    local jar_file
    jar_file=$(ls -1 "$lib_dir"/rocksdbjni*.jar 2>/dev/null | sort -V | tail -n1 || true)
    if [ -z "${jar_file:-}" ]; then
        echo "Error: No rocksdbjni*.jar found under '$lib_dir'" >&2
        return 1
    fi

    ensure_libaio_symlink "$dest_dir"
    if ! download_and_setup_jemalloc "$top"; then
        echo "Warning: jemalloc is unavailable; continuing without it" >&2
    fi
    extract_so_with_jar "$jar_file" "$dest_dir"
    if ! extract_html_css_from_jar "$jar_file" "$dest_dir"; then
        echo "Warning: failed to extract optional ToplingDB web resources; continuing" >&2
    fi
    if [ -d "$dest_dir" ]; then
        if [[ ":${LD_LIBRARY_PATH:-}:" != *":$dest_dir:"* ]]; then
            export LD_LIBRARY_PATH="$dest_dir${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
        fi

        if [ -f "$dest_dir/librocksdbjni-linux64.so" ] && [[ ":${LD_PRELOAD:-}:" != *"librocksdbjni-linux64.so:"* ]]; then
            export LD_PRELOAD="${LD_PRELOAD:+$LD_PRELOAD:}$dest_dir/librocksdbjni-linux64.so"
        fi
    else
        echo "Warn: LD paths skipped, directory '$dest_dir' does not exist." >&2
    fi
}
