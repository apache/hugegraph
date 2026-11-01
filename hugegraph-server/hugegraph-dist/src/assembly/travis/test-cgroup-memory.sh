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

# Verify hard-limit sizing without starting HugeGraph or requiring root.
set -euo pipefail
UTIL=${1:?Usage: $0 UTIL_SH}
# The fixture supplies util.sh; calc_xmx invokes the free_memory hooks below.
# shellcheck disable=SC1090,SC2329
source "$UTIL"
FIXTURE=$(mktemp -d)
trap 'chmod -R u+rwX "$FIXTURE"; rm -rf "$FIXTURE"' EXIT
ROOT="$FIXTURE/cgroup"
MEMBERSHIP="$FIXTURE/membership"
MOUNTINFO="$FIXTURE/mountinfo"
mkdir -p "$ROOT"
printf '0\n' > "$ROOT/memory.min"
assert_memory() {
    local expected=$1 actual
    actual=$(cgroup_free_memory "$ROOT" "$MEMBERSHIP" "$MOUNTINFO")
    [[ "$actual" == "$expected" ]] || {
        echo "expected cgroup MiB '$expected', got '$actual'" >&2
        exit 1
    }
}
# Verify unknown usage and its zero-budget path through the actual launcher helpers.
assert_unknown_usage() {
    local usage_file=$1 valid_usage=$2
    printf 'invalid\n' > "$usage_file"
    assert_memory 0
    : > "$usage_file"
    assert_memory 0
    rm "$usage_file"
    assert_memory 0
    (
        # Only supply host data; the cgroup helper still reads the real fixture.
        # shellcheck disable=SC2329
        uname() { [[ "${1:-}" != -m ]] && echo Linux || echo x86_64; }
        # shellcheck disable=SC2329
        cat() {
            if [[ "$1" == /proc/meminfo ]]; then
                printf 'MemFree: 1048576 kB\nBuffers: 0 kB\nCached: 0 kB\n'
            else
                command cat "$@"
            fi
        }
        # shellcheck disable=SC2329
        cgroup_free_memory() {
            bash -c 'source "$1"; cgroup_free_memory "$2" "$3" "$4"' fixture \
                "$UTIL" "$ROOT" "$MEMBERSHIP" "$MOUNTINFO"
        }
        [[ $(free_memory) == 0 ]]
        if (calc_xmx 512 32768); then
            echo 'unknown cgroup usage fell back to host memory' >&2
            exit 1
        fi
    )
    printf '%s\n' "$valid_usage" > "$usage_file"
    chmod 000 "$usage_file"
    if [[ ! -r "$usage_file" ]]; then
        assert_memory 0
    fi
    chmod 600 "$usage_file"
}
# No controller keeps the original host fallback.
assert_memory ""
printf '0::/\n' > "$MEMBERSHIP"
printf 'max\n' > "$ROOT/memory.max"
printf '12345\n' > "$ROOT/memory.current"
assert_memory ""
# Four GiB limit, one GiB already used: three GiB remain, not host RAM.
printf '4294967296\n' > "$ROOT/memory.max"
printf '1073741824\n' > "$ROOT/memory.current"
assert_memory 3072
assert_unknown_usage "$ROOT/memory.current" 1073741824
assert_memory 3072
# Usage at/over limit must not become a negative or wrapped number.
printf '4294967296\n' > "$ROOT/memory.current"
assert_memory 0
printf '5368709120\n' > "$ROOT/memory.current"
assert_memory 0
# A finite ancestor constrains an unlimited child.
mkdir -p "$ROOT/team/service"
printf '0\n' > "$ROOT/team/memory.min"
printf '0\n' > "$ROOT/team/service/memory.min"
printf '0::/team/service\n' > "$MEMBERSHIP"
printf 'max\n' > "$ROOT/team/service/memory.max"
printf '0\n' > "$ROOT/team/service/memory.current"
printf '2147483648\n' > "$ROOT/team/memory.max"
printf '536870912\n' > "$ROOT/team/memory.current"
printf '1073741824\n' > "$ROOT/memory.current"
assert_memory 1536
# A stricter child wins; usage is per-level, not subtracted twice.
printf '1073741824\n' > "$ROOT/team/service/memory.max"
printf '268435456\n' > "$ROOT/team/service/memory.current"
assert_memory 768
# Host-relative membership in a private namespace falls back to mount root.
printf '0::/not-visible/container\n' > "$MEMBERSHIP"
assert_memory 3072
# Malformed limits do not introduce arithmetic errors or an artificial zero.
printf 'invalid\n' > "$ROOT/memory.max"
assert_memory ""
# The v1 fixture represents a known 64-bit Linux kernel, independently of the
# test host's userspace or machine name (the checks also run on macOS).
# shellcheck disable=SC2329
uname() { [[ "${1:-}" != -m ]] && command uname "$@" || echo x86_64; }
export -f uname
PAGE_SIZE=$(getconf PAGESIZE)
V1_UNLIMITED=$((9223372036854775807 / PAGE_SIZE * PAGE_SIZE))
mkdir -p "$ROOT/memory/job"
printf '5:cpu,cpuacct:/job\n6:memory:/job\n' > "$MEMBERSHIP"
printf '%s\n' "$V1_UNLIMITED" > "$ROOT/memory/memory.limit_in_bytes"
printf '0\n' > "$ROOT/memory/memory.usage_in_bytes"
printf '2147483648\n' > "$ROOT/memory/job/memory.limit_in_bytes"
printf '536870912\n' > "$ROOT/memory/job/memory.usage_in_bytes"
assert_memory 1536
assert_unknown_usage "$ROOT/memory/job/memory.usage_in_bytes" 536870912
assert_memory 1536
# A v1 unlimited child must not reject unreadable usage or hide a finite ancestor.
printf '%s\n' "$V1_UNLIMITED" > "$ROOT/memory/job/memory.limit_in_bytes"
printf '2147483648\n' > "$ROOT/memory/memory.limit_in_bytes"
printf '536870912\n' > "$ROOT/memory/memory.usage_in_bytes"
printf 'invalid\n' > "$ROOT/memory/job/memory.usage_in_bytes"
assert_memory 1536
# The existing 64-bit/4-KiB sentinel is also an unlimited boundary.
printf '9223372036854771712\n' > "$ROOT/memory/job/memory.limit_in_bytes"
assert_memory 1536
printf '%s\n' "$V1_UNLIMITED" > "$ROOT/memory/job/memory.limit_in_bytes"
rm "$ROOT/memory/job/memory.usage_in_bytes"
assert_memory 1536
printf '536870912\n' > "$ROOT/memory/job/memory.usage_in_bytes"
chmod 000 "$ROOT/memory/job/memory.usage_in_bytes"
assert_memory 1536
chmod 600 "$ROOT/memory/job/memory.usage_in_bytes"
# Native-query failures must keep unknown numeric-limit usage conservative.
rm "$ROOT/memory/job/memory.usage_in_bytes"
# The page immediately below the sentinel is finite and still needs usage.
printf '%s\n' "$((V1_UNLIMITED - PAGE_SIZE))" > "$ROOT/memory/job/memory.limit_in_bytes"
assert_memory 0
printf '%s\n' "$V1_UNLIMITED" > "$ROOT/memory/job/memory.limit_in_bytes"
(
    # shellcheck disable=SC2329
    getconf() { return 127; }
    assert_memory 0
)
(
    # shellcheck disable=SC2329
    getconf() { echo invalid; }
    assert_memory 0
)
(
    # shellcheck disable=SC2329
    getconf() { [[ "$1" != PAGESIZE ]] || return 0; }
    assert_memory 0
)
# A 32-bit userspace cannot turn an 8-TiB finite limit into unlimited.
(
    # shellcheck disable=SC2329
    getconf() { [[ "$1" != LONG_BIT ]] && echo 4096 || echo 32; }
    printf '8796093022208\n' > "$ROOT/memory/memory.limit_in_bytes"
    printf '8795824586752\n' > "$ROOT/memory/memory.usage_in_bytes"
    printf '9223372036854771712\n' > "$ROOT/memory/job/memory.limit_in_bytes"
    for machine in x86_64 aarch64 riscv64; do
        # shellcheck disable=SC2329
        uname() { echo "$machine"; }
        assert_memory 256
    done
    # Unknown/32-bit-looking names, including personality-masked uname, must
    # retain finite limits rather than infer a 32-bit unlimited sentinel.
    printf '8796093022208\n' > "$ROOT/memory/job/memory.limit_in_bytes"
    printf '8795824586752\n' > "$ROOT/memory/job/memory.usage_in_bytes"
    for machine in unknown i686 armv8l; do
        assert_memory 256
        rm "$ROOT/memory/job/memory.usage_in_bytes"
        assert_memory 0
        printf '9223372036854771712\n' > "$ROOT/memory/job/memory.limit_in_bytes"
        assert_memory 0
        printf '8796093022208\n' > "$ROOT/memory/job/memory.limit_in_bytes"
        printf '8795824586752\n' > "$ROOT/memory/job/memory.usage_in_bytes"
    done
    # shellcheck disable=SC2329
    uname() { return 127; }
    assert_memory 256
    rm "$ROOT/memory/job/memory.usage_in_bytes"
    assert_memory 0
    printf '9223372036854771712\n' > "$ROOT/memory/job/memory.limit_in_bytes"
    assert_memory 0
)
# Restore the original finite-ancestor/unlimited-child fixture.
printf '2147483648\n' > "$ROOT/memory/memory.limit_in_bytes"
printf '536870912\n' > "$ROOT/memory/memory.usage_in_bytes"
printf '%s\n' "$V1_UNLIMITED" > "$ROOT/memory/job/memory.limit_in_bytes"
# With no finite limits, unknown unlimited usage still permits host sizing.
printf '%s\n' "$V1_UNLIMITED" > "$ROOT/memory/memory.limit_in_bytes"
rm "$ROOT/memory/memory.usage_in_bytes"
assert_memory ""
(
    # Supply host data; the actual helper reads the unlimited fixture.
    # shellcheck disable=SC2329
    uname() { [[ "${1:-}" != -m ]] && echo Linux || echo x86_64; }
    # shellcheck disable=SC2329
    cat() {
        if [[ "$1" == /proc/meminfo ]]; then
            printf 'MemFree: 1048576 kB\nBuffers: 0 kB\nCached: 0 kB\n'
        else
            command cat "$@"
        fi
    }
    # shellcheck disable=SC2329
    cgroup_free_memory() {
        bash -c 'source "$1"; cgroup_free_memory "$2" "$3" "$4"' fixture \
            "$UTIL" "$ROOT" "$MEMBERSHIP" "$MOUNTINFO"
    }
    [[ $(free_memory) == 1024 ]]
    [[ $(calc_xmx 512 32768) == 512 ]]
)
printf '0\n' > "$ROOT/memory/memory.usage_in_bytes"
printf '2147483648\n' > "$ROOT/memory/job/memory.limit_in_bytes"
printf '536870912\n' > "$ROOT/memory/job/memory.usage_in_bytes"
assert_memory 1536
# Keep launcher min/max semantics; insufficient effective memory fails.
printf '0::/unified\n6:memory:/job\n' > "$MEMBERSHIP"
assert_memory 1536
# A combined v1 controller can live outside /sys/fs/cgroup/memory.
CUSTOM="$FIXTURE/combined"
mkdir -p "$CUSTOM/job"
cp "$ROOT/memory/job/"memory.* "$CUSTOM/job/"
printf '29 23 0:26 / %s rw - cgroup cgroup rw,cpu,memory\n' "$CUSTOM" > "$MOUNTINFO"
assert_memory 1536
# The mounted controller root may already be a delegated ancestor.
printf '6:memory:/delegated/job\n' > "$MEMBERSHIP"
printf '29 23 0:26 /delegated %s rw - cgroup cgroup rw,memory\n' "$CUSTOM" > "$MOUNTINFO"
assert_memory 1536
# Mountinfo escapes spaces, including custom controller mount points.
SPACED="$FIXTURE/combined controller"
mkdir -p "$SPACED/job"
cp "$CUSTOM/job/"memory.* "$SPACED/job/"
ESCAPED=${SPACED// /\\040}
printf '29 23 0:26 /delegated %s rw - cgroup cgroup rw,memory\n' "$ESCAPED" > "$MOUNTINFO"
assert_memory 1536
# Skip unrelated bind mounts before applying namespace fallback.
UNRELATED="$FIXTURE/unrelated"
mkdir -p "$UNRELATED"
printf '0\n' > "$UNRELATED/memory.min"
printf '17179869184\n' > "$UNRELATED/memory.max"
printf '0\n' > "$UNRELATED/memory.current"
printf '0::/team/service\n' > "$MEMBERSHIP"
printf '29 23 0:26 /other %s rw - cgroup2 cgroup rw\n' "$UNRELATED" > "$MOUNTINFO"
printf '30 23 0:26 / %s rw - cgroup2 cgroup rw\n' "$ROOT" >> "$MOUNTINFO"
assert_memory 768
# A matching delegated view must not hide a tighter ancestor in a wider view.
printf '29 23 0:26 /team/service %s rw - cgroup2 cgroup rw\n' "$ROOT/team/service" > "$MOUNTINFO"
printf '30 23 0:26 / %s rw - cgroup2 cgroup rw\n' "$ROOT" >> "$MOUNTINFO"
printf '536870912\n' > "$ROOT/team/memory.max"
printf '0\n' > "$ROOT/team/memory.current"
assert_memory 512
printf '2147483648\n' > "$ROOT/team/memory.max"
printf '536870912\n' > "$ROOT/team/memory.current"
# The same selection rule applies to v1 and decoded delegated roots.
printf '6:memory:/delegated group/job\n' > "$MEMBERSHIP"
printf '29 23 0:26 /other %s rw - cgroup cgroup rw,memory\n' "$UNRELATED" > "$MOUNTINFO"
printf '30 23 0:26 /delegated\\040group %s rw - cgroup cgroup rw,memory\n' "$ESCAPED" >> "$MOUNTINFO"
assert_memory 1536
# Warm inactive file cache is reclaimable; active usage still consumes budget.
printf '0::/\n' > "$MEMBERSHIP"
: > "$MOUNTINFO"
printf '4294967296\n' > "$ROOT/memory.max"
printf '4026531840\n' > "$ROOT/memory.current"
printf 'inactive_file 4026531840\n' > "$ROOT/memory.stat"
assert_memory 4096
# shellcheck disable=SC2329
free_memory() { cgroup_free_memory "$ROOT" "$MEMBERSHIP" "$MOUNTINFO"; }
[[ $(calc_xmx 512 32768) == 2048 ]]
# Inconsistent snapshots and malformed statistics receive no cache credit.
printf 'inactive_file 8589934592\n' > "$ROOT/memory.stat"
assert_memory 256
printf 'inactive_file invalid\n' > "$ROOT/memory.stat"
assert_memory 256
rm "$ROOT/memory.stat"
assert_memory 256
# Reclaimable cache is checked independently at every ancestor.
printf '0::/team/service\n' > "$MEMBERSHIP"
printf 'inactive_file 268435456\n' > "$ROOT/team/service/memory.stat"
printf 'inactive_file 536870912\n' > "$ROOT/team/memory.stat"
printf 'inactive_file 4026531840\n' > "$ROOT/memory.stat"
assert_memory 1024
# v1's total statistic includes descendants; the local statistic does not.
printf '6:memory:/job\n' > "$MEMBERSHIP"
printf 'inactive_file 0\ntotal_inactive_file 536870912\n' > "$ROOT/memory/job/memory.stat"
assert_memory 2048
# A separate v2 hierarchy preserves ordinary dual/inherited warm limits.
ROOT="$FIXTURE/v2-warm"
mkdir -p "$ROOT/target" "$ROOT/sibling" "$ROOT/notes"
for dir in "$ROOT" "$ROOT/target" "$ROOT/sibling"; do
    printf '0\n' > "$dir/memory.min"
    printf 'max\n' > "$dir/memory.max"
    printf '0\n' > "$dir/memory.current"
    printf 'inactive_file 0\n' > "$dir/memory.stat"
done
printf '0::/target\n' > "$MEMBERSHIP"
: > "$MOUNTINFO"
printf '2147483648\n' > "$ROOT/memory.max"
printf '1610612736\n' > "$ROOT/memory.current"
printf 'inactive_file 1610612736\n' > "$ROOT/memory.stat"
printf '2147483648\n' > "$ROOT/target/memory.max"
printf '1610612736\n' > "$ROOT/target/memory.current"
printf 'inactive_file 1610612736\n' > "$ROOT/target/memory.stat"
# A directory without controller files is not a cgroup child.
printf '1610612736\n' > "$ROOT/notes/memory.min"
assert_memory 2048
printf 'max\n' > "$ROOT/target/memory.max"
assert_memory 2048
# Protection is relative to the reclaim target: its own min is ignored.
printf 'max\n' > "$ROOT/memory.max"
printf '2147483648\n' > "$ROOT/target/memory.max"
printf '1610612736\n' > "$ROOT/target/memory.min"
assert_memory 2048
printf '0\n' > "$ROOT/target/memory.min"
printf '2147483648\n' > "$ROOT/memory.max"
# Move the warm cache to a protected sibling; the ancestor must not credit it.
printf '0\n' > "$ROOT/target/memory.current"
printf 'inactive_file 0\n' > "$ROOT/target/memory.stat"
printf '1610612736\n' > "$ROOT/sibling/memory.current"
printf 'inactive_file 1610612736\n' > "$ROOT/sibling/memory.stat"
printf '1610612736\n' > "$ROOT/sibling/memory.min"
assert_memory 512
printf 'invalid\n' > "$ROOT/sibling/memory.min"
assert_memory 512
rm "$ROOT/sibling/memory.min"
assert_memory 512
printf '0\n' > "$ROOT/sibling/memory.min"
chmod 000 "$ROOT/sibling/memory.min"
if [[ ! -r "$ROOT/sibling/memory.min" ]]; then
    assert_memory 512
fi
chmod 600 "$ROOT/sibling/memory.min"
assert_memory 2048
# Hidden child cgroups (as used by systemd) have the same protection semantics.
mv "$ROOT/sibling" "$ROOT/.sibling"
printf '1610612736\n' > "$ROOT/.sibling/memory.min"
assert_memory 512
# Actual filesystem permissions must not disguise a cgroup as an ordinary dir.
printf '0\n' > "$ROOT/.sibling/memory.min"
chmod 000 "$ROOT/.sibling"
if [[ ! -x "$ROOT/.sibling" ]]; then
    assert_memory 512
else
    # Root with DAC override can still read the actual zero protection value.
    assert_memory 2048
fi
chmod 700 "$ROOT/.sibling"
# Search permission alone permits known files but cannot enumerate children.
chmod 111 "$ROOT"
if [[ ! -r "$ROOT" ]]; then
    assert_memory 512
else
    # Root with DAC override can enumerate the unprotected children.
    assert_memory 2048
fi
chmod 700 "$ROOT"
# shellcheck disable=SC2329
free_memory() { echo 3072; }
[[ $(calc_xmx 512 32768) == 1536 ]]
# shellcheck disable=SC2329
free_memory() { echo 256; }
if (calc_xmx 512 32768); then
    echo 'heap selection accepted memory below its minimum' >&2
    exit 1
fi
free_memory() { echo 100000; }
[[ $(calc_xmx 512 32768) == 32768 ]]
printf 'cgroup memory sizing checks passed\n'
