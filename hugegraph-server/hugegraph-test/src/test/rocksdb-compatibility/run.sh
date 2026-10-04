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
if [[ $# -ne 3 ]]; then
    echo "Usage: $0 <before-source-directory> <after-source-directory> <new-output-directory>" >&2
    exit 2
fi
script_dir=$(cd "$(dirname "$0")" && pwd)
before=$(cd "$1" && pwd)
after=$(cd "$2" && pwd)
# A new directory contains only synthetic test data; never reuse a previous run.
mkdir "$3"
output=$(cd "$3" && pwd)
mkdir "$output/models" "$output/jars" "$output/cases"
exec > >(tee "$output/run.log") 2>&1
trap 'result=$?; echo "RESULT exit=$result output=$output"' EXIT
run() {
    local result
    printf 'COMMAND '; printf '%q ' "$@"; printf '\n'
    if "$@"; then result=0; else result=$?; fi
    echo "COMMAND_EXIT=$result"
    return "$result"
}
for variable in JAVA_TOOL_OPTIONS JDK_JAVA_OPTIONS _JAVA_OPTIONS LD_PRELOAD DYLD_INSERT_LIBRARIES; do
    if [[ -n "${!variable:-}" ]]; then
        echo "Unset $variable before running the standard RocksDB compatibility test" >&2
        exit 2
    fi
done
# Maven resolves properties and inheritance. Only these three owner modules matter.
for side in before after; do
    source_dir=${!side}
    for entry in server:hugegraph-server/hugegraph-rocksdb pd:hugegraph-pd/hg-pd-core \
                 store:hugegraph-store/hg-store-rocksdb; do
        component=${entry%%:*}
        module=${entry#*:}
        (cd "$source_dir" && run mvn -B -ntp -N -f "$module/pom.xml" \
            org.apache.maven.plugins:maven-help-plugin:3.2.0:effective-pom \
            "-Doutput=$output/models/$component-$side.xml")
    done
done
python3 - "$output" <<'PYTHON'
from pathlib import Path
import re
import sys
import xml.etree.ElementTree as ET

output = Path(sys.argv[1])
ns = {'m': 'http://maven.apache.org/POM/4.0.0'}
pairs = {}
for component in ('server', 'pd', 'store'):
    versions = []
    for side in ('before', 'after'):
        model = ET.parse(output / 'models' / f'{component}-{side}.xml').getroot()
        matches = [dep.findtext('m:version', namespaces=ns)
                   for dep in model.findall('m:dependencies/m:dependency', ns)
                   if dep.findtext('m:groupId', namespaces=ns) == 'org.rocksdb'
                   and dep.findtext('m:artifactId', namespaces=ns) == 'rocksdbjni']
        if len(matches) != 1 or not re.fullmatch(r'[0-9][0-9A-Za-z._-]*', matches[0] or ''):
            raise SystemExit(f'Cannot resolve one concrete RocksDB version: {component} {side}: {matches}')
        versions.append(matches[0])
    print(f'VERSION {component}: {versions[0]} -> {versions[1]}')
    if versions[0] != versions[1]:
        pairs.setdefault(tuple(versions), []).append(component)
with (output / 'pairs.tsv').open('w') as stream:
    for (old, new), components in pairs.items():
        stream.write(f'{old}\t{new}\t{",".join(components)}\n')
PYTHON
if [[ ! -s "$output/pairs.tsv" ]]; then
    echo "SKIP: actual RocksDB versions are unchanged in all three components"
    exit 0
fi
# Fetch only versions in this change; inputs are not part of uploaded evidence.
cut -f1,2 "$output/pairs.tsv" | tr '\t' '\n' | sort -u > "$output/versions.txt"
while read -r version; do
    (cd "$after" && run mvn -B -ntp -N dependency:copy \
        "-Dartifact=org.rocksdb:rocksdbjni:$version" "-DoutputDirectory=$output/jars")
done < "$output/versions.txt"
pairs=0
while IFS=$'\t' read -r old new components; do
    case_dir="$output/cases/$old-to-$new"
    mkdir -p "$case_dir/classes" "$case_dir/empty-native-path"
    old_jar="$output/jars/rocksdbjni-$old.jar"
    new_jar="$output/jars/rocksdbjni-$new.jar"
    echo "PAIR $old -> $new components=$components"
    run javac -cp "$old_jar" -d "$case_dir/classes" "$script_dir/RocksDBCompatibilityTest.java"
    run java -Djava.library.path="$case_dir/empty-native-path" -cp "$case_dir/classes:$old_jar" \
        RocksDBCompatibilityTest seed "$case_dir/original" "$old" "$old_jar"
    run cp -R "$case_dir/original" "$case_dir/reopened"
    run java -Djava.library.path="$case_dir/empty-native-path" -cp "$case_dir/classes:$new_jar" \
        RocksDBCompatibilityTest upgrade "$case_dir/reopened" "$new" "$new_jar"
    run java -Djava.library.path="$case_dir/empty-native-path" -cp "$case_dir/classes:$new_jar" \
        RocksDBCompatibilityTest verify "$case_dir/reopened" "$new" "$new_jar"
    pairs=$((pairs + 1))
done < "$output/pairs.tsv"
echo "PASS compatibilityPairs=$pairs JVMPhases=$((pairs * 3)) scope=synthetic-SST-WAL-only"
