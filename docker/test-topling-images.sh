#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
set -euo pipefail
repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
python3 - "$repo" <<'PY'
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import zipfile

repo = Path(sys.argv[1])

# Lock the source/build contracts that do not require a Docker daemon.
bake = (repo / "docker/bake.hcl").read_text()
assert 'condition     = RUNTIME_VARIANT == "standard" || RUNTIME_VARIANT == "topling"' in bake
assert 'error_message = "RUNTIME_VARIANT must be standard or topling"' in bake
standalone = (repo / "hugegraph-server/Dockerfile").read_text()
assert "mv ./conf/graphs ./rocksdb-data/.hugegraph-config/graphs" in standalone
assert "ln -s ../rocksdb-data/.hugegraph-config/graphs ./conf/graphs" in standalone
assert "cp -a ./conf/graphs ./conf/graphs.default" in standalone
assert "VOLUME /hugegraph-server/rocksdb-data" in standalone
for generic in (repo / "install-dist/release-docs/LICENSE",
                repo / "install-dist/release-docs/NOTICE"):
    assert "ToplingDB JNI" not in generic.read_text(), generic
assert not (repo / "install-dist/release-docs/licenses/LICENSE-toplingdb-jni-8.10.2.txt").exists()
inventory = (repo / "install-dist/scripts/dependency/optional-image-dependencies.txt").read_text()
for entry in ("installed_license_directory=topling/licenses/",
              "installed_license_summary=topling/LICENSE",
              "installed_notice=topling/NOTICE"):
    assert entry in inventory, entry

# Execute the actual RUN chain with failures injected after payload creation.
# Ownership adjustment must never hide a failed capability/license preparation.
for dockerfile in ("hugegraph-server/Dockerfile", "hugegraph-pd/Dockerfile", "hugegraph-store/Dockerfile"):
    commands = re.findall(r"(?m)^RUN (.*(?:\n[ \t].*)*)", (repo / dockerfile).read_text())
    preparation = next(command for command in commands if "bash /tmp/prepare-topling-image.sh" in command)
    preparation = re.sub(r"--mount=\S+\s*", "", preparation.replace("\\\n", " "))
    stubs = ('bash() { echo PREPARE; return "$PREPARE_STATUS"; }; '
             'chown() { echo OWNERSHIP; return "$CHOWN_STATUS"; }; '
             'rm() { echo CLEANUP; }; ')
    for variant, prepare_status, chown_status, expected in (
            ("topling", 17, 0, 17), ("topling", 0, 23, 23),
            ("topling", 0, 0, 0), ("standard", 17, 23, 0)):
        environment = dict(os.environ, RUNTIME_VARIANT=variant, HG_UID="10001", HG_GID="10001",
                           PREPARE_STATUS=str(prepare_status), CHOWN_STATUS=str(chown_status))
        result = subprocess.run(["sh", "-c", stubs + preparation], env=environment,
                                capture_output=True, text=True)
        assert result.returncode == expected, (dockerfile, variant, result.returncode, result.stdout)
        assert ("CLEANUP" in result.stdout) == (expected == 0), result.stdout
        assert ("OWNERSHIP" in result.stdout) == (variant == "topling" and prepare_status == 0)
    print(f"PASS: {dockerfile} preserves preparation/ownership failures before non-root runtime")

docker = shutil.which("docker")
if docker is not None:
    buildx = subprocess.run([docker, "buildx", "version"], capture_output=True, text=True)
    if buildx.returncode == 0:
        invalid = subprocess.run(
            [docker, "buildx", "bake", "-f", str(repo / "docker/bake.hcl"), "--print", "pd"],
            env={**os.environ, "RUNTIME_VARIANT": "toplingdb"},
            capture_output=True, text=True)
        assert invalid.returncode != 0, "Bake accepted an invalid runtime variant"
        assert "RUNTIME_VARIANT must be standard or topling" in invalid.stderr + invalid.stdout
    else:
        print("SKIP: Docker buildx is unavailable; Bake validation checked statically")
else:
    print("SKIP: Docker CLI is unavailable; Bake validation checked statically")

required = {
    "pd": {
        "HG_PD_GRPC_HOST": "pd",
        "HG_PD_RAFT_ADDRESS": "pd:8610",
        "HG_PD_RAFT_PEERS_LIST": "pd:8610",
        "HG_PD_INITIAL_STORE_LIST": "store:8500",
        "HG_PD_AUTH_SECRET_KEY": "test-secret",
    },
    "store": {
        "HG_STORE_PD_ADDRESS": "pd:8686",
        "HG_STORE_GRPC_HOST": "store",
        "HG_STORE_RAFT_ADDRESS": "store:8510",
    },
}
with tempfile.TemporaryDirectory(prefix="topling-entrypoints-") as temporary:
    for component, settings in required.items():
        directory = Path(temporary) / component
        (directory / "bin").mkdir(parents=True)
        entrypoint = repo / f"hugegraph-{component}/hg-{component}-dist/docker/docker-entrypoint.sh"
        shutil.copy(entrypoint, directory / "entrypoint.sh")
        launcher = directory / f"bin/start-hugegraph-{component}.sh"
        launcher.write_text('#!/bin/bash\nprintf \'%s\' "$SPRING_APPLICATION_JSON" > spring.json\n')
        launcher.chmod(0o755)
        environment = dict(os.environ, **settings)
        environment.pop("TOPLINGDB_ROCKSDB_PROVIDER", None)
        for provider in (None, "rocksdb", "topling"):
            if provider is not None:
                environment["TOPLINGDB_ROCKSDB_PROVIDER"] = provider
            result = subprocess.run(["bash", "entrypoint.sh"], cwd=directory, env=environment,
                                    capture_output=True, text=True)
            assert result.returncode == 0, result.stderr
            document = json.loads((directory / "spring.json").read_text())
            if provider is None:
                assert "rocksdb" not in document, "default must preserve mounted provider"
                assert "rocksdb.provider=<preserved from mounted configuration>" in result.stdout
                assert "rocksdb.provider=rocksdb" not in result.stdout
            else:
                assert document["rocksdb"]["provider"] == provider, document
                assert f"rocksdb.provider={provider}" in result.stdout
        (directory / "spring.json").unlink()
        environment["TOPLINGDB_ROCKSDB_PROVIDER"] = 'topling", "unexpected":true'
        result = subprocess.run(["bash", "entrypoint.sh"], cwd=directory, env=environment,
                                capture_output=True, text=True)
        assert result.returncode != 0 and not (directory / "spring.json").exists()
        print(f"PASS: {component} provider selection, default preservation and invalid input")

    environment = dict(os.environ)
    environment.pop("TOPLING_JNI_SHA256", None)
    environment.pop("TOPLING_JNI_URL", None)
    helper = repo / "docker/prepare-topling-image.sh"
    for values in ({}, {"TOPLING_JNI_SHA256": "not-a-sha"},
                   {"TOPLING_JNI_SHA256": "a" * 64, "TOPLING_JNI_URL": "http://example.invalid/jni.jar"}):
        result = subprocess.run(["bash", str(helper)], cwd=temporary, env=dict(environment, **values),
                                capture_output=True, text=True)
        assert result.returncode != 0, "unverified artifact accepted"
        assert "Error: Topling builds require" in result.stderr, result.stderr
    print("PASS: image preparation rejects missing/untrusted artifact inputs")

    # Exercise the real image helper through preparation, notice extraction and
    # profile installation, with only native preparation stubbed.
    fixture_jar = Path(temporary) / "rocksdbjni.jar"
    with zipfile.ZipFile(fixture_jar, "w") as jar:
        jar.writestr("META-INF/licenses/LICENSE.Apache", "Apache fixture\n")
        jar.writestr("META-INF/licenses/LICENSE.leveldb", "BSD fixture\n")
        jar.writestr("META-INF/licenses/manifest.sha256", "fixture manifest\n")
        jar.writestr("META-INF/topling-build-info.txt", "fixture build\n")

    distribution = Path(temporary) / "distribution"
    (distribution / "bin").mkdir(parents=True)
    (distribution / "conf").mkdir()
    (distribution / "tools").mkdir()
    original_profile = repo / "hugegraph-server/hugegraph-dist/src/assembly/static/conf/toplingdb.yaml"
    original_bytes = original_profile.read_bytes()
    shutil.copy(original_profile, distribution / "conf/toplingdb.yaml")
    preparer = distribution / "bin/prepare-topling.sh"
    preparer.write_text("#!/bin/bash\n"
                        "test -r \"$TOPLING_JNI_JAR\" && test \"${#TOPLING_JNI_SHA256}\" -eq 64\n"
                        "mkdir -p topling\n"
                        "cp \"$TOPLING_JNI_JAR\" topling/rocksdbjni.jar\n")
    downloader = distribution / "tools/curl"
    downloader.write_text("#!/bin/bash\n"
                          "while (( $# )); do\n"
                          "  if [[ $1 == --output ]]; then cp \"$FIXTURE_JAR\" \"$2\"; exit 0; fi\n"
                          "  shift\n"
                          "done\n"
                          "exit 1\n")
    downloader.chmod(0o755)
    environment.update(TOPLING_JNI_SHA256="a" * 64,
                       TOPLING_JNI_URL="https://example.invalid/immutable.jar",
                       FIXTURE_JAR=str(fixture_jar))
    environment["PATH"] = str(distribution / "tools") + os.pathsep + environment["PATH"]
    result = subprocess.run(["bash", str(helper)], cwd=distribution, env=environment,
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    community_profile = repo / "docker/toplingdb.yaml"
    assert (distribution / "conf/toplingdb.yaml").read_bytes() == community_profile.read_bytes()
    assert original_profile.read_bytes() == original_bytes, "distribution source profile changed"
    assert (distribution / "topling/LICENSE").is_file()
    assert (distribution / "topling/NOTICE").is_file()
    assert (distribution / "topling/licenses/LICENSE.Apache").read_text() == "Apache fixture\n"
    assert (distribution / "topling/licenses/LICENSE.leveldb").read_text() == "BSD fixture\n"
    assert (distribution / "topling/licenses/manifest.sha256").is_file()
    assert "ToplingDB JNI community runtime" in (distribution / "topling/NOTICE").read_text()

    profile = community_profile.read_text()
    # This profile contains only indentation-based mappings. Reject duplicate
    # paths instead of silently letting the YAML reader discard an earlier key.
    seen = set()
    parents = []
    for line in profile.splitlines():
        match = re.match(r"^( *)([A-Za-z_][\w]*):", line)
        if match is None:
            continue
        indent, key = len(match[1]), match[2]
        while parents and parents[-1][0] >= indent:
            parents.pop()
        path = tuple(parent[1] for parent in parents) + (key,)
        assert path not in seen, "duplicate YAML key: " + ".".join(path)
        seen.add(path)
        parents.append((indent, key))
    classes = set(re.findall(r"^\s+class:\s*(\S+)$", profile, re.MULTILINE))
    assert classes == {"LRUCache", "SkipList", "BlockBasedTable"}, classes
    assert "CSPP" not in profile and "ToplingZip" not in profile
    for setting in ("capacity: 64M", "write_buffer_size: 16M", "max_write_buffer_number: 3",
                    "memtable_as_log_index: false", "auto_start_http: false"):
        assert setting in profile, setting
    print("PASS: community image installs notices, small native profile and preserves full distribution profile")
PY
