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
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)
SCRIPT=${1:-"$REPO_ROOT/hugegraph-server/hugegraph-dist/src/assembly/static/bin/wait-partition.sh"}
# One real loopback socket and GNU timeout; no JVM, database or Docker.
python3 - "$SCRIPT" <<'PY'
import os
import pathlib
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

script = pathlib.Path(sys.argv[1]).resolve()
with tempfile.TemporaryDirectory(prefix='hugegraph-partition-diagnostic-') as directory:
    for mode, expected in [('healthy', 0), ('stalled', 124), ('TERM', 143), ('groupTERM', 143)]:
        work = pathlib.Path(directory) / mode
        work.mkdir()
        tools = work / 'tools'
        tools.mkdir()
        (tools / 'curl').write_text('''#!/bin/bash
if [[ "$1" == '-v' ]]; then
    echo "$$" > ./curl.pid
    echo "$PPID" > ./timeout.pid
fi
exec "$REAL_CURL" "$@"
''')
        (tools / 'curl').chmod(0o755)
        listener = socket.socket()
        listener.bind(('127.0.0.1', 0))
        listener.listen(2)
        accepted, stop = threading.Event(), threading.Event()

        def serve():
            for _ in range(2):
                connection, _ = listener.accept()
                request = connection.recv(8192)
                if b' /v1/partitions ' in request:
                    accepted.set()
                    if mode != 'healthy':
                        stop.wait(10)
                        connection.close()
                        return
                    body = b'{"partitions":[]}'
                else:
                    body = b'{"partitionCount":1}'
                response = b'HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n' % len(body)
                connection.sendall(response + body)
                connection.close()

        threading.Thread(target=serve, daemon=True).start()
        env = dict(os.environ, STORE_REST='127.0.0.1:%d' % listener.getsockname()[1],
                   WAIT_PARTITION_TIMEOUT_S='1' if mode == 'stalled' else '10',
                   PATH=str(tools) + ':' + os.environ['PATH'], REAL_CURL=shutil.which('curl'),
                   NO_PROXY='127.0.0.1', no_proxy='127.0.0.1')
        started = time.monotonic()
        with (work / 'output').open('wb') as output:
            process = subprocess.Popen(['bash', str(script)], cwd=work, env=env,
                                       stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                assert accepted.wait(3), 'final diagnostic did not reach the real socket'
                if mode == 'TERM':
                    os.kill(process.pid, signal.SIGTERM)
                elif mode == 'groupTERM':
                    os.killpg(process.pid, signal.SIGTERM)
                actual = process.wait(timeout=4)
                assert actual == expected, (mode, expected, actual, (work / 'output').read_text())
                for pid_file in [work / 'curl.pid', work / 'timeout.pid']:
                    owned = int(pid_file.read_text())
                    try:
                        os.kill(owned, 0)
                    except ProcessLookupError:
                        pass
                    else:
                        raise AssertionError(str(pid_file) + ' survived helper exit')
                print('PASS: final diagnostic %s -> %d (%.2fs); curl/timeout reaped' %
                      (mode, actual, time.monotonic() - started))
            finally:
                # A regression may leave GNU timeout in its own process group.
                # Only this fixture's recorded process IDs/groups are touched.
                for pid_file in [work / 'timeout.pid', work / 'curl.pid']:
                    if pid_file.exists():
                        owned = int(pid_file.read_text())
                        try:
                            os.killpg(owned, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        try:
                            os.kill(owned, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                try:
                    process.wait(timeout=0.5)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait(timeout=2)
                stop.set()
                listener.close()
PY
