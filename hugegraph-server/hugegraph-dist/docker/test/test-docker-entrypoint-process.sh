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
# Real shell parents and children, with only REST readiness and Java replaced.
# Stalled-probe cases use one temporary localhost socket; no JVM, Docker or DB.
python3 - "$REPO_ROOT" <<'PY'
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

repo = pathlib.Path(sys.argv[1])
static = repo / 'hugegraph-server/hugegraph-dist/src/assembly/static/bin'
entrypoint = repo / 'hugegraph-server/hugegraph-dist/docker/docker-entrypoint.sh'

denied = subprocess.run(['bash', '-c', 'source "$1"; print_jvm_thread_hint',
                         'bash', str(static / 'util.sh')],
                        input='pthread_create failed (EPERM)\n', text=True, capture_output=True)
assert denied.returncode == 0, denied.stderr
assert '--security-opt seccomp=unconfined' in denied.stderr, denied.stderr
assert 'nproc' not in denied.stderr and 'thread limits' not in denied.stderr, denied.stderr

for message in ['pthread_create failed (EAGAIN) for attributes: stacksize: 1024k',
                'pthread_create failed: Resource temporarily unavailable']:
    exhausted = subprocess.run(['bash', '-c', 'source "$1"; print_jvm_thread_hint',
                               'bash', str(static / 'util.sh')],
                              input=message, text=True, capture_output=True)
    assert exhausted.returncode == 0, exhausted.stderr
    assert 'insufficient memory, or process/thread limits' in exhausted.stderr, exhausted.stderr
    assert 'JVM thread creation was denied' not in exhausted.stderr, exhausted.stderr
mixed = subprocess.run(['bash', '-c', 'source "$1"; print_jvm_thread_hint',
                        'bash', str(static / 'util.sh')],
                       input='pthread_create failed (EAGAIN)\npthread_create failed (EPERM)\n',
                       text=True, capture_output=True)
assert mixed.returncode == 0 and 'denied (EPERM)' in mixed.stderr, mixed.stderr
assert 'nproc' not in mixed.stderr and 'Possible causes' not in mixed.stderr, mixed.stderr
for message in ['unrelated I/O failed (EAGAIN)', 'Resource temporarily unavailable', 'Address already in use']:
    unrelated = subprocess.run(['bash', '-c', 'source "$1"; print_jvm_thread_hint',
                                'bash', str(static / 'util.sh')],
                               input=message, text=True, capture_output=True)
    assert unrelated.returncode == 1 and not unrelated.stderr, unrelated.stderr


def await_file(path, process):
    deadline = time.monotonic() + 5
    while not path.exists():
        assert process.poll() is None, 'entrypoint exited before ' + str(path)
        assert time.monotonic() < deadline, 'fixture did not reach ' + str(path)
        time.sleep(0.02)


with tempfile.TemporaryDirectory(prefix='hugegraph-entrypoint-process-') as directory:
    for mode, expected in [('exit0', 0), ('exit42', 42), ('kill137', 137),
                           ('TERM', 143), ('INT', 130), ('groupTERM', 143),
                           ('early42', 42), ('early0', 1), ('timeout', 1),
                           ('early42-eperm-stdout', 42),
                           ('early42-file-eperm-stdout', 42), ('early42-file-eperm-log', 42),
                           ('early42-file-output', 42), ('early42-file-output-no-crash', 42),
                           ('early42-stale-stdout', 42), ('early42-truncated-stdout', 42),
                           ('early42-resource', 42), ('early42-stale-file', 42),
                           ('early42-stale-log', 42), ('early37-init', 37),
                           ('timeout-ignore', 1), ('partition42', 42),
                           ('partitionTERM', 143), ('partitionINT', 130),
                           ('partitiongroupTERM', 143), ('readinessTERM', 143),
                           ('readinessINT', 130), ('readinessgroupTERM', 143),
                           ('partitionDiagnostic42', 42), ('partitionDiagnosticTERM', 143),
                           ('partitionDiagnosticINT', 130), ('partitionDiagnosticgroupTERM', 143)]:
        work = pathlib.Path(directory) / mode
        for name in ['bin', 'tools', 'conf/graphs', 'docker', 'logs']:
            (work / name).mkdir(parents=True)
        shutil.copy(entrypoint, work / 'entrypoint.sh')
        for name in ['start-hugegraph.sh', 'util.sh']:
            shutil.copy(static / name, work / 'bin' / name)
        (work / 'docker/init_complete').touch()
        if mode == 'early42-stale-file':
            stale = work / 'logs/hs_err_pid1.log'
            stale.write_text('pthread_create failed (EPERM)\n')
            os.utime(stale, (1, 1))
        if mode == 'early42-stale-log':
            (work / 'logs/hugegraph-server.log').write_text('pthread_create failed (EPERM)\n')
        if mode in ['early42-stale-stdout', 'early42-truncated-stdout']:
            (work / 'logs/hugegraph-server-stdout.log').write_text('pthread_create failed (EPERM)\n' * 10)
        backend = 'hstore' if mode.startswith('partition') else 'rocksdb'
        (work / 'conf/graphs/hugegraph.properties').write_text('backend=' + backend + '\n')
        (work / 'conf/rest-server.properties').write_text(
            'restserver.url=http://127.0.0.1:1\ngremlinserver.url=http://127.0.0.1:2\n')
        listener = None
        accepted, stop_http = threading.Event(), threading.Event()
        if mode.startswith('partitionDiagnostic'):
            listener = socket.socket()
            listener.bind(('127.0.0.1', 0))
            listener.listen(2)
            port = listener.getsockname()[1]

            def hold_partition_diagnostic():
                # Real curl first receives positive readiness, then stalls on
                # the distinct final diagnostic endpoint.
                for _ in range(2):
                    connection, _ = listener.accept()
                    request = connection.recv(8192)
                    if b' /v1/partitions ' in request:
                        accepted.set()
                        stop_http.wait(10)
                        connection.close()
                        return
                    body = b'{"partitionCount":1}'
                    response = b'HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n' % len(body)
                    connection.sendall(response + body)
                    connection.close()

            threading.Thread(target=hold_partition_diagnostic, daemon=True).start()
        if mode.startswith('readiness'):
            listener = socket.socket()
            listener.bind(('127.0.0.1', 0))
            listener.listen(1)
            port = listener.getsockname()[1]
            (work / 'conf/rest-server.properties').write_text(
                'restserver.url=http://127.0.0.1:%d\ngremlinserver.url=http://127.0.0.1:2\n' % port)
            # The fixture HTTP listener replaces the JVM listener. Skip only
            # launcher port preflight; readiness still runs the real helper.
            with (work / 'bin/util.sh').open('a') as utility:
                utility.write('\ncheck_port() { :; }\n')

            def hold_http():
                connection, _ = listener.accept()
                connection.recv(8192)
                accepted.set()
                stop_http.wait(10)
                connection.close()

            threading.Thread(target=hold_http, daemon=True).start()
        (work / 'bin/init-store.sh').write_text('#!/bin/bash\nexit 0\n')
        if mode == 'early37-init':
            (work / 'bin/init-store.sh').write_text(
                '#!/bin/bash\nprintf \'Failed to start thread "GC Thread#0" - '
                'pthread_create failed (EPERM)\\n\'\n'
                'printf "Cannot create worker GC thread\\n" > logs/hs_err_pid37.log\nexit 37\n')
        (work / 'bin/wait-partition.sh').write_text(
            '#!/bin/bash\ntouch ./partition-ran\nexit 7\n')
        if mode.startswith('partition') and mode != 'partition42':
            shutil.copy(static / 'wait-partition.sh', work / 'bin/wait-partition.sh')
        (work / 'tools/curl').write_text('''#!/bin/bash
case "$TEST_CHILD_MODE" in
    early* | timeout*) printf '000'; exit 28 ;;
esac
if [[ "$1" != '-I' && "$TEST_CHILD_MODE" == partitionDiagnostic* ]]; then
    if [[ "$1" == '-v' ]]; then
        echo "$$" > ./partition-curl.pid
        echo "$PPID" > ./partition-timeout.pid
        ps -o ppid= -p "$PPID" | tr -d ' ' > ./partition-wrapper.pid
        touch ./partition-ran
    fi
    exec "$TEST_REAL_CURL" "$@"
fi
if [[ "$TEST_CHILD_MODE" == readiness* ]]; then
    ancestor=$$
    for index in 1 2 3 4; do
        echo "$ancestor" > "./readiness-owned${index}.pid"
        name=$(ps -o comm= -p "$ancestor")
        if [[ "${name##*/}" == *timeout ]]; then
            echo "$ancestor" > ./readiness-timeout.pid
            break
        fi
        ancestor=$(ps -o ppid= -p "$ancestor" | tr -d ' ')
    done
    exec "$TEST_REAL_CURL" "$@"
fi
if [[ "$1" != '-I' && "$TEST_CHILD_MODE" == partition* ]]; then
    echo "$PPID" > ./partition-shell.pid
    timeout_pid=$(ps -o ppid= -p "$PPID" | tr -d ' ')
    echo "$timeout_pid" > ./partition-timeout.pid
    ps -o ppid= -p "$timeout_pid" | tr -d ' ' > ./partition-wrapper.pid
    touch ./partition-ran
    printf '{"partitionCount":0}'
    exit 0
fi
touch ./ready-probe
printf '200'
''')
        (work / 'bin/hugegraph-server.sh').write_text('''#!/usr/bin/env python3
import os, pathlib, signal, sys, time
work = pathlib.Path.cwd()
mode = os.environ['TEST_CHILD_MODE']
def stop(signum, frame):
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    (work / 'term-observed').touch()
    # Expose a duplicate group signal killing a wrapper before it reaps us.
    time.sleep(0.2)
    sys.exit(143)
signal.signal(signal.SIGTERM, signal.SIG_IGN if mode == 'timeout-ignore' else stop)
(work / 'child-running').touch()
if mode.startswith('early'):
    if mode in ['early42-file-output', 'early42-file-output-no-crash', 'early42-truncated-stdout']:
        (work / 'logs/hugegraph-server-stdout.log').write_text('pthread_create failed (EPERM)\\n')
        if mode == 'early42-file-output':
            (work / 'logs/hs_err_pid42.log').write_text('Cannot create worker GC thread\\n')
    elif mode in ['early42-file-eperm-stdout', 'early42-file-eperm-log']:
        if mode == 'early42-file-eperm-stdout':
            target = work / 'logs/hugegraph-server-stdout.log'
            message = 'Exception in thread "main" java.nio.file.FileSystemException: /data/LOCK: Operation not permitted'
        else:
            target = work / 'logs/hugegraph-server.log'
            message = '[pool-3-thread-1] [ERROR] RocksDBStore - Failed to open RocksDB: lock: Operation not permitted'
        with target.open('a') as output:
            output.write(message + '\\n')
    elif mode in ['early42-eperm-stdout', 'early42-resource']:
        if mode == 'early42-eperm-stdout':
            print('Failed to start thread "GC Thread#0" - pthread_create failed (EPERM)', flush=True)
        (work / 'logs/hs_err_pid42.log').write_text('Cannot create worker GC thread\\n')
    elif mode == 'early42-stale-log':
        with (work / 'logs/hugegraph-server.log').open('a') as output:
            output.write('unrelated startup failure\\n')
    sys.exit(int(mode[5:].split('-')[0]))
while not (work / 'child-exit').exists():
    time.sleep(0.02)
sys.exit(int((work / 'child-exit').read_text()))
''')
        for folder in ['bin', 'tools']:
            for script in (work / folder).iterdir():
                script.chmod(0o755)
        env = {key: value for key, value in os.environ.items()
               if not key.startswith('HG_SERVER_') and key not in ['PASSWORD', 'BACKEND', 'PD_PEERS']}
        env.update(PATH=str(work / 'tools') + ':' + os.environ['PATH'],
                   TEST_CHILD_MODE=mode, HG_SERVER_STARTUP_TIMEOUT_S='3',
                   TEST_REAL_CURL=shutil.which('curl'), NO_PROXY='127.0.0.1', no_proxy='127.0.0.1')
        if mode in ['early42-file-output', 'early42-file-output-no-crash',
                    'early42-stale-stdout', 'early42-truncated-stdout']:
            env['STDOUT_MODE'] = 'false'
        if mode.startswith('readiness'):
            env['HG_SERVER_STARTUP_TIMEOUT_S'] = '60'
        if mode.startswith('partitionDiagnostic'):
            env['STORE_REST'] = '127.0.0.1:%d' % port
            env['WAIT_PARTITION_TIMEOUT_S'] = '1' if mode.endswith('42') else '10'
        started = time.monotonic()
        with (work / 'output').open('wb') as output:
            process = subprocess.Popen(['bash', './entrypoint.sh'], cwd=work, env=env,
                                       stdout=output, stderr=subprocess.STDOUT,
                                       start_new_session=True)
            try:
                if mode.startswith('timeout'):
                    await_file(work / 'child-running', process)
                if mode.startswith('readiness'):
                    await_file(work / 'child-running', process)
                    assert accepted.wait(5), 'real curl did not reach stalled HTTP fixture'
                    if mode == 'readinessgroupTERM':
                        os.killpg(process.pid, signal.SIGTERM)
                    else:
                        signum = signal.SIGINT if mode.endswith('INT') else signal.SIGTERM
                        os.kill(process.pid, signum)
                elif not mode.startswith(('early', 'timeout')):
                    await_file(work / 'child-running', process)
                    await_file(work / 'ready-probe', process)
                    if mode.startswith('partition'):
                        await_file(work / 'partition-ran', process)
                    if mode.startswith('partitionDiagnostic'):
                        assert accepted.wait(5), 'final real curl did not reach the stalled endpoint'
                    if mode in ['TERM', 'INT', 'partitionTERM', 'partitionINT',
                                'partitionDiagnosticTERM', 'partitionDiagnosticINT']:
                        signum = signal.SIGINT if mode.endswith('INT') else signal.SIGTERM
                        os.kill(process.pid, signum)
                    elif mode in ['groupTERM', 'partitiongroupTERM', 'partitionDiagnosticgroupTERM']:
                        os.killpg(process.pid, signal.SIGTERM)
                    elif mode == 'kill137':
                        os.kill(int((work / 'bin/pid').read_text()), signal.SIGKILL)
                    else:
                        status = 42 if mode in ['partition42', 'partitionDiagnostic42'] else int(mode[4:])
                        # Publish the exit status only after its contents are
                        # complete; the launcher polls for the final path.
                        pending = work / 'child-exit.pending'
                        pending.write_text(str(status))
                        pending.replace(work / 'child-exit')
                actual = process.wait(timeout=5)
                assert actual == expected, (mode, expected, actual, (work / 'output').read_text())
                diagnostic = (work / 'output').read_text()
                if mode in ['early42-file-eperm-stdout', 'early42-file-eperm-log']:
                    assert 'JVM thread creation was denied' not in diagnostic, diagnostic
                    assert '--security-opt seccomp=unconfined' not in diagnostic, diagnostic
                if mode in ['early42-file-output', 'early42-file-output-no-crash', 'early42-truncated-stdout']:
                    assert 'JVM thread creation was denied (EPERM)' in diagnostic, diagnostic
                    assert 'Possible causes' not in diagnostic, diagnostic
                    assert '--ulimit nproc' not in diagnostic, diagnostic
                elif mode in ['early42-eperm-stdout', 'early42-resource', 'early37-init']:
                    assert '--security-opt seccomp=unconfined' in diagnostic, diagnostic
                    assert '--ulimit nproc=65535:65535' in diagnostic, diagnostic
                    assert 'Possible causes include seccomp denial, insufficient memory' in diagnostic, diagnostic
                    assert 'logs/hugegraph-server-stdout.log with STDOUT_MODE=false' in diagnostic, diagnostic
                    assert 'JVM thread creation was denied' not in diagnostic, diagnostic
                    if mode != 'early42-resource':
                        assert 'Failed to start thread "GC Thread#0"' in diagnostic, diagnostic
                else:
                    assert '--security-opt seccomp=unconfined' not in diagnostic, diagnostic
                if mode.startswith('timeout'):
                    assert time.monotonic() - started < 5, 'startup cleanup exceeded the deadline'
                if mode in ['TERM', 'INT', 'groupTERM', 'partitionTERM', 'partitionINT',
                            'partitiongroupTERM', 'readinessTERM', 'readinessINT',
                            'readinessgroupTERM', 'partitionDiagnosticTERM',
                            'partitionDiagnosticINT', 'partitionDiagnosticgroupTERM']:
                    assert (work / 'term-observed').exists(), 'TERM did not reach the JVM substitute'
                if not mode.startswith('timeout') and mode != 'early37-init':
                    owned = ([work / 'bin/pid'] + list(work.glob('partition-*.pid')) +
                             list(work.glob('readiness-*.pid')))
                    for pid_file in owned:
                        child = int(pid_file.read_text())
                        try:
                            os.kill(child, 0)
                        except ProcessLookupError:
                            pass
                        else:
                            raise AssertionError(str(pid_file) + ' survived entrypoint exit')
                if mode in ['partition42', 'partitionDiagnostic42']:
                    assert 'WARN: partitions not assigned yet' in (work / 'output').read_text()
                print('PASS: ' + mode + ' -> ' + str(actual))
            finally:
                # Timeout intentionally permits an unresponsive process until
                # container termination. The fixture owns and kills that group.
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                # GNU timeout owns a separate process group. Clean it too if
                # a regression prevents the wrapper from forwarding shutdown.
                for pid_file in [work / 'partition-timeout.pid', work / 'readiness-timeout.pid']:
                    if pid_file.exists():
                        try:
                            os.killpg(int(pid_file.read_text()), signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                for pid_file in list(work.glob('partition-*.pid')) + list(work.glob('readiness-*.pid')):
                    try:
                        os.kill(int(pid_file.read_text()), signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                process.wait(timeout=2)
                if listener is not None:
                    stop_http.set()
                    listener.close()
PY

# This process fixture is already invoked by the Docker/Server CI entrypoint tests.
bash "$(dirname "${BASH_SOURCE[0]}")/test-wait-partition.sh"
