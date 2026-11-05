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
# Usage: bash test-arthas-cli.sh <built-server-or-store-distribution>
# Real native attachment; all processes and files below belong to this test.
set -euo pipefail
[ "$#" -eq 1 ] || { echo "Supply a built Server or Store distribution" >&2; exit 1; }
python3 - "$1" <<'PY'
import os
import pathlib
import pty
import re
import select
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import zipfile

built = pathlib.Path(sys.argv[1]).resolve()
store = (built / "bin/start-hugegraph-store.sh").is_file()
component = "Store" if store else "Server"
startup = "start-hugegraph-store.sh" if store else "start-hugegraph.sh"
other_startup = "start-hugegraph.sh" if store else "start-hugegraph-store.sh"
assert (built / "bin" / startup).is_file(), f"Not a Server or Store distribution: {built}"
java_home = os.environ.get("JAVA_HOME")
java = str(pathlib.Path(java_home) / "bin/java") if java_home else "java"
javac = str(pathlib.Path(java_home) / "bin/javac") if java_home else "javac"
with tempfile.TemporaryDirectory(prefix="arthas-test-") as work:
    root = (pathlib.Path(work) / "native fixture").resolve()
    for name in ("bin", "lib", "classes", "client-temp"):
        (root / name).mkdir(parents=True)
    for name in ("arthas.sh", "ArthasAttach.java"):
        shutil.copy(built / "bin" / name, root / "bin" / name)
    (root / "lib/arthas").symlink_to(built / "lib/arthas", target_is_directory=True)
    (root / "bin" / startup).touch()
    main = ("org.apache.hugegraph.store.node.StoreNodeApplication" if store else
            "org.apache.hugegraph.bootstrap.HugeGraphServerBootstrap")
    package, name = main.rsplit(".", 1)
    source = root / f"{name}.java"
    source.write_text(f"package {package}; public class {name} {{ "
                      "public static void main(String[] args) throws Exception { "
                      "Thread.sleep(180000); }}")
    subprocess.run([javac, "-d", str(root / "classes"), str(source)], check=True)
    if store:
        # Store starts from an executable jar; exercise that PID validation path
        # using the built distribution's real bundled agent and attach helper.
        fixture_jar = root / "lib/hg-store-node-fixture.jar"
        with zipfile.ZipFile(fixture_jar, "w") as jar:
            jar.writestr("META-INF/MANIFEST.MF", f"Manifest-Version: 1.0\nMain-Class: {main}\n\n")
            for compiled in (root / "classes").rglob("*.class"):
                jar.write(compiled, compiled.relative_to(root / "classes"))
        launch = ["-jar", str(fixture_jar)]
    else:
        launch = ["-cp", str(root / "classes"), main]
    reservations = [socket.socket() for _ in range(3)]
    for reservation in reservations:
        reservation.bind(("127.0.0.1", 0))
    safe_port, unsafe_telnet, unsafe_http = [s.getsockname()[1] for s in reservations]
    for reservation in reservations:
        reservation.close()
    tunnel = socket.socket()
    tunnel.bind(("127.0.0.1", 0))
    tunnel.listen()
    tunnel.settimeout(0.5)
    tunnel_url = f"ws://127.0.0.1:{tunnel.getsockname()[1]}/ws"
    unsafe = root / "unsafe.properties"
    unsafe.write_text(f"arthas.config.overrideAll=true\narthas.ip=0.0.0.0\n"
                      f"arthas.telnetPort={unsafe_telnet}\narthas.httpPort={unsafe_http}\n"
                      f"arthas.mcpEndpoint=/mcp\narthas.tunnelServer={tunnel_url}\n"
                      "arthas.disabledCommands=stop,session\n")
    target_env = dict(os.environ, ARTHAS_CONFIG_LOCATION=str(unsafe),
                      ARTHAS_CONFIG_OVERRIDE_ALL="true", ARTHAS_CONFIG_OVERRIDEALL="true", ARTHAS_IP="0.0.0.0",
                      ARTHAS_TUNNELSERVER=tunnel_url, ARTHAS_TUNNEL_SERVER=tunnel_url,
                      ARTHAS_DISABLEDCOMMANDS="stop,session")
    target_env["arthas.config.location"] = str(unsafe)
    target_env["arthas.tunnelServer"] = tunnel_url
    client_env = dict(os.environ, TMPDIR=str(root / "client-temp"), JAVA_TOOL_OPTIONS=f'-Djava.io.tmpdir="{root / "client-temp"}"')
    script = ["bash", str(root / "bin/arthas.sh")]
    client = script + ["--telnet-port", str(safe_port)]

    def run(arguments, success=True, env=None):
        result = subprocess.run(arguments, env=client_env if env is None else env, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, input="", timeout=45)
        assert (result.returncode == 0) == success, result.stdout
        return result.stdout

    def can_connect(host, port):
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            return False

    def wait_port(opened, host="127.0.0.1"):
        deadline = time.monotonic() + 15
        while can_connect(host, safe_port) != opened:
            assert time.monotonic() < deadline, f"Listener did not become {'open' if opened else 'closed'}"
            time.sleep(0.1)

    class Terminal:
        # fork gives the helper a real controlling terminal: closing its master
        # reproduces terminal hangup, including the kernel's process-group HUP.
        def __init__(self, shell=False, options=()):
            self.pid, self.master = pty.fork()
            if self.pid == 0:
                arguments = ["bash", "--noprofile", "--norc", "-i"] if shell else client + list(options)
                os.execvpe(arguments[0], arguments, client_env)
            self.returncode = None
            self.output = ""
            if shell:
                os.write(self.master, (shlex.join(client) + "\n").encode())

        def poll(self):
            if self.returncode is None:
                child, status = os.waitpid(self.pid, os.WNOHANG)
                if child:
                    self.returncode = os.waitstatus_to_exitcode(status)
            return self.returncode

        def drain(self):
            if self.master is not None and select.select([self.master], [], [], 0)[0]:
                try:
                    output = os.read(self.master, 65536).decode(errors="replace")
                    self.output += output
                    log.write(output)
                    log.flush()
                except OSError:
                    pass  # The slave closes when the helper exits.

        def expect(self, pattern, offset=0):
            deadline = time.monotonic() + 15
            while True:
                self.drain()
                match = re.search(pattern, self.output[offset:])
                if match:
                    return offset + match.end()
                assert self.poll() is None and time.monotonic() < deadline, self.output
                time.sleep(0.02)

        def ready(self, offset=0):
            return self.expect(r"\[arthas@\d+\]\$ ", offset)

        def command(self, command, expected):
            offset = len(self.output)
            os.write(self.master, (command + "\n").encode())
            offset = self.expect(expected, offset)
            self.ready(offset)

        def wait(self, timeout=30):
            deadline = time.monotonic() + timeout
            while self.poll() is None:
                self.drain()
                assert time.monotonic() < deadline, "Interactive helper did not exit"
                time.sleep(0.02)
            self.drain()
            if self.master is not None:
                os.close(self.master)
                self.master = None
            return self.returncode

    interactive = None
    with (root / "jvm.log").open("w") as log:
        process = subprocess.Popen([java, "-Darthas.config.location=" + str(unsafe),
                                    "-Darthas.config.overrideAll=true", "-Darthas.ip=0.0.0.0",
                                    "-Darthas.tunnelServer=" + tunnel_url,
                                    "-Darthas.disabledCommands=stop,session",
                                    *launch, "fixture"], env=target_env, stdout=log, stderr=subprocess.STDOUT)
        try:
            (root / "bin/pid").write_text(str(process.pid))
            time.sleep(1)
            for options in (["--pid", "-1"], ["--pid", "1; unsafe"], ["--pid", "9999999999"],
                            ["--telnet-port", "65536"], ["--target-ip", "::not-an-ip"],
                            ["--username", "line\nbreak"],
                            ["--http-port", "8563"], ["--tunnel-server", tunnel_url]):
                output = run(script + options + ["--command", "version"], success=False)
                if options[0] == "--username":
                    assert "single-line values" in output and "Waiting for PID" not in output, output
                    assert not can_connect("127.0.0.1", safe_port)
            # EOF and piped input must fail before attaching; batch callers need --command.
            for input_text in ("", "version\n"):
                result = subprocess.run(client, env=client_env, input=input_text, text=True,
                                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=10)
                assert result.returncode != 0 and "use --command" in result.stdout, result.stdout
                assert not can_connect("127.0.0.1", safe_port)
            # A live PID from a different component must not receive the agent.
            (root / "bin" / startup).unlink()
            (root / "bin" / other_startup).touch()
            run(client + ["--command", "version"], success=False)
            (root / "bin" / other_startup).unlink()
            (root / "bin" / startup).touch()
            output = run(client + ["--pid", str(process.pid), "--command", "version"])
            assert re.search(r"\n4\.1\.5\s*\n", output), output
            assert (f"Process {process.pid} already using port {safe_port}" in output or
                    f"The target process already listen port {safe_port}, skip attach." in output), output
            wait_port(False)
            assert process.poll() is None, "One-time diagnosis stopped the application JVM"
            # Keep a real interactive client open while inspecting the live agent.
            interactive = Terminal()
            wait_port(True)
            assert not can_connect("127.0.0.1", unsafe_telnet)
            assert not can_connect("127.0.0.1", unsafe_http)
            try:
                connection, address = tunnel.accept()
            except socket.timeout:
                pass
            else:
                connection.close()
                raise AssertionError(f"Arthas contacted the overridden tunnel endpoint: {address}")
            # Inspect actual bind addresses; VPNs may redirect connection probes.
            if sys.platform.startswith("linux"):
                inodes = set()
                for descriptor in pathlib.Path(f"/proc/{process.pid}/fd").iterdir():
                    try:
                        link = os.readlink(descriptor)
                    except FileNotFoundError:
                        continue
                    if link.startswith("socket:["):
                        inodes.add(link[8:-1])
                addresses = []
                for table in ("tcp", "tcp6"):
                    for line in pathlib.Path(f"/proc/{process.pid}/net/{table}").read_text().splitlines()[1:]:
                        fields = line.split()
                        address, port = fields[1].split(":")
                        if fields[3] == "0A" and fields[9] in inodes:
                            addresses.append((address, int(port, 16)))
                assert len(addresses) == 1 and addresses[0][1] == safe_port, addresses
                assert addresses[0][0] in ("0100007F", "0000000000000000FFFF00000100007F"), addresses
            else:
                listeners = subprocess.check_output(["lsof", "-nP", "-a", "-p", str(process.pid),
                                                     "-iTCP", "-sTCP:LISTEN"], text=True).splitlines()[1:]
                assert len(listeners) == 1 and f"127.0.0.1:{safe_port} " in listeners[0], listeners
            os.write(interactive.master, b"quit\n")
            assert interactive.wait(45) == 0, (root / "jvm.log").read_text()
            wait_port(False)
            # Interactive Bash propagates the terminal HUP to its foreground job.
            # Closing a direct helper PTY alone does not reproduce that signal sequence.
            interactive = Terminal(shell=True)
            wait_port(True)
            interactive.ready()
            os.close(interactive.master)
            interactive.master = None
            status = interactive.wait()
            assert status in (-signal.SIGHUP, 128 + signal.SIGHUP), status
            wait_port(False)
            assert process.poll() is None, "Terminal hangup stopped the application JVM"
            # Signals sent only to the helper must also clean up its child/client.
            for termination in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
                interactive = Terminal()
                if termination == signal.SIGTERM:
                    # Terminate during source compilation/attachment, before a listener exists.
                    time.sleep(0.2)
                else:
                    wait_port(True)
                    interactive.ready()
                os.kill(interactive.pid, termination)
                if termination != signal.SIGTERM:
                    # Repeated/mixed signals during cleanup must not interrupt stop_agent.
                    for repeat in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM) * 5:
                        time.sleep(0.02)
                        if interactive.poll() is None:
                            os.kill(interactive.pid, repeat)
                assert interactive.wait() == 128 + termination, (root / "jvm.log").read_text()
                wait_port(False)
                assert process.poll() is None, "Terminal/signal cleanup stopped the application JVM"
            # Linux mktemp honors TMPDIR, exposing only this fixture's marker.
            # macOS mktemp ignores it; the other terminal/signal cases run there.
            if sys.platform.startswith("linux"):
                # Pause after validation so loadAgent cannot finish before the
                # signal reaches both the helper and its attach JVM.
                for termination in (signal.SIGTERM, signal.SIGHUP):
                    interactive = Terminal()
                    deadline = time.monotonic() + 15
                    while not list((root / "client-temp").glob("tmp.*/validated")):
                        interactive.drain()
                        assert interactive.poll() is None and time.monotonic() < deadline, interactive.output
                        time.sleep(0.001)
                    os.kill(process.pid, signal.SIGSTOP)
                    try:
                        assert not can_connect("127.0.0.1", safe_port), "Agent started before the attach pause"
                        # Queue the attach request before signaling the group.
                        time.sleep(0.1)
                        os.killpg(interactive.pid, termination)
                        if termination == signal.SIGHUP:
                            # Kernel and interactive Bash can deliver HUP almost together.
                            time.sleep(0.00006)
                            os.killpg(interactive.pid, termination)
                        time.sleep(1)
                        assert interactive.poll() is None, "Group signal interrupted in-flight attachment"
                    finally:
                        os.kill(process.pid, signal.SIGCONT)
                    assert interactive.wait() == 128 + termination, interactive.output
                    wait_port(False)
                    assert process.poll() is None, "Group signal stopped the application JVM"
            # macOS requires an explicit interface alias for 127.0.0.2; Linux
            # routes all of 127/8 locally and exercises that address in CI.
            hosts = ("127.0.0.2", "::1", "localhost") if sys.platform.startswith("linux") else ("::1", "localhost")
            for host in (*hosts, "0.0.0.0", "::"):
                output = run(client + ["--target-ip", host, "--command", "version"])
                assert re.search(r"\n4\.1\.5\s*\n", output), output
                connect_host = {"localhost": "127.0.0.1", "0.0.0.0": "127.0.0.1", "::": "127.0.0.1"}.get(host, host)
                endpoint = f"[{connect_host}]:{safe_port}" if ":" in connect_host else f"{connect_host}:{safe_port}"
                assert output.count(f"Arthas listener closed at {endpoint}") == 1, output
                wait_port(False, connect_host)
            # Reproduce the cleanup race after an explicit stop: the first
            # session probe loses its connection while the agent shuts down.
            # All attachment, stop and subsequent probes still use the real JVM.
            wrapper_home = root / "java-wrapper"
            (wrapper_home / "bin").mkdir(parents=True)
            marker = root / "cleanup-race"
            injected = root / "cleanup-race-injected"
            wrapper = wrapper_home / "bin/java"
            wrapper.write_text(f"#!{sys.executable}\n" +
                               "import pathlib, subprocess, sys\n" +
                               f"marker = pathlib.Path({str(marker)!r})\n" +
                               "args = sys.argv[1:]\n" +
                               "if '-f' in args and marker.exists():\n" +
                               "    batch = pathlib.Path(args[args.index('-f') + 1]).read_text()\n" +
                               "    if batch.strip() == 'session':\n" +
                               f"        marker.rename({str(injected)!r})\n" +
                               "        print('Connection reset during shutdown', file=sys.stderr)\n" +
                               "        sys.exit(1)\n" +
                               f"status = subprocess.call([{java!r}] + args)\n" +
                               "if '--command' in args and args[args.index('--command') + 1] == 'stop':\n" +
                               "    marker.touch()\n" +
                               "sys.exit(status)\n")
            wrapper.chmod(0o755)
            output = run(client + ["--command", "stop"], env=dict(client_env, JAVA_HOME=str(wrapper_home)))
            assert injected.is_file() and not marker.exists(), "Shutdown race was not exercised"
            assert output.count(f"Arthas listener closed at 127.0.0.1:{safe_port}") == 1, output
            wait_port(False)
            # Disable native local authentication bypass for the following
            # attachments; earlier signal cases exercise native no-auth defaults.
            run(client + ["--command", "sysprop arthas.localConnectionNonAuth false"])
            wait_port(False)
            password = "p a;ss'\"\\word"
            auth = ["--username", "operator name", "--password", password]
            for options in (auth, ["--password", password]):
                output = run(client + options + ["--command", "version"])
                assert re.search(r"\n4\.1\.5\s*\n", output), output
                assert password not in output and "auth --username" not in output, output
                wait_port(False)
                assert process.poll() is None, "Authenticated command stopped the application JVM"
            interactive = Terminal(options=auth)
            wait_port(True)
            interactive.ready()
            # An independent unauthenticated loopback client must be rejected.
            unauthenticated = run([java, "-jar", str(root / "lib/arthas/arthas-client.jar"),
                                   "-c", "version", "127.0.0.1", str(safe_port)])
            assert "command not permitted" in unauthenticated, unauthenticated
            assert not re.search(r"\n4\.1\.5\s*\n", unauthenticated), unauthenticated
            # A running agent retains its credentials; invalid credentials must
            # fail diagnosis/cleanup without stopping the existing listener.
            wrong = run(client + ["--username", "operator name", "--password", "incorrect ; secret",
                                  "--command", "version"], success=False)
            assert "command not permitted" in wrong and "incorrect ; secret" not in wrong, wrong
            assert "credentials rejected" in wrong and "agent left running" in wrong, wrong
            wait_port(True)
            quoted_password = "'" + password.replace("'", "'\\''") + "'"
            # The terminal can discard queued input while an earlier command is running.
            # Wait for its result and the next complete prompt before sending another command.
            interactive.command(f'auth --username "operator name" {quoted_password}',
                                r"Authentication result: true, username: operator name")
            interactive.command("version", r"[\r\n]4\.1\.5\s*[\r\n]")
            os.write(interactive.master, b"quit\n")
            assert interactive.wait(45) == 0, interactive.output
            assert re.search(r"[\r\n]4\.1\.5\s*[\r\n]", interactive.output), interactive.output
            wait_port(False)
            assert process.poll() is None, "Authenticated quit stopped the application JVM"
            # Authentication echoes must not let one JVM impersonate another
            # agent's session row. Both JVMs are valid targets in this fixture.
            other = subprocess.Popen([java, "-Darthas.localConnectionNonAuth=false", *launch, "other fixture"],
                                     env=target_env, stdout=log, stderr=subprocess.STDOUT)
            try:
                time.sleep(1)
                impersonating = ["--username", f"prefix JAVA_PID {process.pid} suffix",
                                 "--password", "mismatch ; secret"]
                interactive = Terminal(options=["--pid", str(other.pid), *impersonating])
                wait_port(True)
                interactive.ready()
                missing = run(script + ["--pid", str(other.pid), "--telnet-port", str(unsafe_http),
                                        *impersonating, "--command", "version"], success=False)
                assert f"no Arthas listener at 127.0.0.1:{unsafe_http}" in missing, missing
                assert "API default 8562" in missing and "retry with --telnet-port" in missing, missing
                assert "Arthas listener closed at" not in missing, missing
                assert can_connect("127.0.0.1", safe_port), "Missing-port diagnosis stopped the existing agent"
                mismatch = run(client + ["--pid", str(process.pid), *impersonating,
                                         "--command", "version"], success=False)
                assert f"belongs to PID {other.pid}, expected {process.pid}" in mismatch, mismatch
                assert can_connect("127.0.0.1", safe_port), "PID mismatch stopped another JVM's agent"
                assert process.poll() is None and other.poll() is None
                # This helper owns the other JVM. The misleading username must
                # also not prevent its legitimate exit cleanup.
                os.kill(interactive.pid, signal.SIGTERM)
                assert interactive.wait() == 128 + signal.SIGTERM, interactive.output
                wait_port(False)
                assert "Arthas listener closed at" in interactive.output, interactive.output
                assert other.poll() is None, "Agent cleanup stopped the other application JVM"
            finally:
                if interactive is not None and interactive.poll() is None:
                    os.kill(interactive.pid, signal.SIGKILL)
                    interactive.wait(10)
                other.terminate()
                try:
                    other.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    other.kill()
                    other.wait()
            assert not can_connect("127.0.0.1", safe_port)
            assert not list((root / "client-temp").glob("hugegraph-arthas-*.properties"))
            assert not list((root / "client-temp").glob("tmp.*")), "Helper state was not removed"
            print(f"PASS: {component} PID/argument boundaries, hostile config, default loopback bind, no tunnel, "
                  "command/quit/terminal/group/repeated-signal cleanup, explicit binds/authentication without restart")
        finally:
            if interactive is not None and interactive.poll() is None:
                os.kill(interactive.pid, signal.SIGKILL)
                interactive.wait(10)
            tunnel.close()
            if process.poll() is None:
                os.kill(process.pid, signal.SIGCONT)
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
PY
