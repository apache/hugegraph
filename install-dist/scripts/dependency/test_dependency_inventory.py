# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import contextlib
import io
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
import zipfile

import dependency_inventory as inventory


class DependencyInventoryTest(unittest.TestCase):

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / 'checkout'
        self.runtime = self.root / 'runtime'
        self.runtime.mkdir(parents=True)
        self.jar(self.runtime / 'resolved-1.jar')
        for kind in ('server', 'pd', 'store'):
            directory = self.root / ('hugegraph-' + kind) / f'apache-hugegraph-{kind}-1.7.0/lib'
            directory.mkdir(parents=True)
            nested = self.jar_bytes({'example/Type.class': b'class fixture'})
            self.jar(directory / 'hugegraph-executable.jar', {
                'BOOT-INF/lib/injected-2.jar': nested,
            })

    def jar_bytes(self, entries=None):
        data = io.BytesIO()
        with zipfile.ZipFile(data, 'w') as archive:
            for name, value in (entries or {'example/Type.class': b'class fixture'}).items():
                archive.writestr(name, value)
        return data.getvalue()

    def jar(self, path, entries=None):
        path.write_bytes(self.jar_bytes(entries))

    def test_runtime_and_nested_shipped_union(self):
        self.assertEqual(inventory.collect(self.runtime, self.root, '1.7.0'),
                         {'resolved-1.jar', 'injected-2.jar'})

    def test_missing_runtime_empty_runtime_and_package_are_failures(self):
        (self.root / 'empty').mkdir()
        for directory in (self.root / 'absent', self.root / 'empty'):
            with self.assertRaises(inventory.InventoryFailure):
                inventory.collect(directory, self.root, '1.7.0')
        shutil.rmtree(self.root / 'hugegraph-store')
        with self.assertRaisesRegex(inventory.InventoryFailure, 'Missing packaged store'):
            inventory.collect(self.runtime, self.root, '1.7.0')

    def test_corrupt_nested_or_runtime_jar_is_failure(self):
        self.jar(self.root / 'hugegraph-pd/apache-hugegraph-pd-1.7.0/lib/hugegraph-executable.jar', {
            'BOOT-INF/lib/injected-2.jar': b'not a jar',
        })
        with self.assertRaisesRegex(inventory.InventoryFailure, 'Cannot inspect shipped jar'):
            inventory.collect(self.runtime, self.root, '1.7.0')
        (self.runtime / 'resolved-1.jar').write_text('not a jar')
        with self.assertRaisesRegex(inventory.InventoryFailure, 'Cannot inspect resolved jar'):
            inventory.collect(self.runtime, self.root, '1.7.0')

    def test_checker_strict_additions_removals_and_missing_inputs(self):
        known, current = self.root / 'known.txt', self.root / 'current.txt'
        known.write_text('one-1.jar\ntwo-2.jar\n')
        current.write_text('two-2.jar\none-1.jar\n')
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(inventory.compare(known, current), 0)
            for value in ('one-1.jar\n', 'one-1.jar\ntwo-2.jar\nnew-3.jar\n'):
                current.write_text(value)
                self.assertEqual(inventory.compare(known, current), 1)
        for value in ('', 'not-a-jar\n', 'one-1.jar\none-1.jar\n'):
            current.write_text(value)
            with self.assertRaises(inventory.InventoryFailure):
                inventory.compare(known, current)
        current.unlink()
        with self.assertRaises(OSError):
            inventory.compare(known, current)

    def script_fixture(self):
        directory = self.root / 'install-dist/scripts/dependency'
        directory.mkdir(parents=True)
        original = Path(__file__).parent
        for name in ('regenerate_known_dependencies.sh', 'check_dependencies.sh', 'dependency_inventory.py'):
            shutil.copyfile(original / name, directory / name)
        maven = self.root / 'fake-mvn'
        maven.write_text('''#!/usr/bin/env python3
import os, pathlib, sys, zipfile
if os.environ.get('FAKE_MAVEN_FAIL'):
    sys.exit(int(os.environ['FAKE_MAVEN_FAIL']))
if 'dependency:copy-dependencies' in sys.argv:
    value = next(value for value in sys.argv if value.startswith('-DoutputDirectory='))
    directory = pathlib.Path(value.split('=', 1)[1])
    directory.mkdir(parents=True)
    with zipfile.ZipFile(directory / 'resolved-1.jar', 'w') as archive:
        archive.writestr('example/Type.class', b'class fixture')
else:
    print('1.7.0')
''')
        maven.chmod(0o755)
        return directory, dict(os.environ, MAVEN_COMMAND=str(maven))

    def test_shell_producer_consumer_contract(self):
        directory, environment = self.script_fixture()
        current = self.root / 'target/dependency/current.txt'
        generated = subprocess.run(['bash', str(directory / 'regenerate_known_dependencies.sh'), str(current)],
                                   env=environment, capture_output=True, text=True)
        self.assertEqual(generated.returncode, 0, generated.stderr)
        self.assertEqual(inventory.read_inventory(current), ['injected-2.jar', 'resolved-1.jar'])
        known = directory / 'known-dependencies.txt'
        known.write_text(current.read_text())
        checked = subprocess.run(['bash', str(directory / 'check_dependencies.sh'), str(known), str(current)],
                                 capture_output=True, text=True)
        self.assertEqual(checked.returncode, 0, checked.stderr)
        current.unlink()
        missing = subprocess.run(['bash', str(directory / 'check_dependencies.sh'), str(known), str(current)],
                                 capture_output=True, text=True)
        self.assertNotEqual(missing.returncode, 0)

    def test_shell_maven_failure_does_not_publish_inventory(self):
        directory, environment = self.script_fixture()
        current = self.root / 'current.txt'
        result = subprocess.run(['bash', str(directory / 'regenerate_known_dependencies.sh'), str(current)],
                                env=dict(environment, FAKE_MAVEN_FAIL='17'), capture_output=True, text=True)
        self.assertEqual(result.returncode, 17)
        self.assertFalse(current.exists())

    def test_shell_missing_package_does_not_publish_inventory(self):
        directory, environment = self.script_fixture()
        shutil.rmtree(self.root / 'hugegraph-store')
        current = self.root / 'current.txt'
        result = subprocess.run(['bash', str(directory / 'regenerate_known_dependencies.sh'), str(current)],
                                env=environment, capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Missing packaged store', result.stderr)
        self.assertFalse(current.exists())


if __name__ == '__main__':
    unittest.main()
