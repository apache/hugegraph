#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
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

"""Native-parser regression fixtures and local-only writer protocol controls."""

import copy
from contextlib import redirect_stdout
import datetime
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import textwrap
import unittest
import urllib.error
import warnings
import zipfile
from unittest.mock import patch
from urllib.parse import urlsplit


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


producer = load_module(Path(__file__).with_name('dependency-snapshot.py'), 'producer')
inventory_producer = load_module(Path(__file__).with_name('dependency-inventory.py'), 'inventory_producer')


def artifact(identifier, **fields):
    return dict(id=identifier, groupId='org.example', artifactId=identifier, version='1.0', types=['jar'], **fields)


class SnapshotTest(unittest.TestCase):

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        workflow = Path(__file__).resolve().parents[1] / 'workflows/submit-dependencies.yml'
        # Only the fixed workflow program is loaded as code. Artifacts remain JSON data.
        program = workflow.read_text().split("          python3 -I - <<'PY'\n", 1)[1].split('\n          PY', 1)[0]
        script = self.root / 'fixed-writer.py'
        script.write_text(textwrap.dedent(program))
        self.writer = load_module(script, 'writer')
        self.env = {'EXPECTED_REPOSITORY': 'apache/hugegraph', 'EXPECTED_SHA': 'a' * 40,
                    'RUN_ID': '321', 'RUN_ATTEMPT': '2', 'ARTIFACT_ID': '789', 'INVENTORY_ARTIFACT_ID': '987',
                    'RUNNER_TEMP': str(self.root),
                    'SNAPSHOT_TOKEN': 'public-synthetic-sentinel'}
        directory = self.root / 'dependency-snapshot'
        directory.mkdir()
        self.source = directory / 'snapshot.json'
        graph = {'graphName': 'module', 'artifacts': [artifact('root'), artifact('one'), artifact('two')],
                 'dependencies': [{'from': 'root', 'to': 'one'}, {'from': 'one', 'to': 'two'}]}
        now = datetime.datetime.now(datetime.timezone.utc)
        self.snapshot = {'version': 0, 'sha': self.env['EXPECTED_SHA'], 'ref': 'refs/heads/master',
                         'job': {'id': '321', 'correlator': producer.CORRELATOR,
                                 'html_url': 'https://github.com/apache/hugegraph/actions/runs/321'},
                         'detector': producer.DETECTOR, 'metadata': {'serializer': producer.SERIALIZER,
                                                                  'producer': 'generate', 'run_attempt': '2'},
                         'scanned': now.isoformat(), 'manifests': {'module': producer.manifest(graph, 'pom.xml')}}
        self.inventory = {'version': 1, 'sha': 'a' * 40, 'repository': 'apache/hugegraph', 'run_id': '321',
                          'run_attempt': '2', 'producer': 'inventory', 'resolver': inventory_producer.RESOLVER,
                          'scanned': now.isoformat(), 'manifests': {'pom.xml': {
                              key: {'scope': value['scope'], 'relationship': value['relationship']}
                              for key, value in self.snapshot['manifests']['module']['resolved'].items()}}}
        self.responses = {
            'actions/runs/321/attempts/2': {'id': 321, 'run_attempt': 2, 'head_sha': 'a' * 40,
                'head_branch': 'master', 'event': 'push', 'repository': {'full_name': 'apache/hugegraph'},
                'path': '.github/workflows/submit-dependencies.yml',
                'run_started_at': (now - datetime.timedelta(minutes=2)).isoformat()},
            'actions/runs/321/attempts/2/jobs?per_page=100': {'total_count': 3,
                'jobs': [{'name': 'generate', 'conclusion': 'success', 'status': 'completed',
                          'run_id': 321, 'head_sha': 'a' * 40}]},
            'actions/artifacts/789': {'id': 789, 'expired': False, 'name': 'dependency-snapshot-321-2',
                'created_at': now.isoformat(),
                'workflow_run': {'id': 321, 'head_sha': 'a' * 40, 'head_branch': 'master'}},
            'actions/artifacts/987': {'id': 987, 'expired': False, 'name': 'dependency-inventory-321-2',
                'created_at': now.isoformat(),
                'workflow_run': {'id': 321, 'head_sha': 'a' * 40, 'head_branch': 'master'}},
            'git/trees/' + 'a' * 40 + '?recursive=1': {'truncated': False,
                'tree': [{'path': 'pom.xml', 'type': 'blob', 'mode': '100644'}]},
            'git/ref/heads/master': {'object': {'sha': 'a' * 40}},
            'dependency-graph/snapshots': {'result': 'SUCCESS'},
        }
        self.posts = []
        self.blob_headers = []

    def run_writer(self, snapshot=None, raw=None, archive=None, artifact_changes=None, inventory=None,
                   inventory_archive=None, inventory_artifact_changes=None):
        if archive is None:
            payload = raw if raw is not None else json.dumps(snapshot or self.snapshot)
            archive = self.zip_bytes([('snapshot.json', payload)])
        self.responses['actions/artifacts/789'].update(size_in_bytes=len(archive),
                                                      digest='sha256:' + hashlib.sha256(archive).hexdigest())
        self.responses['actions/artifacts/789'].update(artifact_changes or {})
        if inventory_archive is None:
            inventory_archive = self.zip_bytes([('inventory.json', json.dumps(inventory or self.inventory))])
        self.responses['actions/artifacts/987'].update(size_in_bytes=len(inventory_archive),
            digest='sha256:' + hashlib.sha256(inventory_archive).hexdigest())
        self.responses['actions/artifacts/987'].update(inventory_artifact_changes or {})

        def open_request(request, timeout):
            url = urlsplit(request.full_url)
            self.assertEqual('https', url.scheme)
            if url.netloc == 'storage.blob.core.windows.net':
                self.blob_headers.append(dict(request.header_items()))
                self.assertIsNone(request.get_header('Authorization'))
                response = io.BytesIO(inventory_archive if url.path == '/987.zip' else archive)
                response.status = 200
                return response
            self.assertEqual('api.github.com', url.netloc)
            self.assertTrue(url.path.startswith('/repos/apache/hugegraph/'))
            self.assertEqual('Bearer public-synthetic-sentinel', request.get_header('Authorization'))
            path = url.path.removeprefix('/repos/apache/hugegraph/') + ('?' + url.query if url.query else '')
            if path in ['actions/artifacts/789/zip', 'actions/artifacts/987/zip']:
                raise urllib.error.HTTPError(request.full_url, 302, 'Found',
                    {'Location': 'https://storage.blob.core.windows.net/' + path.split('/')[2] + '.zip?sig=public'},
                    io.BytesIO())
            if request.data is not None:
                self.assertEqual('dependency-graph/snapshots', path)
                self.posts.append(json.loads(request.data))
            response = io.BytesIO(json.dumps(self.responses[path]).encode())
            response.status = 201 if request.data is not None else 200
            return response

        opener = self.writer.urllib.request.OpenerDirector()
        opener.open = open_request
        with patch.dict(os.environ, self.env), patch.object(self.writer.urllib.request,
                                                          'build_opener', return_value=opener):
            with redirect_stdout(io.StringIO()):
                self.writer.main()

    @staticmethod
    def zip_bytes(entries, mode=None):
        output = io.BytesIO()
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', UserWarning)
            with zipfile.ZipFile(output, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
                for name, data in entries:
                    entry = zipfile.ZipInfo(name)
                    entry.compress_type = zipfile.ZIP_DEFLATED
                    if mode is not None:
                        entry.create_system = 3
                        entry.external_attr = mode << 16
                    archive.writestr(entry, data)
        return output.getvalue()

    def test_native_package_url_encoding(self):
        # Expected strings were captured from the pinned action's actual parser.
        cases = [
            (dict(groupId='org.图//组/', artifactId='测试/名@', version='1+2/slash~',
                  classifiers=['测试 c/+~*'], types=['jar']),
             'pkg:maven/org.%E5%9B%BE/%E7%BB%84/%E6%B5%8B%E8%AF%95%2F%E5%90%8D%40@1+2%2Fslash~'
             '?classifier=%E6%B5%8B%E8%AF%95%20c%2F%2B%7E*&type=jar'),
            (dict(groupId="g:!/x~*'()", artifactId="n:!~*'()", version="v:!~*'()+",
                  classifiers=[' c/+~* '], types=[' jar ', 'war']),
             "pkg:maven/g:!/x~*'()/n%3A!~*'()@v:!~*'()+?classifier=c%2F%2B%7E*&type=jar"),
        ]
        for value, expected in cases:
            self.assertEqual(expected, producer.package_url(value))

    def test_native_cycle_alias_scope_and_direct_precedence(self):
        graph = {'graphName': 'module', 'artifacts': [artifact('root'), artifact('one', scopes=['test']),
            dict(artifact('one'), id='alias', scopes=['provided']), artifact('two'), artifact('three')],
            'dependencies': [{'from': 'root', 'to': 'one'}, {'from': 'one', 'to': 'two'},
                             {'from': 'two', 'to': 'one'}, {'from': 'alias', 'to': 'three'},
                             {'from': 'root', 'to': 'two'}]}
        resolved = producer.manifest(graph, 'pom.xml')['resolved']
        one, two, three = [f'pkg:maven/org.example/{name}@1.0?type=jar' for name in ['one', 'two', 'three']]
        self.assertEqual({'package_url': one, 'relationship': 'direct', 'scope': 'runtime',
                          'dependencies': [two, three]}, resolved[one])
        self.assertEqual({'direct'}, {entry['relationship'] for entry in resolved.values()})
        self.assertEqual({}, producer.manifest({'graphName': 'parent', 'artifacts': [artifact('root')]},
                                              'pom.xml')['resolved'])

    def test_native_mixed_scopes_match_independent_inventory(self):
        for scope in ['compile', 'runtime', 'provided', 'system']:
            for scopes in [[scope, 'test'], ['test', scope]]:
                with self.subTest(scopes=scopes):
                    graph = {'graphName': 'module', 'artifacts': [
                        artifact('root'), artifact('parent', scopes=['test']), artifact('one', scopes=scopes)],
                        'dependencies': [{'from': 'root', 'to': 'parent'}, {'from': 'parent', 'to': 'one'},
                                         {'from': 'root', 'to': 'one'}]}
                    self.snapshot['manifests']['module'] = producer.manifest(graph, 'pom.xml')
                    tree = InventoryTest.node('root', children=[
                        InventoryTest.node('parent', 'test', [InventoryTest.node('one', 'test')]),
                        InventoryTest.node('one', scope)])
                    self.inventory['manifests']['pom.xml'] = inventory_producer.packages(tree)
                    self.posts.clear()
                    self.run_writer()
                    self.assertEqual([self.snapshot], self.posts)

    def test_transitive_only_package_is_indirect(self):
        graph = {'graphName': 'module', 'artifacts': [artifact('root'), artifact('parent'), artifact('leaf')],
                 'dependencies': [{'from': 'root', 'to': 'parent'}, {'from': 'parent', 'to': 'leaf'}]}
        parent = 'pkg:maven/org.example/parent@1.0?type=jar'
        leaf = 'pkg:maven/org.example/leaf@1.0?type=jar'
        self.assertEqual({
            parent: {'package_url': parent, 'relationship': 'direct', 'scope': 'runtime', 'dependencies': [leaf]},
            leaf: {'package_url': leaf, 'relationship': 'indirect', 'scope': 'runtime', 'dependencies': []},
        }, producer.manifest(graph, 'pom.xml')['resolved'])

    def test_direct_relationship_overrides_indirect_path(self):
        graph = {'graphName': 'module', 'artifacts': [artifact('root'), artifact('parent'), artifact('shared')],
                 'dependencies': [{'from': 'root', 'to': 'parent'}, {'from': 'parent', 'to': 'shared'},
                                  {'from': 'root', 'to': 'shared'}]}
        parent = 'pkg:maven/org.example/parent@1.0?type=jar'
        shared = 'pkg:maven/org.example/shared@1.0?type=jar'
        self.assertEqual({
            parent: {'package_url': parent, 'relationship': 'direct', 'scope': 'runtime', 'dependencies': [shared]},
            shared: {'package_url': shared, 'relationship': 'direct', 'scope': 'runtime', 'dependencies': []},
        }, producer.manifest(graph, 'pom.xml')['resolved'])

    def test_generation_rejects_missing_empty_duplicate_and_malformed_graphs(self):
        with self.assertRaises(FileNotFoundError):
            producer.create_snapshot(self.root, ['pom.xml'], 'a' * 40, 'apache/hugegraph', '321', '2')
        report = self.root / 'target' / producer.REPORT
        report.parent.mkdir()
        report.write_text(json.dumps({'graphName': 'parent', 'artifacts': [artifact('root')]}))
        with self.assertRaisesRegex(ValueError, 'empty dependency inventory'):
            producer.create_snapshot(self.root, ['pom.xml'], 'a' * 40, 'apache/hugegraph', '321', '2')
        for graph in [{'graphName': 'bad', 'artifacts': [artifact('root'), artifact('root')]},
                      {'graphName': 'bad', 'artifacts': [artifact('root')], 'dependencies': False},
                      {'graphName': 'bad', 'artifacts': [artifact('root', scopes='test')]}]:
            with self.assertRaises(ValueError):
                producer.manifest(graph, 'pom.xml')
        with self.assertRaises(ValueError):
            json.loads('{"sha":"a","sha":"b"}', object_pairs_hook=producer.unique_object)

    def test_writer_preserves_snapshot_and_never_executes_data(self):
        marker = self.root / 'must-not-exist'
        name = f"__import__('pathlib').Path({str(marker)!r}).touch()"
        self.snapshot['manifests'][name] = self.snapshot['manifests'].pop('module')
        self.snapshot['manifests'][name]['name'] = name
        self.run_writer()
        self.assertEqual([self.snapshot], self.posts)
        self.assertFalse(marker.exists())

    def test_writer_accepts_master_path_suffix_and_rejects_other_refs(self):
        run = self.responses['actions/runs/321/attempts/2']
        path = '.github/workflows/submit-dependencies.yml'
        run['path'] = path + '@master'
        self.run_writer()
        self.assertEqual([self.snapshot], self.posts)
        self.posts.clear()
        for ref in ['main', 'feature', 'refs/heads/master', '']:
            run['path'] = path + '@' + ref
            with self.subTest(ref=ref), self.assertRaisesRegex(ValueError, 'Run identity'):
                self.run_writer()
        self.assertEqual([], self.posts)

    def test_writer_rejects_snapshot_changes_before_post(self):
        mutations = [('sha', 'b' * 40), ('ref', 'refs/heads/feature'), ('version', False),
                     ('job', {}), ('detector', {}), ('metadata', {}), ('manifests', {}),
                     ('scanned', '2000-01-01T00:00:00Z'), ('scanned', '2999-01-01T00:00:00Z')]
        for key, value in mutations:
            with self.subTest(key=key, value=value), self.assertRaises((ValueError, KeyError)):
                snapshot = copy.deepcopy(self.snapshot)
                snapshot[key] = value
                self.run_writer(snapshot)
        for field, value in [('source_location', 'other/pom.xml'), ('resolved', {}), ('scope', 'optional'),
                             ('dependencies', ['pkg:maven/missing/name@1']),
                             ('package_url', 'pkg:maven/invalid%Q0/a@1')]:
            snapshot = copy.deepcopy(self.snapshot)
            manifest = snapshot['manifests']['module']
            target = manifest['file'] if field == 'source_location' else manifest if field == 'resolved' else next(
                iter(manifest['resolved'].values()))
            target[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.run_writer(snapshot)
        with self.assertRaises(ValueError):
            self.run_writer(raw=json.dumps(self.snapshot)[:-1] + ',"sha":"b"}')
        self.assertEqual([], self.posts)

    def test_writer_rejects_failed_producer_wrong_artifact_and_stale_master(self):
        for path, field, value in [('actions/runs/321/attempts/2', 'run_attempt', 1),
                                  ('actions/artifacts/789', 'name', 'dependency-snapshot-321-1'),
                                  ('actions/artifacts/789', 'expired', True),
                                  ('actions/artifacts/789', 'workflow_run', {'id': 123}),
                                  ('actions/runs/321/attempts/2/jobs?per_page=100', 'jobs',
                                   [{'name': 'generate', 'conclusion': 'failure'}]),
                                  ('git/trees/' + 'a' * 40 + '?recursive=1', 'truncated', True),
                                  ('git/ref/heads/master', 'object', {'sha': 'b' * 40})]:
            original = copy.deepcopy(self.responses[path])
            self.responses[path][field] = value
            with self.subTest(path=path, field=field), self.assertRaises((ValueError, KeyError)):
                self.run_writer()
            self.responses[path] = original
        self.assertEqual([], self.posts)

    def test_dependency_baseline_concurrency_isolates_nonmaster_dispatch(self):
        workflow = (Path(__file__).resolve().parents[1] / "workflows/submit-dependencies.yml").read_text()
        template = next(line.removeprefix("  group: ") for line in workflow.splitlines()
                        if line.startswith("  group: "))

        def group(event, ref):
            return template.replace("${{ github.repository }}", "apache/hugegraph").replace(
                "${{ github.ref }}", ref).replace("${{ github.event_name }}", event)

        master = group("push", "refs/heads/master")
        self.assertEqual(master, group("workflow_dispatch", "refs/heads/master"))
        self.assertNotEqual(master, group("workflow_dispatch", "refs/heads/feature"))

    def test_writer_requires_one_successful_combined_producer(self):
        path = 'actions/runs/321/attempts/2/jobs?per_page=100'
        producer = self.responses[path]['jobs'][0]
        cases = [[], [producer, producer]]
        for field, value in [('conclusion', 'failure'), ('status', 'in_progress'),
                             ('run_id', 123), ('head_sha', 'b' * 40)]:
            cases.append([dict(producer, **{field: value})])
        for jobs in cases:
            with self.subTest(jobs=jobs), self.assertRaisesRegex(ValueError, 'Producer did not succeed'):
                self.responses[path]['jobs'] = jobs
                self.run_writer()
        self.assertEqual([], self.posts)

    def test_writer_rejects_incomplete_package_inventory_before_post(self):
        resolved = self.snapshot['manifests']['module']['resolved']
        for key in list(resolved):
            candidate = copy.deepcopy(self.snapshot)
            packages = candidate['manifests']['module']['resolved']
            packages.pop(key)
            for package in packages.values():
                package['dependencies'] = [child for child in package['dependencies'] if child != key]
            with self.subTest(missing=key), self.assertRaises(ValueError):
                self.run_writer(snapshot=candidate)
        for field, value in [('scope', 'development'), ('relationship', 'direct')]:
            candidate = copy.deepcopy(self.snapshot)
            key = next(key for key, value in resolved.items() if value['relationship'] == 'indirect')
            candidate['manifests']['module']['resolved'][key][field] = value
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, 'inventory mismatch'):
                self.run_writer(snapshot=candidate)
        candidate = copy.deepcopy(self.snapshot)
        packages = candidate['manifests']['module']['resolved']
        key = next(key for key, value in packages.items() if value['relationship'] == 'indirect')
        altered = key.replace('@1.0?', '@2.0?')
        packages[altered] = packages.pop(key)
        packages[altered]['package_url'] = altered
        for package in packages.values():
            package['dependencies'] = [altered if child == key else child for child in package['dependencies']]
        with self.assertRaisesRegex(ValueError, 'inventory mismatch'):
            self.run_writer(snapshot=candidate)
        self.assertEqual([], self.posts)

    def test_writer_rejects_foreign_or_incomplete_inventory(self):
        for key, value in [('sha', 'b' * 40), ('repository', 'other/repo'), ('run_id', '123'),
                           ('run_attempt', '1'), ('producer', 'generate'), ('resolver', 'unknown'),
                           ('version', False), ('scanned', '2000-01-01T00:00:00Z'), ('manifests', {})]:
            candidate = copy.deepcopy(self.inventory)
            candidate[key] = value
            with self.subTest(field=key), self.assertRaises(ValueError):
                self.run_writer(inventory=candidate)
        for fields in [{'expired': True}, {'name': 'dependency-inventory-321-1'},
                       {'workflow_run': {'id': 321, 'head_sha': 'b' * 40, 'head_branch': 'master'}},
                       {'digest': 'sha256:' + '0' * 64}]:
            original = copy.deepcopy(self.responses['actions/artifacts/987'])
            with self.subTest(fields=fields), self.assertRaises((ValueError, KeyError)):
                self.run_writer(inventory_artifact_changes=fields)
            self.responses['actions/artifacts/987'] = original
        self.assertEqual([], self.posts)

    def test_writer_rejects_inventory_zip_wrong_member_without_extraction(self):
        before = set(self.root.rglob('*'))
        for entries in [[('snapshot.json', '{}')], [('inventory.json', '{}'), ('inventory.json', '{}')],
                        [('../inventory.json', '{}')]]:
            with self.subTest(entries=entries), self.assertRaises(ValueError):
                self.run_writer(inventory_archive=self.zip_bytes(entries))
        self.assertEqual(before, set(self.root.rglob('*')))
        self.assertEqual([], self.posts)

    def test_writer_rejects_zip_paths_duplicates_symlinks_and_bounds_without_writes(self):
        body = json.dumps(self.snapshot)
        fixtures = [self.zip_bytes([('snapshot.json', body), ('x/../../outside', 'must not write')]),
                    self.zip_bytes([('snapshot.json', body), ('snapshot.json', body)]),
                    self.zip_bytes([('snapshot.json', body)], 0o120777),
                    self.zip_bytes([('snapshot.json', '0' * (self.writer.LIMIT + 1))]),
                    b'0' * (self.writer.LIMIT + 1)]
        fixtures += [self.zip_bytes([(path, body)]) for path in
                     ['../snapshot.json', 'x/../../snapshot.json', '/snapshot.json', 'x\\snapshot.json']]
        before = set(self.root.rglob('*'))
        for archive in fixtures:
            with self.subTest(size=len(archive)), self.assertRaises(ValueError):
                self.run_writer(archive=archive)
        self.assertEqual(before, set(self.root.rglob('*')))
        self.assertFalse((self.root / 'outside').exists())
        self.assertEqual([], self.posts)

    def test_writer_download_has_no_cross_host_authorization_or_followup_redirect(self):
        self.run_writer()
        self.assertEqual([{}, {}], self.blob_headers)
        self.posts.clear()
        for metadata in [{'digest': 'sha256:' + '0' * 64}, {'size_in_bytes': 1}]:
            with self.assertRaisesRegex(ValueError, 'digest/size mismatch'):
                self.run_writer(artifact_changes=metadata)
        calls = []

        def redirected(request, timeout):
            calls.append(request)
            if len(calls) == 1:
                raise urllib.error.HTTPError(request.full_url, 302, 'Found',
                    {'Location': 'https://storage.blob.core.windows.net/archive.zip'}, io.BytesIO())
            self.assertIsNone(request.get_header('Authorization'))
            raise urllib.error.HTTPError(request.full_url, 302, 'Found',
                                         {'Location': 'https://other.example'}, io.BytesIO())

        opener = self.writer.urllib.request.OpenerDirector()
        opener.open = redirected
        with patch.dict(os.environ, self.env), patch.object(self.writer.urllib.request,
                                                          'build_opener', return_value=opener):
            with self.assertRaises(urllib.error.HTTPError):
                self.writer.download_snapshot('apache/hugegraph', '789', {})
        self.assertEqual(2, len(calls))
        self.assertEqual([], self.posts)


class InventoryTest(unittest.TestCase):

    @staticmethod
    def node(name, scope='', children=None, **fields):
        return dict(groupId='org.example', artifactId=name, version='1.0', type='jar', scope=scope,
                    classifier='', optional='false', children=children or [], **fields)

    def test_independent_tree_scopes_classifier_and_direct_precedence(self):
        leaf = self.node('leaf', 'test')
        parent = self.node('parent', 'compile', [leaf])
        classified = self.node('native', 'provided')
        classified['classifier'] = 'linux-aarch_64'
        tree = self.node('root', children=[parent, self.node('leaf', 'compile'), classified])
        prefix = 'pkg:maven/org.example/'
        self.assertEqual({
            prefix + 'parent@1.0?type=jar': {'relationship': 'direct', 'scope': 'runtime'},
            prefix + 'leaf@1.0?type=jar': {'relationship': 'direct', 'scope': 'runtime'},
            prefix + 'native@1.0?classifier=linux-aarch_64&type=jar':
                {'relationship': 'direct', 'scope': 'runtime'},
        }, inventory_producer.packages(tree))
        self.assertEqual({}, inventory_producer.packages(self.node('aggregate')))

    def test_inventory_refuses_invalid_nodes(self):
        for field, value in [('scope', 'compile'), ('children', {}), ('optional', None),
                             ('classifier', None), ('version', '')]:
            node = self.node('root')
            node[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                inventory_producer.packages(node)
        with self.assertRaises(ValueError):
            inventory_producer.packages(self.node('root', children=[self.node('bad', 'unknown')]))

    def test_inventory_requires_every_report_and_refuses_empty_or_duplicate_poms(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / 'target'
            target.mkdir()
            report = target / inventory_producer.REPORT
            report.write_text(json.dumps(self.node('root', children=[self.node('leaf', 'test')])))
            result = inventory_producer.create_inventory(root, ['pom.xml'], 'a' * 40, 'apache/hugegraph', '1', '1')
            self.assertEqual('development', next(iter(result['manifests']['pom.xml'].values()))['scope'])
            with self.assertRaises(ValueError):
                inventory_producer.create_inventory(root, ['pom.xml', 'pom.xml'], 'a' * 40, 'repo', '1', '1')
            with self.assertRaises(FileNotFoundError):
                inventory_producer.create_inventory(root, ['module/pom.xml'], 'a' * 40, 'repo', '1', '1')
            report.write_text(json.dumps(self.node('root')))
            with self.assertRaisesRegex(ValueError, 'Empty independent'):
                inventory_producer.create_inventory(root, ['pom.xml'], 'a' * 40, 'repo', '1', '1')
            report.unlink()
            report.symlink_to(target / 'outside')
            with self.assertRaises(ValueError):
                inventory_producer.create_inventory(root, ['pom.xml'], 'a' * 40, 'repo', '1', '1')


if __name__ == '__main__':
    unittest.main()
