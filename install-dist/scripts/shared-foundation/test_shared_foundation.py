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

import io
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET
import zipfile

import check_shared_foundation as check


class FoundationChecksTest(unittest.TestCase):

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.addCleanup(self.temporary.cleanup)

    def jar(self, artifact, classes=(), entries=None, version='1.8.0', group=check.GROUP):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, 'w') as archive:
            archive.writestr(f'META-INF/maven/{group}/{artifact}/pom.properties',
                             f'groupId={group}\nartifactId={artifact}\nversion={version}\n')
            for name in classes:
                archive.writestr(name, b'class fixture')
            for name, value in (entries or {}).items():
                archive.writestr(name, value)
        return buffer.getvalue()

    def distribution(self, kind='server', nested=False):
        path = self.root / kind
        (path / 'lib').mkdir(parents=True)
        jars = {}
        for artifact in check.REQUIRED_ARTIFACTS[kind]:
            name = artifact.replace('-', '_')
            jars[artifact] = self.jar(artifact, [f'org/apache/hugegraph/{name}.class'])
        if nested:
            executable = 'hg-pd-service' if kind == 'pd' else 'hg-store-node'
            entries = {f'BOOT-INF/lib/{name}.jar': value for name, value in jars.items()
                       if name != executable}
            data = self.jar(executable, [f'BOOT-INF/classes/org/apache/hugegraph/{executable}.class'], entries)
            (path / 'lib' / 'service.jar').write_bytes(data)
        else:
            for name, value in jars.items():
                (path / 'lib' / (name + '.jar')).write_bytes(value)
        if kind == 'server':
            (path / 'conf').mkdir()
            (path / 'conf/gremlin-server.yaml').write_text(
                'classImports: [org.apache.hugegraph.hugegraph_core]\n')
        return path

    def tree(self, artifact, descendants=''):
        path = self.root / 'tree.txt'
        path.write_text(f'{check.GROUP}:{artifact}:jar:1.8.0\n' + descendants)
        return path

    def test_shipped_classpaths_flat_and_boot(self):
        for kind in ('server', 'pd', 'store'):
            check.check_distribution(self.distribution(kind, nested=kind != 'server'), kind, set())

    def test_duplicate_class_in_two_shipped_jars_fails(self):
        path = self.distribution()
        (path / 'lib' / 'copy.jar').write_bytes(self.jar('copy', ['org/apache/hugegraph/hugegraph_struct.class']))
        with self.assertRaisesRegex(check.CheckFailure, 'Duplicate shipped FQCN'):
            check.check_distribution(path, 'server', set())

    def test_duplicate_in_nested_boot_jar_fails(self):
        duplicate = 'org/apache/hugegraph/Conflict.class'
        jars = {f'BOOT-INF/lib/{name}.jar': self.jar(name, [duplicate]) for name in ('one', 'two')}
        path = self.root / 'boot.jar'
        path.write_bytes(self.jar('boot', entries=jars))
        with self.assertRaisesRegex(check.CheckFailure, 'Duplicate shipped FQCN'):
            check.scan_jar(path, str(path), {}, set(), set(), 'pd')

    def test_multi_release_variants_in_one_jar_are_one_owner(self):
        path = self.root / 'mr.jar'
        path.write_bytes(self.jar('variant', ['org/apache/hugegraph/Variant.class',
                                             'META-INF/versions/11/org/apache/hugegraph/Variant.class']))
        owners = {}
        check.scan_jar(path, str(path), owners, set(), set(), 'server')
        self.assertEqual(list(owners), ['org.apache.hugegraph.Variant'])

    def test_missing_distribution_and_required_artifact_fail_closed(self):
        with self.assertRaisesRegex(check.CheckFailure, 'Missing shipped'):
            check.check_distribution(self.root / 'absent', 'server', set())
        path = self.distribution()
        (path / 'lib' / 'hugegraph-struct.jar').unlink()
        with self.assertRaisesRegex(check.CheckFailure, 'Missing expected artifacts'):
            check.check_distribution(path, 'server', set())

    def test_corrupt_jar_is_failure(self):
        path = self.distribution()
        (path / 'lib' / 'broken.jar').write_text('not a zip')
        with self.assertRaisesRegex(check.CheckFailure, 'Invalid shipped jar'):
            check.check_distribution(path, 'server', set())

    def test_nested_core_shipped_with_pd_is_failure(self):
        path = self.distribution('pd', nested=True)
        (path / 'lib' / 'extra.jar').write_bytes(self.jar('extra', entries={
            'BOOT-INF/lib/core.jar': self.jar('hugegraph-core', ['org/apache/hugegraph/Core.class'])
        }))
        with self.assertRaisesRegex(check.CheckFailure, 'Forbidden hugegraph-core shipped'):
            check.check_distribution(path, 'pd', set())

    def test_transitive_forbidden_dependencies_fail(self):
        for module, forbidden in (('hugegraph-pd/hg-pd-service', 'hugegraph-core'),
                                  ('hugegraph-store/hg-store-core', 'hugegraph-core'),
                                  ('hugegraph-struct', 'hg-pd-client'),
                                  ('hugegraph-struct', 'hugegraph-core'),
                                  ('hugegraph-commons/hugegraph-common', 'hg-store-client')):
            with self.subTest(module=module, forbidden=forbidden):
                artifact = module.rsplit('/', 1)[-1]
                descendants = (f'+- external:parent:jar:1.0:compile\n'
                               f'|  \\- {check.GROUP}:{forbidden}:jar:1.8.0:compile\n')
                with self.assertRaisesRegex(check.CheckFailure, 'Forbidden resolved dependency'):
                    check.check_tree(self.tree(artifact, descendants), module)

    def test_struct_rejects_all_pd_store_component_dependencies(self):
        for name in ('hg-pd-service', 'hg-store-common', 'hugegraph-pd', 'hugegraph-store'):
            for prefix in ('', '+- external:parent:jar:1.0:compile\n|  '):
                with self.subTest(name=name, transitive=bool(prefix)):
                    descendants = prefix + f'\\- {check.GROUP}:{name}:jar:1.8.0:compile\n'
                    with self.assertRaisesRegex(check.CheckFailure, 'Forbidden resolved dependency'):
                        check.check_tree(self.tree('hugegraph-struct', descendants), 'hugegraph-struct')
        # The boundary concerns HugeGraph components, not similarly named external artifacts.
        tree = self.tree('hugegraph-struct', '\\- external:hg-pd-service:jar:1.0:compile\n')
        check.check_tree(tree, 'hugegraph-struct')

    def test_allowed_kryo_dependency_and_classifier(self):
        tree = self.tree('hugegraph-struct', '\\- org.apache.tinkerpop:gremlin-shaded:jar:1.0:compile\n'
                         '+- external:native:jar:linux:1.0:runtime (optional)\n')
        check.check_tree(tree, 'hugegraph-struct')

    def test_missing_wrong_or_malformed_tree_fails_closed(self):
        with self.assertRaisesRegex(check.CheckFailure, 'Missing resolved'):
            check.parse_tree(self.root / 'absent', 'hugegraph-struct')
        for value in ('', 'this is a Maven error\n', f'{check.GROUP}:wrong:jar:1.8.0\n',
                      f'{check.GROUP}:hugegraph-struct:jar:1.8.0\n+- broken:data\n'):
            path = self.root / 'bad.txt'
            path.write_text(value)
            with self.subTest(value=value), self.assertRaises(check.CheckFailure):
                check.parse_tree(path, 'hugegraph-struct')

    def sources(self):
        self.declare_server_module('hugegraph-core')
        for index, module in enumerate(check.SOURCE_MODULES):
            path = self.root / module / 'src/main/java/org/apache/hugegraph' / f'Unique{index}.java'
            path.parent.mkdir(parents=True)
            path.write_text(f'package org.apache.hugegraph;\npublic class Unique{index} {{}}\n')

    def declare_server_module(self, module):
        namespace = '{http://maven.apache.org/POM/4.0.0}'
        root = self.root / 'hugegraph-server'
        root.mkdir(exist_ok=True)
        pom = root / 'pom.xml'
        if not pom.exists():
            pom.write_text('<project xmlns="http://maven.apache.org/POM/4.0.0">'
                           '<artifactId>hugegraph-server</artifactId><modules/></project>')
        tree = ET.parse(pom)
        modules = tree.getroot().find(namespace + 'modules')
        if module not in [entry.text for entry in modules]:
            ET.SubElement(modules, namespace + 'module').text = module
        tree.write(pom, encoding='unicode')
        child = root / module / 'pom.xml'
        child.parent.mkdir(parents=True, exist_ok=True)
        if not child.exists():
            child.write_text('<project xmlns="http://maven.apache.org/POM/4.0.0">'
                             '<artifactId>' + Path(module).name + '</artifactId></project>')
        return child

    def test_target_reactor_tree_must_exist_exactly_once(self):
        lines = ['org.apache.hugegraph:hugegraph: pom:1.8.0'.replace(': ', ':'),
                 'org.apache.hugegraph:hugegraph-struct:jar:1.8.0',
                 '+- external:one:jar:1.0:compile',
                 'org.apache.hugegraph:hg-store-core:jar:1.8.0',
                 '\\- org.apache.hugegraph:hugegraph-struct:jar:1.8.0:compile']
        self.assertEqual(check.select_tree(lines, 'hugegraph-struct'), '\n'.join(lines[1:3]) + '\n')
        with self.assertRaisesRegex(check.CheckFailure, 'exactly one'):
            check.select_tree(lines, 'missing')
        with self.assertRaisesRegex(check.CheckFailure, 'exactly one'):
            check.select_tree(lines + lines[1:3], 'hugegraph-struct')

    def test_source_duplicates_and_retired_owner_are_rejected(self):
        self.sources()
        check.check_sources(self.root, set())
        path = self.root / check.SOURCE_MODULES[1] / 'src/main/java/org/apache/hugegraph/Unique0.java'
        path.write_text('package org.apache.hugegraph;\npublic class Unique0 {}\n')
        with self.assertRaisesRegex(check.CheckFailure, 'Duplicate source FQCN'):
            check.check_sources(self.root, set())
        path.unlink()
        with self.assertRaisesRegex(check.CheckFailure, 'Retired shared implementation'):
            check.check_sources(self.root, {'org.apache.hugegraph.Unique0'})

    def test_manual_dev_sources_cannot_duplicate_shared_owners(self):
        self.sources()
        # Both the shared modules' alternate source roots and hand-maintained
        # grpc helpers are production input, unlike generated grpc Java sources.
        for module in (check.SOURCE_MODULES[1], 'hugegraph-store/hg-store-grpc'):
            with self.subTest(module=module):
                path = self.root / module / 'src/main/dev/org/apache/hugegraph/Unique0.java'
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text('package org.apache.hugegraph;\npublic class Unique0 {}\n')
                with self.assertRaisesRegex(check.CheckFailure, 'Duplicate source FQCN'):
                    check.check_sources(self.root, set())
                path.unlink()

    def test_server_api_backend_and_dist_source_duplicates_are_rejected(self):
        self.sources()
        for module in ('hugegraph-api', 'hugegraph-hstore', 'hugegraph-dist'):
            self.declare_server_module(module)
            with self.subTest(module=module):
                path = (self.root / 'hugegraph-server' / module /
                        'src/main/java/org/apache/hugegraph/Unique0.java')
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text('package org.apache.hugegraph;\npublic class Unique0 {}\n')
                with self.assertRaisesRegex(check.CheckFailure, 'Duplicate source FQCN'):
                    check.check_sources(self.root, set())
                path.unlink()
        self.declare_server_module('hugegraph-test')
        fixture = (self.root / 'hugegraph-server/hugegraph-test/'
                   'src/main/java/org/apache/hugegraph/Unique0.java')
        fixture.parent.mkdir(parents=True)
        fixture.write_text('package org.apache.hugegraph;\npublic class Unique0 {}\n')
        check.check_sources(self.root, set())

    def test_retired_owner_cannot_return_through_core_dev_sources(self):
        self.sources()
        path = self.root / check.SOURCE_MODULES[0] / 'src/main/dev/org/apache/hugegraph/Retired.java'
        path.parent.mkdir(parents=True)
        path.write_text('package org.apache.hugegraph;\npublic class Retired {}\n')
        with self.assertRaisesRegex(check.CheckFailure, 'Retired shared implementation'):
            check.check_sources(self.root, {'org.apache.hugegraph.Retired'})

    def test_retired_owner_in_core_jar_is_rejected(self):
        retired = check.retired_classes()
        for name in ('org.apache.hugegraph.backend.id.Id', 'org.apache.hugegraph.backend.store.Shard'):
            with self.subTest(name=name):
                self.assertIn(name, retired)
                path = self.root / 'core.jar'
                entry = name.replace('.', '/') + '$Inner.class'
                path.write_bytes(self.jar('hugegraph-core', [entry]))
                with self.assertRaisesRegex(check.CheckFailure, 'Retired shared class shipped'):
                    check.scan_jar(path, str(path), {}, set(), retired, 'server')

    def test_canonical_shard_allowed_and_old_core_owner_rejected(self):
        self.sources()
        retired = check.retired_classes()
        relative = 'src/main/java/org/apache/hugegraph/backend/Shard.java'
        canonical = self.root / 'hugegraph-struct' / relative
        canonical.parent.mkdir(parents=True)
        canonical.write_text('package org.apache.hugegraph.backend;\npublic class Shard {}\n')
        check.check_sources(self.root, retired)
        old = (self.root / check.SOURCE_MODULES[0] /
               'src/main/java/org/apache/hugegraph/backend/store/Shard.java')
        old.parent.mkdir(parents=True)
        old.write_text('package org.apache.hugegraph.backend.store;\npublic class Shard {}\n')
        with self.assertRaisesRegex(check.CheckFailure, 'Retired shared implementation'):
            check.check_sources(self.root, retired)

    def test_retired_nested_source_owner_preserves_entry_and_iterators(self):
        self.sources()
        retired = check.retired_classes()
        owner = 'org.apache.hugegraph.backend.store.BackendEntry$BackendColumn'
        self.assertIn(owner, retired)
        path = (self.root / check.SOURCE_MODULES[0] /
                'src/main/java/org/apache/hugegraph/backend/store/BackendEntry.java')
        path.parent.mkdir(parents=True)
        source = '''package org.apache.hugegraph.backend.store;
interface BackendEntry {
    String text = "class BackendColumn {}";
    // class BackendColumn {}
    Class<?> reference = Other.BackendColumn.class;
    interface BackendColumnIterator { class EmptyIterator {} }
    class Other { class BackendColumn {} }
    default void example() { class BackendColumn {} }
}
'''
        path.write_text(source)
        check.check_sources(self.root, retired)
        for declaration in ('class BackendColumn {}', 'interface BackendColumn {}',
                            'enum BackendColumn { VALUE }'):
            with self.subTest(declaration=declaration):
                path.write_text(source.replace('interface BackendEntry {',
                                               'interface BackendEntry {\n' + declaration))
                with self.assertRaisesRegex(check.CheckFailure, 'Retired shared implementation'):
                    check.check_sources(self.root, retired)

    def test_retired_nested_jar_owner_preserves_entry_and_iterators(self):
        retired = check.retired_classes()
        prefix = 'org/apache/hugegraph/backend/store/BackendEntry'
        path = self.root / 'core.jar'
        allowed = [prefix + suffix + '.class' for suffix in
                   ('', '$BackendColumnIterator', '$BackendColumnIterator$EmptyIterator',
                    '$Other$BackendColumn')]
        path.write_bytes(self.jar('hugegraph-core', allowed))
        check.scan_jar(path, str(path), {}, set(), retired, 'server')
        for suffix in ('$BackendColumn', '$BackendColumn$Child'):
            with self.subTest(suffix=suffix):
                path.write_bytes(self.jar('hugegraph-core', allowed + [prefix + suffix + '.class']))
                with self.assertRaisesRegex(check.CheckFailure, 'Retired shared class shipped'):
                    check.scan_jar(path, str(path), {}, set(), retired, 'server')

    def test_retired_index_sources_preserve_transaction_and_shared_models(self):
        self.sources()
        retired = check.retired_classes()
        self.assertIn('org.apache.hugegraph.structure.HugeIndex', retired)
        self.assertIn('org.apache.hugegraph.backend.tx.GraphIndexTransaction$MatchedIndex', retired)
        for package, name in (('structure', 'Index'), ('query', 'MatchedIndex')):
            path = (self.root / 'hugegraph-struct/src/main/java/org/apache/hugegraph' /
                    package / (name + '.java'))
            path.parent.mkdir(parents=True)
            path.write_text(f'package org.apache.hugegraph.{package};\npublic class {name} {{}}\n')
        transaction = (self.root / check.SOURCE_MODULES[0] /
                       'src/main/java/org/apache/hugegraph/backend/tx/GraphIndexTransaction.java')
        transaction.parent.mkdir(parents=True)
        source = '''package org.apache.hugegraph.backend.tx;
class GraphIndexTransaction {
    String text = "class MatchedIndex {}";
    class MatchedIndexIterator {}
    class Other { class MatchedIndex {} }
}
'''
        transaction.write_text(source)
        check.check_sources(self.root, retired)
        huge_index = (self.root / check.SOURCE_MODULES[0] /
                      'src/main/java/org/apache/hugegraph/structure/HugeIndex.java')
        huge_index.parent.mkdir(parents=True)
        huge_index.write_text('package org.apache.hugegraph.structure;\nclass HugeIndex {}\n')
        with self.assertRaisesRegex(check.CheckFailure, 'Retired shared implementation'):
            check.check_sources(self.root, retired)
        huge_index.unlink()
        transaction.write_text(source.replace('class GraphIndexTransaction {',
                                              'class GraphIndexTransaction {\nprivate static class MatchedIndex {}'))
        with self.assertRaisesRegex(check.CheckFailure, 'Retired shared implementation'):
            check.check_sources(self.root, retired)

    def test_retired_index_bytecode_including_children_is_rejected(self):
        retired = check.retired_classes()
        path = self.root / 'core.jar'
        transaction = 'org/apache/hugegraph/backend/tx/GraphIndexTransaction'
        allowed = [transaction + suffix + '.class' for suffix in
                   ('', '$MatchedIndexIterator', '$Other$MatchedIndex')]
        path.write_bytes(self.jar('hugegraph-core', allowed))
        check.scan_jar(path, str(path), {}, set(), retired, 'server')
        shared = self.root / 'struct.jar'
        shared.write_bytes(self.jar('hugegraph-struct', ['org/apache/hugegraph/structure/Index.class',
                                                       'org/apache/hugegraph/query/MatchedIndex.class']))
        check.scan_jar(shared, str(shared), {}, set(), retired, 'server')
        for name in ('org/apache/hugegraph/structure/HugeIndex',
                     'org/apache/hugegraph/structure/HugeIndex$IdWithExpiredTime',
                     transaction + '$MatchedIndex', transaction + '$MatchedIndex$Child'):
            with self.subTest(name=name):
                path.write_bytes(self.jar('hugegraph-core', allowed + [name + '.class']))
                with self.assertRaisesRegex(check.CheckFailure, 'Retired shared class shipped'):
                    check.scan_jar(path, str(path), {}, set(), retired, 'server')

    def test_rpc_interfaces_owned_by_common_not_core_or_rpc_implementation(self):
        self.sources()
        retired = check.retired_classes()
        for name in ('RpcServiceConfig4Client', 'RpcServiceConfig4Server'):
            relative = 'src/main/java/org/apache/hugegraph/rpc/' + name + '.java'
            source = f'package org.apache.hugegraph.rpc;\npublic interface {name} {{}}\n'
            common = self.root / 'hugegraph-commons/hugegraph-common' / relative
            common.parent.mkdir(parents=True, exist_ok=True)
            common.write_text(source)
            self.assertIn('org.apache.hugegraph.rpc.' + name, retired)
            check.check_sources(self.root, retired)
            for module, message in ((check.SOURCE_MODULES[0], 'Retired shared implementation'),
                                    ('hugegraph-commons/hugegraph-rpc', 'Duplicate source FQCN')):
                with self.subTest(name=name, module=module):
                    duplicate = self.root / module / relative
                    duplicate.parent.mkdir(parents=True, exist_ok=True)
                    duplicate.write_text(source)
                    with self.assertRaisesRegex(check.CheckFailure, message):
                        check.check_sources(self.root, retired)
                    duplicate.unlink()

    def test_all_source_gremlin_configs_resolve_current_shared_class_names(self):
        current = 'org.apache.hugegraph.id.IdGenerator'
        directions = 'org.apache.hugegraph.type.define.Directions'
        for relative in check.SOURCE_IMPORT_CONFIGS:
            path = self.root / relative
            path.parent.mkdir(parents=True)
            path.write_text(f'classImports: [{current}, {directions}, java.lang.Math]\n')
        check.check_source_imports(self.root, {current, directions})
        path = self.root / check.SOURCE_IMPORT_CONFIGS[-1]
        path.write_text('classImports: [org.apache.hugegraph.backend.id.IdGenerator]\n')
        with self.assertRaisesRegex(check.CheckFailure, 'class is unavailable'):
            check.check_source_imports(self.root, {current, directions})
        path.unlink()
        with self.assertRaisesRegex(check.CheckFailure, 'Missing Gremlin import configuration'):
            check.check_source_imports(self.root, {current, directions})

    def test_gremlin_import_reader_flow_and_block_lists(self):
        path = self.root / 'gremlin.yaml'
        path.write_text('classImports: [\n "example.One", # first import\n\'example.Two\', java.lang.Math\n]\n')
        self.assertEqual(check.class_imports(path), ['example.One', 'example.Two', 'java.lang.Math'])
        path.write_text('plugins:\n  classImports:\n    - "example.One"\n    - \'example.Two\'\n'
                        '  methodImports: [example.One#*]\n')
        self.assertEqual(check.class_imports(path), ['example.One', 'example.Two'])
        for value in ('classImports: *alias\n', 'classImports: [example.One\n',
                      'classImports: [example.*]\n', 'classImports: []\n'):
            with self.subTest(value=value):
                path.write_text(value)
                with self.assertRaises(check.CheckFailure):
                    check.class_imports(path)

    def test_packaged_gremlin_imports_resolve_shared_and_external_classes(self):
        path = self.distribution()
        (path / 'lib/hugegraph-struct.jar').write_bytes(self.jar('hugegraph-struct', [
            'org/apache/hugegraph/id/IdGenerator.class', 'org/apache/hugegraph/type/define/Directions.class'
        ]))
        (path / 'lib/external.jar').write_bytes(self.jar('external', entries={
            'BOOT-INF/lib/inner.jar': self.jar('external-inner', ['example/External.class'])
        }))
        config = path / 'conf/gremlin-server.yaml'
        config.write_text('classImports: [org.apache.hugegraph.id.IdGenerator,\n'
                          'org.apache.hugegraph.type.define.Directions, example.External, java.lang.Math]\n')
        check.check_distribution(path, 'server', check.retired_classes())
        config.write_text('classImports: [org.apache.hugegraph.backend.id.IdGenerator]\n')
        with self.assertRaisesRegex(check.CheckFailure, 'class is unavailable'):
            check.check_distribution(path, 'server', check.retired_classes())
        config.unlink()
        with self.assertRaisesRegex(check.CheckFailure, 'Missing Gremlin import configuration'):
            check.check_distribution(path, 'server', check.retired_classes())

    def test_forbidden_dependencies_are_matched_by_exact_group(self):
        for module in ('hugegraph-struct', 'hugegraph-commons/hugegraph-common',
                       'hugegraph-commons/hugegraph-rpc', 'hugegraph-pd/hg-pd-service',
                       'hugegraph-store/hg-store-core'):
            artifact = module.rsplit('/', 1)[-1]
            with self.subTest(module=module):
                tree = self.tree(artifact, '\\- external:hugegraph-core:jar:1.0:compile\n')
                check.check_tree(tree, module)
                tree = self.tree(artifact, f'\\- {check.GROUP}:hugegraph-core:jar:1.8.0:compile\n')
                with self.assertRaisesRegex(check.CheckFailure, 'Forbidden resolved dependency'):
                    check.check_tree(tree, module)

    def test_struct_and_rpc_framework_dependency_boundaries(self):
        module = 'hugegraph-commons/hugegraph-rpc'
        self.assertIn(module, check.DEPENDENCY_MODULES)
        targets = [('hugegraph-struct', 'hugegraph-rpc')]
        targets.extend((module, name) for name in ('hugegraph-core', 'hugegraph-struct', 'hg-pd-client',
                                                  'hg-store-common', 'hugegraph-pd', 'hugegraph-store'))
        for caller, dependency in targets:
            artifact = caller.rsplit('/', 1)[-1]
            for prefix in ('', '+- external:parent:jar:1.0:compile\n|  '):
                with self.subTest(caller=caller, dependency=dependency, transitive=bool(prefix)):
                    tree = self.tree(artifact, prefix + f'\\- {check.GROUP}:{dependency}:jar:1.8.0:compile\n')
                    with self.assertRaisesRegex(check.CheckFailure, 'Forbidden resolved dependency'):
                        check.check_tree(tree, caller)
        check.check_tree(self.tree('hugegraph-rpc', f'\\- {check.GROUP}:hugegraph-common:jar:1.8.0:compile\n'), module)

    def test_stale_project_versions_in_direct_and_nested_distributions_fail(self):
        for kind in ('server', 'pd', 'store'):
            with self.subTest(kind=kind):
                path = self.distribution(kind, nested=kind != 'server')
                check.check_distribution(path, kind, set(), expected_revision='1.8.0')
                extra = path / 'lib/extra.jar'
                stale = self.jar('stale-helper', ['org/apache/hugegraph/Stale.class'], version='1.7.0')
                extra.write_bytes(stale)
                with self.assertRaisesRegex(check.CheckFailure, 'Stale shipped artifact'):
                    check.check_distribution(path, kind, set(), expected_revision='1.8.0')
                extra.write_bytes(self.jar('boot-helper', entries={'BOOT-INF/lib/stale.jar': stale}))
                with self.assertRaisesRegex(check.CheckFailure, 'Stale shipped artifact'):
                    check.check_distribution(path, kind, set(), expected_revision='1.8.0')
                extra.write_bytes(self.jar('hugegraph-core', ['example/External.class'],
                                          group='external', version='9.0'))
                check.check_distribution(path, kind, set(), expected_revision='1.8.0')

    def test_retired_sources_cannot_return_in_server_api_or_backends(self):
        self.sources()
        retired = check.retired_classes()
        # Canonical owners preserve their original FQCNs even though those
        # implementations were removed from Server; their imports/aliases stay legal.
        for module, package, name in (('hugegraph-struct', 'type', 'HugeType'),
                                      ('hugegraph-commons/hugegraph-common', 'auth', 'TokenGenerator')):
            path = self.root / module / 'src/main/java/org/apache/hugegraph' / package / (name + '.java')
            path.parent.mkdir(parents=True)
            path.write_text(f'package org.apache.hugegraph.{package};\npublic class {name} {{}}\n')
        check.check_sources(self.root, retired)
        for module in ('hugegraph-api', 'hugegraph-hstore'):
            self.declare_server_module(module)
            path = (self.root / 'hugegraph-server' / module /
                    'src/main/java/org/apache/hugegraph/backend/id/Id.java')
            path.parent.mkdir(parents=True)
            path.write_text('package org.apache.hugegraph.backend.id;\npublic interface Id {}\n')
            with self.subTest(module=module), self.assertRaisesRegex(check.CheckFailure,
                                                                    'Retired shared implementation'):
                check.check_sources(self.root, retired)
            path.unlink()
            nested = (self.root / 'hugegraph-server' / module /
                      'src/main/java/org/apache/hugegraph/backend/store/BackendEntry.java')
            nested.parent.mkdir(parents=True)
            nested.write_text('package org.apache.hugegraph.backend.store;\n'
                              'interface BackendEntry { class BackendColumn {} }\n')
            with self.assertRaisesRegex(check.CheckFailure, 'Retired shared implementation'):
                check.check_sources(self.root, retired)
            nested.write_text('package org.apache.hugegraph.backend.store;\n'
                              'interface BackendEntry { String alias = "class BackendColumn {}"; }\n')
            check.check_sources(self.root, retired)
            nested.unlink()
        self.declare_server_module('hugegraph-test')
        fixture = (self.root / 'hugegraph-server/hugegraph-test/'
                   'src/main/java/org/apache/hugegraph/backend/id/Id.java')
        fixture.parent.mkdir(parents=True)
        fixture.write_text('package org.apache.hugegraph.backend.id;\ninterface Id {}\n')
        check.check_sources(self.root, retired)

    def test_retired_bytecode_cannot_return_in_any_server_artifact(self):
        retired = check.retired_classes()
        jar = self.root / 'owner.jar'
        for artifact in check.SERVER_ARTIFACTS:
            for name in ('org/apache/hugegraph/backend/id/Id.class',
                         'org/apache/hugegraph/backend/store/BackendEntry$BackendColumn.class'):
                with self.subTest(artifact=artifact, name=name):
                    jar.write_bytes(self.jar(artifact, [name]))
                    with self.assertRaisesRegex(check.CheckFailure, 'Retired shared class shipped'):
                        check.scan_jar(jar, str(jar), {}, set(), retired, 'server')
        for artifact, name in (('hugegraph-struct', 'org/apache/hugegraph/type/HugeType.class'),
                               ('hugegraph-common', 'org/apache/hugegraph/auth/TokenGenerator.class'),
                               ('hugegraph-test', 'org/apache/hugegraph/backend/id/Id.class')):
            jar.write_bytes(self.jar(artifact, [name]))
            check.scan_jar(jar, str(jar), {}, set(), retired, 'server')

    def test_new_server_module_artifacts_are_included_in_retirement_policy(self):
        self.declare_server_module('new-backend')
        ids = check.server_artifact_ids(self.root)
        self.assertIn('new-backend', ids)
        jar = self.root / 'new.jar'
        jar.write_bytes(self.jar('new-backend', ['org/apache/hugegraph/backend/id/Id.class']))
        with self.assertRaisesRegex(check.CheckFailure, 'Retired shared class shipped'):
            check.scan_jar(jar, str(jar), {}, set(), check.retired_classes(), 'server', server_artifacts=ids)

    def test_nested_profile_module_sources_and_shipped_artifacts_are_checked(self):
        self.sources()
        aggregator = self.declare_server_module('backend')
        aggregator.write_text('<project xmlns="http://maven.apache.org/POM/4.0.0">'
                              '<artifactId>backend-parent</artifactId><profiles><profile><id>future-platform</id>'
                              '<modules><module>new-backend</module></modules></profile></profiles></project>')
        module = aggregator.parent / 'new-backend'
        module.mkdir()
        (module / 'pom.xml').write_text('<project xmlns="http://maven.apache.org/POM/4.0.0">'
                                       '<artifactId>future-server-backend</artifactId></project>')
        identities = check.server_artifact_ids(self.root)
        self.assertIn('future-server-backend', identities)
        retired = check.retired_classes()
        for kind in ('java', 'dev'):
            path = module / 'src/main' / kind / 'org/apache/hugegraph/backend/id/Id.java'
            path.parent.mkdir(parents=True)
            path.write_text('package org.apache.hugegraph.backend.id;\ninterface Id {}\n')
            with self.subTest(kind=kind), self.assertRaisesRegex(check.CheckFailure,
                                                                'Retired shared implementation'):
                check.check_sources(self.root, retired)
            path.unlink()
        alias = module / 'src/main/java/org/apache/hugegraph/backend/NewBackend.java'
        alias.write_text('package org.apache.hugegraph.backend;\n'
                         'class NewBackend { String legacy = "org.apache.hugegraph.backend.id.Id"; }\n')
        check.check_sources(self.root, retired)
        jar = self.root / 'nested-backend.jar'
        data = self.jar('future-server-backend', ['org/apache/hugegraph/backend/id/Id.class'])
        for value in (data, self.jar('boot-wrapper', entries={'BOOT-INF/lib/backend.jar': data})):
            jar.write_bytes(value)
            with self.assertRaisesRegex(check.CheckFailure, 'Retired shared class shipped'):
                check.scan_jar(jar, str(jar), {}, set(), retired, 'server', server_artifacts=identities)

    def test_declared_reactor_failures_are_closed(self):
        self.sources()
        aggregator = self.declare_server_module('backend')
        cases = (('missing-child', 'Missing declared Server POM'),
                 ('../', 'Cyclic Server module declaration'),
                 ('../../outside-server', 'escapes its reactor'),
                 ('${unknown.module}', 'nonliteral Server module declaration'),
                 ('', 'Empty or nonliteral Server module declaration'))
        for value, message in cases:
            with self.subTest(module=value):
                aggregator.write_text('<project><artifactId>backend-parent</artifactId>'
                                      '<modules><module>' + value + '</module></modules></project>')
                with self.assertRaisesRegex(check.CheckFailure, message):
                    check.server_module_inventory(self.root)
        aggregator.write_text('<project><artifactId>backend-parent')
        with self.assertRaisesRegex(check.CheckFailure, 'Invalid Server module POM'):
            check.server_module_inventory(self.root)
        aggregator.unlink()
        with self.assertRaisesRegex(check.CheckFailure, 'Missing declared Server POM'):
            check.server_module_inventory(self.root)

    def test_reactor_inventory_ignores_unlisted_copied_and_target_modules(self):
        self.sources()
        for relative in ('target/copied-backend', 'apache-hugegraph-server-1.8.0'):
            directory = self.root / 'hugegraph-server' / relative
            source = directory / 'src/main/dev/org/apache/hugegraph/backend/id/Id.java'
            source.parent.mkdir(parents=True)
            source.write_text('package org.apache.hugegraph.backend.id;\ninterface Id {}\n')
            (directory / 'pom.xml').write_text('<project><artifactId>unlisted-copy</artifactId></project>')
        check.check_sources(self.root, check.retired_classes())
        self.assertNotIn('unlisted-copy', check.server_artifact_ids(self.root))

    def test_profile_redeclarations_deduplicate_and_test_subtree_is_excluded(self):
        self.sources()
        test = self.declare_server_module('hugegraph-test')
        test.write_text('<project><artifactId>hugegraph-test</artifactId>'
                        '<modules><module>fixtures</module></modules></project>')
        child = test.parent / 'fixtures'
        child.mkdir()
        (child / 'pom.xml').write_text('<project><artifactId>test-fixtures</artifactId></project>')
        path = child / 'src/main/java/org/apache/hugegraph/backend/id/Id.java'
        path.parent.mkdir(parents=True)
        path.write_text('package org.apache.hugegraph.backend.id;\ninterface Id {}\n')
        root_pom = self.root / 'hugegraph-server/pom.xml'
        project = ET.parse(root_pom)
        namespace = '{http://maven.apache.org/POM/4.0.0}'
        profiles = ET.SubElement(project.getroot(), namespace + 'profiles')
        profile = ET.SubElement(profiles, namespace + 'profile')
        modules = ET.SubElement(profile, namespace + 'modules')
        ET.SubElement(modules, namespace + 'module').text = './hugegraph-core'
        project.write(root_pom, encoding='unicode')
        check.check_sources(self.root, check.retired_classes())
        identities = check.server_artifact_ids(self.root)
        self.assertEqual(identities, {'hugegraph-server', 'hugegraph-core'})


if __name__ == '__main__':
    unittest.main()
