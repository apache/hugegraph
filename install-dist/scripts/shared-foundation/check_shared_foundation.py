#!/usr/bin/env python3
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

"""Check shared ownership, resolved dependencies and shipped classpaths.

Run after a reactor install/package:
  python3 install-dist/scripts/shared-foundation/check_shared_foundation.py --revision 1.8.0

--source-only is an explicitly narrower check for fast source-edit iterations.
The full check always regenerates dependency trees; stale tree files are not evidence.
"""

import argparse
import io
from pathlib import Path
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
import zipfile


SOURCE_MODULES = (
    'hugegraph-server/hugegraph-core',
    'hugegraph-struct',
    'hugegraph-commons/hugegraph-common',
    'hugegraph-commons/hugegraph-rpc',
)
DEPENDENCY_MODULES = (
    'hugegraph-struct',
    'hugegraph-commons/hugegraph-common',
    'hugegraph-commons/hugegraph-rpc',
    'hugegraph-pd/hg-pd-service',
    'hugegraph-store/hg-store-core',
    'hugegraph-store/hg-store-node',
)
REQUIRED_ARTIFACTS = {
    'server': {'hugegraph-core', 'hugegraph-struct', 'hugegraph-common', 'hugegraph-rpc'},
    'pd': {'hg-pd-service', 'hg-pd-core', 'hugegraph-common'},
    'store': {'hg-store-node', 'hg-store-core', 'hugegraph-struct', 'hugegraph-common'},
}
SCOPES = {'compile', 'runtime', 'provided', 'test', 'system', 'import'}
GROUP = 'org.apache.hugegraph'
SERVER_ARTIFACTS = {'hugegraph-server', 'hugegraph-core', 'hugegraph-api', 'hugegraph-dist',
                    'hugegraph-hstore', 'hugegraph-hbase', 'hugegraph-rocksdb', 'hugegraph-example'}
SOURCE_IMPORT_CONFIGS = (
    'hugegraph-server/hugegraph-dist/src/assembly/static/conf/gremlin-server.yaml',
    'hugegraph-server/hugegraph-dist/src/assembly/travis/conf-raft1/gremlin-server.yaml',
    'hugegraph-server/hugegraph-dist/src/assembly/travis/conf-raft2/gremlin-server.yaml',
    'hugegraph-server/hugegraph-dist/src/assembly/travis/conf-raft3/gremlin-server.yaml',
)


class CheckFailure(Exception):
    pass


def retired_classes():
    path = Path(__file__).with_name('retired-core-classes.txt')
    result = {line.strip() for line in path.read_text().splitlines()
              if line.strip() and not line.startswith('#')}
    if not result or any(not name.startswith(GROUP + '.') for name in result):
        raise CheckFailure(f'Missing or invalid retired-owner manifest: {path}')
    return result


def java_member_classes(source, package):
    # Strip comments/literals before matching declarations, so references to a
    # retired type in documentation, strings or class literals are harmless.
    source = re.sub(r'//[^\n]*|/\*.*?\*/|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'',
                    '', source, flags=re.DOTALL)
    tokens = re.findall(r'[A-Za-z_$][\w$]*|[{}.()]', source)
    scopes = []
    pending = None
    names = set()
    for index, token in enumerate(tokens):
        if (token in ('class', 'interface', 'enum') and
                (index == 0 or tokens[index - 1] != '.') and
                (not scopes or scopes[-1] is not None) and index + 1 < len(tokens) and
                re.fullmatch(r'[A-Za-z_$][\w$]*', tokens[index + 1])):
            pending = tokens[index + 1]
        elif token == '{':
            scopes.append(pending)
            if pending is not None:
                names.add(package + '.' + '$'.join(name for name in scopes if name is not None))
            pending = None
        elif token == '}' and scopes:
            scopes.pop()
    return names


def is_retired_class(name, retired):
    return any(name == owner or name.startswith(owner + '$') for owner in retired)


def server_module_inventory(root):
    boundary = (root / 'hugegraph-server').resolve()
    projects, visited, active = {}, set(), set()

    def visit(pom):
        try:
            pom = pom.resolve()
        except (OSError, RuntimeError) as error:
            raise CheckFailure(f'Cannot resolve declared Server POM: {pom}: {error}') from error
        try:
            pom.relative_to(boundary)
        except ValueError as error:
            raise CheckFailure(f'Declared Server POM escapes its reactor: {pom}') from error
        if pom in active:
            raise CheckFailure(f'Cyclic Server module declaration: {pom}')
        if pom in visited:
            return
        if not pom.is_file():
            raise CheckFailure(f'Missing declared Server POM: {pom}')
        try:
            project = ET.parse(pom).getroot()
        except ET.ParseError as error:
            raise CheckFailure(f'Invalid Server module POM: {pom}: {error}') from error
        if project.tag.rsplit('}', 1)[-1] != 'project':
            raise CheckFailure(f'Invalid Server project element: {pom}')
        namespace = project.tag[:-len('project')]
        artifact = (project.findtext(namespace + 'artifactId') or '').strip()
        if not re.fullmatch(r'[A-Za-z0-9_.-]+', artifact):
            raise CheckFailure(f'Missing or nonliteral Server artifactId: {pom}')
        if pom.parent in projects and projects[pom.parent] != artifact:
            raise CheckFailure(f'Server projects share a source directory: {pom.parent}')
        projects[pom.parent] = artifact
        active.add(pom)
        # Include every possible profile, irrespective of the current host or
        # active profiles. Inactive modules must not become a retirement loophole.
        modules = project.findall(namespace + 'modules/' + namespace + 'module')
        modules.extend(project.findall(namespace + 'profiles/' + namespace + 'profile/' +
                                       namespace + 'modules/' + namespace + 'module'))
        for declaration in modules:
            value = (declaration.text or '').strip()
            if not value or '$' in value:
                raise CheckFailure(f'Empty or nonliteral Server module declaration: {pom}: {value}')
            target = pom.parent / value
            visit(target if target.suffix == '.xml' or target.is_file() else target / 'pom.xml')
        active.remove(pom)
        visited.add(pom)

    visit(boundary / 'pom.xml')
    test_roots = [directory for directory, artifact in projects.items() if artifact == 'hugegraph-test']
    return {directory: artifact for directory, artifact in projects.items()
            if not any(directory == test or test in directory.parents for test in test_roots)}


def check_sources(root, retired):
    root = root.resolve()
    owners = {}
    counts = {}
    production_roots = set()
    for module in SOURCE_MODULES:
        directories = [root / module / 'src/main' / kind for kind in ('java', 'dev')]
        if not any(list(directory.rglob('*.java')) for directory in directories):
            raise CheckFailure(f'Missing production sources: {root / module / "src/main"}')
        production_roots.update(directories)
    # Use the same recursively declared reactor inventory as shipped ownership,
    # never search copied distributions or target directories for module POMs.
    for module in server_module_inventory(root):
        production_roots.update(module / 'src/main' / kind for kind in ('java', 'dev'))
    # Generated grpc sources in src/main/java are outside the shared-owner
    # scan. Hand-maintained production sources beside them must still be checked.
    for pattern in ('hugegraph-*/src/main/dev', 'hugegraph-*/*/src/main/dev'):
        production_roots.update(directory for directory in root.glob(pattern)
                                if directory.relative_to(root).parts[0] != 'hugegraph-server')
    for directory in sorted(production_roots):
        module = str(directory.parent.parent.parent.relative_to(root))
        files = sorted(directory.rglob('*.java'))
        counts[module] = counts.get(module, 0) + len(files)
        for path in files:
            source = path.read_text()
            package = re.search(r'^\s*package\s+([\w.]+)\s*;', source, re.MULTILINE)
            if not package:
                raise CheckFailure(f'Cannot determine source package: {path}')
            name = package.group(1) + '.' + path.stem
            if not name.startswith(GROUP + '.'):
                continue
            if module == 'hugegraph-server' or module.startswith('hugegraph-server/'):
                declared = java_member_classes(source, package.group(1)) | {name}
                if any(is_retired_class(declaration, retired) for declaration in declared):
                    raise CheckFailure(f'Retired shared implementation returned to Server: {path}')
            if name in owners:
                raise CheckFailure(f'Duplicate source FQCN {name}: {owners[name]} and {path}')
            owners[name] = path
    print(f'Source ownership checked: {counts}')
    return owners


def class_imports(path):
    if not path.is_file():
        raise CheckFailure(f'Missing Gremlin import configuration: {path}')
    # This is a deliberately narrow reader for classImports flow/block lists,
    # not a YAML implementation. Unknown shapes fail rather than evade checks.
    lines = [re.sub(r'#.*$', '', line) for line in path.read_text().splitlines()]
    imports = []
    for index, line in enumerate(lines):
        match = re.match(r'^(\s*)classImports\s*:\s*(.*)$', line)
        if not match:
            continue
        indent, tail = len(match.group(1)), match.group(2).strip()
        if tail.startswith('['):
            body = '\n'.join([tail] + lines[index + 1:])
            end = body.find(']')
            if end < 0:
                raise CheckFailure(f'Unterminated classImports list: {path}')
            values = body[1:end].split(',')
        elif not tail:
            values = []
            for following in lines[index + 1:]:
                if not following.strip():
                    continue
                following_indent = len(following) - len(following.lstrip())
                entry = following.strip()
                if following_indent < indent or (following_indent == indent and not entry.startswith('-')):
                    break
                if not entry.startswith('- '):
                    raise CheckFailure(f'Unsupported classImports entry: {path}: {entry}')
                values.append(entry[2:])
        else:
            raise CheckFailure(f'Unsupported classImports shape: {path}: {tail}')
        for value in values:
            name = value.strip().strip('"\'')
            if not name:
                continue
            if not re.fullmatch(r'[A-Za-z_$][\w$]*(?:\.[A-Za-z_$][\w$]*)+', name):
                raise CheckFailure(f'Invalid classImports class name: {path}: {name}')
            imports.append(name)
    if not imports:
        raise CheckFailure(f'Missing or empty classImports list: {path}')
    return imports


def check_class_imports(path, available, local_only=False):
    imports = class_imports(path)
    for name in imports:
        # JDK classes are supplied by the runtime, not distribution jars.
        # Gremlin startup tests verify runtime loading, beyond this ownership check.
        if name.startswith('java.') or (local_only and not name.startswith(GROUP + '.')):
            continue
        if name not in available:
            raise CheckFailure(f'Gremlin classImports class is unavailable: {name} ({path})')
    print(f'Gremlin classImports checked: {path} ({len(imports)} imports)')


def check_source_imports(root, owners):
    for path in SOURCE_IMPORT_CONFIGS:
        check_class_imports(root / path, owners, local_only=True)


def parse_tree(path, expected_artifact):
    if not path.is_file():
        raise CheckFailure(f'Missing resolved dependency tree: {path}')
    nodes = []
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        coordinate = re.sub(r'^[\s|+\\-]*', '', line).strip()
        coordinate = re.sub(r' \(optional\)$', '', coordinate)
        parts = coordinate.split(':')
        if (len(parts) < 4 or len(parts) > 6 or
                any(not part or re.search(r'\s', part) for part in parts)):
            raise CheckFailure(f'Malformed resolved tree line in {path}: {line}')
        if nodes and parts[-1] not in SCOPES:
            raise CheckFailure(f'Missing dependency scope in {path}: {line}')
        nodes.append((parts[0], parts[1]))
    if not nodes or nodes[0] != (GROUP, expected_artifact):
        raise CheckFailure(f'Wrong or empty dependency tree root: {path}; expected {expected_artifact}')
    return nodes


def check_tree(path, module):
    artifact = module.rsplit('/', 1)[-1]
    nodes = parse_tree(path, artifact)
    forbidden = set()
    if module.startswith(('hugegraph-pd/', 'hugegraph-store/')):
        forbidden.add('hugegraph-core')
    if artifact == 'hugegraph-struct':
        forbidden.update(('hugegraph-core', 'hugegraph-rpc'))
    if artifact == 'hugegraph-rpc':
        forbidden.update(('hugegraph-core', 'hugegraph-struct'))
    if artifact == 'hugegraph-common':
        forbidden.update(('hugegraph-core', 'hugegraph-struct', 'hugegraph-server',
                          'hugegraph-store', 'hugegraph-pd'))
    for group, name in nodes[1:]:
        if group != GROUP:
            continue
        component_dependency = (name.startswith(('hg-pd-', 'hg-store-')) or
                                name in ('hugegraph-pd', 'hugegraph-store'))
        if name in forbidden or (artifact in ('hugegraph-struct', 'hugegraph-common', 'hugegraph-rpc') and
                                 component_dependency):
            raise CheckFailure(f'Forbidden resolved dependency of {artifact}: {group}:{name} ({path})')
    print(f'Resolved dependency boundary checked: {artifact} ({len(nodes) - 1} dependencies)')


def select_tree(lines, artifact):
    starts = [i for i, line in enumerate(lines) if line.startswith(GROUP + ':')]
    selected = []
    for index, start in enumerate(starts):
        if lines[start].split(':')[1] == artifact:
            stop = starts[index + 1] if index + 1 < len(starts) else len(lines)
            selected.append(lines[start:stop])
    if len(selected) != 1:
        raise CheckFailure(f'Expected exactly one current reactor tree for {artifact}')
    return '\n'.join(selected[0]) + '\n'


def resolve_dependencies(root, maven):
    directory = root / 'target/shared-foundation'
    directory.mkdir(parents=True, exist_ok=True)
    modules = set(DEPENDENCY_MODULES)
    # Cover every distributed submodule, including standalone client/transfer
    # modules and tests, not only dependencies currently reachable from services.
    for parent in ('hugegraph-pd', 'hugegraph-store'):
        modules.update(str(path.parent.relative_to(root))
                       for path in (root / parent).glob('*/pom.xml'))
    for module in modules:
        if not (root / module / 'pom.xml').is_file():
            raise CheckFailure(f'Missing expected Maven module: {module}')
    output = directory / 'reactor-dependencies.txt'
    output.unlink(missing_ok=True)
    # Resolve current reactor models, never reuse old tree files. Upstream
    # project POMs are read from this checkout rather than a stale local install.
    subprocess.run([
        maven, '-B', '-ntp', '-pl', ','.join(sorted(modules)), '-am',
        'org.apache.maven.plugins:maven-dependency-plugin:3.8.1:tree',
        '-DoutputType=text', f'-DoutputFile={output}', '-DappendOutput=true',
    ], cwd=root, check=True)
    if not output.is_file():
        raise CheckFailure(f'Missing resolved reactor dependencies: {output}')
    lines = output.read_text().splitlines()
    for module in sorted(modules):
        artifact = module.rsplit('/', 1)[-1]
        selected = directory / (artifact + '.txt')
        selected.write_text(select_tree(lines, artifact))
        check_tree(selected, module)


def properties(text):
    return {key.strip(): value.strip() for line in text.splitlines()
            if line.strip() and not line.lstrip().startswith(('#', '!')) and '=' in line
            for key, value in [line.split('=', 1)]}


def server_artifact_ids(root):
    return set(server_module_inventory(root).values())


def scan_jar(source, label, owners, artifacts, retired, distribution, available=None,
             expected_revision=None, server_artifacts=None):
    if server_artifacts is None:
        server_artifacts = SERVER_ARTIFACTS
    try:
        archive = zipfile.ZipFile(source)
    except (OSError, zipfile.BadZipFile) as error:
        raise CheckFailure(f'Invalid shipped jar {label}: {error}') from error
    with archive:
        names = archive.namelist()
        identities = set()
        for name in names:
            if name.startswith('META-INF/maven/') and name.endswith('/pom.properties'):
                values = properties(archive.read(name).decode('utf-8'))
                if values.get('groupId') == GROUP:
                    artifact = values.get('artifactId')
                    if not artifact or not values.get('version'):
                        raise CheckFailure(f'Incomplete artifact metadata in {label}: {name}')
                    if expected_revision is not None and values['version'] != expected_revision:
                        raise CheckFailure(f'Stale shipped artifact {GROUP}:{artifact}:{values["version"]} '
                                           f'in {label}; expected {expected_revision}')
                    identities.add(artifact)
        artifacts.update(identities)
        if distribution in ('pd', 'store') and 'hugegraph-core' in identities:
            raise CheckFailure(f'Forbidden hugegraph-core shipped in {distribution}: {label}')
        seen_entries = set()
        for name in names:
            if name.startswith(('BOOT-INF/lib/', 'WEB-INF/lib/')) and name.endswith('.jar'):
                scan_jar(io.BytesIO(archive.read(name)), f'{label}!/{name}', owners,
                         artifacts, retired, distribution, available, expected_revision, server_artifacts)
                continue
            normalized = re.sub(r'^(?:BOOT-INF/classes/|WEB-INF/classes/|META-INF/versions/\d+/)', '', name)
            if available is not None and normalized.endswith('.class'):
                available.add(normalized[:-6].replace('/', '.'))
            if not normalized.startswith('org/apache/hugegraph/') or not normalized.endswith('.class'):
                continue
            if name in seen_entries:
                raise CheckFailure(f'Duplicate class entry inside jar {label}: {name}')
            seen_entries.add(name)
            fqcn = normalized[:-6].replace('/', '.')
            if identities & server_artifacts and is_retired_class(fqcn, retired):
                raise CheckFailure(f'Retired shared class shipped in Server: {fqcn} ({label})')
            previous = owners.get(fqcn)
            if previous is not None and previous != label:
                raise CheckFailure(f'Duplicate shipped FQCN {fqcn}: {previous} and {label}')
            # Multi-release variants within the same jar are a single owner.
            owners[fqcn] = label


def check_distribution(path, kind, retired, expected_revision=None, server_artifacts=None):
    library = path / 'lib'
    jars = sorted(library.rglob('*.jar'))
    if not library.is_dir() or not jars:
        raise CheckFailure(f'Missing shipped {kind} lib jars: {library}')
    for name in ('ext', 'plugins'):
        if (path / name).exists():
            jars.extend(sorted((path / name).rglob('*.jar')))
    owners = {}
    artifacts = set()
    available = set()
    for jar in jars:
        scan_jar(jar, str(jar), owners, artifacts, retired, kind, available, expected_revision, server_artifacts)
    missing = REQUIRED_ARTIFACTS[kind] - artifacts
    if missing:
        raise CheckFailure(f'Missing expected artifacts in {kind} distribution: {sorted(missing)}')
    if not owners:
        raise CheckFailure(f'No HugeGraph classes found in shipped {kind} jars')
    if kind == 'server':
        check_class_imports(path / 'conf/gremlin-server.yaml', available)
    print(f'Shipped {kind} classpath checked: {len(owners)} HugeGraph classes, '
          f'{len(artifacts)} project artifacts')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument('--source-only', action='store_true')
    parser.add_argument('--revision', help='Exact version of the freshly assembled distributions')
    parser.add_argument('--maven', default='mvn', help='Maven executable')
    args = parser.parse_args()
    if not args.source_only and not args.revision:
        parser.error('--revision is required for resolved dependency and distribution checks')
    root = args.root.resolve()
    try:
        retired = retired_classes()
        owners = check_sources(root, retired)
        check_source_imports(root, owners)
        if args.source_only:
            print('Source-only check passed; dependencies and distributions were NOT checked.')
            return 0
        resolve_dependencies(root, args.maven)
        server_artifacts = server_artifact_ids(root)
        for kind in REQUIRED_ARTIFACTS:
            path = root / ('hugegraph-' + kind) / f'apache-hugegraph-{kind}-{args.revision}'
            check_distribution(path, kind, retired, args.revision, server_artifacts)
    except (CheckFailure, OSError, UnicodeError, subprocess.CalledProcessError) as error:
        print(f'Shared foundation check FAILED: {error}', file=sys.stderr)
        return 1
    print('Shared foundation ownership, resolved dependencies and shipped classpaths passed.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
