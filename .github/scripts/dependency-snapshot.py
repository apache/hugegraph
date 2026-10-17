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

"""Serialize native Ferstl 4.0.3 graphs; no token or submission code.

Uses maven-dependency-submission-action 6.0.1's Maven parser/toolkit conventions:
first classifier/type, shared PURL packages, direct precedence, child edge order
and cycle handling. Non-test scopes take precedence over test to match the
independent inventory. See docs/ci.md for the producer boundary.
"""

import datetime
import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import quote

REPORT = 'maven-dependency-submission-action-depgraph.json'
DETECTOR = {'name': 'maven-dependency-submission-action', 'version': '6.0.1',
            'url': 'https://github.com/advanced-security/maven-dependency-submission-action'}
CORRELATOR = '.github/workflows/submit-dependencies.yml:submit'
SERIALIZER = 'Ferstl 4.0.3; repository stdlib adapter for Maven parser 6.0.1'


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f'Duplicate JSON key: {key}')
        result[key] = value
    return result


def package_url(artifact):
    # packageurl-js uses encodeURIComponent for components and URLSearchParams
    # for qualifiers; notably '+' in versions and '/' in namespaces survive.
    namespace = '/'.join(part for part in artifact['groupId'].split('/') if part)
    name, version = artifact['artifactId'].strip(), artifact['version'].strip()
    if not namespace or not name or not version:
        raise ValueError('Incomplete Maven coordinates')
    encoded_namespace = quote(namespace, safe="!~*'():/")
    encoded_name = quote(name, safe="!~*'()")
    encoded_version = quote(version, safe="!~*'():+")
    purl = f'pkg:maven/{encoded_namespace}/{encoded_name}@{encoded_version}'
    qualifiers = {}
    for key, field in [('classifier', 'classifiers'), ('type', 'types')]:
        values = artifact.get(field) or []
        if values and values[0].strip():
            qualifiers[key] = values[0].strip()
    if qualifiers:
        purl += '?' + '&'.join(f'{key}={quote(value, safe="*").replace("~", "%7E")}'
                               for key, value in sorted(qualifiers.items()))
    return purl


def manifest(graph, pom):
    if not isinstance(graph, dict) or not isinstance(graph.get('graphName'), str) or not graph['graphName'].strip():
        raise ValueError(f'Malformed graph name: {pom}')
    artifacts, edges = graph.get('artifacts'), graph.get('dependencies') or []
    if not isinstance(artifacts, list) or not artifacts or not isinstance(edges, list):
        raise ValueError(f'Empty or malformed graph: {pom}')
    if graph.get('dependencies') is not None and not isinstance(graph['dependencies'], list):
        raise ValueError(f'Malformed dependency list: {pom}')
    ids, packages, scopes = {}, {}, {}
    for artifact in artifacts:
        if not isinstance(artifact, dict) or any(not isinstance(artifact.get(key), str) or not artifact[key].strip()
                                                for key in ['id', 'groupId', 'artifactId', 'version']):
            raise ValueError(f'Malformed artifact: {pom}')
        for key in ['scopes', 'types', 'classifiers']:
            values = artifact.get(key)
            if values is not None and (not isinstance(values, list) or any(not isinstance(x, str) for x in values)):
                raise ValueError(f'Malformed artifact {key}: {pom}')
        identifier, purl = artifact['id'], package_url(artifact)
        if identifier in ids:
            raise ValueError(f'Duplicate artifact ID: {identifier}')
        ids[identifier] = purl
        packages.setdefault(purl, [])
        scopes[purl] = 'development' if set(artifact.get('scopes') or []) == {'test'} else 'runtime'
    children, incoming = {}, set()
    for edge in edges:
        if not isinstance(edge, dict) or any(not isinstance(edge.get(key), str) for key in ['from', 'to']):
            raise ValueError(f'Malformed dependency edge: {pom}')
        source, target = edge['from'], edge['to']
        if source not in ids or target not in ids:
            raise ValueError(f'Unknown dependency edge: {pom}')
        children.setdefault(source, []).append(target)
        incoming.add(target)
    # Aliased artifact IDs share one package and append all their child edges.
    for source, targets in children.items():
        packages[ids[source]].extend(ids[target] for target in targets)
    direct = dict.fromkeys(target for source in ids if source not in incoming
                           for target in children.get(source, []))
    resolved = {}

    def entry(purl, relationship):
        return {'package_url': purl, 'relationship': relationship,
                'scope': scopes[purl], 'dependencies': packages[purl]}

    for identifier in direct:
        purl = ids[identifier]
        resolved[purl] = entry(purl, 'direct')
        seen, pending = set(), list(reversed(packages[purl]))
        while pending:
            child = pending.pop()
            if child in seen:
                continue
            seen.add(child)
            resolved.setdefault(child, entry(child, 'indirect'))
            pending.extend(reversed(packages[child]))
    return {'name': graph['graphName'], 'file': {'source_location': pom}, 'resolved': resolved}


def create_snapshot(workspace, poms, sha, repository, run_id, run_attempt):
    manifests = {}
    for pom in poms:
        report = workspace / Path(pom).parent / 'target' / REPORT
        if report.is_symlink() or report.stat().st_size > 25 * 1024 * 1024:
            raise ValueError(f'Invalid report: {pom}')
        item = manifest(json.loads(report.read_text(), object_pairs_hook=unique_object), pom)
        if item['name'] in manifests:
            raise ValueError(f'Duplicate graph name: {item["name"]}')
        manifests[item['name']] = item
    if not manifests or not any(item['resolved'] for item in manifests.values()):
        raise ValueError('Refusing empty dependency inventory')
    return {'version': 0, 'sha': sha, 'ref': 'refs/heads/master',
            'job': {'id': run_id, 'correlator': CORRELATOR,
                    'html_url': f'https://github.com/{repository}/actions/runs/{run_id}'},
            'detector': DETECTOR, 'metadata': {'serializer': SERIALIZER, 'producer': 'generate',
                                            'run_attempt': run_attempt},
            'scanned': datetime.datetime.now(datetime.timezone.utc).isoformat().replace('+00:00', 'Z'),
            'manifests': manifests}


if __name__ == '__main__':
    poms = subprocess.check_output(['git', 'ls-files', '-z', '*pom.xml']).decode().split('\0')
    poms = [pom for pom in poms if Path(pom).name == 'pom.xml']
    snapshot = create_snapshot(Path.cwd(), poms, os.environ['GITHUB_SHA'],
                               os.environ['GITHUB_REPOSITORY'], os.environ['GITHUB_RUN_ID'],
                               os.environ['GITHUB_RUN_ATTEMPT'])
    Path(sys.argv[1]).write_text(json.dumps(snapshot, indent=2) + '\n')
