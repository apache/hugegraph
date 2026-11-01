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

"""Canonical inventory from independent Apache Maven dependency:tree JSON."""

import datetime
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys

spec = importlib.util.spec_from_file_location('snapshot', Path(__file__).with_name('dependency-snapshot.py'))
snapshot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(snapshot)

REPORT = 'dependency-inventory.json'
RESOLVER = 'org.apache.maven.plugins:maven-dependency-plugin:3.8.1:tree'
LIMIT = 25 * 1024 * 1024
FIELDS = {'groupId', 'artifactId', 'version', 'type', 'scope', 'classifier', 'optional', 'children'}
SCOPES = {'compile', 'runtime', 'provided', 'system', 'test'}


def packages(tree):
    result = {}
    pending = [(tree, 0)]
    while pending:
        node, depth = pending.pop()
        if not isinstance(node, dict) or set(node) - FIELDS:
            raise ValueError('Invalid inventory node')
        for field in ['groupId', 'artifactId', 'version', 'type']:
            if not isinstance(node.get(field), str) or not node[field].strip():
                raise ValueError('Incomplete inventory coordinates')
        if not isinstance(node.get('classifier'), str) or node.get('optional') not in ['true', 'false']:
            raise ValueError('Invalid inventory classifier/optional')
        scope = node.get('scope')
        if (depth == 0 and scope != '') or (depth > 0 and scope not in SCOPES):
            raise ValueError('Invalid inventory scope')
        children = node.get('children', [])
        if not isinstance(children, list):
            raise ValueError('Invalid inventory children')
        pending.extend((child, depth + 1) for child in children)
        if depth == 0:
            continue
        purl = snapshot.package_url(dict(node, types=[node['type']], classifiers=[node['classifier']]))
        value = {'scope': 'development' if scope == 'test' else 'runtime',
                 'relationship': 'direct' if depth == 1 else 'indirect'}
        # Maven may expose the same selected package along more than one path.
        previous = result.get(purl)
        if previous:
            if previous['scope'] == 'runtime':
                value['scope'] = 'runtime'
            if previous['relationship'] == 'direct':
                value['relationship'] = 'direct'
        result[purl] = value
    return result


def create_inventory(workspace, poms, sha, repository, run_id, run_attempt):
    manifests = {}
    for pom in poms:
        if pom in manifests:
            raise ValueError('Duplicate inventory POM')
        report = workspace / Path(pom).parent / 'target' / REPORT
        if report.is_symlink():
            raise ValueError('Invalid inventory report')
        with report.open('rb') as stream:
            raw = stream.read(LIMIT + 1)
        if not raw or len(raw) > LIMIT:
            raise ValueError('Invalid inventory report size')
        manifests[pom] = packages(json.loads(raw, object_pairs_hook=snapshot.unique_object))
    if not manifests or not any(manifests.values()):
        raise ValueError('Empty independent dependency inventory')
    return {'version': 1, 'sha': sha, 'repository': repository, 'run_id': run_id,
            'run_attempt': run_attempt, 'producer': 'inventory', 'resolver': RESOLVER,
            'scanned': datetime.datetime.now(datetime.timezone.utc).isoformat().replace('+00:00', 'Z'),
            'manifests': manifests}


if __name__ == '__main__':
    poms = subprocess.check_output(['git', 'ls-files', '-z', '*pom.xml']).decode().split('\0')
    poms = [pom for pom in poms if Path(pom).name == 'pom.xml']
    inventory = create_inventory(Path.cwd(), poms, os.environ['GITHUB_SHA'], os.environ['GITHUB_REPOSITORY'],
                                 os.environ['GITHUB_RUN_ID'], os.environ['GITHUB_RUN_ATTEMPT'])
    Path(sys.argv[1]).write_text(json.dumps(inventory, indent=2) + '\n')
