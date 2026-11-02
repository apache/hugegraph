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

"""Copy ordinary-label schema to an empty graph, preserving IDs for data copy."""
import argparse
import base64
import copy
import json
import os
import urllib.request

KINDS = ('propertykeys', 'vertexlabels', 'edgelabels', 'indexlabels')


def client(base, role):
    user = os.environ.get('HG_' + role + '_USERNAME', 'admin')
    password = os.environ['HG_' + role + '_PASSWORD']
    token = base64.b64encode((user + ':' + password).encode()).decode()

    def request(method, suffix, body=None):
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(base.rstrip('/') + suffix, data=data,
                                     method=method, headers={
                                         'Authorization': 'Basic ' + token,
                                         'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=60) as response:
            raw = response.read().decode()
            return json.loads(raw) if raw else None
    return request


def normalize(schema):
    result = copy.deepcopy(schema)
    for kind in KINDS:
        for element in result[kind]:
            element.pop('status', None)
            element.get('user_data', {}).pop('~create_time', None)
            for key in ('properties', 'nullable_keys', 'index_labels'):
                if key in element:
                    element[key].sort()
        result[kind].sort(key=lambda element: element['name'])
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True, help='Source graph URL, including graph name')
    parser.add_argument('--target', required=True, help='Target graph URL, including graph name')
    args = parser.parse_args()
    source = client(args.source, 'SOURCE')
    schema = source('GET', '/schema')
    for label in schema['edgelabels']:
        label_type = label.get('edgelabel_type', 'NORMAL')
        if label_type != 'NORMAL':
            raise SystemExit(
                f"Unsupported edge label type {label_type!r} for {label['name']!r}: "
                'this example only supports NORMAL edge labels; '
                'inherited PARENT/SUB edge labels are not supported.')
    target = client(args.target, 'TARGET')
    target_schema = target('GET', '/schema')
    if any(target_schema[kind] for kind in KINDS):
        raise SystemExit('Target schema must be empty; use a new graph.')
    original_mode = target('GET', '/mode')['mode']
    if str(original_mode).upper() != 'NONE':
        raise SystemExit('Target graph must be in NONE mode before copying schema.')
    target('PUT', '/mode', 'RESTORING')
    try:
        for kind in KINDS:
            for element in schema[kind]:
                body = copy.deepcopy(element)
                body.pop('status', None)
                body.pop('index_labels', None)
                if kind == 'indexlabels':
                    body['rebuild'] = False  # Empty graph; subsequent data writes build indexes.
                target('POST', '/schema/' + kind, body)
        if normalize(schema) != normalize(target('GET', '/schema')):
            raise RuntimeError('Schema comparison failed')
    except Exception as error:
        raise SystemExit(
            'Schema copy failed; the target remains in RESTORING mode. '
            'Inspect it or delete and recreate the graph before copying data: '
            f'{error}') from error
    try:
        target('PUT', '/mode', original_mode)
    except Exception as error:
        raise SystemExit(
            'Schema copy verified, but restoring the target mode failed; '
            'the target remains in RESTORING mode: '
            f'{error}') from error
    print(json.dumps({'schema_equal': True, 'ids_preserved': True,
                      'counts': {kind: len(schema[kind]) for kind in KINDS}}))


if __name__ == '__main__':
    main()
