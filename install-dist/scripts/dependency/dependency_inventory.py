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

"""Inventory the union of Maven runtime dependencies and actually shipped jars.

Keep the historical filename exclusions for project artifacts. Boot repackaging
can inject jars or include provided dependencies absent from Maven's runtime
scope, so collect flat and nested distribution libraries as well. Comparison
remains strict: removals and additions both require updating the approved list.
"""

import argparse
import difflib
import io
import os
from pathlib import Path
import re
import sys
import tempfile
import zipfile


class InventoryFailure(Exception):
    pass


def third_party(name):
    # Preserve the former egrep -v '^hg|hugegraph|hubble' inventory convention.
    return not re.search(r'^hg|hugegraph|hubble', name)


def validate_name(name):
    if not re.fullmatch(r'[A-Za-z0-9_.+-]+\.jar', name):
        raise InventoryFailure(f'Invalid dependency jar filename: {name}')


def scan_shipped_jar(source, name, origin, result):
    validate_name(name)
    try:
        with zipfile.ZipFile(source) as archive:
            if third_party(name):
                result.add(name)
            for entry in archive.namelist():
                if entry.startswith(('BOOT-INF/lib/', 'WEB-INF/lib/')) and entry.endswith('.jar'):
                    scan_shipped_jar(io.BytesIO(archive.read(entry)), Path(entry).name,
                                     origin + '!/' + entry, result)
    except (OSError, zipfile.BadZipFile, RuntimeError) as error:
        raise InventoryFailure(f'Cannot inspect shipped jar {origin}: {error}') from error


def collect(runtime, root, revision):
    if not re.fullmatch(r'[A-Za-z0-9_.+-]+', revision):
        raise InventoryFailure(f'Invalid distribution revision: {revision}')
    if not runtime.is_dir():
        raise InventoryFailure(f'Missing Maven runtime dependency directory: {runtime}')
    files = sorted(runtime.iterdir())
    if not files:
        raise InventoryFailure(f'Empty Maven runtime dependency directory: {runtime}')
    result = set()
    for path in files:
        validate_name(path.name)
        try:
            # Runtime copy has already resolved each artifact; validate its jar
            # without treating nested project packaging as new Maven coordinates.
            with zipfile.ZipFile(path):
                pass
        except (OSError, zipfile.BadZipFile) as error:
            raise InventoryFailure(f'Cannot inspect resolved jar {path}: {error}') from error
        if third_party(path.name):
            result.add(path.name)
    for kind in ('server', 'pd', 'store'):
        distribution = root / ('hugegraph-' + kind) / f'apache-hugegraph-{kind}-{revision}'
        library = distribution / 'lib'
        jars = sorted(library.rglob('*.jar'))
        if not library.is_dir() or not jars:
            raise InventoryFailure(f'Missing packaged {kind} dependency jars: {library}')
        for directory in ('ext', 'plugins'):
            jars.extend(sorted((distribution / directory).rglob('*.jar')))
        for path in jars:
            scan_shipped_jar(path, path.name, str(path), result)
    if not result:
        raise InventoryFailure('No third-party dependencies found')
    return result


def read_inventory(path):
    values = []
    for line in path.read_text().splitlines():
        name = line.strip()
        if name:
            validate_name(name)
            values.append(name)
    if not values:
        raise InventoryFailure(f'Empty dependency inventory: {path}')
    if len(values) != len(set(values)):
        raise InventoryFailure(f'Duplicate dependency inventory entries: {path}')
    return sorted(values)


def compare(known, current):
    approved, observed = read_inventory(known), read_inventory(current)
    difference = ''.join(difflib.unified_diff([name + '\n' for name in approved],
                                            [name + '\n' for name in observed],
                                            fromfile=str(known), tofile=str(current), n=0))
    if difference:
        print(difference, end='')
        return 1
    print('All third-party dependencies match the approved runtime and shipped inventory.')
    return 0


def write_inventory(output, names):
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', dir=output.parent, delete=False) as handle:
            temporary = Path(handle.name)
            handle.write('\n'.join(sorted(names)) + '\n')
        os.replace(temporary, output)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    producer = commands.add_parser('collect')
    producer.add_argument('--runtime', type=Path, required=True)
    producer.add_argument('--root', type=Path, required=True)
    producer.add_argument('--revision', required=True)
    producer.add_argument('--output', type=Path, required=True)
    checker = commands.add_parser('check')
    checker.add_argument('--known', type=Path, required=True)
    checker.add_argument('--current', type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == 'check':
            return compare(args.known, args.current)
        names = collect(args.runtime, args.root, args.revision)
        write_inventory(args.output, names)
        print(f'Wrote {len(names)} runtime and shipped dependency filenames to {args.output}')
        return 0
    except (InventoryFailure, OSError, UnicodeError) as error:
        print(f'Dependency inventory FAILED: {error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
