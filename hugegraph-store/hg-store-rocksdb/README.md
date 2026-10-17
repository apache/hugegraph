<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# RocksDB path and snapshot validation

`RocksDBSession.resolveExistingDBPath(dbDataPath, graphName, version)` uses the
same version-directory selection as the constructor, without opening, creating,
pruning or renaming databases. It returns `null` when the selected path has no
`CURRENT` and no existing database version is present. If an incomplete selected
version would hide another existing database, it fails without changing either
path. Directory listing or `CURRENT` inspection failures are rejected; unknown
access or I/O state is not treated as an absent database. Paths queued for removal retain the constructor's existing treatment.
The normal constructor continues to open read/write and prune old versions.

`RocksDBFactory.createGraphDB(path, name, version, Consumer<String> validator)`
validates existing DB versions before creating an uncached native session. It
uses the existing factory creation lock and cache: cached sessions skip validation.
Each existing candidate not queued for removal is passed to the validator through
the read-only resolver before constructor pruning or cache publication. Validation
failure leaves these DB paths intact. The original overloads keep their behavior;
this generic hook does not depend on Store graph-ID rules.

`loadSnapshot(snapshotPath, version, Consumer<String> validator)` first copies
and verifies the staging database with the session's configured column-family
options, then passes its path to the validator before
replacing the target version. The validator must inspect it read-only. A rejected
validation preserves active databases, target versions and staging evidence;
the exception cause is retained. The original two-argument overload remains
available and uses a no-op validator.

These APIs provide generic inspection hooks. They do not validate graph ownership,
migrate historical data, or replace deployment-specific recovery and upgrade checks.
