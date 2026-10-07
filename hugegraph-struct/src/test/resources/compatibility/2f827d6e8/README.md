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

# Historical codec fixtures

These files preserve historical payloads and the producer evidence needed to reproduce them. The `2f827d6e8` directory identifies the pre-consolidation source revision: [apache/hugegraph `2f827d6e8c9c62ae858f2fc122b3a192d015e2f4`](https://github.com/apache/hugegraph/commit/2f827d6e8c9c62ae858f2fc122b3a192d015e2f4). Core and Store/struct fixtures use their respective original implementations at that revision. Tests load the historical payloads described below without regenerating them with the current writer. The index-builder contract file records producer output; `SharedIndexTest` uses its captured Server hash as an assertion rather than loading that file.

## Files and purpose

| Fixture | Original producer | Compatibility contract |
|---------|-------------------|------------------------|
| `core-codecs.hex`, four `core-*.json.txt` files | `LegacyCodecFixtureWriter.java.txt`, baseline core | Scalar/ID/index bytes and `ConditionQuery.bytes()` JSON |
| `struct-tagged-properties.hex` | `LegacyStructFixtureWriter.java.txt`, baseline struct | Tagged scalar/LIST/SET properties and metadata recovery |
| `core-rows.properties` | `LegacyRowFixtureWriter.java.txt`, baseline core | Complete `BinarySerializer` rows, including property framing, TTL, OLAP and indexes |
| `store-index-rows.properties` | `LegacyStoreIndexFixtureWriter.java.txt`, baseline struct | Original `BinaryElementSerializer.writeIndex` keys and expiry values |
| `core-kryo-objects.properties` | `LegacyKryoFixtureWriter.java.txt`, baseline core | Relocated class descriptors and the original reader's supported/unsupported cases |
| `index-builder-contract.properties` | `LegacyStoreIndexBuilderProbe.java.txt` and `LegacyServerIndexQueryProbe.java.txt` | Existing Store long-text index/Server query mismatch |
| `core-relation-ordinals.properties` | `RelationFixtureWriter.java.txt` and pinned enum declarations | Legacy/canonical enum ordinals; see [provenance](relation-ordinals-provenance.md) |

The `.java.txt` producers preserve reproducibility while keeping legacy imports outside test compilation. Store ingress and Server serializer tests carry copies of the relevant payloads because each module loads its own test resources. Keep those copies byte-identical when intentionally updating a fixture.

`core-codecs.hex` contains 65 named `key=hex` payloads, with properties-style line continuations for large IDs. The JSON files wrap exact UTF-8 query output in an ASF comment prolog and final newline; tests remove those container boundaries. The tagged-property file contains 12 payloads. Source/fixture license headers must be retained.

## Reproduce with the historical writers

Use Java 11, UTF-8 and UTC in an isolated checkout of the pinned revision:

```sh
git clone https://github.com/apache/hugegraph.git legacy-core
cd legacy-core
git checkout --detach 2f827d6e8c9c62ae858f2fc122b3a192d015e2f4
mvn install -pl hugegraph-server/hugegraph-core -am -DskipTests
mvn dependency:build-classpath -pl hugegraph-server/hugegraph-core \
    -Dmdep.outputFile="$PWD/core-classpath.txt"
mvn install -pl hugegraph-struct -am -DskipTests
mvn dependency:build-classpath -pl hugegraph-struct \
    -Dmdep.outputFile="$PWD/struct-classpath.txt"
```

Copy each required `.java.txt` producer to a separate working directory as a `.java` file. Compile with `javac -proc:none -encoding UTF-8` against the relevant baseline module jar and its Maven runtime classpath. Put that baseline jar first; exclude the current shared implementations and other HugeGraph versions. Use separate core and struct classpaths, with no core jar for struct producers. Keep the pinned IndexBuilder's analyzer dependencies even for secondary-index probes.

Run with `java -Dfile.encoding=UTF-8 -Duser.timezone=UTC` and these arguments:

| Producer class | Arguments |
|----------------|-----------|
| `LegacyCodecFixtureWriter` | `<output-directory>` |
| `LegacyStructFixtureWriter` | `<output-directory> <producer-source-file>` |
| `LegacyRowFixtureWriter` | `<output-directory> <producer-source-file>` |
| `LegacyStoreIndexFixtureWriter` | `<output-directory> <producer-source-file>` |
| `LegacyKryoFixtureWriter` | `<output-directory> <producer-source-file>` |
| `LegacyStoreIndexBuilderProbe` | `<output-directory> <producer-source-file>` |
| `LegacyServerIndexQueryProbe` | `<output-directory>`; run after the Store probe, in a separate core-classpath process, to append the Server prefix |

Compare output payloads with the committed resources before accepting changes. The source revision, producer source and payload bytes are the provenance; rebuilt jar hashes can differ because of Maven archive timestamps. Do not replace historical inputs with output from a new writer. Enum-shell reproduction has its own instructions in [relation ordinal provenance](relation-ordinals-provenance.md).

## Scalar and query coverage

The scalar fixtures cover signed varint/varlong limits and width boundaries; long, Unicode string and UUID IDs, string-header width changes and the 16 KiB limit; incoming/outgoing edge IDs with parent/sublabel; every scalar property type, including the shaded Kryo fallback; schema-driven SINGLE/LIST/SET values; and string plus 4/8-byte range index IDs. The range-index input is an explicit type/label/value layout, not a full index-builder output.

Query fixtures cover vertex labels, numeric ranges, typed IN lists, dates, OR, limit/offset, edge ownership/direction/page tokens and executable COUNT(*)/AVG(age) strategies with their JSON aggregate shape. Legacy JSON dates encode local wall time without a zone: `2021-01-01 00:00:00.123` represents epoch `1609459200123` in UTC and `1609430400123` in Asia/Singapore. Tests calculate expected local time without changing the JVM default zone after adapters initialize.

Core schema-driven properties are untagged; Store/struct properties carry a cardinality/type tag. Both formats share scalar suffixes but remain separate wire entry points. Tests compare their suffixes against independently produced core bytes; they must not read untagged core values as tagged Store properties.

## Complete row and TTL contracts

`core-rows.properties` contains 15 complete rows produced by the real baseline `HugeVertex`, `HugeEdge`, `HugeIndex`, properties and `BinarySerializer`. A proxy supplies schema lookup and a fixed clock and rejects unexpected graph calls. The producer validates rows with the original reader; its OLAP reader subclass only exposes `parseVertexOlap`, without overriding encoding. Row IDs, names and values are hex; `type`, `ttl` and `columns` retain `BackendEntry` metadata.

| Schema | Definition |
|--------|------------|
| Properties 1/2/3 | `age` INT SINGLE = 7; `scores` INT LIST = `[7, -1, 7]`; `groups` INT SET = `{-1, 7}` |
| Property 9 | `rank` INT SINGLE OLAP_COMMON = 42 |
| Properties 16/17 | `weight` LONG SINGLE (range-long); `name` TEXT SINGLE (secondary) |
| Vertex labels 4/10 | `person` TTL 60000 / `timeless` without TTL; both use properties 1/2/3 |
| Edge labels 5/11 | `knows` TTL 60000 / `untimed` without TTL; both link 4→4 and use properties 1/2/3 |
| Index labels 6/7/8 | Secondary/range-int/range-long on vertex label 4 |
| Index labels 12/13/14 | Secondary/range-int/range-long on vertex label 10 |
| Index label 15 | Secondary on vertex label 4, hashed long-string key |

The clock is `1609459200000`; timed rows expire at `1609459260000`. Vertex IDs are 1 (timed) and 3 (untimed). Edges connect 1 and 2 with sort value `2026!x`, or an empty sort value for the untimed edge. Index values are `alice`, integer 7, long 7, or `"long-field-".repeat(20)`. Default serializer flags enable key/index ID prefixes and disable partition prefixes.

The OLAP column name is the vertex ID; its value is the property ID plus an untagged scalar. Merged-row tests supply an existing vertex, matching the original reader's main-row-then-OLAP-columns convention.

Core system vertex/edge label indexes receive positive expiry but retain stable keys and empty values without expiry suffixes. Their fields are vertex label 4 and edge label 5 (with the full edge ID). Store's original user secondary/range and system-label indexes use the same expiration above, with no key expiry suffix and a 13-byte value envelope: `0x00 + Base64(big-endian long)`.

Shared schema readers must read those Store rows while ordinary schema writers retain the Server key/hash/TTL layout. Tagged writers retain their Store format. Label TTL regression checks cover recovered expiry, `expiredElementIds`, clone/reset isolation, elimination, append and re-read: the exact original key remains stable, core's primitive writer emits its original empty value and the storage writer retains its expiry envelope.

`property.OBJECT_UNSUPPORTED_COLLECTION` is a skip-only fixture: skipping must retain the trailing marker and TTL without instantiating Kryo data. It does not assert that the collection is readable. These row fixtures do not cover every partition layout, storage transaction or RocksDB/HStore runtime path.

## Relocated Kryo descriptors

`core-kryo-objects.properties` contains 36 unmodified original OBJECT payloads. Its `baseline_readable` fields record the original default reader's behavior. Twenty-four were readable: query/ID enums, Userdata with string/int/date entries, Class literals, a `BackendColumn` and populated array, null-element moved-type arrays/matrices, and nested mutable maps/lists. These must decode to the canonical runtime types and values. New writes may use canonical names; semantic roundtrips do not require retaining deprecated Java API names.

Twelve written payloads were unreadable because the original reader required missing no-arg constructors: LongId/StringId/UuidId/EdgeId/BinaryId/Shard, four schema instances, IdWithExpiredTime and Arrays$ArrayList. Preserve class resolution without a ClassNotFound regression; these fixtures do not change the general instantiation policy or promise newly supported model instances. The readable OBJECT collection is a mutable `ArrayList` rather than `Arrays.asList`.

Schema/index descriptor fixtures include TaskWithSchema, five P classes, four Builder interfaces, HugeIndex/IdWithExpiredTime Class literals and typed arrays. Empty/null-element arrays and Class literals remain supported even for types whose populated instances are unreadable. No readable live-graph HugeIndex OBJECT instance is claimed. Resolver tests separately accept the JVM 255-dimension boundary, reject 256/5000 dimensions before allocation, and delegate unknown and primitive descriptors to the normal resolver.

## Existing IndexBuilder/query mismatch

For TEXT property 17, index label 15, vertex label 4 and `"long-field-".repeat(20)`, the original Store `buildIndex` truncates the 220-character value to `long-field-long-fiel` (20 characters). Its key starts with that literal. The original Server `BinarySerializer.writeQuery`, using `ConditionQuery.concatValues` on the full value, emits hashed prefix `53493a463a323234303534613200`. The Store key does not match that query.

The fixture records both executed paths, rather than combining guessed helper outputs. Writer/query alignment tests use the original Server prefix while retaining old Store row decoding. Installations using old Store rebuilding may need to rebuild affected indexes; decoding alone cannot repair the mismatch.

## Payload integrity

These SHA-256 values identify committed fixture bytes, independent of rebuilt jar timestamps:

| File | SHA-256 |
|------|---------|
| `core-rows.properties` | `d90cfe0b349edb832bf7425b6b8e2ddb06b3eff3f29e834cf8363b0d2fb84662` |
| `store-index-rows.properties` | `e9e9c967884d0d91b6cf6d5eabf2050a9bad0fe7898d1c47b58535ebdf1b9431` |
| `core-kryo-objects.properties` | `45c10201a1f88b38771fd5bd4e609f461729ac1d5e8fb94304fa176bfd5cf369` |
| `index-builder-contract.properties` | `3be36ba02c4afd58aab383549b041368fab4b9e8e32474122457b5608970161b` |

Scalar/query and complete-row tests establish these codec contracts, not full storage transactions or query-runtime compatibility. Those require integration and storage tests.
