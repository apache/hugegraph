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

# Relation ordinal fixture provenance

`core-relation-ordinals.properties` contains raw `Kryo.writeClassAndObject` payloads without a BytesBuffer property-length prefix. The Kryo enum serializer bundled in `gremlin-shaded` 3.5.1 encodes minimal enum shells with these exact class names and declaration orders:

| Enum declaration | Source revision | Values |
|------------------|-----------------|--------|
| Core `org.apache.hugegraph.backend.query.Condition.RelationType` | [2f827d6e8c9c62ae858f2fc122b3a192d015e2f4](https://github.com/apache/hugegraph/blob/2f827d6e8c9c62ae858f2fc122b3a192d015e2f4/hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/backend/query/Condition.java) | 15 |
| Struct `org.apache.hugegraph.query.Condition.RelationType` | [9c52f40f9a8e5a6c25feb78669d57e45ba0f4ec8](https://github.com/apache/hugegraph/blob/9c52f40f9a8e5a6c25feb78669d57e45ba0f4ec8/hugegraph-struct/src/main/java/org/apache/hugegraph/query/Condition.java) | 27 |

Enum methods and predicate lambdas do not contribute to Kryo EnumSerializer bytes. These payloads were independently encoded from the pinned declarations, not produced by a full baseline core build. The legacy GTE bytes equal `relation_type.hex` in `core-kryo-objects.properties` after removing its `3d` property-length prefix, providing a cross-check against the original core writer.

## Regeneration

Work outside production sources, using the preserved `.java.txt` files:

1. Copy `LegacyRelationEnum.java.txt` to `org/apache/hugegraph/backend/query/Condition.java` and `CanonicalRelationEnum.java.txt` to `org/apache/hugegraph/query/Condition.java`.
2. Copy `RelationFixtureWriter.java.txt` to `RelationFixtureWriter.java`.
3. Compile all three against `gremlin-shaded-3.5.1.jar`, then run `RelationFixtureWriter` with that jar and the output classes on the classpath.
4. Preserve the ASF header when saving the printed properties, and compare the resulting payloads with the committed fixture before accepting a change.

## Invariants

Populated typed arrays, matrices and nested values exercise repeated class descriptors and nullable components. The interleaved sample combines legacy and canonical scalar/array descriptors and repeats arrays to check object identity. New writers retain canonical default ordinal encoding. This fixture tests enum encoding compatibility; it does not establish deployed use of enum OBJECT properties.

Fixture SHA-256: `93ee6acba21f94737cae3f929931fc09c879b930f47f0d43697a2b7f2654d1b4`.
