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

# Historical fixtures for Store ingress tests

`StoredRowIngressTest` loads `core-rows.properties` for complete legacy Server rows (properties, TTL, OLAP and indexes), and `store-index-rows.properties` for original Store index-expiry envelopes.

These byte-identical copies allow tests to load fixtures from this module's own resource classpath. Keep them synchronized with the shared resources. The [canonical fixture guide](../../../../../../../hugegraph-struct/src/test/resources/compatibility/2f827d6e8/README.md) contains the pinned source revision, historical producers, regeneration instructions, compatibility limits and SHA-256 checksums. Regenerate against those historical writers only, never the current writer.
