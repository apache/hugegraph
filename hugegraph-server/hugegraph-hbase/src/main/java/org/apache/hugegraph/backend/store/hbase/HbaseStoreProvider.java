/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.backend.store.hbase;

import org.apache.hugegraph.backend.store.AbstractBackendStoreProvider;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.config.HugeConfig;

public class HbaseStoreProvider extends AbstractBackendStoreProvider {

    protected String namespace() {
        // HBase namespace names can only contain alphanumeric characters and underscores
        // Replace '/' with '_' to make it compatible with HBase naming rules
        return this.graph().toLowerCase().replace('/', '_');
    }

    @Override
    protected BackendStore newSchemaStore(HugeConfig config, String store) {
        return new HbaseStore.HbaseSchemaStore(config, this, this.namespace(), store);
    }

    @Override
    protected BackendStore newGraphStore(HugeConfig config, String store) {
        return new HbaseStore.HbaseGraphStore(config, this, this.namespace(), store);
    }

    @Override
    protected BackendStore newSystemStore(HugeConfig config, String store) {
        return new HbaseStore.HbaseSystemStore(config, this, this.namespace(), store);
    }

    @Override
    public String type() {
        return "hbase";
    }

    @Override
    public String driverVersion() {
        /*
         * Versions history:
         * [1.0] HugeGraph-1328: supports backend table version checking
         * [1.1] HugeGraph-1322: add support for full-text search
         * [1.2] #296: support range sortKey feature
         * [1.3] #287: support pagination when doing index query
         * [1.4] #270 & #398: support shard-index and vertex + sortkey prefix,
         *                    also split range table to rangeInt, rangeFloat,
         *                    rangeLong and rangeDouble
         * [1.5] #633: support unique index
         * [1.6] #680: update index element-id to bin format
         * [1.7] #746: support userdata for indexlabel
         * [1.8] #820: store vertex properties in one column
         * [1.9] #894: encode label id in string index
         * [1.10] #295: support ttl for vertex and edge
         * [1.11] #1333: support read frequency for property key
         * [1.12] #1533: add meta table in system store
         */
        return "1.12";
    }
}
