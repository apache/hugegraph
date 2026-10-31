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

package org.apache.hugegraph.backend.store.rocksdb;

import java.io.File;

import org.apache.hugegraph.backend.store.AbstractBackendStoreProvider;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.util.ConfigUtil;
import org.apache.hugegraph.util.E;

public class RocksDBStoreProvider extends AbstractBackendStoreProvider {

    private static final String IMAGE_PROVIDER = "TOPLINGDB_ROCKSDB_PROVIDER";

    protected String database() {
        return this.graph().toLowerCase();
    }

    @Override
    public BackendStore loadSchemaStore(HugeConfig config) {
        applyImageProvider(config);
        return super.loadSchemaStore(config);
    }

    @Override
    public BackendStore loadGraphStore(HugeConfig config) {
        applyImageProvider(config);
        return super.loadGraphStore(config);
    }

    @Override
    public BackendStore loadSystemStore(HugeConfig config) {
        applyImageProvider(config);
        return super.loadSystemStore(config);
    }

    @Override
    protected BackendStore newSchemaStore(HugeConfig config, String store) {
        return new RocksDBStore.RocksDBSchemaStore(this, this.database(), store);
    }

    @Override
    protected BackendStore newGraphStore(HugeConfig config, String store) {
        return new RocksDBStore.RocksDBGraphStore(this, this.database(), store);
    }

    @Override
    public void onCloneConfig(HugeConfig config, String newGraph) {
        super.onCloneConfig(config, newGraph);

        // NOTE: rocksdb can't use same data path for different graph
        String suffix = "_" + newGraph;
        String dataPath = config.get(RocksDBOptions.DATA_PATH);
        config.setProperty(RocksDBOptions.DATA_PATH.name(), dataPath + suffix);

        String walPath = config.get(RocksDBOptions.WAL_PATH);
        config.setProperty(RocksDBOptions.WAL_PATH.name(), walPath + suffix);
    }

    @Override
    public void onDeleteConfig(HugeConfig config) {
        super.onDeleteConfig(config);

        String dataPath = config.get(RocksDBOptions.DATA_PATH);
        String walPath = config.get(RocksDBOptions.WAL_PATH);
        ConfigUtil.deleteFile(new File(dataPath));
        ConfigUtil.deleteFile(new File(walPath));
    }

    @Override
    protected BackendStore newSystemStore(HugeConfig config, String store) {
        return new RocksDBStore.RocksDBSystemStore(this, this.database(), store);
    }

    private static void applyImageProvider(HugeConfig config) {
        applyImageProvider(config, System.getenv(IMAGE_PROVIDER));
    }

    private static void applyImageProvider(HugeConfig config, String provider) {
        if (config == null || config.containsKey(RocksDBOptions.PROVIDER.name()) ||
            provider == null || provider.isBlank()) {
            return;
        }
        E.checkArgument("rocksdb".equals(provider) || "topling".equals(provider),
                        "Unsupported %s value '%s'", IMAGE_PROVIDER, provider);
        // Keep image selection above persisted graph/PD configuration so an
        // image rollback can switch the runtime without rewriting metadata.
        config.setProperty(RocksDBOptions.PROVIDER.name(), provider);
    }

    @Override
    public String type() {
        return "rocksdb";
    }

    @Override
    public String driverVersion() {
        /*
         * Versions history:
         * [1.0] HugeGraph-1328: supports backend table version checking
         * [1.1] HugeGraph-1322: add support for full-text search
         * [1.2] #296: support range sortKey feature
         * [1.3] #270 & #398: support shard-index and vertex + sortkey prefix,
         *                    also split range table to rangeInt, rangeFloat,
         *                    rangeLong and rangeDouble
         * [1.4] #633: support unique index
         * [1.5] #680: update index element-id to bin format
         * [1.6] #746: support userdata for indexlabel
         * [1.7] #820: store vertex properties in one column
         * [1.8] #894: encode label id in string index
         * [1.9] #295: support ttl for vertex and edge
         * [1.10] #1333: support read frequency for property key
         * [1.11] #1506: support olap properties
         * [1.11] #1533: add meta table in system store
         */
        return "1.11";
    }
}
