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

package org.apache.hugegraph.backend.store.memory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hugegraph.backend.store.AbstractBackendStoreProvider;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.store.memory.InMemoryDBStore.InMemoryGraphStore;
import org.apache.hugegraph.backend.store.memory.InMemoryDBStore.InMemorySchemaStore;
import org.apache.hugegraph.backend.store.memory.InMemoryDBStore.InMemorySystemStore;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.util.Events;

public class InMemoryDBStoreProvider extends AbstractBackendStoreProvider {

    public static final String TYPE = "memory";

    private static Map<String, InMemoryDBStoreProvider> providers = null;

    public static boolean matchType(String type) {
        return TYPE.equalsIgnoreCase(type);
    }

    public static synchronized InMemoryDBStoreProvider instance(String graph) {
        if (providers == null) {
            providers = new ConcurrentHashMap<>();
        }
        if (!providers.containsKey(graph)) {
            InMemoryDBStoreProvider p = new InMemoryDBStoreProvider(graph);
            providers.putIfAbsent(graph, p);
        }
        return providers.get(graph);
    }

    private InMemoryDBStoreProvider(String graph) {
        this.open(graph);
    }

    @Override
    public void open(String graph) {
        super.open(graph);
        /*
         * Memory store need to init some system property,
         * like task related property-keys and vertex-labels.
         * don't notify from store.open() due to task-tx will
         * call it again and cause dead
         */
        this.notifyAndWaitEvent(Events.STORE_INIT);
    }

    @Override
    protected BackendStore newSchemaStore(HugeConfig config, String store) {
        return new InMemorySchemaStore(this, this.graph(), store);
    }

    @Override
    protected BackendStore newGraphStore(HugeConfig config, String store) {
        return new InMemoryGraphStore(this, this.graph(), store);
    }

    @Override
    protected BackendStore newSystemStore(HugeConfig config, String store) {
        return new InMemorySystemStore(this, this.graph(), store);
    }

    @Override
    public String type() {
        return TYPE;
    }

    /** The in-memory tables hold TextBackendEntry rows. */
    @Override
    public String serializerName() {
        return "text";
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
         * [1.4] #746: support userdata for indexlabel
         * [1.5] #820: store vertex properties in one column
         * [1.6] #894: encode label id in string index
         * [1.7] #1333: support read frequency for property key
         * [1.8] #1533: add meta table in system store
         */
        return "1.8";
    }
}
