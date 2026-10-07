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

package org.apache.hugegraph.backend.store.rocksdbsst;

import java.util.List;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBSessions;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBStore;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBTables;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.type.HugeType;
import org.rocksdb.RocksDBException;

public abstract class RocksDBSstStore extends RocksDBStore {

    public RocksDBSstStore(final BackendStoreProvider provider,
                           final String database, final String store) {
        super(provider, database, store);
    }

    @Override
    protected RocksDBSessions openSessionPool(HugeConfig config,
                                              String dataPath, String walPath,
                                              List<String> tableNames)
            throws RocksDBException {
        if (tableNames == null) {
            return new RocksDBSstSessions(config, this.database(), this.store(), dataPath);
        } else {
            return new RocksDBSstSessions(config, this.database(), this.store(),
                                          dataPath, tableNames);
        }
    }

    /***************************** Store defines *****************************/

    public static class RocksDBSstGraphStore extends RocksDBSstStore {

        public RocksDBSstGraphStore(BackendStoreProvider provider,
                                    String database, String store) {
            super(provider, database, store);

            registerTableManager(HugeType.VERTEX, new RocksDBTables.Vertex(database));

            registerTableManager(HugeType.EDGE_OUT, RocksDBTables.Edge.out(database));
            registerTableManager(HugeType.EDGE_IN, RocksDBTables.Edge.in(database));

            registerTableManager(HugeType.SECONDARY_INDEX,
                                 new RocksDBTables.SecondaryIndex(database));
            registerTableManager(HugeType.VERTEX_LABEL_INDEX,
                                 new RocksDBTables.VertexLabelIndex(database));
            registerTableManager(HugeType.EDGE_LABEL_INDEX,
                                 new RocksDBTables.EdgeLabelIndex(database));
            registerTableManager(HugeType.RANGE_INT_INDEX,
                                 new RocksDBTables.RangeIntIndex(database));
            registerTableManager(HugeType.RANGE_FLOAT_INDEX,
                                 new RocksDBTables.RangeFloatIndex(database));
            registerTableManager(HugeType.RANGE_LONG_INDEX,
                                 new RocksDBTables.RangeLongIndex(database));
            registerTableManager(HugeType.RANGE_DOUBLE_INDEX,
                                 new RocksDBTables.RangeDoubleIndex(database));
            registerTableManager(HugeType.SEARCH_INDEX, new RocksDBTables.SearchIndex(database));
            registerTableManager(HugeType.SHARD_INDEX, new RocksDBTables.ShardIndex(database));
            registerTableManager(HugeType.UNIQUE_INDEX, new RocksDBTables.UniqueIndex(database));
        }

        @Override
        public boolean isSchemaStore() {
            return false;
        }

        @Override
        public Id nextId(HugeType type) {
            throw new UnsupportedOperationException("RocksDBSstGraphStore.nextId()");
        }

        @Override
        public void increaseCounter(HugeType type, long increment) {
            throw new UnsupportedOperationException("RocksDBSstGraphStore.increaseCounter()");
        }

        @Override
        public long getCounter(HugeType type) {
            throw new UnsupportedOperationException("RocksDBSstGraphStore.getCounter()");
        }
    }
}
