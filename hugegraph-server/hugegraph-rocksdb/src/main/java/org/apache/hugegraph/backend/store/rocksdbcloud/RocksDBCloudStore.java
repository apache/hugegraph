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

package org.apache.hugegraph.backend.store.rocksdbcloud;

import java.util.List;

import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBSessions;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBStore;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.util.Log;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;

/**
 * RocksDB store that persists SST files to Amazon S3 (or S3-compatible storage)
 * via {@link RocksDBCloudSessions}.
 *
 * <p>The only behavioural difference vs the standard {@link RocksDBStore} is the
 * session-pool construction: {@link #openSessionPool} returns a
 * {@link RocksDBCloudSessions} that uses the AWS SDK v2 S3 client for snapshot
 * upload/download, and can be extended to use RocksDB's cloud env when a
 * rocksdb-cloud native library is available.
 */
public abstract class RocksDBCloudStore extends RocksDBStore {

    private static final Logger LOG = Log.logger(RocksDBCloudStore.class);

    public RocksDBCloudStore(final BackendStoreProvider provider,
                             final String database,
                             final String store) {
        super(provider, database, store);
        LOG.info("RocksDBCloudStore created for '{}/{}'", database, store);
    }

    // -------------------------------------------------------------------------
    // Override session-pool factory — uses S3-backed cloud sessions
    // -------------------------------------------------------------------------

    @Override
    protected RocksDBSessions openSessionPool(HugeConfig config,
                                              String dataPath,
                                              String walPath,
                                              List<String> tableNames)
            throws RocksDBException {
        if (tableNames == null) {
            return new RocksDBCloudSessions(config, this.database(), this.store(),
                                            dataPath, walPath);
        } else {
            return new RocksDBCloudSessions(config, this.database(), this.store(),
                                            dataPath, walPath, tableNames);
        }
    }

    // -------------------------------------------------------------------------
    // Concrete inner stores — delegate to parent inner-store hierarchy
    // but use this class's overridden openSessionPool
    // -------------------------------------------------------------------------

    public static class RocksDBCloudSchemaStore extends RocksDBStore.RocksDBSchemaStore {

        public RocksDBCloudSchemaStore(BackendStoreProvider provider,
                                       String database, String store) {
            super(provider, database, store);
        }

        @Override
        protected RocksDBSessions openSessionPool(HugeConfig config,
                                                  String dataPath,
                                                  String walPath,
                                                  List<String> tableNames)
                throws RocksDBException {
            if (tableNames == null) {
                return new RocksDBCloudSessions(config, this.database(), this.store(),
                                                dataPath, walPath);
            } else {
                return new RocksDBCloudSessions(config, this.database(), this.store(),
                                                dataPath, walPath, tableNames);
            }
        }
    }

    public static class RocksDBCloudGraphStore extends RocksDBStore.RocksDBGraphStore {

        public RocksDBCloudGraphStore(BackendStoreProvider provider,
                                      String database, String store) {
            super(provider, database, store);
        }

        @Override
        protected RocksDBSessions openSessionPool(HugeConfig config,
                                                  String dataPath,
                                                  String walPath,
                                                  List<String> tableNames)
                throws RocksDBException {
            if (tableNames == null) {
                return new RocksDBCloudSessions(config, this.database(), this.store(),
                                                dataPath, walPath);
            } else {
                return new RocksDBCloudSessions(config, this.database(), this.store(),
                                                dataPath, walPath, tableNames);
            }
        }
    }

    public static class RocksDBCloudSystemStore extends RocksDBStore.RocksDBSystemStore {

        public RocksDBCloudSystemStore(BackendStoreProvider provider,
                                       String database, String store) {
            super(provider, database, store);
        }

        @Override
        protected RocksDBSessions openSessionPool(HugeConfig config,
                                                  String dataPath,
                                                  String walPath,
                                                  List<String> tableNames)
                throws RocksDBException {
            if (tableNames == null) {
                return new RocksDBCloudSessions(config, this.database(), this.store(),
                                                dataPath, walPath);
            } else {
                return new RocksDBCloudSessions(config, this.database(), this.store(),
                                                dataPath, walPath, tableNames);
            }
        }
    }
}

