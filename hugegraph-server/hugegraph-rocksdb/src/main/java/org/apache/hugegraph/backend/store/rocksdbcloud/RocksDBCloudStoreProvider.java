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

import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBStoreProvider;
import org.apache.hugegraph.config.HugeConfig;

/**
 * Backend store provider for the {@code rocksdb-cloud} backend.
 *
 * <p>Register this type in {@code hugegraph.properties} with:
 * <pre>
 *   backend=rocksdb-cloud
 *   serializer=binary
 *
 *   rocksdb.data_path=rocksdb-cloud-data/data
 *   rocksdb.wal_path=rocksdb-cloud-data/wal
 *
 *   rocksdb.cloud.s3_bucket_name=my-hugegraph-bucket
 *   rocksdb.cloud.s3_region=us-east-1
 *   rocksdb.cloud.s3_object_prefix=hugegraph/
 * </pre>
 */
public class RocksDBCloudStoreProvider extends RocksDBStoreProvider {

    @Override
    protected BackendStore newSchemaStore(HugeConfig config, String store) {
        return new RocksDBCloudStore.RocksDBCloudSchemaStore(this, this.database(), store);
    }

    @Override
    protected BackendStore newGraphStore(HugeConfig config, String store) {
        return new RocksDBCloudStore.RocksDBCloudGraphStore(this, this.database(), store);
    }

    @Override
    protected BackendStore newSystemStore(HugeConfig config, String store) {
        return new RocksDBCloudStore.RocksDBCloudSystemStore(this, this.database(), store);
    }

    @Override
    public String type() {
        return "rocksdb-cloud";
    }

    @Override
    public String driverVersion() {
        /*
         * Versions history:
         * [1.0] Initial RocksDB Cloud backend (S3-backed SST storage)
         *       Compatible with rocksdb backend driver version 1.11
         */
        return "1.0";
    }
}

