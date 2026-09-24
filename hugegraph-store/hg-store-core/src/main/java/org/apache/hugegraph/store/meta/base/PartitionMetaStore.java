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

package org.apache.hugegraph.store.meta.base;

import org.apache.hugegraph.rocksdb.access.RocksDBSession;

/**
 * Metadata is stored in the default cf of the partition.
 */
public class PartitionMetaStore extends MetaStoreBase {

    public static final String DEFAULT_CF_NAME = "default";

    private final DBSessionBuilder sessionBuilder;
    private final Integer partitionId;

    public PartitionMetaStore(DBSessionBuilder sessionBuilder, int partId) {
        this.sessionBuilder = sessionBuilder;
        this.partitionId = partId;
    }

    @Override
    protected RocksDBSession getRocksDBSession() {
        return sessionBuilder.getSession(this.partitionId);
    }

    @Override
    protected String getCFName() {
        return DEFAULT_CF_NAME;
    }

    public void flush() {
        try (RocksDBSession dbSession = getRocksDBSession()) {
            dbSession.flush(true);
        }
    }
}
