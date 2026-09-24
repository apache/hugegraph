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

package org.apache.hugegraph.store.util;

import java.util.List;

import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.meta.base.MetaStoreBase;

public class PartitionMetaStoreWrapper {

    private static final InnerMetaStore store = new InnerMetaStore();

    public void put(int partitionId, byte[] key, byte[] value) {
        store.setPartitionId(partitionId);
        store.put(key, value);
    }

    public <T> T get(int partitionId, byte[] key, com.google.protobuf.Parser<T> parser) {
        store.setPartitionId(partitionId);
        return store.get(parser, key);
    }

    public byte[] get(int partitionId, byte[] key) {
        store.setPartitionId(partitionId);
        return store.get(key);
    }

    public void delete(int partitionId, byte[] key) {
        store.setPartitionId(partitionId);
        store.delete(key);
    }

    public <T> List<T> scan(int partitionId, com.google.protobuf.Parser<T> parser, byte[] prefix) {
        store.setPartitionId(partitionId);
        return store.scan(parser, prefix);
    }

    public void close(int partitionId) {
        HgStoreEngine.getInstance().getBusinessHandler().getSession(partitionId).close();
    }

    private static class InnerMetaStore extends MetaStoreBase {

        private int partitionId;

        private void setPartitionId(int partitionId) {
            this.partitionId = partitionId;
        }

        @Override
        protected RocksDBSession getRocksDBSession() {
            return HgStoreEngine.getInstance().getBusinessHandler().getSession(this.partitionId);
        }

        @Override
        protected String getCFName() {
            return "default";
        }
    }
}
