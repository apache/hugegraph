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

package org.apache.hugegraph.pd.meta;

import java.util.List;

import org.apache.hugegraph.pd.common.HgAssert;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.store.RaftKVStore;

public class QueueStore extends MetadataRocksDBStore {

    QueueStore(PDConfig pdConfig) {
        super(pdConfig);
    }

    public void addItem(Metapb.QueueItem queueItem) throws PDException {
        HgAssert.isArgumentNotNull(queueItem, "queueItem");
        byte[] key = MetadataKeyHelper.getQueueItemKey(queueItem.getItemId());
        put(key, queueItem.toByteString().toByteArray());
    }

    public void removeItem(String itemId) throws PDException {
        if (RaftEngine.getInstance().isLeader()) {
            remove(MetadataKeyHelper.getQueueItemKey(itemId));
        } else {
            var store = getStore();
            // todo: delete record via client
            if (store instanceof RaftKVStore raftStore) {
                raftStore.doRemove(MetadataKeyHelper.getQueueItemKey(itemId));
            }
        }
    }

    public List<Metapb.QueueItem> getQueue() throws PDException {
        byte[] prefix = MetadataKeyHelper.getQueueItemPrefix();
        return scanPrefix(Metapb.QueueItem.parser(), prefix);
    }
}
