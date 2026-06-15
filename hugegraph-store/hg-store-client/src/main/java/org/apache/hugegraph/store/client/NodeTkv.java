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

package org.apache.hugegraph.store.client;

import java.util.Objects;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.HgStoreSession;

/**
 * created on 2021/10/26
 */
@ThreadSafe
class NodeTkv {

    private final HgNodePartition nodePartition;
    private final String table;
    private final HgOwnerKey key;
    private final HgOwnerKey endKey;
    private HgStoreSession session;

    NodeTkv(HgNodePartition nodePartition, String table, HgOwnerKey key) {
        this.nodePartition = nodePartition;
        this.table = table;
        this.key = key;
        this.endKey = key;
        this.key.setKeyCode(this.nodePartition.getKeyCode());
    }

    NodeTkv(HgNodePartition nodePartition, String table, HgOwnerKey key, int keyCode) {
        this.nodePartition = nodePartition;
        this.table = table;
        this.key = key;
        this.endKey = key;

        this.key.setKeyCode(keyCode);
    }

    NodeTkv(HgNodePartition nodePartition, String table, HgOwnerKey startKey,
            HgOwnerKey endKey) {
        this.nodePartition = nodePartition;
        this.table = table;
        this.key = startKey;
        this.endKey = endKey;
        this.key.setKeyCode(nodePartition.getStartKey());
        this.endKey.setKeyCode(nodePartition.getEndKey());
    }

    public Long getNodeId() {
        return this.nodePartition.getNodeId();
    }

    public String getTable() {
        return table;
    }

    public HgOwnerKey getKey() {
        return key;
    }

    public HgOwnerKey getEndKey() {
        return endKey;
    }

    public NodeTkv setKeyCode(int code) {
        this.key.setKeyCode(code);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NodeTkv nptKv = (NodeTkv) o;
        return Objects.equals(nodePartition, nptKv.nodePartition) &&
               Objects.equals(table, nptKv.table)
               && Objects.equals(key, nptKv.key)
               && Objects.equals(endKey, nptKv.endKey);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(nodePartition, table, key, endKey);
        return result;
    }

    @Override
    public String toString() {
        return "NptKv{" +
               "nodePartition=" + nodePartition +
               ", table='" + table + '\'' +
               ", key=" + key +
               ", endKey=" + endKey +
               '}';
    }

    public HgStoreSession getSession() {
        return session;
    }

    public void setSession(HgStoreSession session) {
        this.session = session;
    }
}
