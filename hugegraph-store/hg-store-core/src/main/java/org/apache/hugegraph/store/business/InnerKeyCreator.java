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

package org.apache.hugegraph.store.business;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hugegraph.store.meta.GraphIdManager;
import org.apache.hugegraph.store.term.Bits;
import org.apache.hugegraph.store.util.HgStoreException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InnerKeyCreator {

    final BusinessHandler businessHandler;
    private final Map<Integer, GraphIdManager> graphIdCache = new ConcurrentHashMap<>();

    public InnerKeyCreator(BusinessHandler businessHandler) {
        this.businessHandler = businessHandler;
    }

    public int getGraphId(Integer partId, String graphName) throws HgStoreException {
        try {
            GraphIdManager manager = graphIdCache.computeIfAbsent(partId,
                                                                  id -> new GraphIdManager(
                                                                          businessHandler, id));
            return (int) manager.getGraphId(graphName);
        } catch (
                Exception e) {
            throw new HgStoreException(HgStoreException.EC_RKDB_PD_FAIL, e.getMessage());
        }
    }

    /**
     * @param partId    partition id
     * @param graphName graph name
     * @return 65, 535 if absent
     * @throws HgStoreException
     */
    public int getGraphIdOrCreate(Integer partId, String graphName) throws HgStoreException {
        try {
            GraphIdManager manager;
            if ((manager = graphIdCache.get(partId)) == null) {
                manager = new GraphIdManager(businessHandler, partId);
                graphIdCache.put(partId, manager);
            }
            return (int) manager.getGraphIdOrCreate(graphName);
        } catch (Exception e) {
            throw new HgStoreException(HgStoreException.EC_RKDB_PD_FAIL, e.getMessage());
        }
    }

    public void delGraphId(Integer partId, String graphName) {
        if (graphIdCache.containsKey(partId)) {
            graphIdCache.get(partId).releaseGraphId(graphName);
        } else {
            new GraphIdManager(businessHandler, partId).releaseGraphId(graphName);
        }
    }

    public void clearCache(Integer partId) {
        graphIdCache.remove(partId);
    }

    /**
     * Parse keyCode from key
     */
    public int parseKeyCode(byte[] innerKey) {
        return Bits.getShort(innerKey, innerKey.length - Short.BYTES);
    }

    public byte[] getKeyOrCreate(Integer partId, String graph, int code, byte[] key) {
        int graphId = getGraphIdOrCreate(partId, graph);
        byte[] buf = new byte[Short.BYTES + key.length + Short.BYTES];
        Bits.putShort(buf, 0, graphId);
        Bits.put(buf, Short.BYTES, key);
        Bits.putShort(buf, key.length + Short.BYTES, code);
        return buf;
    }

    public byte[] getKey(Integer partId, String graph, int code, byte[] key) {
        int graphId = getGraphId(partId, graph);
        byte[] buf = new byte[Short.BYTES + key.length + Short.BYTES];
        Bits.putShort(buf, 0, graphId);
        Bits.put(buf, Short.BYTES, key);
        Bits.putShort(buf, key.length + Short.BYTES, code);
        return buf;
    }

    /**
     * @param partId
     * @param graph
     * @param key
     * @return
     */
    public byte[] getKey(Integer partId, String graph, byte[] key) {
        int graphId = getGraphId(partId, graph);
        byte[] buf = new byte[Short.BYTES + key.length];
        Bits.putShort(buf, 0, graphId);
        Bits.put(buf, Short.BYTES, key);
        return buf;
    }

    public byte[] getStartKey(Integer partId, String graph) {
        int graphId = getGraphId(partId, graph);
        byte[] buf = new byte[Short.BYTES];
        Bits.putShort(buf, 0, graphId);
        return buf;
    }

    public byte[] getStartKey(Integer partId, String graph, byte[] key) {
        int graphId = getGraphId(partId, graph);
        byte[] buf = new byte[Short.BYTES + key.length];
        Bits.putShort(buf, 0, graphId);
        Bits.put(buf, Short.BYTES, key);
        return buf;
    }

    public byte[] getEndKey(Integer partId, String graph) {
        int graphId = getGraphId(partId, graph);
        byte[] buf = new byte[Short.BYTES];
        Bits.putShort(buf, 0, graphId + 1);
        return buf;
    }

    public byte[] getEndKey(Integer partId, String graph, byte[] key) {
        int graphId = getGraphId(partId, graph);
        byte[] buf = new byte[Short.BYTES + key.length];
        Bits.putShort(buf, 0, graphId);
        Bits.put(buf, Short.BYTES, key);
        return buf;
    }

    public byte[] getPrefixKey(Integer partId, String graph, byte[] prefix) {
        int graphId = getGraphId(partId, graph);
        byte[] buf = new byte[Short.BYTES + prefix.length];
        Bits.putShort(buf, 0, graphId);
        Bits.put(buf, Short.BYTES, prefix);
        return buf;
    }
}
