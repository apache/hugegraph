/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.backend.tx;

import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.E;

public class IdCounter {

    private static final int TIMES = 10000;
    private static final int DELTA = 10000;
    private static final String DELIMITER = "/";
    private static final Map<String, Map.Entry<AtomicLong, AtomicLong>> ids =
        new ConcurrentHashMap<>();
    private final PDClient pdClient;
    private final String graphName;

    public IdCounter(PDClient pdClient, String graphName) {
        this.graphName = graphName;
        this.pdClient = pdClient;
    }

    public Id nextId(HugeType type) {
        long counter = this.getCounter(type);
        E.checkState(counter != 0L, "Please check whether '%s' is OK",
                     this.pdClient.toString());
        return IdGenerator.of(counter);
    }

    public void setCounterLowest(HugeType type, long lowest) {
        long current = this.getCounter(type);
        if (current >= lowest) {
            return;
        }
        long increment = lowest - current;
        this.increaseCounter(type, increment);
    }

    public long getCounter(HugeType type) {
        return this.getCounterFromPd(type);
    }

    public synchronized void increaseCounter(HugeType type, long lowest) {
        String key = toKey(this.graphName, type);
        getCounterFromPd(type);
        Map.Entry<AtomicLong, AtomicLong> idPair = ids.get(key);
        AtomicLong currentId = idPair.getKey();
        AtomicLong maxId = idPair.getValue();
        if (currentId.longValue() >= lowest) {
            return;
        }
        if (maxId.longValue() >= lowest) {
            currentId.set(lowest);
            return;
        }
        synchronized (ids) {
            try {
                this.pdClient.getIdByKey(key, (int) (lowest - maxId.longValue()));
                ids.remove(key);
            } catch (Exception e) {
                throw new BackendException(e);
            }
        }
    }

    protected String toKey(String graphName, HugeType type) {
        return new StringBuilder().append(graphName)
                                  .append(DELIMITER)
                                  .append(type.code()).toString();
    }

    public long getCounterFromPd(HugeType type) {
        AtomicLong currentId;
        AtomicLong maxId;
        Map.Entry<AtomicLong, AtomicLong> idPair;
        String key = toKey(this.graphName, type);
        if ((idPair = ids.get(key)) == null) {
            synchronized (ids) {
                if ((idPair = ids.get(key)) == null) {
                    try {
                        currentId = new AtomicLong(0);
                        maxId = new AtomicLong(0);
                        idPair = new AbstractMap.SimpleImmutableEntry<>(currentId, maxId);
                        ids.put(key, idPair);
                    } catch (Exception e) {
                        throw new BackendException(String.format(
                                "Failed to get the ID from pd,%s", e));
                    }
                }
            }
        }
        currentId = idPair.getKey();
        maxId = idPair.getValue();
        for (int i = 0; i < TIMES; i++) {
            synchronized (currentId) {
                if ((currentId.incrementAndGet()) <= maxId.longValue()) {
                    return currentId.longValue();
                }
                if (currentId.longValue() > maxId.longValue()) {
                    try {
                        Pdpb.GetIdResponse idByKey = pdClient.getIdByKey(key, DELTA);
                        idPair.getValue().getAndSet(idByKey.getId() +
                                                    idByKey.getDelta());
                        idPair.getKey().getAndSet(idByKey.getId());
                    } catch (Exception e) {
                        throw new BackendException(String.format(
                                "Failed to get the ID from pd,%s", e));
                    }
                }
            }
        }
        E.checkArgument(false,
                        "Having made too many attempts to get the" +
                        " ID for type '%s'", type.name());
        return 0L;
    }
}
