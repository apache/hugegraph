/*
 * Copyright 2017 HugeGraph Authors
 *
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

package org.apache.hugegraph.kvstore;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.store.Shard;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.variables.HugeVariables;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class KvStoreImpl implements KvStore {

    private final HugeGraph g;
    private HugeVariables variables;

    public KvStoreImpl(HugeGraph graph) {
        assert graph != null : "graph can't be null";
        this.g = graph;
        this.variables = (HugeVariables) graph.variables();
    }

    @Override
    public void set(String key, String value) {
        try {
            this.variables.set(key, value);
            g.tx().commit();
        } catch (Throwable e) {
            g.tx().rollback();
            throw new HugeException("Failed to commit set kv", e);
        }
    }

    @Override
    public String get(String key) {
        Optional<Object> value = this.variables.get(key);
        if (value.isEmpty()) {
            return null;
        }
        return (String) value.get();
    }

    @Override
    public List<String> mget(String... keys) {
        List<Optional<Object>> values = this.variables.mget(keys);
        List<String> list = new ArrayList<>();
        for (Optional<Object> value : values) {
            if (value.isEmpty()) {
                list.add(null);
            } else {
                list.add((String) value.get());
            }
        }
        return list;
    }

    @Override
    public void remove(String key) {
        try {
            this.variables.remove(key);
            g.tx().commit();
        } catch (Throwable e) {
            g.tx().rollback();
            throw new HugeException("Failed to commit remove kv", e);
        }
    }

    @Override
    public Boolean contains(String key) {
        Optional<Object> value = this.variables.get(key);
        return value.isPresent();
    }

    @Override
    public Number count() {
        return this.variables.count();
    }

    @Override
    public void clearAll() {
        this.g.truncateBackend();
        // 图的删除操作之后，variables schema 需要初始化
        this.variables = (HugeVariables) g.variables();
    }

    @Override
    public List<Shard> shards(long splitSize) {
        List<Shard> shards = this.g.metadata(HugeType.TASK, "splits", splitSize);
        return shards;
    }

    @Override
    public Iterator<Vertex> queryVariablesByShard(String start, String end, String page,
                                                  long pageLimit) {
        return this.variables.queryVariablesByShard(start, end, page, pageLimit);
    }

    @Override
    public Map<String, Object> batchSet(Map<String, Object> params) {
        try {
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                this.variables.set(entry.getKey(), entry.getValue());
            }
            g.tx().commit();
        } catch (Throwable e) {
            g.tx().rollback();
            throw new HugeException("Failed to commit batch set kv", e);
        }
        return params;
    }
}
