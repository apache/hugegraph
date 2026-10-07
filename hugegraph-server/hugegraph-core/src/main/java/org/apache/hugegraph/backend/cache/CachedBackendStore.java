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

package org.apache.hugegraph.backend.cache;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.query.ConditionQuery;
import org.apache.hugegraph.backend.query.ConditionQueryFlatten;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendFeatures;
import org.apache.hugegraph.backend.store.BackendMutation;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.backend.store.SystemSchemaStore;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.iterator.ExtendableIterator;
import org.apache.hugegraph.iterator.MapperIterator;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.StringEncoding;

/**
 * This class is unused now, just for debug or test
 */
public class CachedBackendStore implements BackendStore {

    private BackendStore store = null;
    private Cache<Id, Object> cache = null;

    public CachedBackendStore(BackendStore store) {
        this.store = store;
        this.cache = CacheManager.instance().cache("store-" + store());
        // Set expire 30s
        this.cache.expire(30 * 1000L);
    }

    @Override
    public String store() {
        return this.store.store();
    }

    @Override
    public String storedVersion() {
        return this.store.storedVersion();
    }

    @Override
    public String database() {
        return this.store.database();
    }

    @Override
    public BackendStoreProvider provider() {
        return this.store.provider();
    }

    @Override
    public SystemSchemaStore systemSchemaStore() {
        return this.store.systemSchemaStore();
    }

    @Override
    public void open(HugeConfig config) {
        this.store.open(config);
    }

    @Override
    public void close() {
        this.store.close();
    }

    @Override
    public boolean opened() {
        return this.store.opened();
    }

    @Override
    public void init() {
        this.store.init();
    }

    @Override
    public void clear(boolean clearSpace) {
        this.store.clear(clearSpace);
    }

    @Override
    public boolean initialized() {
        return this.store.initialized();
    }

    @Override
    public void truncate() {
        this.store.truncate();
    }

    @Override
    public void beginTx() {
        this.store.beginTx();
    }

    @Override
    public void commitTx() {
        this.store.commitTx();
    }

    @Override
    public void rollbackTx() {
        this.store.rollbackTx();
    }

    @Override
    public <R> R metadata(HugeType type, String meta, Object[] args) {
        return this.store.metadata(type, meta, args);
    }

    @Override
    public BackendFeatures features() {
        return this.store.features();
    }

    @Override
    public Id nextId(HugeType type) {
        return this.store.nextId(type);
    }

    @Override
    public void increaseCounter(HugeType type, long increment) {
        this.store.increaseCounter(type, increment);
    }

    @Override
    public long getCounter(HugeType type) {
        return this.store.getCounter(type);
    }

    @Override
    public boolean isSchemaStore() {
        return this.store.isSchemaStore();
    }

    @Override
    public void mutate(BackendMutation mutation) {
        // TODO: invalid cache, or set expire time at least
        this.store.mutate(mutation);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<BackendEntry> query(Query query) {
        if (query.empty()) {
            return this.store.query(query);
        }

        QueryId id = new QueryId(query);
        Object result = this.cache.get(id);
        if (result != null) {
            return (Iterator<BackendEntry>) result;
        } else {
            Iterator<BackendEntry> rs = this.store.query(query);
            if (rs.hasNext()) {
                this.cache.update(id, rs);
            }
            return rs;
        }
    }

    @Override
    public Iterator<Iterator<BackendEntry>> query(Iterator<Query> queries,
                                                  Function<Query, Query> queryWriter,
                                                  HugeGraph hugeGraph) {
        return new MapperIterator<>(queries, query -> {
            assert query instanceof ConditionQuery;
            List<ConditionQuery> flattenQueryList =
                    ConditionQueryFlatten.flatten((ConditionQuery) query);

            if (flattenQueryList.size() > 1) {
                ExtendableIterator<BackendEntry> itExtend
                        = new ExtendableIterator<>();
                flattenQueryList.forEach(cq -> {
                    Query cQuery = queryWriter.apply(cq);
                    itExtend.extend(this.query(cQuery));
                });
                return itExtend;
            } else {
                return this.query(queryWriter.apply(query));
            }
        });
    }

    @Override
    public Number queryNumber(Query query) {
        return this.store.queryNumber(query);
    }

    /**
     * Query as an Id for cache
     */
    public static class QueryId implements Id {

        protected String query;
        protected int hashCode;

        public QueryId(Query q) {
            this.query = q.toString();
            this.hashCode = q.hashCode();
        }

        public QueryId(String query, int hashCode) {
            this.query = query;
            this.hashCode = hashCode;
        }

        @Override
        public IdType type() {
            return IdType.UNKNOWN;
        }

        @Override
        public int hashCode() {
            return this.hashCode;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof QueryId)) {
                return false;
            }
            return this.query.equals(((QueryId) other).query);
        }

        @Override
        public int compareTo(Id o) {
            return this.query.compareTo(o.asString());
        }

        @Override
        public Object asObject() {
            return this.query;
        }

        @Override
        public String asString() {
            return this.query;
        }

        @Override
        public long asLong() {
            // TODO: improve
            return 0L;
        }

        @Override
        public byte[] asBytes() {
            return StringEncoding.encode(this.query);
        }

        @Override
        public String toString() {
            return this.query;
        }

        @Override
        public int length() {
            return this.query.length();
        }
    }
}
