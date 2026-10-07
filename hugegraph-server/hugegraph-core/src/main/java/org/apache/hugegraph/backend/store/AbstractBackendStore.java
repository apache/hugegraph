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

package org.apache.hugegraph.backend.store;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.query.Condition;
import org.apache.hugegraph.query.ConditionQuery;
import org.apache.hugegraph.backend.query.ConditionQueryFlatten;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.exception.ConnectionException;
import org.apache.hugegraph.iterator.ExtendableIterator;
import org.apache.hugegraph.iterator.FlatMapperIterator;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.HugeKeys;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

public abstract class AbstractBackendStore<Session extends BackendSession>
        implements BackendStore {

    // TODO: move SystemSchemaStore into backend like MetaStore
    private final SystemSchemaStore systemSchemaStore;
    private final MetaDispatcher<Session> dispatcher;

    public AbstractBackendStore() {
        this.systemSchemaStore = new SystemSchemaStore();
        this.dispatcher = new MetaDispatcher<>();
    }

    protected MetaDispatcher<Session> metaDispatcher() {
        return this.dispatcher;
    }

    protected List<HugeType> getHugeTypes(Query sampleQuery) {
        Set<HugeType> typeSet = new HashSet<>();
        for (Condition c : sampleQuery.conditions()) {
            if (c.isRelation() && c.isSysprop()) {
                Condition.SyspropRelation sr = (Condition.SyspropRelation) c;
                if (sr.relation() == Condition.RelationType.EQ) {
                    if (sr.key().equals(HugeKeys.DIRECTION)) {
                        typeSet.add(((Directions) sr.value()).type());
                    }
                }
            } else if (c.type() == Condition.ConditionType.OR && c.isSysprop()) {
                for (Condition.Relation r : c.relations()) {
                    if (r.relation() == Condition.RelationType.EQ) {
                        if (r.key().equals(HugeKeys.DIRECTION)) {
                            typeSet.add(((Directions) r.value()).type());
                        }
                    }
                }
            }
        }
        return new ArrayList<>(typeSet);
    }

    @Override
    public Iterator<Iterator<BackendEntry>> query(Iterator<Query> queries,
                                                  Function<Query, Query> queryWriter,
                                                  HugeGraph hugeGraph) {
        List<Iterator<BackendEntry>> result = new ArrayList<>();

        FlatMapperIterator<Query, BackendEntry> it =
                new FlatMapperIterator<>(queries, query -> {
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
        result.add(it);
        return result.iterator();
    }

    public void registerMetaHandler(String name, MetaHandler<Session> handler) {
        this.dispatcher.registerMetaHandler(name, handler);
    }

    @Override
    public String storedVersion() {
        throw new UnsupportedOperationException(
                "AbstractBackendStore.storedVersion()");
    }

    @Override
    public SystemSchemaStore systemSchemaStore() {
        return this.systemSchemaStore;
    }

    // Get metadata by key
    @Override
    public <R> R metadata(HugeType type, String meta, Object[] args) {
        Session session = this.session(type);
        MetaDispatcher<Session> dispatcher;
        if (type == null) {
            dispatcher = this.metaDispatcher();
        } else {
            BackendTable<Session, ?> table = this.table(type);
            dispatcher = table.metaDispatcher();
        }
        return dispatcher.dispatchMetaHandler(session, meta, args);
    }

    protected void checkOpened() throws ConnectionException {
        if (!this.opened()) {
            throw new ConnectionException(
                    "The '%s' store of %s has not been opened",
                    this.database(), this.provider().type());
        }
    }

    @Override
    public String toString() {
        return String.format("%s/%s", this.database(), this.store());
    }

    protected abstract BackendTable<Session, ?> table(HugeType type);

    protected static HugeType convertTaskOrServerToVertex(HugeType type) {
        if (HugeType.TASK.equals(type) || HugeType.SERVER.equals(type)) {
            return HugeType.VERTEX;
        }
        return type;
    }

    // NOTE: Need to support passing null
    protected abstract Session session(HugeType type);
}
