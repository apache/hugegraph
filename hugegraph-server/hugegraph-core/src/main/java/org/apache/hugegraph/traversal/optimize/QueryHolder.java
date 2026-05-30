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

package org.apache.hugegraph.traversal.optimize;

import java.util.Iterator;
import java.util.List;

import org.apache.hugegraph.backend.query.Aggregate;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.iterator.Metadatable;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;

public interface QueryHolder extends Metadatable {

    List<HasContainer> getHasContainers();

    void addHasContainer(HasContainer has);

    String SYSPROP_PAGE = "~page";

    Iterator<?> lastTimeResults();

    @Override
    default Object metadata(String meta, Object... args) {
        Iterator<?> results = this.lastTimeResults();
        if (results instanceof Metadatable) {
            return ((Metadatable) results).metadata(meta, args);
        }
        throw new IllegalStateException("Original results is not Metadatable");
    }

    Query queryInfo();

    default void orderBy(String key, Order order) {
        this.queryInfo().order(TraversalUtil.string2HugeKey(key),
                               TraversalUtil.convOrder(order));
    }

    default long setRange(long start, long end) {
        return this.queryInfo().range(start, end);
    }

    default void setPage(String page) {
        this.queryInfo().page(page);
    }

    default void setCount() {
        this.queryInfo().capacity(Query.NO_CAPACITY);
    }

    default void setAggregate(Aggregate.AggregateFunc func, String key) {
        this.queryInfo().aggregate(func, key);
    }

    default <Q extends Query> Q injectQueryInfo(Q query) {
        query.copyBasic(this.queryInfo());
        return query;
    }
}
