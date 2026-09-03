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

package org.apache.hugegraph.backend.query;

import java.util.Iterator;
import java.util.List;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.type.define.Directions;

public class EdgesQueryIterator implements Iterator<Query> {

    private final List<Id> labels;
    private final Directions directions;
    private final long limit;
    private final Iterator<Id> sources;

    public EdgesQueryIterator(Iterator<Id> sources,
                              Directions directions,
                              List<Id> labels,
                              long limit) {
        this.sources = sources;
        this.labels = labels;
        this.directions = directions;
        // Traverse NO_LIMIT and Query.NO_LIMIT are different
        this.limit = limit < 0 ? Query.NO_LIMIT : limit;
    }

    @Override
    public boolean hasNext() {
        return this.sources.hasNext();
    }

    @Override
    public Query next() {
        Id sourceId = this.sources.next();
        ConditionQuery query = GraphTransaction.constructEdgesQuery(sourceId,
                                                                    this.directions,
                                                                    this.labels);
        if (this.limit != Query.NO_LIMIT) {
            query.limit(this.limit);
            query.capacity(this.limit);
        } else {
            query.capacity(Query.NO_CAPACITY);
        }
        return query;
    }
}
