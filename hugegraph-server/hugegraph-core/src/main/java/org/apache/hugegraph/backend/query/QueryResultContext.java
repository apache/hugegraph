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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.ConditionQuery.OptimizedType;
import org.apache.hugegraph.backend.query.ConditionQuery.ResultsFilter;

/** Decisions captured for one batch; queries retain shared index cleanup data. */
public final class QueryResultContext {

    private final List<Query> queries;
    private final List<Id> inputIds;
    private final boolean mustSortByInputIds;
    private final ConditionQuery matchQuery;
    private final ResultsFilter resultsFilter;
    private final OptimizedType optimizedType;
    private final boolean showExpired;
    private final boolean showHidden;
    private final boolean showDeleting;

    public QueryResultContext(Query query) {
        this(query, false);
    }

    public QueryResultContext(Query query, boolean inputOrderSatisfied) {
        List<Query> chain = new ArrayList<>();
        ConditionQuery match = null;
        ResultsFilter filter = null;
        OptimizedType optimized = OptimizedType.NONE;
        Query visibility = query;
        for (Query current = query; current != null; current = current.originQuery()) {
            chain.add(current);
            visibility = current;
            if (current instanceof ConditionQuery) {
                ConditionQuery condition = (ConditionQuery) current;
                if (optimized == OptimizedType.NONE) {
                    optimized = condition.optimized();
                }
                if (filter == null) {
                    filter = condition.resultsFilter();
                }
                if (current.resultType().isGraph()) {
                    match = condition;
                }
            }
        }
        this.queries = Collections.unmodifiableList(chain);
        this.inputIds = Collections.unmodifiableList(new ArrayList<>(query.ids()));
        this.mustSortByInputIds = !inputOrderSatisfied && query instanceof IdQuery &&
                                 ((IdQuery) query).mustSortByInput();
        this.matchQuery = match;
        this.resultsFilter = filter;
        this.optimizedType = optimized;
        this.showExpired = visibility.showExpired();
        this.showHidden = visibility.showHidden();
        this.showDeleting = visibility.showDeleting();
    }

    public List<Query> queries() {
        return this.queries;
    }

    public List<Id> inputIds() {
        return this.inputIds;
    }

    public boolean mustSortByInputIds() {
        return this.mustSortByInputIds;
    }

    public ConditionQuery matchQuery() {
        return this.matchQuery;
    }

    public ResultsFilter resultsFilter() {
        return this.resultsFilter;
    }

    public OptimizedType optimizedType() {
        return this.optimizedType;
    }

    public boolean conditionFilterRequired() {
        return this.optimizedType != OptimizedType.NONE;
    }

    public boolean showExpired() {
        return this.showExpired;
    }

    public boolean showHidden() {
        return this.showHidden;
    }

    public boolean showDeleting() {
        return this.showDeleting;
    }
}
